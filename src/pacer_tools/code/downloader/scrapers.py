import time
import re
import sys
import csv
import json
import logging
import asyncio
import functools
from hashlib import md5
from pathlib import Path
from collections import Counter

import click
import xmltodict
import pandas as pd
from bs4 import BeautifulSoup
from seleniumrequests import Firefox
from selenium.webdriver.common.by import By

sys.path.append(str(Path(__file__).resolve().parents[1]))
from downloader import forms
from downloader import scraper_tools as stools

from support import settings
from support import data_tools as dtools
from support import fhandle_tools as ftools
from support.docket_entry_identification import extract_court_caseno

PACER_ERROR_WRONG_CASE = 'PACER_ERROR_WRONG_CASE'
MAX_DOWNLOAD_ATTEMPTS = 2
MAX_LONGTIME_ATTEMPTS = 15
PSC_TEXT = 'PACER Service Center'

PAUSE = {
    'micro': 0.1,
    'mini': 0.5,
    'second': 1,
    'moment': 5,
    'medium': 10,
    'mega': 30
}

N_WORKERS = 2 # No. of simultaneous scrapers to run
DOCKET_ROW_DOCS_LIMIT = 1000
MODULES = ['query', 'docket', 'summary', 'member','document']
MEM_LIST_OPTS = ['always', 'avoid', 'never']

re_consolidated_cases = re.compile('Consolidated Cases for (?P<case_no>[\S]+)')

def run_in_executor(f):
    '''Decorator to run function as blocking'''
    @functools.wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, lambda: f(*args, **kwargs))
    return inner

def check_time_continue(start=stools.PACER_HOURS_START, end=stools.PACER_HOURS_END):
    ''' Check if scraper should continue'''
    no_run_hours = range(start-1, end-1, -1)
    if stools.get_time_central().hour in no_run_hours:
        logging.info(f'Scraping operating outside of running hours, terminating.')
        return False
    else:
         return True

def get_xml_response(browser, request_url, request_type='GET'):
    '''
    Get an xml request response as a dict

    Inputs:
        - browser (Selenium browser instance)
        - request_url (str): url to request
        - request_type (str): request type
    Output:
        - response ('error', 'not_logged_in', 'missing','success')

        - content (dict) a dictionary representation of the xml reponse,
            returns False if no response or login error
            returns None if response is that cannot find case
    '''
    content = {}
    resp = browser.request(request_type, request_url)

    if resp.status_code != 200:
        response = 'error'
    elif 'Not logged in' in resp.content.decode():
        response = 'not_logged_in'
    elif 'Cannot find' in resp.content.decode():
        response = 'missing'
    else:
        response = 'success'
        # Remove the @-prefix for attributes, convert to dict via json
        content = json.loads(json.dumps(xmltodict.parse(resp.content)).replace('"@','"'))

    return response, content

class PacerCourtDir:
    ''' A court-specific download folder
    Example:

    ilnd
    |-- html
    |   |-- case1.html
    |
    |-- json
    |   |-- case1.json
    |
    |-- queries
    |   |-- 2016cv.html
    |
    |-- docs
    |   |-- case1_1.pdf
    |
    |-- summaries
    |   |-- case1.html
    |
    |-- members
    |   |-- case1.html
    |
    |-- _temp_
    |   |-- 0
    |       | download(7).pdf
    |   |-- 1
    |       | download(5).pdf

    '''

    def __init__(self, dir_path, court):
        # Root
        self.root = Path(dir_path).resolve()

        # Subdirectories
        for subdir in ['html', 'json', 'queries', 'docs', '_temp_', 'summaries', 'members']:
            self.__dict__[subdir] = self.root / subdir
            self.__dict__[subdir].mkdir(parents=True, exist_ok=True)

        # Meta data
        self.court = court
        self.name = self.root.name

    def __repr__(self):
        return f"<PacerCourtDir: {self.root}"

    def make_temp_subdirs(self, n):
        '''Make n subdirectories of _temp_ folder to silo scraper instances'''
        for i in range(n):
            (self._temp_/f'{i}').mkdir(exist_ok=True)

    def temp_subdir(self,i):
        ''' Access a subdirectory of /_temp_'''
        return self._temp_/f'{i}'

class CoreScraper:
    ''' Base class that contains common methods/attributes for all scapers'''

    def __init__(self, court_dir, court, auth_path, headless, verbose, slabels=[], n_workers=N_WORKERS, exclusions_path=None, case_type=None,
                 ind='#', case_limit=None, cost_limit=None, time_restriction=None, rts=None, rte=None):
        self.browser = None
        self.headless = headless
        self.verbose = verbose
        self.slabels = slabels
        self.dir = court_dir
        self.court = court
        self.n_workers = n_workers
        self.exclusions_path = exclusions_path
        self.case_type = case_type
        self.ind = ind
        self.case_limit = case_limit
        self.cost_limit = cost_limit
        self.time_restriction = time_restriction
        self.rts = rts or stools.PACER_HOURS_START
        self.rte = rte or stools.PACER_HOURS_END

        auth_path = Path(auth_path).resolve()
        self.auth = json.load(open(auth_path,'r'))
        self.user_hash = ftools.gen_user_hash(self.auth['user'])

        # Logging
        self.start_time = stools.get_time_central(as_string=True)
        logging.info(f"STARTING: Log file for scraper started at {self.start_time}")

    def launch_browser(self):
        # Pass headless option to selenium
        # Sets instance specific temp download folder (needed for document downloader)
        options = stools.get_firefox_options(self.dir.temp_subdir(self.ind), self.headless)
        self.browser = Firefox(options=options)
        return self.login()

    def close_browser(self):
        if self.browser:
            self.browser.quit()

    def login(self):
        login_url = ftools.get_pacer_url(self.court, 'login')
        return stools.login(self.browser, self.auth, login_url, logging=logging)

    def logout(self):
        logout_url = ftools.get_pacer_url(self.court, 'logout')
        self.browser.get(logout_url)

    def stamp(self, download_url=None, pacer_id=None):
        ''' Download stamp that can be added to bottom of documents as a html comment'''
        data = {
            'user': self.user_hash,
            'time': stools.get_time_central(as_string=True),
            'download_url': download_url,
            'pacer_id': pacer_id,
            'slabels': self.slabels
        }
        data_str = ";".join(f"{k}:{v}" for k,v in data.items())
        return f"\n<!-- SCALESDOWNLOAD;{data_str} -->"

    def stamp_json(self, download_url=None, pacer_id=None, **kwargs):
        ''' Download stamp that can be added to bottom of documents as a html comment, using JSON string serialization '''
        data = {
            'user': self.user_hash,
            'time': stools.get_time_central(as_string=True),
            'download_url': download_url,
            'pacer_id': pacer_id,
            'slabels': self.slabels,
            **kwargs
        }
        data_str = json.dumps(data)
        return f"\n<!--{data_str}-->"
    # Misc
    def get_caseno_info(self, case_no):
        '''
        Get the info from the possible case no request_type
        Inputs:
            - browser (Selenium browser instance)
            - court (str): court abbreviation
            - case_no (str): pacer case no (e.g. "1:16-cv-12345")
        Output:
            - response ('missing', 'success')
            - data (list) of dicts with request_case_no and all other returned fields
            Note: raises ValueError for unexplained api error
        '''
        data = []

        url = stools.get_pacer_url(self.court, 'possible_case') +'?' + case_no
        response, content = get_xml_response(self.browser, url)

        # If login error, log in and try again
        if response=='not_logged_in':
            self.login()
            response, content = get_xml_response(self.browser, url)

        # Handle error
        if response in ('error', 'not_logged_in'):
            raise ValueError(f'Error with case {case_no}')

        elif response=='missing':
            data = []

        elif response=='success' and type(content)==dict:
            # Deal with singleton
            cases = content['request']['case']
            if type(cases)!= list:
                cases = [cases]

            data = [{'ucid': dtools.ucid(self.court, case_no),  **line} for line in cases]

        return response, data

    def get_caseno_info_id(self, case_no, def_no=None):
        '''
        Get the info from possible case no request_type but just return the id

        Inputs:
            - case_no (str): a case no of the form 1:16-cv-12345
            - def_no (str or int): the defendant no. of interest, if None assumes main case
        Output:
            - pacer_id (int)
        '''

        pacer_id = None
        try:
            response, data = self.get_caseno_info(case_no)
        except:
            return

        if response=='success':

            if len(data) == 1:
                pacer_id = data[0].get('id', None)

            else:
                query_def_no = str(def_no) if def_no else '0'
                candidates = list(filter(lambda x: x.get('defendant',None) == query_def_no, data))
                if len(candidates):
                    pacer_id = candidates[0]['id']

        return int(pacer_id) if pacer_id else None

    def get_transaction_table(self):
        """ Find the transaction table element from a page, if it exists """
        cand = self.browser.find_elements(By.CSS_SELECTOR, 'table')[-1]
        if cand.find_element(By.CSS_SELECTOR, 'tr th').text == PSC_TEXT:
            return cand
        else:
            return None

##########################################################
###  QUERY SCRAPER
##########################################################

class QueryScraper(CoreScraper):
    ''' Enters query to Pacer and downloads the search results '''
    def __init__(self, core_args, config, prefix):
        super().__init__(**core_args)
        self.config = config
        self.config_list = stools.split_config(config, [('filed_from', 'filed_to')])
        self.prefix = prefix or f'query_{self.start_time.replace(":","-")}'

        logging.info(f"Initiated Query Scraper")

    def results_found(self):
        ''' Returns False if query leads to "No information was found" page, else True'''
        return not self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent h2').text.startswith('No information was found')

    def submit_btn_disabled(self, query_form):
        ''' Checks if still on query page with the submit/run button is disabled'''
        try:
            submit_btn = query_form.buttons['submit'].locate()
            return submit_btn.get_property('disabled')
        except:
            return False

    def pull_queries(self):
        '''
        Pull all relevant search queries.

        Output:
            - results (list): list of paths to htmls of query results
        '''
        results = []
        if not self.browser:
            login_success = self.launch_browser()
            if not login_success:
                self.close_browser()
                raise ValueError('Cannot log in to PACER')

        if len(self.config_list) >1:
            logging.info(f"Date filed range was greater than one year, search split into {len(self.config_list)} chunks")

        for i, config_chunk in enumerate(self.config_list):
            logging.info(f"Running on chunk: {config_chunk}")
            #Head to query page
            try:
                query_url = ftools.get_pacer_url(self.court, 'query')
                self.browser.get(query_url)

                query_form = forms.FormFiller(self.browser, template='query', fill_values=config_chunk)
                time.sleep(PAUSE['mini'])
                query_form.fill()
                time.sleep(PAUSE['mini'])
                query_form.submit()

                if self.submit_btn_disabled(query_form):
                    query_form.buttons['find_this_case'].locate().click()
                    time.sleep(PAUSE['second'])
                    query_form.submit()
                    time.sleep(PAUSE['second'])


                if self.results_found():
                    # Download html
                    outpath = self.dir.queries/f'{self.prefix}__{i}.html'
                    # Create parent directory incase prefix includes a subdirectory e.g. /{court}/queries/projectA/query__1.html
                    outpath.parent.mkdir(exist_ok=True, parents=True, mode=0o775)


                    time.sleep(PAUSE['micro'])
                    download_url = self.browser.current_url

                    with open(outpath, 'w+') as wfile:
                        # Add stamp to bottom of html as it is being written
                        wfile.write(self.browser.page_source + self.stamp(download_url))
                    results.append(outpath)
                else:
                    logging.info(f"No results found for chunk with index:{i}\
                        (filed_from:{config_chunk.get('filed_from')}, filed_to:{config_chunk.get('filed_to')})")
            except Exception as e:
                logging.info('Error with this chunk: ' + str(e))
                continue

        logging.info(f"Query results saved to {self.dir.queries}")

        return results

##########################################################
###  DOCKET SCRAPER
##########################################################

class DocketScraper(CoreScraper):
    ''' Main scraper, pulls docket reports from search results '''

    re_mem = re.compile('''<a href=[\\\]{0,1}["']/cgi-bin/DktRpt.pl\?[0-9]{1,10}[\\\]{0,1}['"]>[0-9]\:[0-9][0-9]\-c[vr]-[0-9]{3,10}</a>''')

    def __init__(self, core_args, show_member_list='never', docket_update=False, docket_input=None, exclude_parties=False):

        super().__init__(**core_args)
        self.files = None
        self.show_member_list = show_member_list
        self.docket_update = docket_update
        self.docket_input = Path(docket_input).resolve()
        self.update_task_path = stools.get_update_task_file(self.docket_input) if self.docket_update else None
        self.exclude_parties = exclude_parties

        # Retrieve the list of previously seen members only if show_member_list is 'avoid'
        self.member_list_seen = get_member_cases(core_args['court_dir']) if show_member_list=='avoid' else []

    def __repr__(self):
        return f"<Docket Scraper:{self.ind}>"

    def at_docket_report(self):
        '''Check if the page is actually a docket report (success)'''
        if len(self.browser.find_elements(By.CSS_SELECTOR, '#cmecfMainContent h3')):
            return "DOCKET FOR CASE" in self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent h3').text
        else:
            return False

    def at_invalid_case(self):
        ''' Check if at the "Not a valid case" page '''
        return "not a valid case. Please enter a valid value" in self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent').text[:200]

    def at_sealed_case(self):
        ''' Check if at the "Not a valid case" page '''
        if not len(self.browser.find_elements(By.CSS_SELECTOR, '#cmecfMainContent h3')):
            text = self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent').text[:200]
            if re.search(r"seal|restric|redac|protectiv", text, re.I):
                return True
        return False

    def no_docketlines(self):
        ''' Check if no docket lines (message: "...none satisfy the selection criteria")'''
        return bool(re.search(stools.re_no_docket, self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent').text))

    def at_longtime(self):
        ''' Check if at the "The report may take a long time..." page '''
        longtime_str = "report may take a long time to run because this case has many docket entries"
        try:
            page_text = self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent').text[:200]
            return longtime_str in page_text
        except selenium.common.exceptions.NoSuchElementException:
            return False

    @run_in_executor
    def pull_case(self, case, new_member_list_seen):
        '''
        Pull the docket from a single case
        Inputs:
            - case (dict): dictionary with case details (case_no, latest_date, previously_downloaded, def_no)
            - new_member_list_seen (list): list of new member cases being updated during session
        Outputs:
            - outpath (str): the path to the file that was just written
            - cost (float): the cost of this download, as listed in the transaction table at the bottom of the document
        '''
        # Navigate to docket report page
        if not self.browser:
            login_success = self.launch_browser()
            if not login_success:
                self.close_browser()
                raise ValueError('Cannot log in to PACER')


        docket_url = ftools.get_pacer_url(self.court, 'docket')
        self.browser.get(docket_url)

        # Build the input case no to allow for defendant no to be included, for sake of filling the form
        case_no_input = case['case_no'] + f"-{case['def_no']}" if 'def_no' in case.keys() else case['case_no']
        fill_values = {
            'case_no': case_no_input,
            # Sort by oldest date first by default, gets around issue in IASD
            'sort_by': 'oldest date first',
            # Explicitly grab parties and terminated parties
            'include_parties': True,
            'include_terminated': True,
        }

        # If updating, check if file exists but no latest date
        if case.get('previously_downloaded') and not case.get('latest_date'):
            # Go and grab the latest date
            case['latest_date'] = dtools.get_latest_docket_date(case['ucid'])

        if case.get('latest_date'):
            # Calculate next day and add to fill_values
            next_day = (pd.to_datetime(case['latest_date']) + pd.Timedelta(days=1))
            fill_values['date_from'] = next_day.strftime(ftools.FMT_PACERDATE)
            logging.info(f"Getting updated docket for <case:{case['case_no']}> from <date:{fill_values['date_from']}>")

        # If avoiding members list, only select 'include..' if case is not in previous or new members lists
        if self.show_member_list == 'avoid':
            if not (case['case_no'] in self.member_list_seen or case['case_no'] in new_member_list_seen):
                fill_values['include_list_member_cases'] = True

        elif self.show_member_list == 'always':
            fill_values['include_list_member_cases'] = True

        if self.exclude_parties:
            fill_values['include_parties'] = False
            fill_values['include_terminated'] = False

        time.sleep(PAUSE['mini'])
        docket_report_form = forms.FormFiller(self.browser, 'docket', fill_values)
        docket_report_form.fill()

        # Call the possible case no api again to get pacer_id
        pacer_id = self.get_caseno_info_id(case['case_no'], case.get('def_no'))

        # Submit the form
        docket_report_form.submit()
        time.sleep(PAUSE['mini'])

        # Initialise task line (for when in docket-update mode)
        task_line = {k:case[k] for k in ('ucid', 'latest_date', 'previously_downloaded') if k in case}

        # Checks before form submission stage complete
        if not self.at_docket_report():

            #Check if at "may take a long time page"
            if self.at_longtime():

                initially_requested = self.browser.find_elements(By.CSS_SELECTOR, 'input[name="date_from"]')[-1]
                initially_requested.click()
                time.sleep(PAUSE['micro'])

                logging.info(f"{self} LONGTIME: case {case['ucid']} at the 'long time' page, submitting form and pausing to let page load...")
                self.browser.execute_script('ProcessForm()')
                time.sleep(PAUSE['moment'])

                # For very slow loading pages, loop and check if page is fully loaded
                longtime_attempts = 0
                transaction_table = None
                while longtime_attempts < MAX_LONGTIME_ATTEMPTS:
                    print(f'LONGTIME loop {longtime_attempts=}')
                    transaction_table = self.get_transaction_table()
                    longtime_attempts += 1

                    if transaction_table:
                        break
                    else:
                        time.sleep(PAUSE['moment'])

                print(f"Out of LONGTIME loop, {longtime_attempts=}")
                if not transaction_table:
                    print('ERROR: LONGTIME exceeded maximum attempts and page not fully loaded')
                    return
            else:
                self.browser.execute_script('ProcessForm()')
                time.sleep(PAUSE['moment'])

        # Now assume form submitted correctly, check various scenarious
        if self.at_invalid_case():
            print(f'Invalid case: {case["case_no"]}')

            if self.docket_update:
                task_line.update({
                    'completed': True,
                    'downloaded': False,
                    'new_lines': False,
                    'message': 'invalid_case'
                })
                with open(self.update_task_path, 'a', encoding='utf-8') as wfile:
                    wfile.write( json.dumps(task_line)+'\n' )

        elif self.at_sealed_case():
            print(f"Sealed case: {case['case_no']}")
            text = self.browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent').text[:200]
            print(f'Explanation from PACER: {text}')

            if self.docket_update:
                task_line.update({
                    'completed': True,
                    'downloaded': False,
                    'new_lines': False,
                    'message': 'sealed_case'
                })
                with open(self.update_task_path, 'a', encoding='utf-8') as wfile:
                    wfile.write( json.dumps(task_line)+'\n' )

        elif self.at_docket_report():

            # Check for correct caseno and court
            hstring_court, hstring_caseno = extract_court_caseno(self.browser.page_source)
            hstring_caseno = ftools.clean_case_id(hstring_caseno)

            # Training site
            if self.court=='psc':
                hstring_court='psc'

            if not (hstring_court==self.court and hstring_caseno==case['case_no']):
                print(f"PACER_ERROR_WRONG_CASE ucid={case['ucid']} {hstring_court=} {hstring_caseno=}")
                return PACER_ERROR_WRONG_CASE

            # Save the output by case name
            outpath = ftools.get_expected_path(case['ucid'], subdir='html', pacer_path=self.dir.root.parent, def_no=case.get('def_no'))

            no_docketlines = self.no_docketlines()
            if no_docketlines:
                logging.info(f'No new docket lines for case: {case["case_no"]}')

            # If necessary, create "..._n.html" etc. filename for nth update to case
            if case.get('previously_downloaded', False):
                ind = 0
                while outpath.exists():
                    ind += 1
                    outpath = self.dir.html / ftools.generate_docket_filename(case['case_no'], case.get('def_no'), ind=ind)

            time.sleep(PAUSE['micro'])
            download_url = self.browser.current_url
            page_source = self.browser.page_source
            if self.court == 'psc':
                cost = 0 
            else:
                cost = float(ftools.parse_transaction_history(page_source)['cost'])

            # Make sure parent directory exists, which will be the year-part
            outpath.parent.mkdir(exist_ok=True, mode=0o775)
            with open(outpath, "w+") as wfile:
                # Add the stamp to the bottom of the url as it is written
                wfile.write(page_source + self.stamp(download_url, pacer_id=pacer_id))

            if self.docket_update:
                # Get the download path relative to project root folder
                rel_path = outpath.relative_to(self.dir.root.parents[2])
                # Write the task line
                task_line.update({
                    'completed': True,
                    'downloaded': True,
                    'new_lines': not no_docketlines,
                    'outpath': str(rel_path)
                })
                with open(self.update_task_path, 'a', encoding='utf-8') as wfile:
                    wfile.write( json.dumps(task_line)+'\n' )

            #Check to see if it is a member case
            #TODO: also for self.show_member_list=='always'?
            if self.show_member_list=='avoid':
                contents = open(outpath, 'rb').read()
                found_members = self.re_mem.findall(str(contents))
                if found_members:
                    found_case_ids = [x.split('</a>')[0].split('>')[-1] for x in found_members]
                    found_case_ids = [ftools.clean_case_id(x) for x in set(found_case_ids) if x not in self.member_list_seen]
                    new_member_list_seen += found_case_ids

            return outpath, cost

        else:
            print(f'ERROR: <case: {case["ucid"]}> not found, reason unknown')
###
# Support Functions for Docket Scraper
###
def get_member_cases(court_dir, file=settings.MEM_DF):
    ''' Get the list cases that have previously been seen listed as member cases'''
    return pd.read_csv(Path(file)).query("court==@court_dir.court")['case_no'].tolist()



def check_exists(subdir, court=None, case_no=None, ucid=None, def_no=None, pacer_path=settings.PACER_PATH):
    '''
    Check if a case-level file exists

    Inputs:
        - court (str): the court abbreviation, must be provided if no ucid provided
        - case_no (str): the case no, must be provided if no ucid provided
        - ucid (str): the case ucid, if not provided will be generated from case_no, court
        - def_no (str or int): ignored if ucid provided
        - subdir ('html', 'summaries', 'members'): the subdir to look in
    Output:
        (bool) whether the relevant file exists or not
    '''
    if not ucid:

        if not (case_no and court):
            raise ValueError('If no ucid provided, must provide case_no and court')
        else:
            case_no = f"{case_no}-{def_no}"
            ucid = dtools.ucid(court, case_no, allow_def_stub=True)

    exists = ftools.get_expected_path(ucid=ucid, subdir=subdir, pacer_path=pacer_path).exists()

    return exists


def get_excluded_cases(exclusions_path, court):
    ''' Get the excluded cases'''
    dfe = pd.read_csv(exclusions_path)[['ucid']]
    dfe = dfe.assign(**dtools.parse_ucid(dfe.ucid))
    dfe.query("court==@court", inplace=True)
    return dfe

##########################################################
###  SUMMARY SCRAPER
##########################################################

@run_in_executor
def _pull_summary_(ScraperInstance, case):
    '''
        Pull the case summary for a single case

    Inputs:
        - ScraperInstance (CoreScraper): an instance of CoreScraper or any of the
            scraper classes that inherit from it
        - case (dict): dictionary of case details include case_no and ucid
    '''

    self = ScraperInstance

    if not self.browser:
        login_success = self.launch_browser()
        if not login_success:
            self.close_browser()
            raise ValueError('Cannot log in to PACER')

    query_url = stools.get_pacer_url(self.court, 'query')
    self.browser.get(query_url)

    fill_values = {'case_no': case['case_no']}

    try:
        query_form = forms.FormFiller(self.browser, template='query', fill_values=fill_values)
        time.sleep(PAUSE['mini'])
        query_form.fill()
        time.sleep(PAUSE['mini'])
        query_form.submit()
    except:
        logging.info(f"{self} ERROR with {case['case_no']} cannot fill out query form")
        return

    sealed = case_is_sealed(self.browser)
    if sealed!=False:
        logging.info(f"{self} ERROR with {case['case_no']}, case sealed [{sealed}]")
        return

    elif summary_no_data(self.browser):
        logging.info(f"{self} ERROR with {case['case_no']}, no case data for case")
        return

    # Find the Case Summary a tag
    a_candidates = [
        a for a in self.browser.find_elements(By.CSS_SELECTOR, 'td a')
        if 'qrySummary' in (a.get_attribute('href') or '')
    ]

    if len(a_candidates) != 1:
        logging.info(f"{self} ERROR with {case['case_no']} cannot access case summary")
        return

    # Grab the a tag and parse the case pacer_id from it: "...qrySummary.pl?<pacer_id>"
    a_tag = a_candidates[0]
    href = a_tag.get_attribute('href')
    pacer_id = href.strip(ftools.get_pacer_url(self.court, 'summary') + '?')
    a_tag.click()

    # Use the same filename as for a docket html, but in the 'summaries' directory
    outpath = ftools.get_expected_path(case['ucid'], subdir='summaries', pacer_path=self.dir.root.parent, def_no=case.get('def_no'))

    download_url = self.browser.current_url
    with open(outpath, "w+") as wfile:
        # Add the stamp to the bottom of the url as it is written
        wfile.write(self.browser.page_source + self.stamp(download_url, pacer_id=pacer_id))

    return outpath

def case_is_sealed(browser):
    ''' Check if "This case is sealed" has appeared on query form page '''
    case_number_area = browser.find_elements(By.CSS_SELECTOR, '#case_number_area')
    if len(case_number_area):
        text = case_number_area[0].text
        if re.search(r"seal|restric|redac|protectiv", text, re.I):
            return text or ''
    return False

def summary_no_data(browser):
    ''' Check if the "No case data for case <case>" message has appeared'''
    return "No case data for case" in browser.find_element(By.CSS_SELECTOR, '#cmecfMainContent').text[:300]

class SummaryScraper(CoreScraper):

    def __init__(self, core_args):
        super().__init__(**core_args)

    def __repr__(self):
        return f"<Summary Scraper:{self.ind}>"

    def pull_summary(self, case_no):
        return _pull_summary_(self, case_no)


##########################################################
###  MEMBER SCRAPER
##########################################################

def parse_member_case_input(member_input, court):
    '''
    Parse the input to member scraper

    Inputs:
        - member_input (str or Path): str or Path to csv with at least one of ('pacer_id', 'case_no')
        - court (str): the court being scraped
    Output:
        (list) list of dicts with at least one of ('pacer_id', 'case_no') field
    '''

    usecols = lambda x: x in ('pacer_id', 'case_no', 'ucid')

    df = pd.read_csv(member_input, usecols=usecols, dtype={'pacer_id':str})

    # Parse case_no from ucid if present, and use that rather than case_no column
    if 'ucid' in df.columns:
        parsed = dtools.parse_ucid(df.ucid)
        df['court'], df['case_no'] = parsed['court'], parsed['case_no']

        # Filter by court and delete court column
        df = df[df.court.eq(court) | df.pacer_id.notna()]
        del df['court']
        df.fillna('', inplace=True)

    df.drop_duplicates(inplace=True)

    return df.to_dict('records')


class MemberScraper(CoreScraper):

    def __init__(self, core_args):
        super().__init__(**core_args)

    def __repr__(self):
        return f"<Member Scraper:{self.ind}>"

    def none_found(self):
        return 'cannot find any consolidated cases' in self.browser.page_source[:10000]

    @run_in_executor
    def pull_members(self, case):
        '''
        Pull a single members list page

        Inputs:
            - pacer_id (str or int): the interal (court-specific) for the lead case e.g. '392081'
            - case_no (str): the full case no. for the lead case e.g. 1:20-cv-05965

        '''

        pacer_id, case_no = case.get('pacer_id'), case.get('case_no')

        if not pacer_id and not case_no:
            raise ValueError('Must supplied either pacer_id or case_no')

        # Navigate to docket report page
        if not self.browser:
            login_success = self.launch_browser()
            if not login_success:
                self.close_browser()
                raise ValueError('Cannot log in to PACER')

        # Grab the pacer id first, if not supplied
        if case_no and not(pacer_id):
            pacer_id = self.get_caseno_info_id(case_no)

        url = stools.get_pacer_url(self.court, 'members') + f'?{pacer_id}'

        self.browser.get(url)

        if self.none_found():
            logging.info(f"{self} ERROR: no case list found for {pacer_id=} {case_no=}")
            return

        else:
            # Get case_no
            if not case_no:
                match = re_consolidated_cases.search(self.browser.page_source[:10000])
                if not match:
                    logging.info(f"{self} ERROR: Cannot identify case_no for this case {pacer_id=}")
                    return
                else:
                    case_no = match.groupdict()['case_no']

            case_no = ftools.clean_case_id(case_no)

            # Use the same filename as for a docket html, but in the 'members' directory
            outpath = ftools.get_expected_path(case['ucid'], subdir='members', pacer_path=self.dir.root.parent, def_no=case.get('def_no'))

            download_url = self.browser.current_url
            outpath.parent.mkdir(exist_ok=True, mode=0o775)
            with open(outpath, "w+") as wfile:
                # Add the stamp to the bottom of the url as it is written
                stamp = self.stamp_json(download_url=download_url, pacer_id=pacer_id)
                wfile.write(self.browser.page_source + stamp)

            return outpath




##########################################################
###  DOCUMENT SCRAPER
##########################################################

class DocumentScraper(CoreScraper):
    ''' Scraper to pull documents (pdfs) from docket reports'''
    RETRY_LIMIT = 1
    DOWNLOAD_HISTORY_COLS = ['doc_id', 'filepath', 'download_date', 'user']

    def __init__(self, core_args, get_att=True, doc_limit=DOCKET_ROW_DOCS_LIMIT):
        super().__init__(**core_args)
        self.get_att = get_att

        self.previously_downloaded_doc_ids = self.get_previously_downloaded_docs()
        self.doc_limit = doc_limit

    def __repr__(self):
        return f"<Doc Scraper:{self.ind}>"

    def get_previously_downloaded_docs(self):
        '''Get the doc_ids of all the previously downloaded docs in the /docs directory'''
        return [ftools.parse_document_fname(x.name)['doc_id'] for x in self.dir.docs.glob('*.pdf')]

    @run_in_executor
    def pull_docs(self, docket):
        '''
        Pull documents on a given case.
        Inputs:
            - docket (dict): a dict with an 'fpath' key to a single .html file, and a 'doc_no' key)
        '''
        if self.browser is None:
            login_success = self.launch_browser()
            if not login_success:
                self.close_browser()
                raise ValueError('Cannot log in to PACER')


        ucid = docket['ucid']
        fpath = ftools.get_expected_path(ucid, subdir='html', pacer_path=self.dir.root.parent,)
        case_no = ftools.clean_case_id(fpath.stem)

        # Get all the document links for this file
        soup = BeautifulSoup( open(fpath).read(), "html.parser")
        docket_table = soup.select('table')[-2]
        if self.court == 'psc':
            docket_table = soup.select('table')[-1]
        if not re.match('^\s?Date Filed.*', docket_table.text):
            return

        wanted_doc_nos = stools.build_wanted_doc_nos(docket.get('doc_no',''))
        if wanted_doc_nos:
            logging.info(f"{self} Getting the following documents for case {ucid}:\n{wanted_doc_nos}")
        else:
            logging.info(f"{self} Getting all documents for case {ucid}")


        doc_links = stools.get_document_links(docket_table, self.get_att, wanted_doc_nos=wanted_doc_nos)

        # Pull individual documents
        for doc in doc_links:
            if doc['get_line_doc'] == True:
                success = self.pull_doc(doc, ucid)
                if success==None:
                    logging.info(f"{self} SKIPPING: previously downloaded case:{ucid} doc index: {doc['ind']}")
                elif success==False:
                    logging.info(f"{self} ERROR: pull_doc failed for case:{ucid} doc index: {doc['ind']}")

            for att in doc.get('atts', []):
                try:
                    success = self.pull_doc(doc, ucid, att)
                    if success==None:
                        logging.info(f"{self} SKIPPING: previously downloaded case:{ucid} doc index: {doc['ind']} and attachment index: {att['ind']}")
                    elif success==False:
                        logging.info(f"{self} ERROR: pull_doc failed for case:{ucid} doc index: {doc['ind']} and attachment index: {att['ind']}")
                except:
                    logging.info(f"{self} ERROR: pull_doc failed for case:{ucid} doc index: {doc['ind']} and attachment index: {att['ind']}")

    def at_document_selection(self):
        '''Check if browser is at the document selection menu'''
        re_doc_selection = "Document Selection Menu"
        main_content = self.browser.find_elements(By.CSS_SELECTOR, "div#cmecfMainContent")[0]
        return bool(re.match(rf"^\s+{re_doc_selection}.*", main_content.text))

    def at_predownload(self):
        '''Check if at a 'pre-download' pdf embed page, file hasn't downloaded, open button appears'''
        main_content = self.browser.find_elements(By.CSS_SELECTOR, "div#cmecfMainContent")[0]
        iframe = main_content.find_elements(By.CSS_SELECTOR, "iframe")
        return bool(main_content) and len(iframe)

    def cannot_redisplay(self):
        ''' Checkf if at a "Cannot redisplay... already been shown" page '''
        # Check tabs:
        resp = False
        time.sleep(PAUSE['mini'])
        tabs = self.browser.window_handles
        if len(tabs) >1:
            try:
                self.browser.switch_to.window(tabs[-1])
                if self.browser.find_element(By.CSS_SELECTOR, 'body').text\
                                .startswith('Cannot redisplay'):
                    resp = True

                # Close out the 'cannot redisplay' tab before leaving
                if len(self.browser.window_handles) > 1:
                    self.browser.close()
                    self.browser.switch_to.window(tabs[0])
            except:
                self.browser.switch_to.window(tabs[0])

        return resp

    def at_receipt(self):
        ''' Are we at the document receipt page'''
        main_content = self.browser.find_elements(By.CSS_SELECTOR, "div#cmecfMainContent")[0]
        return main_content.text.startswith("To accept charges shown below")

    def at_outside_warning(self):
        ''' Are we at the "Warning... link from outside..." page '''
        main_content = self.browser.find_elements(By.CSS_SELECTOR, "div#cmecfMainContent")[0]
        return main_content.text.startswith('Warning: The link to this page may not have originated from within')

    def at_no_permission(self):
        ''' Are we at the "You do not have permission... " page '''
        main_content = self.browser.find_elements(By.CSS_SELECTOR, "div#cmecfMainContent")[0]
        return main_content.text.startswith("You do not have permission")

    def clean_att_index(self,att_index):
        '''Clean the attachment index (usually a digit like '2' but sometimes a full url)'''
        if att_index==None or att_index.isdigit():
            return att_index
        elif re.match(r"http.*.pdf", att_index):
            # Capture the file name from the url e.g. http://..../../filename.pdf
            att_index = att_index.split('/')[-1].split('.')[0]

        return "".join(c for c in att_index if c.isdigit() or c.isalpha())[:10]

    def is_main_doc_row(self, row):
        '''
        Determine if the row is the 'Main document' row

        Inputs:
            - row (selenium WebElement): the row in question
        Output:
            (bool) True if it is the row that contains the main document
        '''
        row_text = row.text.strip()
        # Example district: ilnd
        if row_text.startswith('Document Number'):
            return True
        # Example district: wvnd
        elif 'main document' in row_text.lower():
            return True
        else:
            return False

    def pull_doc(self, doc, ucid, att=None, retry_count=0, from_doc_selection=False):
        '''
        Download a single document
        Inputs:
            - doc (dict): the doc metadata
            - ucid (str): the ucid for the case
            - fpath (str or Path): new file path

        Outputs:
            success (bool): whether download succeeded, None if previously downloaded and skipping
        '''
        att_index = self.clean_att_index(att['ind']) if att else None
        doc_id = ftools.generate_document_id(ucid, doc['ind'], att_index)
        fname =  ftools.generate_document_fname(doc_id, self.user_hash)

        ucid_data = dtools.parse_ucid(ucid)
        year_part = ftools.decompose_caseno(ucid_data['case_no'])['year']

        fpath = self.dir.docs / year_part / fname

        if not from_doc_selection:
            logging.info(f"{self} downloading document: {doc_id}")

        # Check if previously downloaded
        if doc_id in self.previously_downloaded_doc_ids:
            # Document previously downloaded, skipping
            return None

        url = att['href'] if att else doc['href']
        self.browser.get(url)
        time.sleep(PAUSE['micro'])

        if self.at_outside_warning():
            # Click continue
            self.browser.find_element(By.LINK_TEXT, "Continue").click()

        if self.at_document_selection():
            try:
                # Click the link for the document number at top of the screen
                first_row = self.browser.find_element(By.CSS_SELECTOR, 'tr')
                if self.is_main_doc_row(first_row):
                    atag = first_row.find_element(By.CSS_SELECTOR, 'a')
                    doc = stools.get_single_link(atag, mode='selenium')
                    # Re run pull_doc on new doc gathered from selection screen
                    return self.pull_doc(doc, ucid, att, from_doc_selection=True)
            except:
                logging.info(f"{self} ERROR (pull_doc): Could not select from document selection screen ({doc_id})")
                return False

        elif self.at_receipt():
            # Get relevant info from transaction receipt
            transaction_data = {k:v for k,v in ftools.parse_transaction_history(self.browser.page_source).items()
                                if k in ('billable_pages','cost')}
            if transaction_data:
                logging.info(f"{self} <case:{doc_id}> Transaction Receipt: {json.dumps(transaction_data)}")

            view_selector = 'input[type="submit"][value="View Document"]'
            view_btn = self.browser.find_element(By.CSS_SELECTOR, view_selector)
            # view_btn.click()

            # Use goDLS command directly
            form = self.browser.find_element(By.CSS_SELECTOR, 'form')
            on_submit_command = form.get_attribute('onsubmit')
            # Grab the first command
            go_DLS_command = on_submit_command.split(';', maxsplit=1)[0]
            self.browser.execute_script(go_DLS_command)
            time.sleep(PAUSE['moment'])

        elif self.at_no_permission():
            logging.info(f"{self} ERROR (pull_doc): Do not have permission to access ({doc_id})")
            return False

        # Find downloaded document in the temp folder
        wait_time = 2 if retry_count==0 else 4

        file = stools.get_recent_download(self.dir.temp_subdir(self.ind), wait_time=wait_time, time_buffer=20)
        if file:
            # Move file
            if fpath.exists():
                logging.info(f'{self} File {fpath} already exists, replacing...')
            file.replace(fpath)

            logging.info(f"{self} DOWNLOADED: File downloaded as {fpath.name}")
            return True
        else:
        #     # Retry the download
        #     if retry_count < self.RETRY_LIMIT:
        #         rc = retry_count + 1
        #         return self.pull_doc(doc, ucid, att, retry_count=rc)

            logging.info(f"{self} ERROR (pull_doc): Download not found on disk ({doc_id})")
            return False

###
# Support Functions for Document Scraper
###

def generate_dockets_list(document_input, core_args, skip_seen=True, all_docs=False):
    '''
    Generate the list of docket filepaths for Document Scraper

    Inputs:
        - document_input (str): filepath to a csv of cases that includes a column of ucids
        - core_args
        - skip_seen(bool): whether to skip cases from which docs have previously been downloaded
        - all_docs(bool): whether or not to grab all docs (if doc_no column not supplied)
    Ouptut:
        a list of dicts with filepath and doc_no keys
    '''

    # Get the subset of ucids of interest
    cases_file = Path(document_input).resolve()
    if not cases_file.exists():
        raise ValueError(f'File at {cases_file} does not exist')

    df = pd.read_csv(cases_file)
    keepcols = [x for x in ['ucid', 'doc_no'] if x in df.columns]

    # All docs check
    if ('doc_no' not in df.columns) and (not all_docs):
        logging.info(f'\nALL DOCS: No `doc_no` column found in document-input. Use the --document-all-docs flag if you want all docs per case ($$$)\n')
        return []

    # If there's a doc_no column, remove any rows where the doc_no column is empty (like above, the only way around this is to use --document-all-docs)
    if 'doc_no' in df.columns:
        df = df[df.doc_no.notna()].copy()

    # Filter out cases we don't have htmls for
    df['exists'] = df.ucid.apply(lambda x: ftools.get_expected_path(ucid=x, subdir='html',pacer_path=core_args['court_dir'].root.parent).exists())
    df = df[df.exists].copy()

    # Filter out dockets that have *any* docs downloaded if skip_seen
    if skip_seen and not ('doc_no' in df.columns):
        # Get list of ucids that have previously had docs downloaded
        document_paths = list(core_args['court_dir'].docs.glob('*.pdf'))
        ucid_from_fpath = lambda x: ftools.parse_document_fname(x.name)['ucid']
        seen_ucids = set(ucid_from_fpath(x) for x in document_paths)
        # Limit df to ucids that haven't been seen
        df = df[~df.ucid.isin(seen_ucids)].copy()

    keepcols = [x for x in ['ucid', 'doc_no'] if x in df.columns]
    return df[keepcols].to_dict('records')


##########################################################
###  Sequences
##########################################################

def seq_query(core_args, config, prefix):
    ''' Scraper sequence that handles multiple workers for the Query module '''

    logging.info(f"\n######\n## Query Scraper Sequence [{core_args['court']}]\n######\n")
    QS = QueryScraper(core_args, config, prefix)
    results = QS.pull_queries()
    logging.info("Finished Query Scraper sequence, closing...")
    QS.close_browser()
    return results

async def seq_docket(core_args, query_results, docket_input, docket_update, show_member_list, exclude_parties):
    ''' Scraper sequence that handles multiple workers for the Docket module '''

    async def _scraper_(args, ind):
        ''' Sequence for single instance of Docket Scraper'''
        DktS = DocketScraper(
            core_args = {**args, 'ind':ind },
            show_member_list = show_member_list,
            docket_input = docket_input,
            docket_update = docket_update,
            exclude_parties = exclude_parties
        )
        while len(cases):
            # Check time restriction
            if core_args['time_restriction']:
                if not check_time_continue(DktS.rts, DktS.rte):
                    DktS.close_browser()
                    break

            # Check if case_limit reached
            if DktS.case_limit and ( len(results['success']) >= DktS.case_limit ):
                logging.info(f"{DktS}: case limit ({DktS.case_limit}) reached")
                break

            # check if cost_limit reached
            nonlocal total_cost # i've never used this before! feels ugly...but...it must've been built in for a reason
            if DktS.cost_limit and total_cost >= DktS.cost_limit:
                logging.info(f"{DktS}: cost limit ($%.2f) {'reached' if total_cost==DktS.cost_limit else 'exceeded'}" % DktS.cost_limit)
                break

            # Get a case from the pile
            case = cases.pop(0)
            if case.get('download_attempts', 0) >= MAX_DOWNLOAD_ATTEMPTS:
                logging.info(f"{DktS} skipping {case['ucid']}, exceeded max download attempts")
                return

            exists = check_exists(subdir='html', pacer_path=DktS.dir.root.parent,
                                  ucid=case['ucid'],def_no=case.get('def_no') )

            if exists and not docket_update:
                results['skipped'].append(case['ucid'])
                if DktS.verbose:
                    logging.info(f"{DktS} <case: {case['ucid']}> already exists, skipping")
            else:
                logging.info(f"{DktS} taking {case['case_no']}")

                # Pass the previously_downloaded status in to pull_case
                case['previously_downloaded'] = exists
                docket_path, cost = await DktS.pull_case(case, new_member_list_seen)
                total_cost += cost


                if docket_path==PACER_ERROR_WRONG_CASE:
                    logging.info(f"{DktS} PACER_ERROR_WRONG_CASE: served wrong case, pushing back onto end of queue")

                    # Need to try the case again
                    if 'download_attempts' not in case.keys():
                        case['download_attempts'] = 0
                    case['download_attempts'] += 1

                    cases.append(case)

                elif docket_path:
                    results['success'].append(docket_path)
                    logging.info(f"{DktS} downloaded {case['ucid']} successfully")
                else:
                    results['failure'].append(case['ucid'])
                    logging.info(f"{DktS} ERROR downloading {case['ucid']}")

        logging.info(f"{DktS} finished scraping")
        DktS.close_browser()

    logging.info(f"\n######\n## Docket Scraper Sequence [{core_args['court']}]\n######\n")

    # Handle all of parsing for varied docket_input possibilites
    input_data = stools.parse_docket_input(query_results, docket_input, core_args['case_type'], core_args['court'], logging)

    if not len(input_data):
        logging.info('No input data, ending session')
        return []
    else:
        logging.info(f"Built case list of length: {len(input_data):,}")

    # Build a df of cases, to make dropping/excluding more efficient
    df_cases = pd.DataFrame(input_data)
    df_cases['ucid'] = dtools.ucid(core_args['court'], df_cases['case_no'], allow_def_stub=True)

    prev_downloaded_map = map(
        lambda x: check_exists(subdir='html', ucid=x, pacer_path=core_args['court_dir'].root.parent),
        df_cases.ucid
    )

    prev_downloaded = Counter(prev_downloaded_map)
    logging.info(f"Previously downloaded: {dict(prev_downloaded)}")

    if core_args['exclusions_path']:
        exclude_df = get_excluded_cases(core_args['exclusions_path'], core_args['court'])
        df_cases = df_cases[~df_cases.ucid.isin(exclude_df.ucid)].copy()

    # Check update task file
    if docket_update:
        # Drop any that have already been completed in this task
        task_path = stools.get_update_task_file(docket_input)

        if task_path.exists():
            df_task = pd.read_json(task_path, lines=True)
            if len(df_task):
                task_completed = df_task[df_task.completed.eq(True)].ucid
                df_cases = df_cases[~df_cases.ucid.isin(task_completed)]

        else:
            # Otherwise create the file
            task_path.touch()

        # Ensure latest date column if we're in update mode
        if 'latest_date' not in df_cases.columns:
            df_cases['latest_date'] = ''
        df_cases.latest_date.fillna('', inplace=True)

        keepcols = [col for col in ('case_no', 'ucid', 'latest_date', 'def_no') if col in df_cases.columns ]
    else:
        keepcols = [col for col in ('case_no', 'ucid', 'def_no') if col in df_cases.columns ]
        
    cases = df_cases[keepcols].to_dict('records')
    del df_cases, input_data
    # logging.info(f"Docket Scraper initialised with {len(cases):,} cases.")

    # Initialise lists, will be accessed by all instances of _scraper_
    results = {'success': [], 'failure':[], 'skipped':[]}
    new_member_list_seen = []
    total_cost = 0

    # Initialise scrapers, run asynchronously
    scrapers = [asyncio.create_task(_scraper_(args=core_args, ind=i)) for i in range(core_args['n_workers'])]
    await asyncio.gather(*scrapers)

    # Finished scraping, print results
    logging.info(f'\nDocket Scraper sequence terminated successfully')
    results_tally = {k: f"{len(v):,}" for k,v in results.items()}
    logging.info(str(results_tally))
    logging.info(f'Total cost: $%.2f' % total_cost)


    # When finished scraping, add new_member_list_seen to the member_list file
    if len(new_member_list_seen):
        new_member_list_seen=list(set(new_member_list_seen)) #covering greg's bases (even if I missed something, this much redundancy can't hurt)
        with open(settings.MEM_DF, 'a', encoding='utf-8', newline='\n') as wfile:
            rows = ( (core_args['court'], x) for x in new_member_list_seen)
            csv.writer(wfile).writerows(rows)

        logging.info(f"Added {len(new_member_list_seen)} new member cases to members list")

    return results

async def seq_summary(core_args, summary_input):

    async def _scraper_(args,ind):
        ''' Sequence for single instance of Summary Scraper'''
        SS = SummaryScraper(
            core_args = {**core_args, 'ind':ind }
        )

        while len(cases):
            # Check time restriction
            if core_args['time_restriction']:
                if not check_time_continue(core_args['rts'], core_args['rte']):
                    SS.close_browser()
                    break

            # Check if case_limit reached
            if core_args['case_limit'] and ( len(results['success']) >= core_args['case_limit'] ):
                logging.info(f"{SS}: case limit ({core_args['case_limit']}) reached")
                break

            case = cases.pop(0)

            exists = check_exists(subdir='summaries', pacer_path=core_args['court_dir'].root.parent,
                                    ucid=case['ucid'], def_no=case.get('def_no') )
            if exists:
                logging.debug(f"{SS} <case: {case['ucid']}> already exists, skipping")
                results['skipped'].append(case['ucid'])

            else:

                logging.info(f"{SS} taking {case['case_no']}")
                try:
                    summary_path = await SS.pull_summary(case)
                except:
                    summary_path = None

                if summary_path:
                    results['success'].append(summary_path)
                    logging.info(f"{SS} downloaded {case['ucid']} summary successfully")
                else:
                    results['failure'].append(case['ucid'])
                    logging.info(f"{SS} ERROR downloading {case['ucid']} summary")

        logging.info(f"{SS} finished scraping")
        SS.close_browser()

    logging.info("\n######\n## Summary Scraper Sequence\n######\n")

    # Handle all of parsing for varied docket_input possibilites
    input_data = stools.parse_docket_input(
        query_results=[],
        docket_input=summary_input,
        case_type=core_args['case_type'],
        court=core_args['court'],
        logging=logging
        )
    logging.info(f"Built case list of length: {len(input_data):,}")

    # Build a df of cases, to make dropping/excluding more efficient
    df_cases = pd.DataFrame(input_data)
    df_cases['ucid'] = dtools.ucid(core_args['court'], df_cases['case_no'])

    keepcols = [col for col in ('case_no', 'ucid', 'def_no') if col in df_cases.columns ]
    cases = df_cases[keepcols].to_dict('records')

    # Apply limit
    if core_args['case_limit']:
        cases = cases[:core_args['case_limit']]
        logging.info(f"Applying case limit({core_args['case_limit']}), {len(cases):,} will be downloaded")


    del df_cases, input_data

    logging.info(f"Summary Scraper initialised with {len(cases):,} cases.")

    # Initilise lists, will be accessed by all instances of _scraper_
    results = {'success':[], 'failure':[], 'skipped':[]}

    # Initialise scrapers, run asynchronously
    scrapers = [asyncio.create_task(_scraper_(args=core_args, ind=i)) for i in range(core_args['n_workers'])]
    await asyncio.gather(*scrapers)

    # Finished scraping, print results
    logging.info(f'\nSummary Scraper sequence terminated successfully')
    results_tally = {k: f"{len(v):,}" for k,v in results.items()}
    logging.info(str(results_tally))

    return results

async def seq_member(core_args, member_input):

    async def _scraper_(args, ind):
        ''' Sequence for single instance of Member Scraper'''
        MS = MemberScraper(
            core_args = {**core_args, 'ind':ind }
        )

        while len(cases):
            # Check time restriction
            if core_args['time_restriction']:
                if not check_time_continue(core_args['rts'], core_args['rte']):
                    MS.close_browser()
                    break

            case = cases.pop(0)
            logging.info(f"{MS} taking case: {case}")
            try:
                member_path = await MS.pull_members(case)
            except:
                member_path = None

            if member_path:
                if not case.get('ucid'):
                    case_no = ftools.clean_case_id(member_path.name)
                    case['ucid'] = dtools.ucid(core_args['court'], case_no)

                results.append(member_path)
                logging.info(f"{MS} downloaded {case['ucid']} member list successfully")
            else:
                logging.info(f"{MS} ERROR downloading member list for {case}")

        logging.info(f"{MS} finished scraping")
        MS.close_browser()

    logging.info(f"\n######\n## Member Scraper Sequence [{core_args['court']}]\n######\n")

    # Handle parsing of input
    cases = parse_member_case_input(member_input, core_args['court'])

    logging.info(f"Built case list of length: {len(cases):,}")

    # Apply limit
    if core_args['case_limit']:
        cases = cases[:core_args['case_limit']]
        logging.info(f"Applying case limit({core_args['case_limit']}), {len(cases):,} will be downloaded")

    logging.info(f"Member Scraper initialised with {len(cases):,} cases.")

    # Initilise results list, will be accessed by all instances of _scraper_
    results = []

    # Initialise scrapers, run asynchronously
    scrapers = [asyncio.create_task(_scraper_(args=core_args, ind=i)) for i in range(core_args['n_workers'])]
    await asyncio.gather(*scrapers)
    logging.info(f'\nMember Scraper sequence terminated successfully')

    return results

async def seq_document(core_args, new_dockets, document_input, document_att, skip_seen, document_limit, all_docs):
    ''' Scraper sequence that handles multiple workers for the Document Scraper module '''

    async def _scraper_(args, ind):
        ''' Sub-sequence for single instance of Document Scraper'''
        DocS = DocumentScraper(
            core_args = {**args, 'ind':ind},
            get_att = document_att,
            doc_limit = document_limit
        )
        while len(dockets):
            # Check time restriction
            if core_args['time_restriction']:
                if not check_time_continue(core_args['rts'], core_args['rte']):
                    DocS.close_browser()
                    break
            # Pop a case off the dockets list

            docket = dockets.pop(0)
            logging.info(f"{DocS} taking case {docket['ucid']}")
            try:
                await DocS.pull_docs(docket)
            except:
                logging.info(f'{DocS} Error downloading documents from {file.name}')

        logging.info(f"{DocS} finished scraping")
        DocS.close_browser()

    logging.info(f"\n######\n## Document Scraper Sequence [{core_args['court']}]\n######\n")

    # Handle separate download folders for browser instances
    core_args['court_dir'].make_temp_subdirs(core_args['n_workers'])

    # Pre- Cleanup, to prevent duplication if last run errored out
    logging.info("Pre-clean: Cleaning up temporary download folders")
    stools.clean_temp_download_folders(core_args['court_dir'])

    # Generate list of dockets
    dockets = [{'fpath':x} for x in new_dockets] or generate_dockets_list(document_input, core_args, skip_seen, all_docs)

    # Deal with no. of cases and case limit
    logging.info(f"Document Scraper initialised with {len(dockets)} case dockets.")
    if core_args['case_limit']:
        dockets = dockets[:core_args['case_limit']]
        logging.info(f"Applying case limit({core_args['case_limit']}): {len(dockets)} case dockets will be included in Document Scraper")
    logging.info(f"Document Limit of {document_limit:,} will be applied. Any individual case with more than {document_limit:,} documents will be skipped.\n")

    # Create scraper instances and await completion
    scrapers = [asyncio.create_task(_scraper_(args=core_args, ind=i)) for i in range(core_args['n_workers'])]
    await asyncio.gather(*scrapers)

    # Cleanup
    logging.info("Cleaning up temporary download folders")
    stools.clean_temp_download_folders(core_args['court_dir'])

    logging.info(f'Document Scraper sequence terminated successfully')


@click.command()
@click.argument('inpath') # The directory for the court being scraped
# General Options
@click.option('--mode','-m', type=click.Choice(['all',*MODULES]), prompt=True,
               help="Choose to run all or a single scraper module", show_choices=True)
@click.option('--n-workers', '-nw', type=int, default=N_WORKERS, show_default=True,
               help="No. of workers to run simultaneously (for docket/document)")
@click.option('--court', '-c', prompt=True,
               help="Court abbrev. e.g. 'ilnd'")
@click.option('--case-type', '-ct', default=None,
               help="Single case type to pull from queries, alternatively gets 'cv' and 'cr' ")
@click.option('--auth-path', '-a', prompt=True,
               help='Path to login details .json file where "user" and "pass" are defined')
@click.option('--override-time', default=False, is_flag=True,
               help="Override the time restrictions")
@click.option('--runtime-start','-rts', default=stools.PACER_HOURS_START, show_default=True,
               help="RunTime Start hour (in 24hrs, CDT)", type=click.IntRange(0, 23))
@click.option('--runtime-end','-rte', default=stools.PACER_HOURS_END, show_default=True,
               help="RunTime End hour (in 24hrs, CDT)", type=click.IntRange(0, 23))
@click.option('--case-limit','-cl', default=None,
               help='Sets limit on no. of cases to process, enter "false" for no limit')
@click.option('--cost-limit', type=float, default=None)
@click.option('--headless', '-h', default=False, is_flag=True,
               help='Runs selenium in headless mode if true')
@click.option('--verbose', '-v', default=False, is_flag=True,
               help='Whether or not to log additional info')
@click.option('--slabels', default='',
               help='Scrape labels, a comma delimited list of labels to add to a scrape session, will be added to stamp of HTML files \
                    e.g. "STUB,PROJECT_ABC"')

# Query options
@click.option('--query-conf', '-qc', default=None,
               help="Query scraper: config for the query, can either be a filepath or a valid JSON string, if none specified query builder will run")
@click.option('--query-prefix', default=None,
               help="Query scraper: prefix for the output query filenames, if multiple files will get named '{prefix}__1.html' etc.")

# Docket options
@click.option('--docket-input', default=None,
               help="Docket Scraper: a single query html, a directory of query htmls or a csv with ucids")
@click.option('--docket-mem-list', '-mem',type=click.Choice(MEM_LIST_OPTS), default='never', show_default=True,
               help="Docket Scraper: Whether to include the member list in reports: never, always or avoid "+
                     "(avoid: do not include member list in report if current case id was previously seen as a member case)")
@click.option('--docket-exclude-parties', default=False, is_flag=True, show_default=True,
               help="Docket Scraper: If True, 'Parties and counsel' and 'Terminated parties' will be excluded from docket report")
@click.option('--docket-exclusions','-ex', default=None, show_default=True,
              help="Path to files to exclude (csv with a ucid column)")
@click.option('--docket-update', default=False, show_default=True, is_flag=True,
              help="Check for new docket lines in existing cases")

# Summary Options
@click.option('--summary-input', default=None,
               help="Summary Scraper: a single query html, a directory of query htmls or a csv with ucids")

# Member Options
@click.option('--member-input', default=None,
               help="Member Scraper: a csv that has at least one of (pacer_id, case_no, ucid)")

# Document options
@click.option('--document-input', default=None,
                help="Document Scraper: A file with a list of ucids to limit document to a subset of cases/dockets")
@click.option('--document-all-docs', default=False, show_default=True, is_flag=True,
                help="If true will grab all documents per case (unless `doc_no` column supplied in document-input)")
@click.option('--document-att/--no-document-att', default=True, show_default=True,
               help="Document Scraper: whether or not to get document attachments")
@click.option('--document-skip-seen/--no-document-skip-seen', is_flag=True, default=True, show_default=True,
               help="Document Scraper: Skip seen cases, ignore any cases where we have previously downloaded any documents")
@click.option('--document-limit', default=DOCKET_ROW_DOCS_LIMIT, show_default=True,
               help="Document Scraper: skip cases that have more documents than document_limit")
def scraper(inpath, mode, n_workers, court, case_type, auth_path, override_time, runtime_start, runtime_end, case_limit, cost_limit, headless, verbose, slabels,
         query_conf, query_prefix,
         docket_input, docket_mem_list, docket_exclusions, docket_update, docket_exclude_parties,
         summary_input,
         member_input,
         document_input, document_att, document_skip_seen, document_limit, document_all_docs):
    ''' Handles arguments/options, the run sequence of the 3 modules'''

    Path(settings.LOG_DIR).mkdir(exist_ok=True)
    time_str = stools.get_time_central(as_string=True).replace(':','-')
    logpath = Path(settings.LOG_DIR) / f"log_{time_str}.txt"
    logging.basicConfig(level=logging.INFO, filename=logpath, filemode='w')
    logging.getLogger().addHandler(logging.StreamHandler())

    time_restriction = not override_time

    if time_restriction:
        if not check_time_continue(runtime_start,runtime_end):
            return

    # Create core arguments to be pased to CoreScraper
    court_dir = PacerCourtDir(inpath, court)
    # Ensure docket limit supplied
    if case_limit == None:
       print("\n($$$) Downloading many files from Pacer can be expensive.")
       case_limit = click.prompt("Case Limit: Please enter a limit on the # of cases to download (or enter 'false' for no limit)")

    if case_limit[0].lower() == 'f':
        case_limit = None
    elif case_limit.isdigit():
        case_limit = int(case_limit)
    else:
        raise ValueError('case_limit value is not valid')

    core_args = {
        'slabels': slabels,
        'verbose': verbose,
        'headless': headless,
        'court_dir': court_dir,
        'court': court,
        'case_type': case_type,
        'auth_path': auth_path,
        'case_limit': case_limit,
        'cost_limit': cost_limit,
        'time_restriction': time_restriction,
        'rts': runtime_start,
        'rte': runtime_end,
        'n_workers': n_workers,
        'exclusions_path': Path(docket_exclusions).resolve() if docket_exclusions else None
    }

    # Create the run schedule of which modules to run
    # run_module = {k : bool(mode in ['all', k]) for k in MODULES}
    # if mode=='all':
    #     logging.info('\nMODE:ALL - All three scrapers will run in sequence (query,docket,document)')

    run_module = {k : (k==mode) for k in MODULES}

    # Query Scraper run sequence
    if run_module['query']:
        # Get query config file or run builder
        if query_conf:
            # Load from valid JSON
            if query_conf[0]=='{':
                config = json.loads(query_conf)
            # Otherwise assume filepath and load from file
            else:
                config = json.load(open(query_conf, encoding="utf-8"))
        else:
            logging.info(f"No config_query specified, running query builder...")
            config = forms.config_builder('query')

        query_results = seq_query(core_args, config, query_prefix)
    else:
        query_results = []

    # Docket Scraper run sequence
    if run_module['docket']:
        if not docket_input:
            raise ValueError('Must supply a --docket-input for the docket scraper')
        else:
            docket_input = Path(docket_input).resolve()

        docket_results = asyncio.run(
            seq_docket(
                core_args,
                query_results = query_results,
                docket_input = docket_input,
                docket_update = docket_update,
                show_member_list = docket_mem_list,
                exclude_parties = docket_exclude_parties
            )
        )

    # Summary Scraper run sequence
    if run_module['summary']:
        summary_input = Path(summary_input).resolve() if summary_input else None
        docket_results = asyncio.run(
            seq_summary(core_args, summary_input = summary_input)
        )

    # Member Scraper run sequence
    if run_module['member']:
        member_input = Path(member_input).resolve()
        docket_results = asyncio.run(
            seq_member(core_args, member_input=member_input)
        )

    # Document Scraper run sequence
    if run_module['document']:

        # Get the list of new dockets generated by docket scraper
        new_dockets = docket_results if run_module['docket'] else []

        asyncio.run(seq_document(core_args, new_dockets, document_input,
                                document_att, document_skip_seen, document_limit, document_all_docs))


    term_time = stools.get_time_central(as_string=True)
    logging.info(f"\nScraping session terminated at {term_time}")
    print(f'Scrape terminated, log file available at: {logpath}')

if __name__ == '__main__':
    scraper()
