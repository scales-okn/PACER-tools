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

import click
import xmltodict
import pandas as pd
from bs4 import BeautifulSoup
from seleniumrequests import Firefox

sys.path.append(str(Path(__file__).resolve().parents[1]))
from downloader import forms
from downloader import scraper_tools as stools

from support import settings
from support import data_tools as dtools
from support import fhandle_tools as ftools

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
MODULES = ['query', 'docket', 'document']
MEM_LIST_OPTS = ['always', 'avoid', 'never']

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
    |-- _temp_
    |   |-- 0
    |       | download(7).pdf
    |   |-- 1
    |       | download(5).pdf

    '''

    def __init__(self, dir, court):
        # Root
        self.root = Path(dir).resolve()

        # Subdirectories
        for subdir in ['html', 'json', 'queries', 'docs', '_temp_']:
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

    def __init__(self, court_dir, court, auth_path, n_workers=N_WORKERS, exclusions_path=None, case_type=None,
                 firefox_options=None, ind='#', case_limit=None, time_restriction=None, rts=None, rte=None):
        self.browser = None
        self.firefox_options = firefox_options
        self.dir = court_dir
        self.court = court
        self.n_workers = n_workers
        self.exclusions_path = exclusions_path
        self.case_type = case_type
        self.ind = ind
        self.case_limit = case_limit
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
        self.browser = Firefox(options=self.firefox_options)
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
            'pacer_id': pacer_id
        }
        data_str = ";".join(f"{k}:{v}" for k,v in data.items())
        return f"\n<!-- SCALESDOWNLOAD;{data_str} -->"

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

##########################################################
###  QUERY SCRAPER
##########################################################

class QueryScraper(CoreScraper):
    ''' Enters query to Pacer and downloads the search results '''
    def __init__(self, core_args, config):
        super().__init__(**core_args)
        self.config = config
        self.config_list = stools.split_config(config, [('filed_from', 'filed_to')])

        logging.info(f"Intitiated Query Scraper")

    def results_found(self):
        ''' Returns False if query leads to "No information was found" page, else True'''
        return not self.browser.find_element_by_css_selector('#cmecfMainContent h2').text.startswith('No information was found')

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
                    outpath = self.dir.queries/f'query_{self.start_time.replace(":","-")}__{i}.html'

                    time.sleep(PAUSE['micro'])
                    download_url = self.browser.current_url

                    with open(outpath, 'w+') as wfile:
                        # Add stamp to bottom of html as it is being written
                        wfile.write(self.browser.page_source + self.stamp(download_url))
                    results.append(outpath)
                else:
                    logging.info(f"No results found for chunk with index:{i}\
                        (filed_from:{config_chunk.get('filed_from')}, filed_to:{config_chunk.get('filed_to')})")
            except:
                logging.info('Error with this chunk')
                continue

        logging.info(f"Query results saved to {self.dir.queries}")

        return results

##########################################################
###  DOCKET SCRAPER
##########################################################

class DocketScraper(CoreScraper):
    ''' Main scraper, pulls docket reports from search results '''

    re_mem = re.compile('''<a href=[\\\]{0,1}["']/cgi-bin/DktRpt.pl\?[0-9]{1,10}[\\\]{0,1}['"]>[0-9]\:[0-9][0-9]\-c[vr]-[0-9]{3,10}</a>''')

    def __init__(self, core_args, show_member_list='avoid', docket_update=False, exclude_parties=False):

        super().__init__(**core_args)
        self.files = None
        self.show_member_list = show_member_list
        self.docket_update = docket_update
        self.exclude_parties = exclude_parties

        # Retrieve the list of previously seen members only if show_member_list is 'avoid'
        self.member_list_seen = get_member_cases(core_args['court_dir']) if show_member_list=='avoid' else []

    def __repr__(self):
        return f"<Docket Scraper:{self.ind}>"

    def at_confirm_long_case(self):
        '''Check if we get the 'are you sure you want to download this long case' page'''
        return "The report may take a long time to run" in self.browser.page_source

    def at_docket_report(self):
        '''Check if the page is actually a docket report (success)'''
        if len(self.browser.find_elements_by_css_selector('#cmecfMainContent h3')):
            return "DOCKET FOR CASE" in self.browser.find_element_by_css_selector('#cmecfMainContent h3').text
        else:
            return False

    def at_invalid_case(self):
        ''' Check if at the "Not a valid case" page '''
        return "not a valid case. Please enter a valid value" in self.browser.find_element_by_css_selector('#cmecfMainContent').text[:200]

    def at_sealed_case(self):
        ''' Check if at the "Not a valid case" page '''
        if not len(self.browser.find_elements_by_css_selector('#cmecfMainContent h3')):
            text = self.browser.find_element_by_css_selector('#cmecfMainContent').text[:200]
            if re.search(r"seal|restric|redac|protectiv", text, re.I):
                return True
        return False

    def no_new_docketlines(self):
        ''' Check if no docket lines (message: "...none satisfy the selection criteria")'''
        return bool(re.search(stools.re_no_docket, self.browser.find_element_by_css_selector('#cmecfMainContent').text))

    def at_longtime(self):
        ''' Check if at the "The report may take a long time..." page '''
        longtime_str = "report may take a long time to run because this case has many docket entries"
        return longtime_str in self.browser.find_element_by_css_selector('#cmecfMainContent').text[:200]

    @run_in_executor
    def pull_case(self, case, new_member_list_seen):
        '''
        Pull the docket from a single case
        Inputs
            - case (dict): dictionary with case details (case_no, latest_date, previously_downloaded, def_no)
            - new_member_list_seen (list): list of new member cases being updated during session
        '''
        # Navigate to docket report page
        if not self.browser:
            login_success = self.launch_browser()
            if not login_success:
                self.close_browser()
                raise ValueError('Cannot log in to PACER')

        docket_url = ftools.get_pacer_url(self.court, 'docket')
        self.browser.get(docket_url)

        # Build the input case no to allow for defendant no to be included
        case_no_input = case['case_no'] + f"-{case['def_no']}" if 'def_no' in case.keys() else case['case_no']
        fill_values = {
            'case_no': case_no_input
        }

        latest_date = case.get('latest_date', False)
        # Previously downloaded implies date_from supplied
        if latest_date:
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

        # Call the possible case no api again to get caseno data
        pacer_id = self.get_caseno_info_id(case['case_no'], case.get('def_no'))

        # Submit the form
        docket_report_form.submit()
        time.sleep(PAUSE['mini'])

        # Checks before form submission stage complete
        if not self.at_docket_report():
            #Check if at "may take a longtime page"
            if self.at_longtime():
                initially_requested = self.browser.find_elements_by_css_selector('input[name="date_from"]')[-1]
                initially_requested.click()
                time.sleep(PAUSE['micro'])
                self.browser.execute_script('ProcessForm()')
            else:
                self.browser.execute_script('ProcessForm()')
                time.sleep(PAUSE['moment'])

        # Now assume form submitted correctly, check various scenarious
        if self.at_invalid_case():
            print(f'Invalid case: {case["case_no"]}')

        elif self.at_sealed_case():
            print(f"Sealed case: {case['case_no']}")
            text = self.browser.find_element_by_css_selector('#cmecfMainContent').text[:200]
            print(f'Explanation from PACER: {text}')

        elif self.at_docket_report():
            # Save the output by case name
            outpath = self.dir.html / ftools.generate_docket_filename(case['case_no'], case.get('def_no'))

            if case.get('previously_downloaded',False):
                if self.no_new_docketlines():
                    logging.info(f'No new docket lines for case: {case["case_no"]}')
                    return

                # Create "..._n.html" etc. filename for nth update to case
                ind = 0
                while outpath.exists():
                    ind += 1
                    outpath = self.dir.html / ftools.generate_docket_filename(case['case_no'], case.get('def_no'), ind=ind)

            time.sleep(PAUSE['micro'])
            download_url = self.browser.current_url

            with open(outpath, "w+") as wfile:
                # Add the stamp to the bottom of the url as it is written
                wfile.write(self.browser.page_source + self.stamp(download_url, pacer_id=pacer_id))
            #Write the log
            logging.info(f'Downloaded case: {case["case_no"]}')

            #Check to see if it is a member case
            if self.show_member_list=='avoid':
                contents = open(outpath, 'rb').read()
                found_members = self.re_mem.findall(str(contents))
                if found_members:
                    found_case_ids = [x.split('</a>')[0].split('>')[-1] for x in found_members]
                    found_case_ids = [ftools.clean_case_id(x) for x in set(found_case_ids) if x not in self.member_list_seen]
                    new_member_list_seen += found_case_ids

            return outpath

        else:
            print(f'EROR: <case: {case["ucid"]}> no found, reason unknown')
###
# Support Functions for Docket Scraper
###
def get_member_cases(court_dir, file=settings.MEM_DF):
    ''' Get the list cases that have previously been seen listed as member cases'''
    return pd.read_csv(Path(file)).query("court==@court_dir.court")['case_no'].tolist()

def parse_query_report(html_path, full_dfs, court, case_type):
    ''' Parse a single query report .html file'''

    dfset = pd.read_html(str(html_path))
    # Extract first three columns (may be more depending on search results)
    df = dfset[0].iloc[:, :3].copy()
    df.columns = ['case_id', 'name', 'details']
    df.dropna(inplace=True)
    df['case_type'] = df.case_id.apply(lambda x: x.split('-')[1])
    df['clean_id'] = df.case_id.apply(ftools.clean_case_id)
    df['court'] = df.case_id.apply(lambda x: court)
    df['dates_filed'] = df.details.apply(stools.extract_query_filedate)

    # Case switch if case_type optional argument has been provided
    if case_type:
        df = df[df.case_type==case_type]
    else:
        df = df[df.case_type.isin(['cr', 'cv'])]


    full_dfs.append(df)
    gdf = df.loc[:, ['case_id', 'case_type']].groupby('case_type').agg('count').reset_index()
    gdf.columns = ['case_type', str(html_path)]
    return gdf


def parse_docket_input(query_results, docket_input, case_type, court):
    '''
    Figure out the input for the docket module
    Inputs:
        - query_results (list): List of query results (paths to htmls) from query module, will be [] if query scraper didn't run
        - docket_input (Path): the docket input argument
        - case_type (str)
        - court (str): court abbreviation
        - allow_def_stub
     Outputs:
        - input_data (list of dicts): data to be fed into docket scraper
            [{'case_no': 'caseA' 'latest_date': '...'},...] ('latest_date' may or may not be present)
    '''
    if len(query_results)==0 and docket_input==None:
        raise ValueError('Please provide a docket_input')

    # Check all html scenarios first
    is_html = False

    # If there are results from query scraper, use those
    if len(query_results):
        is_html = True
        query_htmls = query_results

    # If the input is a directory, get all query htmls in directory
    elif docket_input.is_dir():
        is_html=True
        query_htmls = list(docket_input.glob('*.html'))

    # If single html query, then singleton list for query_htmls
    elif docket_input.suffix == '.html':
        is_html = True
        query_htmls = [ docket_input ]

    # If any of the html scenarios reached, build case list from query_htmls
    if is_html:
        case_nos = build_case_list_from_queries(query_htmls, case_type, court)
        input_data = [{'case_no': cn} for cn in case_nos]

    # CSV case
    else:
        if docket_input.suffix !='.csv':
            raise ValueError('Cannot interpret docket_input')
        df = pd.read_csv(docket_input, dtype={'def_no':str})

        # Parse ucid and create court and case_no columns
        df = df.assign(**dtools.parse_ucid(df.ucid))
        # Restrict to just ucids in this court and drop duplicates
        df.query("court==@court", inplace=True)
        df.drop_duplicates('case_no', inplace=True)

        # Fill na for def_no before to_dict
        if 'def_no' in df.columns:
            df['def_no'].fillna('', inplace=True)

        # Keep just case_no and get lastest_date if it's there
        keepcols = [col for col in ('case_no', 'latest_date', 'def_no') if col in df.columns]


        input_data = df[keepcols].to_dict('records')

    return input_data

def build_case_list_from_queries(query_htmls, case_type, court):
    ''' Get the list of cases to scrape, filter out recap cases'''

    full_dfs=[]
    result_set = []
    for html_path in query_htmls:
        gdf = parse_query_report(html_path, full_dfs, court, case_type)
        result_set.append(gdf)

    # Compile cases to a single series, clean case ids and remove duplicates (defendant cases)
    cases = pd.concat([df['case_id'] for df in full_dfs]).map(ftools.clean_case_id).drop_duplicates()
    #
    # cases = [x for x in map(ftools.clean_case_id, list(cases)) if x]
    return cases

def get_downloaded_cases(court_dir):
    '''
    Create a df of all dockets in this court_dir html folder with filepath, ucid columns
    Inputs:
        - court_dir(PacerCourtDir)
    Output:
        DataFrame (with cols: filepath, case_no, ucid)
    '''
    all_dockets = list(court_dir.html.glob('*.html'))
    data = [(str(p), ftools.clean_case_id(p.stem) ) for p in all_dockets]
    df = pd.DataFrame(data, columns=['fpath', 'case_no'])

    # Remove dockets that are docket update html files
    re_up = r'.*_\d{1,3}.html$'
    df = df[~df.fpath.str.match(re_up)].copy()

    df['ucid'] = dtools.ucid(court_dir.court, df.case_no)

    return df[['ucid', 'fpath']]

def get_excluded_cases(exclusions_path, court):
    ''' Get the excluded cases'''
    dfe = pd.read_csv(exclusions_path)[['ucid']]
    dfe = dfe.assign(**dtools.parse_ucid(dfe.ucid))
    dfe.query("court==@court", inplace=True)
    return dfe

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
    def pull_all_docs(self, docket):
        '''
        Pull all documents from a docket report for a single case
        Inputs:
            - docket (dict): a dict with a fpath to a single .html file (and potentially a 'doc_no' key)
        '''
        if self.browser is None:
            login_success = self.launch_browser()
            if not login_success:
                self.close_browser()
                raise ValueError('Cannot log in to PACER')

        fpath = Path(docket['fpath'])
        # Get ucid
        case_no = ftools.clean_case_id(fpath.stem)
        ucid = dtools.ucid(self.court, case_no)

        # Get all the document links for this file
        soup = BeautifulSoup( open(fpath).read(), "html.parser")
        docket_table = soup.select('table')[-2]
        if not re.match('^\s?Date Filed.*', docket_table.text):
            return

        wanted_doc_nos = ftools.parse_document_no(docket.get('doc_no',''))
        if wanted_doc_nos:
            logging.info(f"Getting the following documents for case {ucid}:\n{wanted_doc_nos}")
        else:
            logging.info(f"Getting all documents for case {ucid}")


        doc_links = stools.get_document_links(docket_table, self.get_att, wanted_doc_nos=wanted_doc_nos)

        # Pull individual documents
        for doc in doc_links:
            if doc['get_line_doc'] == True:
                success = self.pull_doc(doc, ucid)
                if not success:
                    logging.info(f"{self} ERROR: pull_doc failed for case:{ucid} doc index :{doc['ind']}")

            for att in doc.get('atts', []):
                try:
                    success = self.pull_doc(doc, ucid, att)
                    if success==False:
                        pass
                except:
                    logging.info(f"ERROR: pull_doc failed for case:{ucid} doc index :{doc['ind']} and attachment index:{att['ind']}")

    def at_document_selection(self):
        '''Check if browser is at the document selection menu'''
        re_doc_selection = "Document Selection Menu"
        main_content = self.browser.find_elements_by_css_selector("div#cmecfMainContent")[0]
        return bool(re.match(rf"^\s+{re_doc_selection}.*", main_content.text))

    def at_predownload(self):
        '''Check if at a 'pre-download' pdf embed page, file hasn't downloaded, open button appears'''
        main_content = self.browser.find_elements_by_css_selector("div#cmecfMainContent")[0]
        iframe = main_content.find_elements_by_css_selector("iframe")
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
                if self.browser.find_element_by_css_selector('body').text\
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
        main_content = self.browser.find_elements_by_css_selector("div#cmecfMainContent")[0]
        return main_content.text.startswith("To accept charges shown below")

    def at_outside_warning(self):
        ''' Are we at the "Warning... link from outside..." page '''
        main_content = self.browser.find_elements_by_css_selector("div#cmecfMainContent")[0]
        return main_content.text.startswith('Warning: The link to this page may not have originated from within')

    def at_no_permission(self):
        ''' Are we at the "You do not have permission... " page '''
        main_content = self.browser.find_elements_by_css_selector("div#cmecfMainContent")[0]
        return main_content.text.startswith("You do not have permission")

    def clean_att_index(self,att_index):
        '''Clean the attachment index (usually a digit like '2' but sometimes a full url)'''
        if att_index==None or att_index.isdigit():
            return att_index
        elif re.match(r"http.*.pdf", att_index):
            # Capture the file name from the url e.g. http://..../../filename.pdf
            att_index = att_index.split('/')[-1].split('.')[0]

        return "".join(c for c in att_index if c.isdigit() or c.isalpha())[:10]

    def pull_doc(self, doc, ucid, att=None, retry_count=0, from_doc_selection=False):
        '''
        Download a single document
        Inputs:
            - doc (dict): the doc metadata
            - ucid (str): the ucid for the case
            - fpath (str or Path): new file path

        Outputs:
            success (bool): whether download succeeded
        '''
        att_index = self.clean_att_index(att['ind']) if att else None
        doc_id = ftools.generate_document_id(ucid, doc['ind'], att_index)
        fname =  ftools.generate_document_fname(doc_id, self.user_hash)
        fpath = self.dir.docs/fname

        if not from_doc_selection:
            logging.info(f"{self} downloading document: {doc_id}")

        # Check if previously downloaded
        if doc_id in self.previously_downloaded_doc_ids:
            logging.info(f'Document previously downloaded, skipping..')
            return

        url = att['href'] if att else doc['href']
        self.browser.get(url)
        time.sleep(PAUSE['micro'])

        if self.at_outside_warning():
            # Click continue
            self.browser.find_element_by_link_text("Continue").click()

        if self.at_document_selection():
            try:
                # Click the link for the document number at top of the screen
                first_row = self.browser.find_element_by_css_selector('tr')
                if first_row.text.strip().startswith('Document Number'):
                    atag = first_row.find_element_by_css_selector('a')
                    doc = stools.get_single_link(atag, mode='selenium')
                    # Re run pull_doc on new doc gathered from selection screen
                    return self.pull_doc(doc, ucid, att, from_doc_selection=True)
            except:
                logging.info(f"{self} ERROR (pull_doc): Could not select from document selection screen ({doc_id})")
                return False

        elif self.at_receipt():
            # Get relevant info from transaction receipt
            transaction_data = {k:v for k,v in dtools.parse_transaction_history(self.browser.page_source).items()
                                if k in ('billable_pages','cost')}
            if transaction_data:
                logging.info(f"{self} <case:{doc_id}> Transaction Receipt: {json.dumps(transaction_data)}")

            view_selector = 'input[type="submit"][value="View Document"]'
            view_btn = self.browser.find_element_by_css_selector(view_selector)
            view_btn.click()
            time.sleep(PAUSE['mini'])

        elif self.at_no_permission():
            logging.info(f"{self} ERROR (pull_doc): Do not have permission to access ({doc_id})")
            return False

        # Find downloaded document in the temp folder
        wait_time = 2 if retry_count==0 else 4

        file = stools.get_recent_download(self.dir.temp_subdir(self.ind), wait_time=wait_time, time_buffer=20)
        if file:
            # Move file
            if fpath.exists():
                logging.info(f'File {fpath} already exists, replacing...')
            file.replace(fpath)

            logging.info(f"File downloaded as {fpath.name}")
            return True
        else:
        #     # Retry the download
        #     if retry_count < self.RETRY_LIMIT:
        #         rc = retry_count + 1
        #         return self.pull_doc(doc, ucid, att, retry_count=rc)

            logging.info(f"{self} ERROR (pull_doc): Download not found on disk ({doc_id})")
            return False

###
# Support Functions for Docket Scraper
###

def generate_dockets_list(document_input, core_args, skip_seen=True):
    '''
    Generate the list of docket filepaths for Document Scraper

    Inputs:
        - document_input (str): filepath to a csv of cases that includes a column of ucids
        - core_args
        - skip_seen(bool): whether to skip cases from which docs have previously been downloaded
    Ouptut:
        a list of dicts with filepath and doc_no keys
    '''
    # Create a df from the list of all dockets in this court_dir html folder
    df = get_downloaded_cases(core_args['court_dir'])

    if document_input:
        # Get the subset of ucids of interest
        cases_file = Path(document_input).resolve()
        if not cases_file.exists():
            raise ValueError(f'File at {cases_file} does not exist')

        df_input = pd.read_csv(cases_file)
        keepcols = [x for x in ['ucid', 'doc_no'] if x in df_input.columns]

        df = df.merge(df_input[keepcols], how='inner', on='ucid')

    # Filter out dockets that have *any* docs downloaded if skip_seen or
    if skip_seen and not ('doc_no' in df.columns):
        # Get list of ucids that have previously had docs downloaded
        document_paths = list(core_args['court_dir'].docs.glob('*.pdf'))
        ucid_from_fpath = lambda x: ftools.parse_document_fname(x.name)['ucid']
        seen_ucids = set(ucid_from_fpath(x) for x in document_paths)
        # Limit df to ucids that haven't been seen
        df = df[~df.ucid.isin(seen_ucids)].copy()

    keepcols = [x for x in ['fpath', 'doc_no'] if x in df.columns]
    return df[keepcols].to_dict('records')

def get_latest_docket_date(fpath, court_dir):
    '''
    Get the filed date of latest docket entry for a single case
    Inputs:
        - fpath (str or Path): filepath of html for case
        - court_dir (PacerCourtDir)
    Output:
        latest_date (str) - returns None if no dates found in docket
    '''
    fname = Path(fpath).name
    jdir = court_dir.json.relative_to(settings.PROJECT_ROOT)
    jpath = settings.PROJECT_ROOT / jdir / fname.replace('.html', '.json')

    # If no json file then leave blank (will get all docket lines)
    if not jpath.exists():
        return ''

    # Load case data and get all dates
    jdata = dtools.load_case(jpath)
    docket_dates = [x['date_filed'] for x in jdata.get('docket', [])]
    if not len(docket_dates):
        return None
    else:
        latest_date = pd.to_datetime(docket_dates).max().strftime(ftools.FMT_PACERDATE)
        return latest_date

##########################################################
###  Sequences
##########################################################

def seq_query(core_args, config):
    ''' Scraper sequence that handles multiple workers for the Query module '''

    logging.info("\n######\n## Query Scraper Sequence\n######\n")
    QS = QueryScraper(core_args, config)
    results = QS.pull_queries()
    logging.info("Finished Query Scraper sequence, closing...")
    QS.close_browser()
    return results

async def seq_docket(core_args, query_results, docket_input, docket_update, show_member_list, exclude_parties):
    ''' Scraper sequence that handles multiple workers for the Docket module '''

    async def _scraper_(ind):
        ''' Sequence for single instance of Docket Scraper'''
        DktS = DocketScraper(
            core_args = {**core_args, 'ind':ind },
            show_member_list = show_member_list,
            docket_update = docket_update,
            exclude_parties = exclude_parties
        )
        while len(cases):
            # Check time restriction
            if core_args['time_restriction']:
                if not check_time_continue(core_args['rts'], core_args['rte']):
                    DktS.close_browser()
                    break
            case = cases.pop(0)
            logging.info(f"{DktS} taking {case['case_no']}")
            docket_path = await DktS.pull_case(case, new_member_list_seen)

            if docket_path:
                results.append(docket_path)
                logging.info(f"{DktS} downloaded {case['ucid']} successfully")
            # else:
            #     logging.info(f"ERROR: {DktS} could not download {case}")

        logging.info(f"{DktS} finished scraping")
        DktS.close_browser()

    logging.info("\n######\n## Docket Scraper Sequence\n######\n")

    # Handle all of parsing for varied docket_input possibilites
    input_data = parse_docket_input(query_results, docket_input, core_args['case_type'], core_args['court'])
    logging.info(f"Built case list of length: {len(input_data):,}")

    # Build a df of cases, to make dropping/excluding more efficient
    df_cases = pd.DataFrame(input_data)
    df_cases['ucid'] = dtools.ucid(core_args['court'], df_cases['case_no'])

    if core_args['exclusions_path']:
        exclude_df = get_excluded_cases(core_args['exclusions_path'], core_args['court'])
        df_cases = df_cases[~df_cases.ucid.isin(exclude_df.ucid)].copy()

    # Check which cases are downloaded
    df_down = get_downloaded_cases(core_args['court_dir'])[['ucid', 'fpath']]
    df_cases = df_cases.merge(df_down, how='left', left_on='ucid', right_on='ucid')
    df_cases['previously_downloaded'] = df_cases['fpath'].notna()

    if docket_update:
        # Ensure latest date column
        if 'latest_date' not in df_cases.columns:
            df_cases['latest_date'] = ''
        df_cases.latest_date.fillna('', inplace=True)

        # Mask for rows that need a latest_date, force fill a latest date if previously downloaded
        needs_date = df_cases.previously_downloaded & df_cases.latest_date.eq('')
        dates_needed = df_cases[needs_date]['fpath'].apply(lambda x: get_latest_docket_date(x, core_args['court_dir'] ))

        # Update dates for those needed
        df_cases.loc[needs_date, 'latest_date'] = dates_needed
        # Form the cases list
        keepcols = [col for col in ('case_no', 'ucid', 'latest_date', 'previously_downloaded','def_no') if col in df_cases.columns ]
        cases = df_cases[keepcols].to_dict('records')
    else:
        logging.info(f"Removing previously downloaded cases...")
        keepcols = [col for col in ('case_no', 'ucid', 'def_no') if col in df_cases.columns ]
        cases = df_cases[~df_cases.previously_downloaded][keepcols].to_dict('records')

    del df_cases, df_down, input_data

    logging.info(f"Docket Scraper initialised with {len(cases):,} cases.")

    # Apply limit
    if core_args['case_limit']:
        cases = cases[:core_args['case_limit']]
        logging.info(f"Applying case limit({core_args['case_limit']}), {len(cases):,} will be downloaded")

    # Initilise lists, will be accessed by all instances of _scraper_
    results = []
    new_member_list_seen = []

    # Initialise scrapers, run asynchronously
    scrapers = [asyncio.create_task(_scraper_(i)) for i in range(core_args['n_workers'])]
    await asyncio.gather(*scrapers)
    logging.info(f'Docket Scraper sequence terminated successfully')

    # When finished scraping, add new_member_list_seen to the member_list file
    if len(new_member_list_seen):
        with open(settings.MEM_DF, 'a', encoding='utf-8', newline='\n') as wfile:
            rows = ( (core_args['court'], x) for x in new_member_list_seen)
            csv.writer(wfile).writerows(rows)

        logging.info(f"Added {len(new_member_list_seen)} new member cases to members list")

    return results

async def seq_document(core_args, new_dockets, document_input, document_att, skip_seen, document_limit):
    ''' Scraper sequence that handles multiple workers for the Document Scraper module '''

    async def _scraper_(args, ind):
        ''' Sub-sequence for single instance of Document Scraper'''
        # Set instance-specific temp download folder
        args['firefox_options'] = stools.get_firefox_options(core_args['court_dir'].temp_subdir(ind))
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
            file = Path(docket['fpath'])
            logging.info(f"{DocS} taking docket {file.name}")
            try:
                await DocS.pull_all_docs(docket)
            except:
                logging.info(f'{DocS} Error downloading documents from {file.name}')

        logging.info(f"{DocS} finished scraping")
        DocS.close_browser()

    logging.info("\n######\n## Document Scraper Sequence\n######\n")

    # Handle separate download folders for browser instances
    core_args['court_dir'].make_temp_subdirs(core_args['n_workers'])

    # Generate list of dockets
    dockets = [{'fpath':x} for x in new_dockets] or generate_dockets_list(document_input, core_args, skip_seen)

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
# Query options
@click.option('--query-conf', '-qc', default=None,
               help="Query scraper: config file for the query, if none specified query builder will run")
# Docket options
@click.option('--docket-input', default=None,
               help="Docket Scraper: a single query html, a directory of query htmls or a csv with ucids")
@click.option('--docket-mem-list', '-mem',type=click.Choice(MEM_LIST_OPTS), default='never', show_default=True,
               help="Docket Scraper: Whether to include the member list in reports: never, always or avoid "+
                     "(avoid: do not include member list in report if current case id was previously seen as a member case)")
@click.option('--docket-exclude-parties', default=False, is_flag=True, show_default=True,
               help="Docket Scraper: If True, 'Parties and counsel' and 'Terminated parties' will be excluded from docket report")
@click.option('--docket-exclusions','-ex', default=settings.EXCLUDE_CASES, show_default=True,
              help="Path to files to exclude (csv with a ucid column)")
@click.option('--docket-update', '-ex', default=False, show_default=True, is_flag=True,
              help="Check for new docket lines in existing cases")
# Document options
@click.option('--document-input', default=None,
                help="Document Scraper: A file with a list of ucids to limit document to a subset of cases/dockets")
@click.option('--document-att/--no-document-att', default=True, show_default=True,
               help="Document Scraper: whether or not to get document attachments")
@click.option('--document-skip-seen/--no-document-skip-seen', is_flag=True, default=True, show_default=True,
               help="Document Scraper: Skip seen cases, ignore any cases where we have previously downloaded any documents")
@click.option('--document-limit', default=DOCKET_ROW_DOCS_LIMIT, show_default=True,
               help="Document Scraper: skip cases that have more documents than document_limit")
def main(inpath, mode, n_workers, court, case_type, auth_path, override_time, runtime_start, runtime_end, case_limit,
         query_conf,
         docket_input, docket_mem_list, docket_exclusions, docket_update, docket_exclude_parties,
         document_input, document_att, document_skip_seen, document_limit):
    ''' Handles arguments/options, the run sequence of the 3 modules'''

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
        'court_dir': court_dir,
        'court': court,
        'case_type': case_type,
        'auth_path': auth_path,
        'case_limit': case_limit,
        'time_restriction': time_restriction,
        'rts': runtime_start,
        'rte': runtime_end,
        'n_workers': n_workers,
        'exclusions_path': Path(docket_exclusions).resolve() if docket_exclusions else None
    }

    # Create the run schedule of which modules to run
    run_module = {k : bool(mode in ['all', k]) for k in MODULES}
    if mode=='all':
        logging.info('\nMODE:ALL - All three scrapers will run in sequence (query,docket,document)')

    # Query Scraper run sequence
    if run_module['query']:
        # Get query config file or run builder
        if query_conf:
            config = json.load(open(query_conf, encoding="utf-8"))
        else:
            logging.info(f"No config_query specified, running query builder...")
            config = forms.config_builder('query')

        query_results = seq_query(core_args, config)
    else:
        query_results = []

    # Docket Scraper run sequence
    if run_module['docket']:
        docket_input = Path(docket_input).resolve() if docket_input else None
        docket_results = asyncio.run( seq_docket(core_args,
                                                query_results = query_results,
                                                docket_input = docket_input,
                                                docket_update = docket_update,
                                                show_member_list = docket_mem_list,
                                                exclude_parties = docket_exclude_parties)
                        )

    # Document Scraper run sequence
    if run_module['document']:

        # Get the list of new dockets generated by docket scraper
        new_dockets = docket_results if run_module['docket'] else []

        asyncio.run(seq_document(core_args, new_dockets, document_input,
                                document_att, document_skip_seen, document_limit))

    print(f'Scrape terminated, log file available at: {logpath}')

if __name__ == '__main__':
    main()
