import json
import time
import os
import re
import sys
from hashlib import md5
from pathlib import Path
from datetime import datetime, timedelta

import pytz
import pandas as pd
from pandas import to_datetime
from selenium.webdriver import FirefoxOptions

sys.path.append(str(Path(__file__).resolve().parents[1]))
from downloader import forms
from support import settings

# Default runtime hours
PACER_HOURS_START = 20
PACER_HOURS_END = 4

# Time formats
FMT_TIME = '%Y-%m-%d-%H:%M:%S'
FMT_TIME_FNAME ='%y%m%d'
FMT_PACERDATE = '%m/%d/%Y'

GODLS = {
    # Ordered list of goDLS arguments
    'args': ['action', 'caseid', 'de_seq_num','got_receipt','pdf_header',
             'pdf_toggle_possible', 'magic_num', 'hdr']
}

# Patterns
re_com = {
    'office': r"[0-9A-Za-z]",
    'year': r"[0-9]{2}",
    'case_type': r"[A-Za-z]{1,4}",
    'case_no': r"[0-9]{3,10}",
    'def_no': r"(?:-[0-9]{1,3})?",
    'href': r'''["']/cgi-bin/DktRpt.pl\?[0-9]{1,10}['"]''',
    'judge_names': r"(?:-[A-Za-z]{2,4}){0,3}",
}

re_case_name = rf"{re_com['office']}:{re_com['year']}-{re_com['case_type']}-{re_com['case_no']}{re_com['judge_names']}"
re_case_link = rf'''<a href={re_com['href']}>{re_case_name}</a>'''
re_lead_link = rf'''Lead case:[ ]{{0,2}}{re_case_link}'''
re_other_court = r'Case in other court.+</td></tr>'
re_members_truncated = r'(View Member Case)'

# Regex Named Groups for case name
rg = lambda k: rf"(?P<{k}>{re_com[k]})"
re_case_no_gr = rf"{rg('office')}:{rg('year')}-{rg('case_type')}-{rg('case_no')}{rg('def_no')}{rg('judge_names')}"

re_mdl_caseno_condensed = rf"{rg('year')}-?{rg('case_type')}-?{rg('case_no')}"

def decompose_caseno(case_no, pattern=re_case_no_gr):
    ''' Decompose a case no. of the fomrat "2:16-cv-01002-ROS" '''
    match =  re.search(pattern, case_no)
    if not match:
        raise ValueError(f"case_no supplied ({case_no})was not in the expected format, see re_case_no_gr")
    else:
        data = match.groupdict()
        judges = data['judge_names'].strip('-').split('-') if data.get('judge_names','') != '' else None
        data['judge_names'] = judges
        data['def_no'] = data['def_no'].lstrip('-') if data.get('def_no','') != '' else None
        return data


def case2file(case_name, ind=None):
    '''
	Converts a case name to a filename. Cannot use colon in a filename so replaces colon with a hyphen.
    Inputs:
        - case_name (str): name of case from Pacer
        - ind (int): index used if saving an update html e.g. "casename_2.html"
    '''
    if ind==None:
        return f"{case_name.replace(':','-')}.html"
    else:
        return f"{case_name.replace(':','-')}_{ind}.html"

def main_limiter(case_no):
    '''
    Returns true if the case_name is for a 'main' case, ie not for an individual
    '''
    try:
        data = decompose_caseno(case_no)
        if data['def_no']:
            return False
        else:
            return True
    except:
        print(case_no)
        if case_no[-2:]=='-1':
            return False
        else:
            return True
    # import sys
    # sys.path.append('..')
    # import support.settings as settings

    # style_dict = json.load( open(settings.PROJECT_ROOT /'code'/'downloader'/'login'/'casename_style.json') )
    # #This is for northern georgia
    # if state_court in style_dict['ending_integer']:
        # try:
            # int(case_name.split('-')[-1].strip())
            # return False
        # except ValueError:
            # return True
    # elif state_court in style_dict['dash_count']:
        # if case_name.count('-')==2:
            # return True
        # else:
            # return False

def gen_user_hash(user):
    '''Generate a has based on username'''
    return md5(user.encode("utf-8")).hexdigest()[:8]

def clean_case_id(case_no, allow_indivs=False):
    '''
    Takes a listed case name and clean anything that isn't the office,year, case type, and case no
    Inputs:
        case_no (str): name of the case from the query
        allow_indivs (bool): allow individual dockets e.g. {case_name}-1 for defendant 1,
            if false returns None
    Outputs:
        Cleaned standardised name
    '''
    if ':' not in case_no:
        case_no = case_no.replace('-', ':', 1)
    try:
        case = decompose_caseno(case_no)
        if not allow_indivs and case['def_no']!=None:
            return
        else:
            return rf"{case['office']}:{case['year']}-{case['case_type']}-{case['case_no']}"
    except ValueError:
        return case_no

def month_chunker(date_from, date_to, chunk_size=31):
    ''' Break a time period into chunks '''

    date_from = pd.to_datetime(date_from)
    date_to = pd.to_datetime(date_to)
    if date_to > date_to:
        raise ValueError('date_to cannot be before date_from')

    month = timedelta(days=chunk_size)
    left_date = date_from
    right_date = left_date + month
    chunks = []
    while right_date < date_to:
        chunks.append((left_date, right_date))
        left_date += month
        right_date += month
    # Add the final chunk (less than a full period)
    chunks.append( (left_date, date_to))
    return chunks

def split_config(config, date_pairs, max_gap=180, chunk_size=31):
    '''
    Takes configs and splits into list of configs
    Inputs:
        - config (dict): a single query config file
        - date_pairs (list): a list of pairs of keys e.g. [('date_from'), ('date_to')]
        - mag_gap (int): the largest allowed date range, above this value gets chunked
        - chunk_size (int): the size of chunks
    Output:
        A list of config files with the dates chunked
    '''
    config_list = []

    # Check if date range is too wide
    for date_from_field, date_to_field in date_pairs:
        if config.get(date_from_field):
            date_from = pd.to_datetime(config.get(date_from_field))
            date_to = pd.to_datetime(config.get(date_to_field)) or datetime.today()

            if abs(date_from - date_to).days >= max_gap:
                date_ranges = month_chunker(date_from, date_to, chunk_size)
                for left, right in date_ranges:
                    new_config = {**config}
                    new_config[date_from_field] = left.strftime('%m/%d/%Y')
                    new_config[date_to_field] = right.strftime('%m/%d/%Y')
                    config_list.append(new_config)
                return config_list

    #Else return singleton list with original config
    return [config]

def login(browser, auth, login_url=None, logging=None):
    '''
    Method to log in to a court
    Inputs:
        - browser: browser instance
        - auth_path (str or Path): path to login details
        - login_url (str): url to login page, if empty assumes browser is already there
        - logging: logging instance
    '''
    if login_url:
        browser.get(login_url)

    # Check if already logged in
    if is_logged_in(browser):
        return True

    fill_values =  {'username':auth['user'], 'password':auth['pass']}
    login_form = forms.FormFiller(browser, 'login',fill_values)
    login_form.fill()
    time.sleep(1)
    login_form.submit()
    time.sleep(2)

    # Check if details correct
    if "Invalid username or password" in browser.page_source:
        logging.error('Invalid Username or Password')
        return False
    else:
        if logging:
            logging.info('Login succesful')
        return True

def parse_goDLS_string(string):
    '''
    Take a goDLS string and return a dictionary of arguments
    Inputs:
        - string: a goDLS string like "goDLS('k','g','b','e','a','c','i','l')"
    Output:
        A dictionary or arguments e.g.
        {'action': '/doc1/02311840388', 'caseid': '56434', ...etc }
    '''
    re_args = rf''',\s?'''.join([rf"'(?P<{arg}>.*?)'" for arg in GODLS['args']])
    re_full = f"goDLS\({re_args}\)"
    match = re.search(re_full, string)
    if match:
        result = match.groupdict()
        # Separate out the doc_id
        result['action_doc_id'] = result['action'].split('/')[-1]
        return result

def build_goDLS(arg_dict, **kwargs):
    ''' Generate a goDLS js string from an argument dictionary'''
    final_args = {**arg_dict, **kwargs}
    arg_string = ",".join([f"'{final_args[k]}'" for k in GODLS['args'] ])
    return f"goDLS({arg_string})"

def get_link_attrs(el, mode, get_godls=False):
    '''
    From a bs4 tag get the href string and goDLS dictionary
    Inputs:
        - el (WebElement)
        - mode ('bs4' or 'selenium')
        - get_godls (bool): whether or not to get the godls data
    '''
    if mode == 'bs4':
        href =  el.attrs.get('href')
        onclick = el.attrs.get('onclick')
    elif mode == 'selenium':
        href =  el.get_attribute('href')
        onclick = el.get_attribute('onclick')

    if get_godls:
        try:
            go_dls_string = onclick.split(';')[0]
            go_dls_dict = parse_goDLS_string(go_dls_string) or {}
        except:
            go_dls_dict = {}
        return {'href': href, 'go_dls':go_dls_dict}

    return {'href': href}

def get_single_link(el, mode='bs4'):
    '''
    Get the doc no, link and goDLS data for a single document (bs4 a-tag)

    Inputs:
        - el (WebElement): the WebElement for the document link a-tag
        - mode ('bs4' or 'selenium')
    Output:
        dict
    '''
    doc = {}
    doc['ind'] = el.text
    doc.update(get_link_attrs(el,mode))
    return doc

def get_document_links(docket_table, get_att=False, filter_fn=None, wanted_doc_nos={} ):
    '''
    Get links to documents from the docket_table
    Inputs:
        - docket_table (WebElement): the docket report main table
        - get_att (bool): if false skips the document attachments
        - filter_fn (function): filter applied to docket text first n characters
            e.g. lambda x: bool(re.search(r"^NOTICE",x,re.I)))
        - wanted_doc_nos (dict): dict of specific doc nos to get with (line_no, attachment) pairs
    Output:
        List of dicts
    '''

    # Get all doc ids from line doc links (second column)
    line_doc_ids = [ x.attrs.get('href').split('/')[-1]
                     for x in docket_table.select('tr td:nth-of-type(2) a') ]

    docs = []
    for row in docket_table.select('tr')[1:]:
        # The number in the second column of table
        td_line = row.select_one('td:nth-of-type(2) a')
        if not td_line:
            continue
        else:
            td_line_no = td_line.text.strip()

        # Decide flow
        get_line_doc = (not bool(wanted_doc_nos)) or ('0' in wanted_doc_nos.get(td_line_no,[]) )

        get_line_atts = (not bool(wanted_doc_nos) and get_att) \
                            or len([x for x in wanted_doc_nos.get(td_line_no,[])if x!='0'])>0

        # # Skip if wanted_doc_nos specified but line_no not one of the wanted lines
        # if wanted_doc_nos and td_line_no not in wanted_doc_nos.keys():
        #     continue

        if not (get_line_doc or get_line_atts):
            continue

        td_docket_text = row.select('td')[2]

        # Get the line doc info if no wanted_doc_nos specified or if '0' in list
        if get_line_doc:
            doc = get_single_link(td_line)
            doc['get_line_doc'] = True
            #
            #
            # docket_text = td_docket_text.text[:200]
            # if filter_fn != None:
            #     if not filter_fn(docket_text):
            #         continue
            # doc['text'] = docket_text


        else:
            doc = {'ind': td_line_no}
            doc['get_line_doc'] = False

        # Only pulls documents that are case_related, not external urls
        atts = []
        if get_line_atts:
            # Isolate the atags of interest
            atags = td_docket_text.select('a')
            # Filter out external links (by only using digit links)
            atags = [a for a in atags if a.text.strip().isdigit()]
            # Filter out references to previous lines
            atags = [a for a in atags if a.attrs.get('href').split('/')[-1] not in line_doc_ids]

            # Filter down by wanted_doc_nos
            if wanted_doc_nos:
                atags = [a for a in atags if a.text.strip() in wanted_doc_nos.get(td_line_no,[]) ]

            atts = [get_single_link(a) for a in atags]

        doc['atts'] = atts
        docs.append(doc)

    return docs

def parse_document_no(doc_no):
    ''' Take input for document numbers (for a single case) and parse
    Input:
        - doc_no (str): a comma-delimited list of doc nos that can be one of 3 things:
                        [1: a single doc e.g. 3][2: an attachment e.g. 3_1][3: a range e.g. 5:7 (inclusive)]
    Output:
        wanted_doc_nos: a dict of (row number, list of docs) key-value pairs where '0' is a placeholder to dowload the line doc
                        e.g. { '1': ['0', '4', '5'],  '2': ['0'], '3': ['9'] }
    '''
    re_doc = r'^\d+$'
    re_att = r'^\d+_\d+(\:\d+)?$'
    re_range = r'^\d+\:\d+$'

    doc_no = str(doc_no).replace(' ','').strip()
    doc_list = []

    # Parse all to extrapolate from ranges
    chunks = doc_no.split(',')
    for chunk in chunks:

        # For line doc e.g. just "3"
        if re.match(re_doc, chunk):
            doc_list.append(chunk)

        # Attachment
        elif re.match(re_att, chunk):
            doc, att = chunk.split('_')

            # x_y
            if re.match(re_doc, att):
                doc_list.append(chunk)

            # x_y:z
            elif re.match(re_range, att):
                a,b = [int(x) for x in att.split(':')]
                doc_list.extend([f"{doc}_{x}" for x in range(a, b+1)])

        # Range (x:y)
        elif re.match(re_range, chunk):
            a,b = [int(x) for x in chunk.split(':')]
            doc_list.extend([str(x) for x in range(a,b+1)])

    # Sort the list, need the assumption of sorted for next part
    doc_list = sorted(set(doc_list))

    # After extrapolation of ranges compile a dict of results
    wanted_doc_nos = {}
    for doc_no in doc_list:
        if re.match(re_doc, doc_no):
            wanted_doc_nos[doc_no] = []
            wanted_doc_nos[doc_no].append('0')

        elif re.match(re_att, doc_no):
            line_no, att_no = doc_no.split('_')
            if line_no not in wanted_doc_nos.keys():
                wanted_doc_nos[line_no] = []

            wanted_doc_nos[line_no].append(att_no)

    return wanted_doc_nos



def generate_document_id(ucid, index, att_index=None, ):
    '''
    Generate a unique file name for case document download
    e.g. akd;;1-16-cv-00054_7_3
    Inputs:
        - ucid (str) - the case ucid
        - index (int) - the index of the document (from the # column in docket report)
        - att_index (int) - the index of the attachment for that given line in docket Report (if it's an attachment)
    '''
    att_index_contribution = '_'+str(att_index) if att_index else ''
    doc_id = f"{ucid.replace(':','-')}_{index}{att_index_contribution}"
    return doc_id

def generate_document_fname(doc_id, user_hash, ext='pdf'):
    '''
    Generate a unique file name for case document download
    e.g. akd;;1-16-cv-00054_7_u939f6298_t20200828-104535.pdf
    Inputs:
        - doc_id (str) - any document id
        - user_hash (str) - the user hash of account used to download
        - ext (str) - the file extension
    '''
    file_time = datetime.now().strftime(FMT_TIME_FNAME)
    return f"{doc_id}_u{user_hash}_t{file_time}.{ext}"

def parse_document_fname(fname):
    ''' Parse a document filename, return the component parts as a dict'''

    re_doc_id = r"(?P<ucid_no_colon>[a-z0-9;\-]+)_(?P<index>\d+)(_(?P<att_index>\d+))?"
    re_download_name = rf"(?P<doc_id>{re_doc_id})_u(?P<user_hash>[a-z0-9]+)_t(?P<download_time>[0-9\-]+)\.(?P<ext>.+)"
    re_old = rf"(?P<doc_id>{re_doc_id})(?P<ext>.+)" #old format

    match = re.match(re_download_name,fname)
    if not match:
        match = re.match(re_old, fname)
    if match:
        res = match.groupdict()
        res['ucid'] = res['ucid_no_colon'].replace('-',':',1)
        del res['ucid_no_colon']
        return res

def get_recent_download(dir, ext=None, wait_time=2, time_buffer=30):
    '''
    Get the most recently downloaded file in the given directory
    Inputs:
        - dir (str or Path): the directory
        - ext (str): the file extension
        - wait_time (int): the amount of time to wait for the download
        - time_buffer (int): the number of seconds buffer to add to wait_time
    Output:
        Path
    '''
    time.sleep(wait_time)
    pattern = "*.{ext}" if ext else "*"
    files = list(x for x in Path(dir).glob(pattern) if x.is_file() )
    if not len(files):
        return

    # Get most recent file base on created time
    candidate = max(files, key=lambda x: x.lstat().st_ctime)
    if (time.time() - candidate.lstat().st_ctime) < (wait_time + time_buffer):
        return candidate

def get_pacer_url(court, page):
    '''
    Get a court-specific pacer url
    Inputs:
        - court (str): court abbrev. e.g. ilnd
        - page (str): the pacer page you want a url for
    Ouput:
        url (str)
    '''
    if court=='psc':
        base_url = "https://dcecf.psc.uscourts.gov/"
    else:
        base_url = f"https://ecf.{court}.uscourts.gov/"

    if page == 'query':
        return base_url + 'cgi-bin/iquery.pl'
    elif page == 'login':
        return base_url + 'cgi-bin/login.pl'
    elif page == 'logout':
        return base_url + 'cgi-bin/login.pl?logout'
    elif page == 'docket':
        return base_url + "cgi-bin/DktRpt.pl"

def remap_date_year_backwards_to_forwards(xdate):
    '''
    Turns a date of type MM/DD/YY to YYYY-MM-DD
    '''
    m, d, y = xdate.strip().split('/')
    return '-'.join(['20'+y, m, d])

def extract_query_filedate(query_date_str):
    '''
    Extracts the filing date from the query date string of type "filed XX/XX/XX closed XX/XX/XX"
    and reworks it to match XXXX-XX-XX
    '''
    if 'closed' in query_date_str:
        filedate = query_date_str.lstrip('filed ').split(' closed')[0]
    else:
        filedate = query_date_str.lstrip('filed ').strip()
    return remap_date_year_backwards_to_forwards(filedate)

def extract_query_termdate(query_date_str):
    '''Extracts the termination date from the query date string of type "filed XX/XX/XX closed XX/XX/XX"'''
    if 'closed' in query_date_str:
        termdate = query_date_str.split(' closed ')[-1]
        return remap_date_year_backwards_to_forwards(termdate)
    else:
        return None

def is_logged_in(browser):
    ''' Check if logged in to pacer, returns true if logout is in the navbar'''

    # Check if the navbar is there
    nav = browser.find_elements_by_css_selector('#topmenu')
    if not len(nav):
        return None
    # Get all the links
    nav_links = nav[0].find_elements_by_css_selector('a')
    # Clean the link text and return true if logout is in
    return 'logout' in [link.text.lower().replace(' ','').strip() for link in nav_links]

def get_firefox_options(download_dir):
    '''Default options for firefox'''
    options = FirefoxOptions()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", str(download_dir))
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
    options.set_preference("plugin.disable_full_page_plugin_for_types", "application/pdf")
    options.set_preference("pdfjs.disabled", True)
    return options

def get_time_central(as_string=False):
    '''Get the current time in central time zone'''
    utc = pytz.timezone('UTC')
    now = utc.localize(datetime.utcnow())

    central = pytz.timezone('US/Central')
    res = now.astimezone(central)
    return res if not as_string else res.strftime(FMT_TIME)

def check_time_continue(start, end, current_ind, logging=None):
    ''' Check if scraper should continue'''
    no_run_hours = range(start-1,end-1,-1)
    if get_time_central().hour in no_run_hours:
        print(f'Scraping operating outside of running hours, terminating (at file index {current_ind})')
        if logging:
            logging.info(f'Scraping operating outside of running hours, terminating (at file index {current_ind})')
        return False
    else:
         return True

def docket_aggregator(fpaths, outfile):
    '''
    Build a docket report from multiple dockets for same case, outputs new html(dl)

    Inputs:
        - fpaths (list): a list of paths to docket htmls (in chronological order)
            the order supplied will be order of table in output (uses last one as base docket)
        - outfile (str or Path): output html file path
    '''
    from bs4 import BeautifulSoup

    def _hash_row(tr):
        ''' Create a hash from the text of: date + # + docket_text[:20]'''
        val = ''
        for cell in tr.select('td'):
            val+= cell.text[:20]
        return hash(row)

    rows = []

    for fpath in fpaths:
        soup = BeautifulSoup(open(fpath).read(), "html.parser")
        docket_table = soup.select('table')[-2]
        rows.extend(docket_table.select('tr')[1:])

    # Use the last soup as the base
    header_row = docket_table.select_one('tr')
    docket_table.clear()
    docket_table.append(header_row)

    hashes = []

    for row in rows:
        rhash = _hash_row(row)
        if rhash not in hashes:
            docket_table.append(row)
            hashes.append(rhash)

    with open(outfile, 'w', encoding="utf-8") as wfile:
        wfile.write(str(soup))
