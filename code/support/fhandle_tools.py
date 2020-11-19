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
from support import settings
from support import data_tools as dtools

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
    'update_ind': r"\d*"
}

re_case_name = rf"{re_com['office']}:{re_com['year']}-{re_com['case_type']}-{re_com['case_no']}{re_com['judge_names']}"
re_case_link = rf'''<a href={re_com['href']}>{re_case_name}</a>'''
re_lead_link = rf'''Lead case:[ ]{{0,2}}{re_case_link}'''
re_other_court = r'Case in other court.+</td></tr>'
re_members_truncated = r'(View Member Case)'
re_billable_pages_cost = r"Billable Pages[\s\S]+?>\s?(?P<billable_pages>\d+)\s<[\s\S]+?Cost[\s\S]+?>\s?(?P<cost>\d+\.\d+)\s?<"

# Regex Named Groups for case name
rg = lambda k: rf"(?P<{k}>{re_com[k]})"
re_case_no_gr = rf"{rg('office')}:{rg('year')}-{rg('case_type')}-{rg('case_no')}{rg('def_no')}{rg('judge_names')}_?{rg('update_ind')}"

re_mdl_caseno_condensed = rf"{rg('year')}-?{rg('case_type')}-?{rg('case_no')}"

# Misc re
re_no_docket = r'(There are )?(P|p)roceedings for case .{1,50} (but none satisfy the selection criteria|are not available)'
re_members_block = r"Member cases: <table [\s\S]+?</table>"

def decompose_caseno(case_no, pattern=re_case_no_gr):
    ''' Decompose a case no. of the fomrat "2:16-cv-01002-ROS" '''
    case_no = colonize(case_no)
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

def generate_docket_filename(case_name, ind=None, ext='html'):
    '''
    Generate filename for dockets (to supercede)
    Inputs:
        - case_name (str): the case name/no, can handle: colon or no colon, trailing judges initials, file extension)
        - ind (int): index, use for docket updates when there are multiple htmls for same case
        - ext (str): generally 'html' or 'json'
    Output:
        filename (str): a filename of the form "1-16-cv-00001.html" (no judge initials)
    '''
    try:
        case_name = clean_case_id(case_name)
    except:
        pass

    case_name = decolonize(case_name)
    ext = ext.lstrip('.')

    if ind:
        return f"{case_name}_{ind}.{ext}"
    else:
        return f"{case_name}.{ext}"

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

def build_case_id(decomposed_case):
    ''' Build a standard case_id from a decomposed case'''
    c = decomposed_case
    return rf"{c['office']}:{c['year']}-{c['case_type']}-{c['case_no']}"

def colonize(case_no):
    ''' Puts colon after office in case_no if it doesn't exist'''
    if ':' not in case_no:
        case_no = case_no.replace('-', ':', 1)
    return case_no

def decolonize(case_no):
    return case_no.replace(':', '-', 1)

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
    case_no = colonize(case_no)
    try:
        case = decompose_caseno(case_no)
        if not allow_indivs and case['def_no']!=None:
            return
        else:
            return build_case_id(case)
    except ValueError:
        return case_no

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

def gen_user_hash(user):
    '''Generate a hash based on username'''
    return md5(user.encode("utf-8")).hexdigest()[:8]

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


def docket_aggregator(fpaths, outfile=None):
    '''
    Build a docket report from multiple dockets for same case, outputs new html(dl)

    Inputs:
        - fpaths (list): a list of paths to docket htmls (in chronological order)
            the order supplied will be order of table in output (uses last one as base docket)
        - outfile (str or Path): output html file path
    Output:
        - soup (bs4 object) - the aggregated html as a soup object
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

    if outfile:
        with open(outfile, 'w', encoding="utf-8") as wfile:
            wfile.write(str(soup))

    return soup

def doc_id_from_pdf_header(fpath):
    '''
    Get the document id from the pdf header
    Inputs:
        - fpath(str or Path): the path to the pdf file
    Outputs:
        - case_no (str): the case no of the form "1:16-cv-00001"
        - row_ind (str): the row index for the document
        - att_ind (str): the attachment index (None if it is not an attachment)
    '''
    import PyPDF2 as pp
    re_header = r"Case: (?P<case_no>\S+) Document #: (?P<row_ind>\d+)(\-(?P<att_ind>\d+))?"

    with open(fpath, 'rb') as f:
        pdf = pp.PdfFileReader(f, 'rb')
        stamp = pdf.getPage(0).extractText()
        parsed = re.search(re_header,stamp).groupdict()

    return parsed['case_no'], parsed['row_ind'], parsed['att_ind']

def get_correct_document_id(fpath, court):
    '''
    Get the correct doc id from the pdf header

    Inputs:
        - fpath (str or Path): to a Pacer downloaded pdf document
        - court (str): court abbreviation
    Output:
        - doc_id (str): of the format "ilnd;;1-16-cv-00001_1_1"

    '''
    case_no, row_ind, att_ind = doc_id_from_pdf_header(fpath)

    ucid = dtools.ucid(court, case_no)
    return generate_document_id(ucid, row_ind, att_ind)


def get_member_list_span(case_string):
    ''' Get the beginning and end positions of member cases

    Inputs:
        - case_string (str): the string of the case html file
    Outputs:
        beg, end (int): the beggining and end positions of the member list block
    '''
    match = re.search(re_members_block, case_string, re.I)
    if match:
        return match.span()
    else:
        return None,None

def get_transaction_data(text):
    ''' Get Pacer transaction data from a blob of text (html source code)'''
    match = re.search(re_billable_pages_cost, text)
    return match.groupdict() if match else {}

def rev_search(sub_str, full_str):
    ''' Return index of (start of) the LAST occurence of sub_str in full_str'''
    try:
        return len(full_str) - full_str[::-1].index(sub_str[::-1]) - len(sub_str)
    except:
        return None

def scrub_tags(html_string):
    ''' Remove all html tags from a string of html source string'''
    return re.sub(r"<[\s\S]+?>", '', html_string)

def parse_transaction_history(html_text):
    '''
    Parse the text of the transaction history receipt table

    Inputs:
        - html_text (str): text of the page
    Output:
        - a dict of results, keys from re named groups (timestamp, user, ... etc)
    '''

    # Srub the html tags from the text
    html_text = scrub_tags(html_text)

    re_transaction_history = \
        r"Transaction Receipt (?P<timestamp>[\s\S]+?)\s+"\
        r"Pacer Login:\s*(?P<user>[a-zA-Z0-9]+)[\s\S]+?"\
        r"Description:\s+(?P<description>[\s\S]+?)"\
        r"\s+[a-zA-Z\s]+:\s+(?P<search_criteria>[\s\S]+?)"\
        r"\s+Billable Pages:\s+(?P<billable_pages>\d+)"\
        r"\s+Cost:\s+(?P<cost>\d+\.\d+)"

    match =  re.search(re_transaction_history, html_text, re.I)
    return match.groupdict() if match else {}

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
    elif page == 'doc_link':
        return base_url + 'doc1/'

