import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.append(str(Path.cwd().resolve().parents[1]))
from support import data_tools as dtools
from support import fhandle_tools as ftools

# Patterns
RE_DEF = '^(?P<name>[\s\S]+?)\s* \((?P<ind>\S+)\)$'
CASE_META_KEYS = ('case_id', 'case_name', 'presiding', 'referral', 'date_filed', 'date_terminated', 'date_of_last_filing')


class Pipeline:
    ''' A simple pipeline structure for parsing'''
    def __init__(self, pipes):
        '''
        Inputs:
            - pipes (list): a list of functions that map (data,extracted) -> (data,extracted)
        '''
        self.pipes = pipes

    def process(self, data, extracted):
        for fn in self.pipes:
            data, extracted = fn(data, extracted)

        return data, extracted

def scrub_bad_tags(data, extracted):
    '''
    Remove the tags that are breaking the parsing because of:
        1. Illegal nesting e.g. <B><U>text</B></U>
        2. Unclosed <p> tags
    Inputs:
        - data (str): raw html string from summary page html
        - extracted (dict)
    Outputs:
        (data,extracted) as above
    '''

    pat = '<B><U>|</B></U>|<U><B>|</U></B>|<p>|</p>'
    data = re.sub(pat,'', data, flags=re.I)

    return data, extracted

def extract_header(data, extracted):
    ''' Extract the header
    Inputs:
        - data (bs4 object): the page soup
        - extracted (dict):
    Outputs:
        (data, extracted) as above
    '''
    soup = data
    header = soup.select_one('#cmecfMainContent center')
    header_vals = []

    for el in header.contents:
        try:
            val =  el.text
        except:
            val = el
        val=val.strip()
        if val:
            header_vals.append(val)

    header_data = {
        'case_id': header_vals[0],
        'case_name': header_vals[1]
    }
    pairs_start = 2

    if 'presiding' in header_vals[pairs_start]:
        header_data['presiding'] = header_vals[pairs_start]
        pairs_start += 1

    if 'referr' in header_vals[pairs_start]:
        header_data['referral'] = header_vals[pairs_start]
        pairs_start += 1

    # Add pairs
    for i in range(pairs_start,len(header_vals),2):
        try:
            key_name = "_".join(header_vals[i].rstrip(':').lower().split())
            header_data[key_name] = header_vals[i+1]
        except IndexError:
            print('Something unseen in header metadata')

    extracted.update(header_data)

    return data, extracted

def extract_cell(tag, as_tuple=False):
    '''
    Extract metadata from a table cell tag

    Inputs:
        - tag (bs4 element): a table cell (td), though if extracting a table will be a tr (see below)

    Outputs:
        - (dict/tuple) dict with a single mapping {key:val} if it's a regular cell, or else a dict with
            multiple keys and values if the cell is itself a table with multiple fields/rows. Unless as_tuple
            is True, then a single (key,val) tuple is returned
    '''
    # Recursively deals with cell if it contains a table
    if tag.select('table'):
        table_data = {}
        for tr in tag.select('tr'):
            table_data.update( extract_cell(tr))
        return table_data
    else:

        key, val = tag.text.split(':',1)
        key = '_'.join(key.lower().strip().split())
        val = val.strip().replace('  ',' ')
    return {key:val} if not as_tuple else (key,val)

def grab_parties_table(table):
    '''
    Grab info from tables that have rows that look like the following:

    Plaintiff/Defendant: <PartyName> represented by <LawyerName>      Phone/Fax/email...

    For criminal cases this will be the Plaintiff table (one for each defendant).
    For civil cases this will correspond to the entire party table.

    Inputs:
        - table (bs4 tag): the <table> tag corresponding to the table to parse
    Outputs:
        - parties (dict): the parties info from the table
    '''
    parties = []

    rows = [ch for ch in table.children if ch.name=='tr']
    if not len(rows):
        tbody = table.select_one('tbody')

        # If there is literally nothing between the <table></table> tags
        if not tbody:
            return parties
        else:
            rows = [ch for ch in table.select_one('tbody').children if ch.name=='tr']

    for i, tr in enumerate(rows):
        party = {}

        cells = [ch for ch in tr.children if ch.name=='td']

        # Get plaintiff role and name first
        role_and_name = cells[0]
        role, name = extract_cell(role_and_name, as_tuple=True)
        party['role'] = role
        party['name'] = name

        # If no representation ifno
        if len(cells) < 3:
            party['represented_by'] = None

        else:
            if cells[1].text.strip() == 'represented by':
                party['represented_by'] = cells[2].text.strip()

                contact = cells[3]
                party.update( extract_cell(contact) )

        parties.append(party)

    return parties

def get_civil_parties(data, extracted):
    '''
    Method to extract main data from summary for civiil cases

    Inputs:
        - data (bs4 object): the soup
        - extracted (dict): the case extracted data

    Outputs:
        (data, extracted) as above

    '''
    main_tables_cv = data.select('#cmecfMainContent >  table')

    if len(main_tables_cv) < 2:
        raise ValueError

    case_data = {}

    meta_table = main_tables_cv[0]
    parties_table = main_tables_cv[1]

    for i, tr in enumerate(ch for ch in meta_table.select('tr') if ch.name=='tr'):


        tr_text = tr.text.strip()

        # Skip blank lines
        if not tr_text:
            continue

        else:
            key=None
            for child in tr.children:
                if child.name=='td':

                    # Check if a value present with no key
                    if not child.select('b') and key is not None:
                        # Use key from previous iteration:
                        extracted[key] = child.text
                    else:


                        key,val = extract_cell(child, as_tuple=True)

                        extracted[key] = val


    # PLAINTIFFS
    parties = grab_parties_table(parties_table)
    extracted['parties'] = parties

    return data, extracted

def get_criminal_def_pla(data, extracted):
    '''
    Method to extract main data from summary for criminal cases

    Inputs:
        - data (bs4 object): the soup
        - extracted (dict): the case extracted data

    Outputs:
        (data, extracted) as above

    '''

    extracted['defendants'] = []

    main_tables = data.select('#cmecfMainContent >  table')


    if not (len(main_tables) % 2 == 0):
        raise ValueError('Imbalanced number of plaintiff/defendant tables')

    # Iterate over the tables in pairs (defendant info, list of plaintiffs)
    for def_ord in range(0, len(main_tables), 2):

        defendant_table = main_tables[def_ord]
        plaintiff_table = main_tables[def_ord+1]

        defendant = {'counts': [], 'complaints':[], 'plaintiffs':[], }
        count_instance, cmplt_instance = None, None

        for i, tr in enumerate(defendant_table.select('tr')):

            tr_text = tr.text.strip()

            # Skip blank lines
            if not tr_text:
                continue

            # First row, grab defendant name
            elif i==0:
                def_text = tr_text
                def_match = re.match(RE_DEF, def_text)
                match_dict = def_match.groupdict() if def_match else {}
                defendant['name'] = match_dict.get('name').strip().replace('  ',' ')
                defendant['ind'] = match_dict.get('ind').strip()
                continue

            # New count row
            elif tr_text.startswith('Count:'):
                count_instance = {}
                for td in tr.select('td'):
                    count_instance.update( extract_cell(td) )

            # If previous line was a new count instance, grab the count_text from this line
            elif count_instance:
                count_instance['text'] = tr_text
                defendant['counts'].append(count_instance.copy())
                # Reset count instance
                count_instance = None

            # New count row
            elif tr_text.startswith('Complaint'):
                #Set count to none
                cmplt_instance = {}

                # Skip the first cell (the 'Complaint' cell, not a k:v pair)
                for td in tr.select('td')[1:]:
                    cmplt_instance.update( extract_cell(td) )

            # If previous line was a new count instance, grab the count_text from this line
            elif cmplt_instance:
                cmplt_instance['text'] = tr_text
                defendant['complaints'].append(cmplt_instance.copy())
                # Reset count instance
                cmplt_instance = None

            # Magistrate info is split over multiple tds, so just pass the whole row
            elif tr_text.startswith('Magistrate'):
                defendant.update( extract_cell(tr) )


            # Otherwise it's general data about the defendant's case, grab it
            else:
                for child in tr.children:
                    if child.name=='td':
                        try:
                            defendant.update( extract_cell(child) )
                        except:
                            if child.text.strip().lower().startswith('complaint'):
                                defendant.update({'complaint':None})


        # flag/flags
        if 'flag' in defendant:
            defendant['flags'] =  defendant['flag']
            del defendant['flag']
        defendant['flags'] = (defendant.get('flags') or '').split(',')

        # other court case/cases
        if 'other_court_cases' in defendant:
            defendant['other_court_case'] = defendant['other_court_cases']
            del defendant['other_court_cases']

        # PLAINTIFFS
        plaintiffs = grab_parties_table(plaintiff_table)
        defendant['plaintiffs'] = plaintiffs

        extracted['defendants'].append(defendant)

    return data, extracted

def get_main_data(data, extracted):
    ''' Get the main data from the summary, switches function between civil and criminal main functions'''

    case_type = ftools.decompose_caseno(extracted['case_id'])['case_type']

    if case_type == 'cv':
        data, extracted = get_civil_parties(data,extracted)

    elif case_type == 'cr':
        data, extracted = get_criminal_def_pla(data, extracted)

    else:
        raise ValueError('Only know how to parse cv and cr cases')

    return data, extracted

def ensure_keys(data, extracted):
    ''' Guarantee key existence for all fields case meta keys, even if they weren't found'''

    for k in CASE_META_KEYS:
        extracted[k] = extracted.get(k,'')

    return data, extracted

def get_summary_transaction_data(data, extracted):
    ''' Get the transaction data for the summary'''
    transaction_data = ftools.parse_transaction_history(str(data))
    extracted['billable_pages'] = int(transaction_data['billable_pages']) if 'billable_pages' in transaction_data.keys() else None
    extracted['cost'] = float(transaction_data['cost']) if 'cost' in transaction_data.keys() else None
    extracted['download_timestamp'] = transaction_data.get('timestamp','')

    return data, extracted

# This is the complete summary pipeline
# Use the inherited Pipeline.process method to process data
SummaryPipeline = Pipeline([
    scrub_bad_tags,
    lambda d,e: (BeautifulSoup(d,'html.parser'), e) ,
    extract_header,
    get_main_data,
    ensure_keys,
    get_summary_transaction_data
])