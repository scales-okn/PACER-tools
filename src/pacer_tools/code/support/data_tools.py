# Standard path imports
import re
import sys
import json
import string
import functools
import asyncio
import numpy as np
import pandas as pd
from pathlib import Path
from itertools import chain, groupby
from datetime import datetime
from tqdm.autonotebook import tqdm

# Non-standard imports
import usaddress
import zlib
import base64
from hashlib import blake2b

# SCALES modules
sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import docket_entry_identification as dei
from support import judge_functions as jf
from support import bundler as bundler
from support.core import std_path
from support import fhandle_tools as ftools
from support import lexicon

tqdm.pandas()



def sign(cookie):
    h = blake2b(digest_size=12, key=b'noacri-loc-generica')
    h.update(cookie)
    return h.hexdigest().encode('utf-8')

def make_span(match, span_offset):
    span_orig = match.span()
    return {'start': span_orig[0]+span_offset, 'end': span_orig[1]+span_offset}

def find_matching_lines(docket, phrases, exclusion_phrases=[], make_lowercase=True, beginning_only=False):
    line_ids = []
    for i, entry in enumerate(docket):
        text = entry['docket_text'].lower() if make_lowercase else entry['docket_text']
        if beginning_only:
            if not any((x == text[:len(x)] for x in exclusion_phrases)) and any((x == text[:len(x)] for x in phrases)):
                line_ids.append(i)
        else:
            try:
                if not any((x in text for x in exclusion_phrases)) and any((x in text for x in phrases)):
                    line_ids.append(i)
            except (AttributeError, TypeError, KeyError): # retained from Adam's code
                pass
    return line_ids

def has_edges(docket):
    for entry in docket:
        if 'edges' in entry and bool(entry['edges']):
            return True
    return False

def get_edges_to_target(target_ind, edges):
    matches = []
    if edges:
        for edge in edges:
            if bool(edge) and edge[1] == target_ind:
                matches.append(edge)
    return matches

def remove_sensitive_info(docket):
    '''
    Take a string (e.g. the raw text of a docket html or json) and remove social security numbers and A-numbers
    '''
    ssn_re = re.compile(r"(?i)([^0-9])((?:ssn?:?)? *#?\d{3}-\d{2}-\d{4})([^0-9])")
    a_number_re = re.compile(r"(?i)([^0-9a-z])(a\d{8,9})([^0-9])")
    redact_text = lambda match: match.group(1) + ' '*len(match.group(2)) + match.group(3)

    docket = re.sub(ssn_re, redact_text, docket)
    docket = re.sub(a_number_re, redact_text, docket)
    return docket


def create_docket_core(case_id):
    '''
    Take a case id 1:15-cr-23422 or 1-15-cr-23422 and turn it into a docket core
    that can be used with the fjc data
    '''
    if ':' in case_id:
        case_parts = case_id.split(':')[1].split('-')
        dcore = case_parts[0] + case_parts[2]
    else:
        id_parts = case_id.split('-')
        dcore = id_parts[1] + id_parts[3]
    return dcore

def nos_number_extractor(nos_text):
    '''
    Given the Nature of Suit text, attempts to return the number
    '''
    import numpy as np
    try:
        first_elem = int(nos_text.split(' ')[0].strip(':'))
        return first_elem
    except:
        return np.nan

def transform_date_slash_to_dash(slashdate):
    '''
    Take a m/y/d and transform to y-m-d
    '''
    try:
        m, d, y = slashdate.split('/')
        return '-'.join([y, m, d])
    except AttributeError:
        return slashdate

def load_recap_dump(indir):
    '''
    Loads a recap dump and transforms it into a dataframe
    input: indir, json folder
    output: dataframe
    '''
    import glob, json, sys
    import pandas as pd
    sys.path.append('..')
    import support.fhandle_tools as ftools

    data = {'filedate':[], 'court':[], 'nos':[], 'recap_case_id':[], 'id':[], 'termdate':[],
            'docket_core':[], 'case_name':[]}
    for jfhandle in glob.glob(indir + '*json'):
        jfile = json.load( open(jfhandle) )
        #IDs
        data['recap_case_id'].append( jfile['docket_number'])
        data['id'].append( jfile['id'] )
        data['docket_core'].append( jfile['docket_number_core'] )
        data['case_name'].append( jfile['case_name'] )
        #Case data
        data['court'].append( jfile['court'] )
        data['nos'].append( jfile['nature_of_suit'] )
        #Dates
        data['filedate'].append( jfile['date_filed'] )
        data['termdate'].append( jfile['date_terminated'] )
    #Convert to the dataframe
    df = pd.DataFrame(data)
    df['case_id'] = df.recap_case_id.apply( ftools.clean_case_id )
    return df

def load_noacri_data(indir):
    '''
    Loads a downloaded json data folder into a dataframe
    input: indir, json folder
    '''
    import glob, json
    import pandas as pd

    noacri_data = {'noacri_case_id':[], 'court':[], 'nos':[], 'filedate':[], 'termdate':[]}
    for jfhandle in glob.glob(indir + '/*json'):
        tfile = json.load( open(jfhandle) )
        #Pull in the data
        noacri_data['noacri_case_id'].append(tfile['case_id'])
        #Get the download court
        if 'download_court' in tfile:
            dlcourt = tfile['download_court']
        else:
            dlcourt = court_trans[jfhandle.split('/json/')[0].split('/')[-1]]
        noacri_data['court'].append(dlcourt)
        #Nature of suit, filedate
        noacri_data['nos'].append(tfile['nature_suit'])
        noacri_data['filedate'].append( transform_date_slash_to_dash(tfile['filing_date']) )
        noacri_data['termdate'].append( transform_date_slash_to_dash(tfile['terminating_date']) )
    #Load it into a dataframe
    noacri_df = pd.DataFrame(noacri_data)
    #Now get the original case id and docket core
    noacri_df['case_id'] = noacri_df.noacri_case_id.apply(create_case_id_colon)
    noacri_df['docket_core'] = noacri_df.noacri_case_id.apply(create_docket_core)
    #Get the nos number
    noacri_df['nos_num'] = noacri_df.nos.apply(nos_number_extractor)
    return noacri_df

def load_query_file(query_file, main_case=True):
    '''
    Loads and cleans a query search result
    input: query_file, html search result,
           main_cases - T/F, restrict to main cases or not
    output: query_df, query dataframe with clean
    '''
    import pandas as pd
    import support.fhandle_tools as ftools

    #Load the dataframe, get rows, and drop the NA dead rows
    dfset = pd.read_html(query_file)
    df = dfset[0]
    df.columns = ['query_case_id', 'name', 'details']
    df.dropna(inplace=True)
    #Pull the state court
    court_abbrev = query_file.split('/')[-1].split('_')[0]
    #Clean up the case ids
    df['case_id'] = df.query_case_id.apply(ftools.clean_case_id)
    df['main_case'] = df.query_case_id.apply(lambda x: ftools.main_limiter(x) )
    df['case_type'] = df.query_case_id.apply(lambda x: x.split('-')[1])
    #Get the other attributes
    df['court'] = df.query_case_id.apply(lambda x: court_abbrev)
    # df['filedate'] = df.details.apply(ftools.extract_file_date)
    # df['termdate'] = df.details.apply(ftools.extract_term_date)
    #Restrict to the primary case only
    if main_case==True:
        maindf = df[df.main_case==True]
    else:
        maindf = df
    return maindf



def line_detagger(string, prettify=False):
    '''
    Removes HTML tags from text (for use in parse_pacer.py)
    input: string (text to clean), prettify (boolean specifying whether to insert spaces in place of dropped tags)
    '''
    delim = ' ' if prettify else ''
    if string != None:
        while string.count('***') > 1: # sometimes Pacer does, e.g., "***DO NOT FILE IN THIS CASE***"
            string = string.split('***')[0] + '***'.join(string.split('***')[2:])
        return re.sub('\<[^<]+?>', delim, string).strip('<>?! ') or None
    else:
        return None

def line_cleaner(string):
    '''
    Cleans up messy text (for use in parse_pacer.py)
    input: string (text to clean)
    '''
    if string != None:
        string = string.replace('&amp;','&').replace('&nbsp;',' ')
        string = string.replace("\\'", "'").replace('\\n','\n').replace('\\t','\t')
        string = string.lstrip(')').rstrip('(')
        string = re.sub(' +', ' ', string)
        string = string.strip()
        return string or None
    else:
        return None

def lawyer_line_cleaner(string):
    if string != None:
        return string.split('<!--')[0].strip('/<> ')
    else:
        return None

def replace_br_with_newlines(string):
    if string != None:
        return re.sub('<br>|<br/>|<br />', '\n', string)
    else:
        return None

def clean_raw_info_for_lawyer_parse(raw_info):
    return [x for x in [lawyer_line_cleaner(y) for y in raw_info.split('<i>')[0].split('<br')] if x and 'PRO SE' not in x]

def separate_terminating_date(ei_raw):
    terminating_date = None
    delim = 'TERMINATED: '
    # take whichever terminating date is encountered last in the block (most likely use case: multiple AKAs w/ same term date for each)
    while ei_raw and delim in ei_raw:
        terminating_date = ei_raw.split(delim,1)[1]
        ei_raw = ei_raw.split(delim,1)[0]
        if '\n' in terminating_date: # is there any extra info besides the terminating date?
            ei_raw += terminating_date.split('\n',1)[1]
            terminating_date = terminating_date.split('\n')[0]
    return terminating_date, ei_raw

def is_address(line):
    address_parse = usaddress.parse(line)
    if address_parse and address_parse[0][1]!='Recipient': # 'Recipient' seems to be the default type in usaddress
        false_positive = ('IntersectionSeparator' in [x[1] for x in address_parse]) or (' ' not in line)
        return (not false_positive)
    return False

def parse_lawyer_extra_info(lines):
    '''
    Helper method for process_entity_and_lawyers() in parse_pacer.py
    Inputs:
        - lines (list): the contents of the text block below a lawyer name in Pacer, split by line break
    Outputs:
        - office (str), address (list), phone (str), fax (str), email (str): all the possible components of the lawyer block, separated out
    '''

    office, address, phone, fax, email = [None]*5

    # parse phone/fax/email (there should only be one of each, but just in case, walk backwards so as to choose the earliest one)
    phone_ind, fax_ind, email_ind = None, None, None
    for i,line in zip(range(len(lines)-1,-1,-1), lines[::-1]):
        phone_match = re.match('(?:(?:1[\-\/\. ]?)?\(?[2-9]\d{2}\)?)?[\-\/\. ]?[2-9]\d{2}[\-\/\. ]?\d{4}(?=$|[^\d])', line)
        if phone_match:
            # make sure this phone-number match isn't an inmate number
            if i==0 or not any((' ' in x for x in lines[:i])): # take a closer look at these suspicious matches
                if i+1<len(lines) and not any((is_address(x) for x in lines[i+1:])): # PACER-field-ordering rule
                    phone, phone_ind = phone_match.group(), i
            else:
                phone, phone_ind = phone_match.group(), i
        if 'Fax: ' in line:
            fax, fax_ind = line.split('Fax: ')[1].lower().split(' fax')[0], i
        if 'Email: ' in line:
            email, email_ind = line.split('Email: ')[1], i

    # parse address and office
    address_end = min([x if x is not None else len(lines) for x in (phone_ind,fax_ind,email_ind)])
    address_start = None
    for i,line in enumerate(lines[:address_end]):
        if is_address(line):
            address_start = i
            break
    if address_start is not None:
        address = '\n'.join(lines[address_start:address_end])
        office = '\n'.join(lines[:address_start])
    else:
        office = '\n'.join(lines[:address_end])

    # clean office
    null_words = ('above for address', 'undeliverable', 'expired', 'unknown')
    if any((x in office.lower() for x in null_words)):
        office = '\n'.join(list(filter(lambda line: not any((x in line.lower() for x in null_words)), office.split('\n'))))
    if not office or office == '.':
        office = None

    return office, address, phone, fax, email

def parse_party_extra_info(chunk, from_recap=False):
    '''
    Helper method for process_entity_and_lawyers() in parse_pacer.py
    Inputs:
        - chunk (str): a chunk of HTML text (split_chunk[0] when called from process_entity_and_lawyers())
        - from_recap (bool): whether or not the method is being called from the Recap remapper
    Outputs:
        - extra_info (str): any info contained in the text block below the party name in Pacer
        - terminating_date (str): a parsed-out date when the text block specifies the 'TERMINATED' date
        - no_italics_in_ei (bool): for Pacer, specifies whether this extra info might actually be pro-se info (seems to happen in flsd, e.g. 9:16-cv-80411)
    '''

    # do some basic cleaning
    ei_raw = chunk or None if chunk.split('</td>')[0] or from_recap else None
    if ei_raw:
        ei_raw = re.sub('^(?:<\/b>|<br ?\/?>|\n| )+|(?:<\/?(?:tr|td|hr|table|tbody|br ?\/?)[^>]*>|\n|\t)+$', '', ei_raw)

    # parse terminating date
    terminating_date = separate_terminating_date(ei_raw)[0]

    # parse phone and email
    office_name, address, phone, fax, email = [None]*5
    if ei_raw:

        phone = re.search('(?:(?:1[\-\/\. ]?)?\(?[2-9]\d{2}\)?)?[\-\/\. ]?[2-9]\d{2}[\-\/\. ]?\d{4}', ei_raw)
        phone = phone.group() if phone else None
        email = re.search('(?i)\w+([\.\-\w]\w)*@\w+([\w\-]*\w)*(\.[\w\-]*\w)*\.\w\w(\w)*', ei_raw)
        email = email.group() if email else None

        # parse address and office
        address_re = ''.join([ f"(?i)(?:, (?:{'|'.join(lexicon.us_state_names)})|",
        f"(?<!seller)(?<!et) (?:{'|'.join([x for x in lexicon.us_state_abbrevs if x!='as'])})|, as)(?: \d|\n|$)" ])

        address, first_address_line = '', None
        if re.search(address_re, ei_raw):
            for line in ei_raw.split('\n'):
                if line and is_address(line):
                    address += line+'\n'
                    if not first_address_line:
                        first_address_line = line
            office_name = '\n'.join([x for x in ei_raw.split(first_address_line)[0].split('\n') if any((y in x for y in string.ascii_letters))])

    extra_info_dict = {'office_name':lawyer_line_cleaner(line_cleaner(line_detagger(office_name))) or None,
            'address': lawyer_line_cleaner(line_cleaner(line_detagger(address))) or None,
            'phone': phone, 'fax': fax, 'email': email,
            'terminating_date': line_cleaner(line_detagger(terminating_date)),
            'raw_info': ei_raw}

    return {} if not any(extra_info_dict.values()) else extra_info_dict

def extra_info_cleaner(raw_info, replace_breaks_with_newlines=True, remove_tags=True, remove_terminating_dates=True,
                        do_misc_tidying=True, discard_when_nonitalic=True):
    '''
    Cleans up the text blocks found beneath party/lawyer names on Pacer (for use in NER, classification, etc)
    Inputs:
        - raw_info (str): the raw text of an entity info block, as captured in parse_pacer.py
        - replace_breaks_with_newlines (bool): whether to replace all <br> tags with newlines
        - remove_tags (bool): whether to remove all HTML tags
        - remove_terminating_dates (bool): whether to remove lines of the form "TERMINATED: m/d/y", which are already captured in the parse
        - do_misc_tidying (bool): whether to generally prettify the return text and remove extraneous bits
        - discard_when_nonitalic (bool): whether to return None if no <i> tags appear, which suggests that the block only contains address info
    Outputs:
        - clean_info (str): the clean version of the block, after applying whichever operations have been called for via the flags
    '''

    clean_info = raw_info

    if clean_info and replace_breaks_with_newlines:
        clean_info = re.sub('<br>|<br/>|<br />', '\n', clean_info)

    if remove_tags:
        clean_info = line_detagger(clean_info)

    if remove_terminating_dates:
        clean_info = separate_terminating_date(clean_info)[1]

    if do_misc_tidying:
        clean_info = line_cleaner(clean_info)
        if not clean_info or clean_info == 'and':
            return None
        clean_info = '\n'.join( list(filter(None, [line_cleaner(x) for x in clean_info.split('\n')] ))) # drop empty lines
        clean_info = clean_info.strip('\n/<> ')

    if clean_info and discard_when_nonitalic:
        italics_test = ''.join(raw_info.split('<i>TERMINATED')) if '<i>TERMINATED' in raw_info else raw_info
        if clean_info and '<i>' not in italics_test:
            return None

    return clean_info or None

def parse_stamp_json(html_text):
    ''' Parse the stamp (from stamp_json) of JSON serialized text'''

    # Find the last line, by searching backwards
    for rev_ind, char in enumerate(reversed(html_text)):
        if char=='\n':
            break
    last_line = html_text[-rev_ind:]


    # Parse the json string
    json_str = last_line.lstrip('<!-').rstrip('->')
    stamp_data = json.loads(json_str)

    return stamp_data

def parse_stamp(html_text, llim=-400):
    '''
    Parse the stamp left by the scraper at the bottom of a html file

    Inputs:
        - html_text (str): the html text for the page
        - llim (int): left limit of where to search in the html
    Output:
        dict with whatever k-v pairs are present (user, time, download_ulr...)
    '''
    try:
        # Try to check for json stamp first
        return parse_stamp_json(html_text)
    except:
        stamp_data = {}
        re_data_str = r"<!-- SCALESDOWNLOAD;([\s\S]+) -->"

        match = re.search(re_data_str,html_text[-llim:])
        if match:
            data_str = match.groups()[0]
            for pair in data_str.split(';'):
                k,v = re.split(':', pair, maxsplit=1)
                stamp_data[k] = v
        return stamp_data



def standardize_recap_date(tdate):
    '''y-m-d to m/d/y'''
    if not tdate:
        return None
    try:
        y,m,d = tdate.split('-')
        return '/'.join([m, d, y])
    except AttributeError:
        return None

def get_recap_parties(rparties):
    with open(settings.ROLE_MAPPINGS, 'r') as f:
        mappings = json.load(f)
    parties = []

    # determine whether any lawyer role info has been erroneously copied between occurrences in the header (a known Recap issue)
    all_lawyer_names = []
    potentially_broken_names = []
    for lawyer in [x for rparty in rparties for x in rparty['attorneys']]:
        if lawyer['name'] in all_lawyer_names:
            roles_raw = [x['role_raw'] for x in lawyer['roles']]
            if len(set(roles_raw))>1: # if there's just one possible value for the text field, then it doesn't matter if it was copied
                potentially_broken_names.append(lawyer['name']) # otherwise, flag this lawyer
        else:
            all_lawyer_names.append(lawyer['name'])

    for rparty in rparties:
        # determine whether parties with the same name have been erroneously copied from other cases (a known Recap issue)
        party_types = rparty['party_types']
        entity_info = parse_party_extra_info(rparty['extra_info'], from_recap=True)
        ei, td = (entity_info['raw_info'], entity_info['terminating_date']) if entity_info else (None, None)
        recap_party_error = False
        if party_types and len(party_types)>1:
            potentially_broken_keys = ['extra_info', 'date_terminated', 'name', 'criminal_counts', 'criminal_complaints',
                                        'highest_offense_level_opening', 'highest_offense_level_terminated']
            potentially_broken_dict = {'extra_info':ei, 'date_terminated':td}
            potentially_broken_dict.update({k:party_types[0][k] for k in potentially_broken_keys[2:]})
            for pt in party_types:
                for k in potentially_broken_keys:
                    if pt[k] != potentially_broken_dict[k]:
                        recap_party_error = True
                if recap_party_error:
                    break

        # make the preliminary party dict
        new_party = {
            'name': rparty['name'] or None,
            'entity_info': entity_info if not recap_party_error else {}
        }

        # basic counsel fields
        lawyer_list = []
        for lawyer in rparty['attorneys']:
            lawyer_name = lawyer['name'] or None
            contact_raw = lawyer['contact_raw'] or None
            is_pro_se = 'PRO SE' in str(lawyer)
            recap_counsel_error = True if lawyer_name in potentially_broken_names else False

            # lawyer-block fields
            if contact_raw:
                info_lines = [x for x in [lawyer_line_cleaner(y) for y in contact_raw.split('\n')] if x and 'PRO SE' not in x]
                office, address, phone, fax, email = parse_lawyer_extra_info(info_lines)
                if office and is_pro_se and lawyer_name==rparty['name']:
                    extra_pro_se_info = office
                    office = None
            else:
                office, address, phone, fax, email = [None]*5

            designation, bar_status, trial_bar_status, tdate = [None]*4
            is_lead, is_pro_hac, is_notice, see_above = [False]*4
            # trial bar status appears in the contact info, so it's not subject to the counsel errors (although it only appears in ILND)
            if contact_raw and any(x in contact_raw for x in ['Trial Bar Status', 'Trial bar Status']):
                trial_bar_status = re.search('tatus: ([A-Za-z \'\-]{1,100})', contact_raw).group(1)

            if not recap_counsel_error:
                full_lawyer_string = str(lawyer)
                # easy non-lawyer-block fields
                is_lead = 'LEAD ATTORNEY' in full_lawyer_string
                is_notice = 'ATTORNEY TO BE NOTICED' in full_lawyer_string
                is_pro_hac = 'PRO HAC VICE' in full_lawyer_string
                see_above = 'above for address' in full_lawyer_string

                # slightly trickier fields (pretty sure designation & bar status don't show up in recap, but they're included just in case)
                if 'Designation' in full_lawyer_string:
                    designation = re.search('Designation: ([A-Za-z \'\-]{1,100})', full_lawyer_string).group(1)
                elif 'Bar Status' in full_lawyer_string:
                    bar_status = re.search('tatus: ([A-Za-z \'\-]{1,100})', full_lawyer_string).group(1)
                # there is almost always only one terminating date (sans counsel errors), so just take the first one we encounter
                for role in lawyer['roles']:
                    if 'TERMINATED' in role['role_raw']:
                        tdate = role['role_raw'].split('TERMINATED: ')[1]

            # append this particular lawyer's info
            lawyer_list.append({
                'name': lawyer_name,
                'entity_info': {
                    'office_name': office,
                    'address': address,
                    'phone': phone,
                    'fax': fax,
                    'email': email,
                    'terminating_date': tdate,
                    'is_pro_se': is_pro_se,
                    'raw_info': contact_raw
                },
                'is_lead_attorney': is_lead,
                'is_pro_hac_vice': is_pro_hac,
                'is_notice_attorney': is_notice,
                'has_see_above': see_above,
                'designation': designation,
                'bar_status': bar_status,
                'trial_bar_status': trial_bar_status
            })

        # role & party_type
        if not recap_party_error:
            rtitle = party_types[0]['name']
            if rtitle not in mappings.keys():
                party_title = rtitle
                party_type = 'misc'
            else:
                party_title = mappings[rtitle]['title']
                party_type = mappings[rtitle]['type']
        else:
            party_title, party_type = [None]*2

        # finish adding party fields
        new_party['counsel'] = lawyer_list
        new_party['role'] = party_title
        new_party['party_type'] = party_type
        new_party['pacer_id'] = None
        parties.append(new_party)

    return parties

def get_recap_docket(court, docket_entries):
    '''
    Remap the recap docket
    Inputs:
        - court (str): the court abbreviation
        - docket_entries (list): the value from the 'docket_entries' key in recap
    Output:
        - list of docket entries same as parsed format
    '''

    def _get_doc_links(row):
        ''' Get links to documents (most rows don't have attachments, some do)'''
        documents = {}
        for doc in row.get('recap_documents', []):

            # Recap encodes document_type=1 for line doc and document_type=2 for attachment
            if doc.get('document_type') == 1:
                ind = 0
            elif doc.get('document_type') == 2 and doc.get('attachment_number', False):
                ind = int(doc['attachment_number'])
            else:
                # Fallback option, use doc_id
                ind = f"_{doc['pacer_doc_id']}"

            document_data = {
                'url': ftools.get_pacer_url(court,'doc_link') + '/' + str(doc['pacer_doc_id']), 'span': {},
                **{f"recap_{k}": doc[k] for k in ('page_count','filepath_ia', 'filepath_local', 'description', 'is_available')}
            }
            documents[ind] = document_data
        return documents

    rows = [
        {'date_filed': standardize_recap_date(row['date_filed']),
         'ind': str(row['entry_number'] or ''),
         'docket_text': row['description'],
         'documents': _get_doc_links(row),
         'edges': []
        }
        for row in docket_entries
    ]
    return rows

def remap_recap_data(recap_fpath=None, rjdata=None):
    '''
    Converts a Recap-style case JSON to a SCALES-style case JSON
    Input: a path to a Recap file (recap_fpath), or the JSON object itself (rjdata)
    output: the same data reformatted to fit the SCALES parse schema
    '''

    # Load the data
    try:
        if not rjdata:
            recap_fpath = std_path(recap_fpath)
            jpath = settings.PROJECT_ROOT / recap_fpath
            rjdata = json.load(open(jpath))
    except:
        print(f"Error loading file {recap_fpath}")
        return {}

    # Get some general fields
    tdate = standardize_recap_date(rjdata['date_terminated'])
    case_status = 'closed' if tdate else 'open'
    judge = rjdata['assigned_to_str'] if rjdata['assigned_to_str'] and rjdata['assigned_to_str']!='Unassigned' else None

    case_type = rjdata['docket_number'].split('-')[1] if rjdata['docket_number'] else None
    if case_type == 'cv':
        # Convert the data
        fdata = {
            'appeals_case_ids': [],
            'case_flags': [],
            'case_id': ftools.clean_case_id(rjdata['docket_number']),
            'case_name': rjdata['case_name'] or None,
            'case_pacer_id': None,
            'case_status': case_status,
            'case_type': case_type,
            'cause': rjdata['cause'] or None,
            'city': None,
            'court': rjdata['court'] or None,
            'docket': get_recap_docket(rjdata['court'], rjdata['docket_entries']),
            'filed_in_error_text': None,
            'filing_date': standardize_recap_date(rjdata['date_filed']),
            'header_case_id': None,
            'judge': judge, # n.b.: might contain junk on the end (e.g. 'Referred')
            'jurisdiction': rjdata['jurisdiction_type'] or None,
            'jury_demand': rjdata['jury_demand'] or None,
            'lead_case_pacer_id': None,
            'lead_case_id': None,
            'magistrate_case_ids': [],
            'monetary_demand': None,
            'nature_suit': dei.nos_matcher(rjdata['nature_of_suit'], short_hand=True) or rjdata['nature_of_suit'],
            'other_courts': [],
            'parties': get_recap_parties(rjdata['parties']),
            'recap_id': rjdata['id'] or None,
            'referred_judges': [rjdata['referred_to_str']] if rjdata['referred_to_str'] else [],
            'related_cases': [],
            'terminating_date': tdate,
            'source': 'recap',
            'summary': {},
            'ucid': ucid(rjdata['court'], ftools.clean_case_id(rjdata['docket_number'])),
            # MDL/Multi keys
            **{k:None for k in ['mdl_code', 'mdl_id_source', 'is_mdl', 'is_multi']},
            # Billing keys
            **{k:None for k in ['billable_pages', 'cost', 'n_docket_reports']},
            # Scraper things
            **{k:None for k in ['download_timestamp', 'download_url', 'docket_available', 'member_case_key', 'is_stub']}
        }
        return fdata
    else:
        return None # throw out criminal cases as well as mj, mc, and the miscellaneous other low-frequency types



def year_check(dstring, year_pull):
    '''
    Checks to see if the file year is the same as year_pull specified
    '''
    #Do the year check
    if year_pull != None:
        try:
            year_diff = int(dstring.split('/')[-1]) - year_pull
        except:
            year_diff =  -9999
    else:
        year_diff = 0
    return year_diff

def get_uft_year(filing_date):
    return filing_date.split('/')[-1]

def generate_unique_filepaths(outfile=None, nrows=None):
    '''
    Create a list of unique filepaths for all case json in the PACER folder and export to .csv
    Inputs:
        - outfile (str or Path) - the output file name (.csv) relative to the project root if none doesn't output
        - nrows (int) - no. of cases to use (for testing)
    Outputs:
        DataFrame of file metadata (also output to outfile if output=True)
    '''
    import pandas as pd
    tqdm.pandas()

    case_jsons = [court_dir.glob('json/*/*.json') for court_dir in settings.PACER_PATH.glob('*')
                    if court_dir.is_dir()]

    file_iter = chain(*case_jsons)

    df = convert_filepaths_list(file_iter=file_iter, nrows=nrows)

    #Write the file
    if outfile:
        df.to_csv(std_path(outfile))

    return df

def unique_filepaths_updater(table_file, update_iter, outfile=None, force=False):
    '''
    A method to update a unique filepaths file

    Inputs:
        - table_file (str or Path): the .csv file with the table to be updated
        - update_iter (iterable): iterable over the new filepaths to check (of json files)
        - outfile (str or Path): path to output the new file to
        - force (bool): if true, will recalculate and overwrite rows for all cases in update_iter

    Output:
        DataFrame of file metadata (also output to outfile if output=True)
    '''
    def _clean_fpath_(x):
        p = std_path(x)
        if settings.PROJECT_ROOT.name in p.parts:
            return str(p.relative_to(settings.PROJECT_ROOT))
        else:
            return str(p)

    # Load the original table (but not using load_unique_files_df loader, as we want to work with dates as strings)
    df = pd.read_csv(table_file, index_col=0)

    # Get the fpaths of the ones to be added to the table
    to_update = [_clean_fpath_(x) for x in update_iter]
    if not force:
        # Filter down to ones that aren't already in df
        new_fpaths_series = pd.Series(to_update)
        to_update = new_fpaths_series[~new_fpaths_series.isin(df.fpath)].to_list()

    if to_update:
        new_df = convert_filepaths_list(file_iter=to_update)

        # Concatenate to make table for output
        out_df = pd.concat((df, new_df))

        # Drop the original lines if force is true
        if force:
            out_df = out_df[~out_df.index.duplicated(keep='last')]

    else:
        out_df = df

    if outfile:
        out_df.to_csv(outfile)

    return out_df




def convert_filepaths_list(infile=None, outfile=None, file_iter=None, nrows=None):
    '''
    Convert the list of unique filepaths into a DataFrame with metadata and exports to csv

    Inputs:
        - infile (str or Path) - the input file, relative to the project root, expects csv with an 'fpath' column
        - outfile (str or Path) - the output file name (.csv) relative to the project root, if None doesn't write to file
        - file_iter (iterable) - list of filepaths, bypasses infile and reads list directly
        - nrows (int) - number of rows, if none then all
    Outputs:
        DataFrame of file metadata (also output to outfile if output=True)
    '''

    # Map of keys to functions that extract their values (avoids keeping separate list of keys/property names)
    #c: case json, f: filepath
    dmap = {
        'court': lambda c,f: c['download_court'] if is_recap(f) else Path(f).parents[2].name,
        'year': lambda c,f: get_uft_year(c['filing_date']),
        'filing_date': lambda c,f: c['filing_date'],
        'terminating_date': lambda c,f: c.get('terminating_date'),
        'case_id': lambda c,f: ftools.clean_case_id(c['case_id']),
        'case_type': lambda c,f: c['case_type'],
        'nature_suit': lambda c,f: dei.nos_matcher(c['nature_suit'], short_hand=True) or '',
        'judge': lambda c,f: jf.clean_name(c.get('judge')),
        # 'recap': lambda c,f: 'recap' in c['source'],
        'is_multi': lambda c,f: c['is_multi'],
        'is_mdl': lambda c,f: c['is_mdl'],
        'mdl_code': lambda c,f: c['mdl_code'],
        # 'has_html': lambda c,f: 'pacer' in c['source'],
        # 'source': lambda c,f: c['source'],
        'is_stub': lambda c,f: c['is_stub'] if 'is_stub' in c else c['stub'] # 'stub' won't be needed after parser v3.5
    }

    properties = list(dmap.keys())

    def get_properties(fpath):
        ''' Get the year, court and type for the case'''
        try:
            case = load_case(fpath, skip_scrubbing=True)
        except:
            print(f'LOAD_ERROR: error loading case {fpath}')
            return 'LOAD_ERROR'
        try:
            return tuple(dmap[key](case,fpath) for key in properties)
        except:
            print(f'DMAP_ERROR: Error with dmap for {fpath}')
            return tuple(
                (case['court'] if i==0 else ftools.clean_case_id(case['case_id']) if i==4 else None)
            for i,k in enumerate(properties))

    # Load fpaths from list or else from infile
    if file_iter is not None:
        if nrows:
            paths = [next(file_iter) for _ in range(nrows)]
        else:
            paths = file_iter
        # Build dataframe of paths relative to PROJECT_ROOT
        df = pd.DataFrame(paths, columns=['fpath'])
    elif infile:
        # Read in text file of filepath names
        df = pd.read_csv(std_path(infile), nrows=nrows)[['fpath']]
    else:
        raise ValueError("Must provide either 'infile' or 'file_list'")

    # Convert filepath to relative format
    def _clean_fpath_(x):
        p = std_path(x)
        if settings.PROJECT_ROOT.name in p.parts:
            return str(p.relative_to(settings.PROJECT_ROOT))
        else:
            return str(p)

    df.fpath = df.fpath.apply(lambda x: _clean_fpath_(x))
    # Build year and court cols

    # Only do progress bar if it's more than 1
    if len(df) > 1:
        print('\nExtracting case properties...')
        properties_vector = df.fpath.progress_map(get_properties)
    else:
        properties_vector = df.fpath.map(get_properties)

    # Filter out load_error
    keep = ~properties_vector.eq('LOAD_ERROR')
    df = df[keep]
    properties_vector = properties_vector[keep]

    prop_cols = zip(*properties_vector)

    # Insert new columns, taking names from ordering of properties
    for i, new_col in enumerate(prop_cols):
        df[properties[i]] = new_col

    # Set UCID index
    df['ucid'] = ucid(df.court, df.case_id)#, series=True) #Not sure why this was here
    df = df.set_index('ucid')

    # Judge matching
#     jmap = jf.unique_mapping(df.judge.unique())
#     df.judge = df.judge.map(jmap)

    columns = properties.copy()
    columns.insert(2,'fpath')

    if outfile:
        df[columns].to_csv(std_path(outfile))
    return df[columns]

def load_docket_filepaths(court_pull = 'all', year_pull = None):
    '''
    input:
        * court - the court to load data for, "all" does everything, otherwise it's the abbreviation
        * year_pull (integer)- the year to pull data for
    output:
        * list of filepaths
    '''
    import pandas as pd

    df = pd.read_csv(settings.UNIQUE_FILES_CSV)
    if court_pull != 'all':
        df = df[df.court==court_pull].copy()
    if year_pull:
        year_pull = int(year_pull)
        df = df[df.year==year_pull].copy()

    return df.fpath.values

def load_dockets(court_pull = 'all', year_pull = None):
    '''
    input:
        * court - the court to load data for, "all" does everything, otherwise it's the abbreviation
        * year_pull (integer)- the year to pull data for
    output:
        * dockets - list of jsons for case files
    '''
    import json, sys

    paths = load_docket_filepaths_unique(court_pull, year_pull)
    dockets = []
    for fpath in paths:
        jdata = json.load(open(settings.PROJECT_ROOT + fpath, encoding="utf-8"))
        dockets.append(jdata)
    return dockets

def load_unique_files_df(file=settings.UNIQUE_FILES_TABLE, fill_cr=False, **kwargs):
    '''
        Load the unique files dataframe

        Inputs:
            - xarelto (bool): whether to include xarelto cases
            - fill_cr (bool): whether to fill nature_suit for all criminal cases to criminal
    '''

    dff = pd.read_csv(file, index_col=0, **kwargs)

    for col in ['filing_date','terminating_date']:
        dff[col] = pd.to_datetime(dff[col], format="%m/%d/%Y")

    # Set nature of suit for all criminal cases to 'criminal'
    if fill_cr and ('nature_suit' in dff.columns) :
        cr_idx = dff[dff.case_type.eq('cr')].index
        dff.loc[cr_idx, 'nature_suit'] = 'criminal'

    return dff

def get_case_counts(gb_cols=[], qstring=None):
    '''
    Returns a dataframe of the counts of cases grouped by year, court and any additional columns
    inputs:
        - cols (list) - a list of strings of column names from unique filepaths table to group by
    '''
    import pandas as pd

    df = load_unique_files_df().query(qstring) if qstring else load_unique_files_df()
    return df.groupby(['year', 'court', *gb_cols], dropna=False).size().reset_index(name='case_count')

def load_case(fpath=None, html=False, recap_orig=False, ucid=None, skip_scrubbing=False, mongo_db=None):
    '''
    Loads the case given its filepath

    input:
        fpath (str or Path): a path relative to the project roots
        html (bool): whether to return the html (only works for Pacer, not Recap)
        recap_orig (bool): whether to return the original recap file, rather than the mapped
        ucid (str): the ucid of the case to load
        mongo_db (pymongo.database.Database): a pymongo database instance, if provided
            will query the database (either the `cases` collection, or else `cases_html` if html=True)

    output:
        the json of the case (or html if html is True)
    '''
    if not (fpath or ucid):
        raise ValueError("Must provide a ucid or fpath")

    elif ucid and not fpath:
        subdir = 'html' if html else 'json'
        fpath = ftools.get_expected_path(ucid, subdir=subdir)

    if mongo_db is not None:
        collection = 'cases_html' if html else 'cases'
        res = mongo_db[collection].find_one({'ucid':ucid})
        return res

    # Standardise across Windows/OSX and make it a Path object
    fpath = std_path(fpath)

    # Absolute path to json file
    if settings.PROJECT_ROOT.name in fpath.parts:
        # Treat as absolute if its not relative to project root
        jpath = fpath
    else:
        jpath = settings.PROJECT_ROOT / fpath

    if html:
        hpath = get_pacer_html(jpath)
        if hpath:
            try:
                html_text = str( open(settings.PROJECT_ROOT / hpath, 'r', encoding='utf-8').read() )
            except:
                html_text = str( open(settings.PROJECT_ROOT / hpath, 'r', encoding='windows-1252').read() )
            return html_text if skip_scrubbing else remove_sensitive_info(html_text)
        else:
            raise FileNotFoundError('HTML file not found')
    else:
        if skip_scrubbing:
            jdata = json.load(open(jpath, encoding="utf-8"))
        else:
            json_text = open(jpath, encoding="utf-8").read()
            jdata = json.loads(remove_sensitive_info(json_text))
        jdata['case_id'] = ftools.clean_case_id(jdata['case_id'])

        if recap_orig:
            if 'recap' in jdata['source']:
                try:
                    recap_id = jdata['recap_id']
                    if skip_scrubbing:
                        return json.load(open(settings.RECAP_PATH/f"{recap_id}.json", encoding="utf-8"))
                    else:
                        json_text = open(settings.RECAP_PATH/f"{recap_id}.json", encoding="utf-8").read()
                        return json.loads(remove_sensitive_info(json_text))
                except:
                    print('Cannot load recap original, returning parsed json instead')

        return jdata

def get_pacer_html(jpath):
    ''' Get a pacer html from the json filepath'''
    jpath = Path(str(jpath))
    hpath = Path(str(jpath).replace('json', 'html'))
    if hpath.exists():
        return hpath

def difference_in_dates(date_x, date_0):
    '''
    Calculate the the number of days after day_0 something on day_x occurs from docket text date strings
    inputs
        - date_x(string): the date of interest from docket
        - day_0(string): the baseline date from docket (e.g. filing date)

    return
        (int) no. of days
    '''

    try:
        dt_x = datetime.strptime(date_x,'%m/%d/%Y')
        dt_0 = datetime.strptime(date_0,'%m/%d/%Y')

        return (dt_x - dt_0).days

    except:
        return None

# Generate unique case id
def ucid(court, case_id, clean=False, allow_def_stub=True):
    '''
    Generate a unique case id (ucid), or a series of ucids

    Inputs:
        - court (str or Series): court abbreviation
        - case_id (str or Series): either colon or hyphen format, will be standardised
        - clean (bool): whether the case_id is already clean (speeds up calculation)
        - allow_def_stub (bool): whether or not to allow defendant docket stubs
    Output:
        (str or Series) like 'nced;;5:16-cv-00843'
    '''
    if type(case_id)==pd.Series:
        if not clean:
            return court + ';;' + case_id.map(lambda x: ftools.clean_case_id(x, allow_def_stub))
        else:
            return court + ';;' + case_id
    else:
        if not clean:
            clean_case_id = ftools.clean_case_id(case_id, allow_def_stub)
            return f"{court};;{clean_case_id}"
        else:
            return f"{court};;{case_id}"

# Alias
get_ucid = ucid

def ucid_from_scratch(court, office, year, case_type, case_no):
    ''' Generate a ucid from all base elements (year is 2-digit)'''
    if type(court)==pd.Series and type(case_no)==pd.Series:
        case_id = office + ':' + year + f"-{case_type}-" + case_no
    else:
        case_id = f"{office}:{year}-{case_type}-{case_no}"
    return ucid(court, case_id)

def get_ucid_weak(ucid):
    '''
    Get a weakened ucid with the office removed

    Inputs:
        - ucid (str): a correctly formed ucid
    Output:
        (str) a weakened ucid e.g. 'ilnd;;16-cv-00001'
     '''

    if type(ucid)==pd.Series:
        return ucid.str.replace(rf"{ftools.re_com['office']}:", '')
    else:
        return re.sub(rf"{ftools.re_com['office']}:", '', ucid)

def parse_ucid(ucid):
    '''
    Split a ucid back into its component parts
    Inputs:
        -ucid (str or Series)
    Output:
        dict (if ucid is a str) or DataFrame (if ucid is a Series)
    '''
    re_ucid = "(?P<court>[a-z]{2,5});;(?P<case_no>.*)"

    if type(ucid)==pd.Series:
        return ucid.str.extract(re_ucid)
    else:
        match = re.match(re_ucid, ucid)
    if match:
        return match.groupdict()

def bundle_from_list(file_list, name, mode='ucid', notes=None, overwrite=False):
    '''
    Bundle files together from list of files
    Inputs:
        - file_list (arr of strings)
        - name (str): name of directory to pass to bundler method
        - mode ('ucid' or 'fpath'): whether the list contains ucids or fpaths
    '''

    df = load_unique_files_df()
    if mode=='ucid':
        df = df[df.index.isin(file_list)]
    elif mode=='fpath':
        df = df[df.fpath.isin(file_list)]

    bundle_from_df(df, name, notes, overwrite)

def bundle_from_df(df, name, notes=None, overwrite=False):
    '''
        Bundle up a collection of files
        Inputs:
            - df (DataFrame): any dataframe with a fpath column
            - name (str): name of directory to bundle into (will be put in /data/{name})
    '''
    bundler.bundler(df,name, notes, overwrite)

def is_recap(fpath):
    '''Determine if a case is a recap case based on the filepath'''
    return 'recap' in std_path(fpath).parts

def group_dockets(docket_fpaths, court=None, use_ucids=None, summary_fpaths=None, recap_df=None):
    '''
    Group filepaths to docket htmls by ucid (will be multiple for docket updates)

    Inputs:
        - docket_fpaths (list of str): list of docket filepaths
        - court (str): the court abbreviation, if none will infer from directory structure
        - use_ucids (list-like): a list of ucids to include. If supplied, will filter out
            any ucids that are not in this list from the output
        - summary_fpaths (list): list of str or Path objects of summary fpaths to connect to docket_fpaths
        - recap_df (pd.DataFrame): a dataframe with columns: 'ucid' and 'fpath' (relative to project root)
    Outputs:
        - a list of dicts grouped by ucid with
            - 'docket_paths': tuples of paths
            - 'summary_path': a single Path (may be of type float('nan') if no summary)

    '''
    df = pd.DataFrame(docket_fpaths, columns=['fpath'])

    # Sort on filename, assumes that this builds the correct order for updated filenames
    df.sort_values('fpath', inplace=True)

    # Get the ucid column
    if court==None:
        #Infer from path
        court = df.fpath.iloc[0].parents[2].name

    df['ucid'] = df.fpath.apply(lambda x: ftools.filename_to_ucid(x, court=court))

    # Filter which ones to use
    df['use'] = True if type(use_ucids) is type(None) else df.ucid.isin(use_ucids)

    # Collapse down to ucids we want to use
    df = df[df.use].copy()
    del df['use']

    if recap_df is not None:

        # Filter by court if court column present
        if 'court' in recap_df.columns:
            recap_df = recap_df[recap_df.court.eq(court)].copy()

        # Reduce down to just ucid and fpath columns
        recap_df = recap_df[['ucid', 'fpath']].copy()
        recap_df.fpath = recap_df.fpath.apply(lambda x: settings.PROJECT_ROOT/x)

        # Collapse down to only use RECAP cases/ucids that overlap with current selection
        recap_df = recap_df[recap_df.ucid.isin(df.ucid)]
        # Append to the main df
        df = df.append(recap_df)

    df['summary_fpath'] = pd.Series(dtype=str)

    if summary_fpaths:

        df_summaries = pd.DataFrame(summary_fpaths, columns=['fpath'])
        df_summaries.sort_values('fpath', inplace=True)
        df_summaries['ucid'] = df_summaries.fpath.apply(lambda x: ftools.filename_to_ucid(x, court=court))

        # Assumes no duplication of summaries
        summary_map = df_summaries.set_index('ucid')['fpath']
        df['summary_path'] = df.ucid.map(summary_map)

        cases = df.groupby('ucid').agg(docket_paths=('fpath',tuple), summary_path=('summary_path','first')).reset_index().to_dict('records')
    else:
        cases = df.groupby('ucid').agg(docket_paths=('fpath',tuple) ).reset_index().to_dict('records')

    return cases

# n.b.: compress_data and decompress_data may no longer be needed after updates to parse_pacer.py (11/2/20)
def compress_data(data):
    '''Compress complex data into a JSON-serializable format (written to generate the 'member_cases' field in JSONs from parse_pacer.py)'''
    return base64.b64encode(zlib.compress(json.dumps(data).encode('utf-8'),9)).decode('utf-8')

def decompress_data(data):
    '''Reverse the effects of compress_data'''
    return json.loads(zlib.decompress(base64.b64decode(data.encode('utf-8'))).decode('utf-8'))

def pacer2scales_ind_all(docket):
    '''
    Get a mapping from pacer index (the 'ind' value in docket, and the # column in table),
    to scales index (the order from the case json docket array)

    Note: this is mainly for testing/exploring purposes.
    There is no guarantee a well-defined mapping exists from pacer ind to scales ind
    e.g. two docket lines neither of which have a pacer ind (both blank in # column)

    Inputs:
        - docket (list): the 'docket' value from a case json
    Output:
        (dict) mapping from pacer index (str) to scales index (int)
        e.g. { '1': 0, '2':1, '3':2, '7':3, ...}
    '''
    return dict( (pacer,scales) for scales, pacer in enumerate([x['ind'] for x in docket]))

def lookup_docket_pacer_ind(ucid, pacer_ind, dff):
    '''
    Helper function to quickly lookup docket lines based on the pacer ind (the # column in docket)
    Note: this is mainly for testing/exploring purposes, see note in pacer2scales_ind_all

    Inputs:
        - ucid (str): the case ucid
        - pacer_ind (int,str or list): throw anything at me! can take int, str or list of neither
        - dff (pd.DataFrame): dataframe of files, either the unique files dataframe,
                        or any subset of it that includes the ucid and fpath
    Output:
        (dict) where the key is the scales ind and the value is the entry from the case docket array

        e.g. lookup_docket_pacer_ind(ucid, [27,28])
            -> { 24: {'ind': '27', 'date_filed': ___, 'docket_text': ___, ... },
                 25: {'ind': '28', 'date_filed': ___, 'docket_text': ___, ... }
               }
    '''

    #Handle either a list or single value for ind
    ind_arr = pacer_ind if type(pacer_ind)==list else [pacer_ind]
    ind_arr = [str(x) for x in ind_arr]

    docket = load_case(dff.loc[ucid].fpath)['docket']
    pacer2scales = pacer2scales_ind_all(docket)
    keepers = [pacer2scales[ind] for ind in ind_arr]

    return {scales_ind:line for scales_ind, line in enumerate(docket) if scales_ind in keepers}

def case_link(ucids, subdir='html' , incl_token=False, server_index=-1):
    '''
    Generate clickable case links for a set of ucids, from within a jupyter notebook
    Inputs:
        - ucids (iterable or str): iterable of ucids to create links for (if you supply a single ucid it will case to a list)
        - ext ('html' or 'json')
        - incl_token (bool): whether to include the token query string (e.g. if you're switching between browsers)
        - server index (int): if you have multiple jupyter servers running, specify which one, defaults to last
    '''
    from notebook import notebookapp
    from IPython.core.display import display, HTML
    # Get server details
    server = list(notebookapp.list_running_servers())[server_index]
    notebook_dir = Path(server['notebook_dir'])
    token = server['token'] if incl_token else None

    # Coerce to iterable if a single ucid supplied
    if type(ucids) is str:
        ucids = (ucids,)

    # Iterate over all ucids
    for ucid in ucids:
        
        #Iterate over all possible case updates (will only really apply to htmls)
        update_ind = 0
        path = ftools.get_expected_path(ucid, subdir=subdir, update_ind=update_ind)
        while path.exists():

            rel_path = path.relative_to(notebook_dir)
            base_url = f"http://{server['hostname']}:{server['port']}/view/"
            full_url = base_url + str(rel_path)

            if incl_token:
                full_url +=  f'?token={token}'
            
            # Construct the label
            label = ucid
            if update_ind>0:
                label += f' (update={update_ind})'

            link = f'{label:<50}: <a target="_blank" href="{full_url}">{full_url}</a>'
            display(HTML(link))
            
            update_ind +=1
            path = ftools.get_expected_path(ucid, subdir=subdir, update_ind=update_ind)
            
        if not path.exists() and update_ind==0:
            print(f'{ucid}: No such file exists at {path}')
            continue

def doc_link(docs, incl_token=False, server_index=-1):
    '''
    Generate clickable pdf links for a set of document ids, from within a jupyter notebook
    Inputs:
        - docs (iterable or str): iterable of document ids (e.g. 'ilnd;;1:09-cr-00001_20')  to create links for (if you supply a single doc id it will cast to a list)
        - incl_token (bool): whether to include the token query string (e.g. if you're switching between browsers)
        - server index (int): if you have multiple jupyter servers running, specify which one, defaults to last
    '''
    from notebook import notebookapp
    from IPython.core.display import display, HTML
    # Get server details
    server = list(notebookapp.list_running_servers())[server_index]
    notebook_dir = Path(server['notebook_dir']).resolve()
    token = server['token'] if incl_token else None

    # Coerce to iterable if a single ucid supplied
    if type(docs) is str:
        docs = (docs,)

    # Iterate over all documents
    for doc_id in docs:
        path = ftools.get_doc_path(doc_id)

        if not path.exists():
            print(f'{doc_id}: No such file exists at {path}')
            continue

        rel_path = path.relative_to(notebook_dir)
        base_url = f"http://{server['hostname']}:{server['port']}/files/"
        full_url = base_url + str(rel_path)

        if incl_token:
            full_url +=  f'?token={token}'

        link = f'{doc_id}: <a target="_blank" href="{full_url}">{full_url}</a>'
        display(HTML(link))

def gen_recap_id_file(file=None):
    ''' Generate the recap id table, using file as the basis (if it already exists) '''

    ###
    # Helpers
    ###
    def _get_id_ucid():
        ''' Get the (recap_id, ucid) pairs '''
        for fpath in settings.RECAP_PATH.glob('*.json'):
            jdata = json.load(open(fpath))
            recap_id = fpath.stem
            ucid = ucid(jdata['court'], jdata['docket_number'])
            yield (recap_id, ucid)

    def attempt_case_type(ucid):
        try:
            case_no = parse_ucid(ucid)['case_no']
            case_type = ftools.decompose_caseno(case_no)['case_type']
        except:
            case_type = None

        return case_type



    # Read file or generate if not supplied
    if file:
        df = pd.read_csv(file)
        df = df[['recap_id', 'ucid']].copy()
    else:
        df = pd.DataFrame( _get_id_ucid(), columns=('recap_id', 'ucid'))

    # Case Type
    df['case_type'] = df.ucid.map(attempt_case_type)

    # Buckets
    df['bucket'] = ''

    # Have html
    dff = load_unique_files_df()
    df['ignore_have_html'] = df.ucid.isin(dff[dff.has_html].index)

    # Non cv
    df['ignore_non_cv'] = ~df.case_type.eq('cv')

    # The rest
    df['use'] = ~(df.ignore_have_html | df.ignore_non_cv)

    return df

def jload_n_hash(jpath):
    ''' Load a json file with a '_hash' key that is a python hash (int) of the file string '''

    jstring = open(jpath).read()
    _hash = hash(jstring)
    jdata = json.loads(jstring)
    jdata['_hash'] = _hash
    return jdata

def get_latest_docket_date(ucid):
    '''
    Get the filed date of latest docket entry for a single case
    Inputs:
        - ucid (str): case ucid
    Output:
        latest_date (str) - returns None if no dates found in docket or case doesn't exist
    '''
    jpath = ftools.get_expected_path(ucid=ucid)

    # If no json file then leave blank (will get all docket lines)
    if not jpath.exists():
        return None

    # Load case data and get all dates
    jdata = load_case(ucid=ucid)
    docket_dates = [x['date_filed'] for x in ( jdata.get('docket') or [] ) ]
    if not len(docket_dates):
        return None
    else:
        latest_date = pd.to_datetime(docket_dates).max().strftime(ftools.FMT_PACERDATE)
        return latest_date


def insert_extra_docketlines(base, extra, threshold_date=0.8, default='before'):

    '''
    Insert extra docketlines into docket data.
    Currently only inserts at the start or end.
    Tries to order first by looking at the docket indexes, if that is inconclusive
    it tries to use the docket dates to place extra either before or after base.

    Inputs:
        - base (list): the baseline of case docket data, a list of dicts (follows the docket schema),
            assumes it has been extracted correctly and is ordered corrrectly
        - extra (list): the additional docket lines to add, follows the docket schema,
            assumes this is a single block of docket lines to add
        - threshold_date (float): score between 0 and 1, the required threshold to decide
            before or after based on the docket entry dates
        - default ('after', 'before'): the default outcome if inconclusive
    Output
        (list of dict): consolidated list of docket entry data
    '''
    def get_ind_int(ind_str):
        try:
            return int(ind_str)
        except:
            return None

    # If either list is empty, then answer is trivial
    if not len(base) or not len(extra):
        result = {'extra_placement': default, 'basis':'empty_list'}

    else:

        # Compare using filing dates
        base_dates = [datetime.strptime(x['date_filed'], ftools.FMT_PACERDATE) for x in base]
        extra_dates = [datetime.strptime(x['date_filed'], ftools.FMT_PACERDATE) for x in extra]

        n_dates = len(extra_dates)
        base_date_min = min(base_dates)
        base_date_max = max(base_dates)

        pct_date_before = sum(x<= base_date_min for x in extra_dates) / n_dates
        pct_date_after = sum(x>= base_date_max for x in extra_dates) / n_dates

        if (pct_date_before > pct_date_after) and (pct_date_before >= threshold_date):
            result = {'extra_placement': 'before', 'basis':'date'}

        elif (pct_date_after > pct_date_before) and (pct_date_after >= threshold_date):
            result = {'extra_placement': 'after', 'basis':'date'}

        else:

            # Use the ind to compare
            base_ind = [get_ind_int(x['ind']) for x in base]
            base_ind = list( filter(None, base_ind) )
            extra_ind = [get_ind_int(x['ind']) for x in extra]
            extra_ind = list( filter(None, extra_ind) )
            n_ind = len(extra_ind)

            if n_ind and len(base_ind):

                base_ind_min = min(base_ind)
                base_ind_max = max(base_ind)

                pct_ind_before = sum(x<= base_ind_min for x in extra_ind) / n_ind
                pct_ind_after = sum(x>= base_ind_max for x in extra_ind) / n_ind

                if pct_ind_before > pct_ind_after:
                    result = {'extra_placement': 'before', 'basis':'ind'}
                else:
                    result = {'extra_placement': 'after', 'basis':'ind'}

            else:
                # Cannot make deicision
                result = {'extra_placement': default, 'basis':'default'}

    # Now make the placement
    if result['extra_placement'] == 'before':
        return extra+base
    elif result['extra_placement'] == 'after':
        return base+extra



def load_sentencing_commission(year_a = 2008, year_b = 2019, small = False, var_cols = []):

    if small:
        return pd.read_json(settings.SENTENCING_COMMISSION/"ussc_2008-2019_smallform.json")

    # import lzma

    eligible_years = list(range(2008,2020))
    csv_files = [(f"opafy{str(i)[-2:].zfill(2)}nid.csv", int(i)) for i in eligible_years]

    if year_a not in eligible_years or year_b not in eligible_years:
        return f"Issue Detected: limit load to one of {eligible_years}"
    else:
        csv_files = [k for k in csv_files if k[1]>=year_a and k[1]<= year_b]

    if not var_cols:
        ## STAN_M --> N = statute, M = count
        ## STA2_1 --> THE SECOND statute, of THE FIRST count
        statutes = []
        for i in range(1,26):
            statutes.append(f"STA1_{i}")
            statutes.append(f"NWSTAT{i}")
            statutes.append(f"STA2_{i}")
        var_cols =  [
            'POOFFICE','CIRCDIST','DISTRICT', 'MONCIRC',
            'DISPOSIT','USSCIDN', 'DOBMON','DOBYR','AGE', 'MONSEX',
            'NEWCNVTN','SOURCES', 'OFFGUIDE','OFFTYP2','OFFTYPSB','CASETYPE',
            'QUARTER','SENTMON','SENTYR',
            'SENTTOT', 'SENTTOT0','SENTIMP','NOCOUNTS','YEARS', 'AMTREST','TOTREST','AMTFINEC',
            *statutes]

    yrs = {}

    print(f"Loading Sentencing Commission Data: {year_a} - {year_b}")
    for file_pair in csv_files:
        # grabbing each years headers, as columns may be out of order year over year
        load_path = settings.SENTENCING_COMMISSION / file_pair[0]
        if load_path.suffix ==".csv":
            with open(load_path) as file:
                content = file.readline()
        # elif load_path.suffix == '.xz':
            # with lzma.open(load_path, mode='rt') as file:
                # content = file.readline()

        # use the column titles to determine index columns to load using pandas
        header = content.split(',')
        grabber = [header.index(i) for i in var_cols if i in header]
        if len(grabber) == 0:
            alt_cols = [col.lower() for col in var_cols]
            grabber = [header.index(i) for i in alt_cols if i in header]
            # print("Lowercased solved:", len(grabber))
        this_year = []
        for chunk in pd.read_csv(load_path, usecols=grabber, chunksize=10000, low_memory=False):
            this_year.append(chunk)

        yrs[file_pair[1]] = pd.concat(this_year)

        print(f"{file_pair[1]} completed.")

    mydfs = []
    for yr, df in yrs.items():
        df['year_file'] = yr
        df.columns = [col.upper() for col in df.columns]
        mydfs.append(df)

    fulldf = pd.concat(mydfs)

    district_walk = pd.read_csv(settings.SENTENCING_COMMISSION / 'ussc_district_metadata.csv')

    CD_lookup = {c:a for c,a in district_walk[['CIRCDIST','Abbr']].to_numpy()}
    fulldf['distr_abbrev'] = fulldf.CIRCDIST.map(CD_lookup)

    fulldf.set_index(pd.Series(range(len(fulldf))), inplace=True)

    unistats = fulldf[[col for col in fulldf.columns if 'NWSTAT' in col]].copy()
    unistats.fillna(-1, inplace=True)

    next_rows = []
    for index, row in tqdm.tqdm(unistats.iterrows(), total=len(unistats)):
        next_rows.append({'join_index':index, 'unique_statutes':set(v for k,v in row.items() if v!=-1)})

    unique_statutes = pd.DataFrame(next_rows)

    settify_unique = fulldf.merge(unique_statutes, how='left', left_index=True, right_on = 'join_index')
    settify_unique.drop([col for col in settify_unique.columns if "NWSTAT" in col]+['join_index'],axis=1, inplace=True)

    settify_unique.to_json(SENTENCING_COMMISSION/"ussc_2008-2019_smallform.json")

    return settify_unique
def run_in_executor(f):
    '''Decorator to run function as blocking'''
    @functools.wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, lambda: f(*args, **kwargs))
    return inner

def load_EDGAR():
    """EDGAR data was scraped from their open source portals and parsed using
    BS4. The resulting data is stored in our annotation directory.
    The scripts used to gather and clean the initial data currently live in
    research_dev/code/research/disambiguation/parties_scrapers.py
    """
    epath = settings.EDGAR
    df = pd.read_csv(epath)

    df['entity_name'] = df.entity_name.astype(str)
    df['normalized'] = df.normalized.astype(str)
    df['cik'] = df.cik.apply(lambda x: str(x).zfill(10))

    return df