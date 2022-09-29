'''
File: parse_pacer.py
Author: Adam Pah, Greg Mangan, Scott Daniel
Description: Parses HTMLs downloaded using the SCALES Pacer scraper
'''

# Standard path imports
from __future__ import division, print_function
import glob
import json
import re
import csv
import sys
import string
import asyncio
import pandas as pd
from bs4 import BeautifulSoup as bs
from pathlib import Path

# Non-standard imports
import click
from concurrent.futures import ThreadPoolExecutor

# SCALES modules
sys.path.append(str(Path(__file__).resolve().parents[1]))
import support.data_tools as dtools
import support.docket_entry_identification as dei
import support.settings as settings
import support.fhandle_tools as ftools
from support.court_functions import COURTS_94
from parsers.parse_summary import SummaryPipeline

LOG_DIR = Path(__file__).parent/'logs'

# Global regex variables
nbsp = '(?:&nbsp;|\xa0)'
any_sp = '(?:&nbsp;|\xa0| )'
br_tag = '\<br ?\/?\>'
re_fdate = re.compile('Date Filed: [0-9]{2}/[0-9]{2}/[0-9]{4}')
re_tdate = re.compile('Date Terminated: [0-9]{2}/[0-9]{2}/[0-9]{4}')
re_judge = re.compile('Assigned to: [ A-Za-z\'\.\,\-\\\\]{1,150}\<')
# re_judge_panel = re.compile(f'(?s)Panel: [ A-Za-z\'\.\,\-\\\\]{{1,150}}(?:.*?\n(?:{nbsp}).+?{br_tag})+')
re_referred_judge = re.compile(f'(?s)Referred to: [ A-Za-z\'\.\,\-\\\\]{{1,150}}(?:.*?\n(?:{nbsp}).+?{br_tag})*')
re_party_charset = '[ A-Za-z0-9#&:;!?@_=\'\"\.\,\-\$\/\(\)\\\\]'
re_cr_base = f'Case title: {re_party_charset}{{1,200}}'
re_cv_base = f'(?:[^\(]{re_party_charset}{{0,200}}vs?(?:\. ?| )| ?in re:?| ?in the matter of){re_party_charset}{{1,200}}'
re_cr_title = re.compile(f'{re_cr_base}(?:\<|\()')
re_cv_title = re.compile(f'(?i)(?:{br_tag}|\)){re_cv_base}(?:\<|\()')
re_nature = re.compile('Nature of Suit: [A-Za-z0-9 :\(\)\.]{1,100}')
re_jury = re.compile('Jury Demand: [A-Za-z0-9 :\(\)\.]{1,100}')
re_cause = re.compile('Cause: [A-Za-z0-9 :\(\)\.\-]{1,100}')
re_jurisdiction = re.compile('Jurisdiction: [A-Za-z0-9 :\(\)\.]{1,100}')
re_lead_case_id = re.compile('Lead case: <a href=(?P<href>[^>]*)>(?P<case_id>[A-Za-z0-9:-]{1,100})')
re_demand = re.compile('Demand: [0-9\,\$]{1,100}')
re_related = re.compile('(?s)>[A-Za-z\- ]{{0,25}}?{0}Cases?:{0}</td><td>.+?</(?:tbody|table)>'.format(any_sp))
re_mag_judge = re.compile('Magistrate{0}judge{0}case{0}numbers?:(?:{0})?</td><td>.+?</(?:tbody|table)>'.format(any_sp))
re_other_court = re.compile('(?s)(?:Case{0}in{0}other{0}court|Other{0}court{0}case{0}numbers?):(?:{0})?</td><td>.+?</(?:tbody|table)>'.format(any_sp))
re_appeals_court = re.compile('Appeals{0}court{0}case{0}numbers?:{0}[A-Za-z0-9 :;\(\)\.\,\-]{{1,100}}'.format(any_sp))
re_role = re.compile('<b><u>([A-Za-z0-9\-\/\,\.\(\) ]{1,100})</u')
re_header_case_id = re.compile('DOCKET FOR CASE #: [A-Za-z0-9 :\-]{1,100}')
re_filed_in_error_text = re.compile(f'(?i)(?:{br_tag}|\))(?:{re_cv_base}|{re_cr_base})?(?:error|filing error|filed in error|opened in error|not used|case number not used|incorrectly filed|do not docket)')




########################
### Helper functions ###
########################


def generic_re_existence_helper(obj, split_text, index, maxsplit=-1):
    if obj != None:
        return dtools.line_detagger(obj.group()).split(split_text, maxsplit)[index]
    else:
        return None


def re_existence_helper(obj):
    if obj != None:
        return dtools.line_detagger(obj.group()).split(': ')[-1]
    else:
        return None




#############################################
### Functions for parties/criminal counts ###
#############################################


def split_on_multiple_separators(text, separators):
    '''
    Do a Python split() with multiple separators - this is used to break up a party table according to all the role titles that appear in it
    Inputs:
        - text: a string to be broken up, of the form '..A...B....C.....'
        - separators: a list of separators with which to break up the string, of the form ['A', 'B', 'C']
    Output:
        - splits: the original text broken up and converted to tuples, of the form [(None, '..'), ('A', '...'), ('B', '....'), ('C', '.....')]
    '''
    splits = [(None, text)] # e.g. text = '..A...A....', separators = ['A']
    for separator in separators:
        new_splits = []
        for (old_key,chunk) in splits: # old_key = None, chunk = '..A...A....'
            if separator in chunk:
                new_split = chunk.split(separator) # new_split = ['..', '...', '....']
                new_splits.append((old_key,new_split[0])) # new_splits.append((None, '..'))
                new_splits.extend([(separator,new_chunk) for new_chunk in new_split[1:]]) # new_splits.extend([('A', '...'), ('A', '....')])
            else:
                new_splits.append((old_key,chunk)) # if we didn't find the separator, just keep things the same
        splits = new_splits
    return splits


def process_entity_and_lawyers(chunk):
    '''
    Parse information about a party's representation (e.g. lawyer name, office, lead attorney status...)
    Inputs:
        - chunk (str): some HTML pertaining to a particular party
    Outputs:
        - chunk_answer (dict): parsed info on this party and their representation
    '''

    # Get the non-lawyer-dependent fields set up
    split_chunk = chunk.split('represented')
    raw_name = split_chunk[0].split('<b>')[-1].split('</b>')[0]
    name = dtools.line_cleaner(dtools.line_detagger( raw_name ))
    extra_info_dict = dtools.parse_party_extra_info(split_chunk[0].split(raw_name or '<b></b>',1)[1])

     # split up into individual lawyers
    lawyer_list = []
    for lawyer_section in split_chunk[1].split('</tr>'):
        if 'by</td>' in lawyer_section:
            lawyer_blocks = lawyer_section.split('by</td>')[-1].split('<b>')
            for lawyer_block in lawyer_blocks[1:]:

                # parse easy fields
                name_split = lawyer_block.split('</b>')
                lawyer_name = dtools.line_cleaner(name_split[0])
                is_lead = 'LEAD ATTORNEY' in lawyer_block
                is_notice = 'ATTORNEY TO BE NOTICED' in lawyer_block
                is_pro_hac = 'PRO HAC VICE' in lawyer_block
                see_above = 'above for address' in lawyer_block

                raw_info = re.sub('^(?:<br ?\/?>)+|(?:<\/td>|<br ?\/?>)+$', '', dtools.line_cleaner(
                    lawyer_block.split('</b>')[1] if '</b>' in lawyer_block else lawyer_block))
                info_clean = raw_info.replace('\n','') # lawyer block uses both \n and <br>, so the former is redundant
                info_lines = [x for x in [dtools.lawyer_line_cleaner(y) for y in info_clean.split(
                    '<i>')[0].split('<br')] if x and 'PRO SE' not in x] # prep contact info

                # use dtools for the trickier fields
                office, address, phone, fax, email = dtools.parse_lawyer_extra_info(info_lines)
                # if office and is_pro_se and lawyer_name==name:
                #     extra_pro_se_info = office
                #     office = None

                # parse designation-type info
                designation, bar_status, trial_bar_status, tdate = [None]*4
                if 'Designation: ' in lawyer_block:
                    designation = lawyer_block.split('Designation: ')[1].split('</i>')[0]
                if any(x in lawyer_block for x in ['Trial Bar Status: ', 'Trial bar Status: ']): # trial bar status seems to appear only in ILND
                    trial_bar_status = dtools.line_cleaner(lawyer_block.split('<!--')[1].split('Status: ')[1].split('-->')[0].strip('<> '))
                elif 'Bar Status: ' in lawyer_block:
                    bar_status = lawyer_block.split('Bar Status: ')[1].split('</i>')[0]
                if 'TERMINATED: ' in lawyer_block:
                    tdate = lawyer_block.split('TERMINATED: ')[1].split('</i>')[0]
                is_pro_se = 'PRO SE' in lawyer_block

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
                        'raw_info': raw_info
                    },
                    'is_pro_se': is_pro_se,
                    'is_lead_attorney': is_lead,
                    'is_notice_attorney': is_notice,
                    'is_pro_hac_vice': is_pro_hac,
                    'has_see_above': see_above,
                    'designation': designation,
                    'bar_status': bar_status,
                    'trial_bar_status': trial_bar_status
                })

    chunk_answer = {'name':name, 'entity_info':extra_info_dict, 'counsel':(lawyer_list or [])}
    return chunk_answer


def process_entity_without_lawyers(chunk): # Handle a party with no lawyers listed (same i/o as process_entity_and_lawyers)
    delim = re.search('<td valign=\"top\"(?: width=\"40%\")?>.{0,20}(?:<b><u>Pending Counts|V\.)', chunk)
    chunk = chunk.split(delim.group() if delim else 'Pending Counts')[0] # trim the chunk if it's from a criminal case
    raw_name = chunk.split('<b>')[1].split('</b>')[0]
    name = dtools.line_cleaner(dtools.line_detagger( raw_name ))
    extra_info_dict = dtools.parse_party_extra_info(chunk.split(raw_name or '<b></b>',1)[1])

    chunk_answer = {'name':name, 'entity_info':extra_info_dict, 'counsel':[]}
    return chunk_answer


def process_criminal_counts(chunk, count_type):
    '''
    Parse information about the charges against a particular defendant
    Inputs:
        - chunk (str): some HTML pertaining to the charges against a party
        - count_type (str): the type of charges to be parsed, either 'Pending Counts' or 'Terminating Counts'
    Outputs:
        - counts (list of dicts): parsed info on the charges against this party
    '''
    counts = []
    try:
        pertinent_counts = chunk.split(count_type)[-1].split('Highest Offense Level')[0]
    except:
        print(f"ERROR: unknown count type ({count_type})")
        return None
    count_table = pd.read_html('<table>' + pertinent_counts + '</table>')[0]
    for row in count_table.values:
        meaningful_row = [dtools.line_cleaner(x.replace('\\xc2\\xa7','§')) for x in row if type(x) == str]
        if len(meaningful_row) >= 1 and meaningful_row[0] != 'None':
            pid = meaningful_row[0].split('(')[-1].split(')')[0]
            text = meaningful_row[0].split('('+pid)[0]
            disp = meaningful_row[1] if len(meaningful_row)>1 else None
            counts.append({'pacer_id':pid, 'text':text, 'disposition':disp})
    return counts


def process_defendant_header_fields(text):
    '''
    Parse header information that pertains to the defendants, which will occur multiple times on a per-defendant basis in criminal cases
    Inputs:
        - text (str): the text to be searched for defendant fields
    Outputs:
        - judge, referred_judges, appeals_case_ids: the defendant fields in question
    '''
    judge_prelim = dtools.line_cleaner(re_existence_helper(re_judge.search(text)))
    judge = None if judge_prelim=='Unassigned' else judge_prelim

    referred_judges_raw = dtools.line_cleaner(re_existence_helper(re_referred_judge.search(text)))
    referred_judges = list(map(dtools.line_cleaner, referred_judges_raw.split('\n'))) if referred_judges_raw else []

    appeals_case_ids_prelim = re_existence_helper(re_appeals_court.search(text))
    appeals_case_ids = appeals_case_ids_prelim.split(', ') if appeals_case_ids_prelim else []

    return judge, referred_judges, appeals_case_ids


def process_parties_and_counts(text, is_cr):
    '''
    Parse information about the parties involved in this case, as well as any charges they may be facing
    Inputs:
        - text (str): the Pacer HTML table containing info on all the parties involved in a particular case
        - is_cr (bool): flag identifying this case as a criminal case
    Outputs:
        - parties (list): parsed info on the parties in this case
    '''
    def _clean_role(role):
        detagged = dtools.line_detagger(role)
        return detagged.split('(')[0].strip() if detagged else None

    # identify party roles (i.e. bolded/underlined words - Dft, Plaintiff, etc) & throw out red herrings (e.g. words from the crim-counts section)
    roles = set()
    terms_to_exclude = ['pending counts', 'terminated counts', 'terminated)', 'offense', 'disposition', 'complaints',
                        'order', 'rule', 'judgment', 'revocation', 'sentencing', 'contempt', '/19', '/20']
    for role_candidate in re_role.findall(text):
        if all(x not in role_candidate.lower() for x in terms_to_exclude):
            roles.add(role_candidate)

    # get additional info from the role-mappings JSON about the roles we found
    with open(settings.ROLE_MAPPINGS, 'r') as f:
        mappings = json.load(f)
    for role in roles:
        role_clean = _clean_role(role)
        if role_clean and role_clean not in mappings.keys():
            mappings[role_clean] = {'title':role_clean, 'type':'misc'} # this role isn't in the dict, so spoof an entry for it
            print(f'WARNING: role title "{role_clean}" not found')

    # split on role headings to create party chunks
    parties = []
    split = split_on_multiple_separators(text, ['<b><u>'+x+'</u></b>' for x in roles]) # add tags to weed out, e.g., 'Defendant' in disp text

    # check for roleless parties
    split_final = split
    potential_roleless = ['service list', 'serivce list', 'sevice list', 'svc lst', 'list service', 'prisoner correspondence']
    if any(('<b>'+x+'</b>' in text.lower() for x in potential_roleless)): # preliminary check
        for i in range(1,len(split)):
            chunk, indices_in_chunk = split[i][1], [None] # set up variables
            insert_point = split_final.index(split[i]) # decide where we should insert the results in the final split list

            # regexes for the detailed roleless-party check - apologies for the ugliness! (I feel like this edge case isn't worth more elegance)
            prefix1 = '<td valign=\"top\" width=\"40%\">'
            prefix2 = f'{prefix1}\n'
            represented1 = 'represented&nbsp;by</td>'
            represented2 = represented1.replace('&nbsp;', '(?:\xa0| )')
            exclude1 = f'(?<!{represented1})(?<!{represented2})'
            exclude2 = f'(?<!{represented1}{prefix1})(?<!{represented1}{prefix2})(?<!{represented2}{prefix1})(?<!{represented2}{prefix2})'

            for match in re.finditer(f"(?i)(?:{exclude1}(?:{prefix1}|{prefix2}))?{exclude2}<b>(?:{'|'.join(potential_roleless)})<\/b>", chunk):
                indices_in_chunk.append(match.start()) # store the index of this missing party

            # add any roleless parties to the list of chunks
            indices_in_chunk += [None]
            new_chunks = [chunk[indices_in_chunk[j]:indices_in_chunk[j+1]] for j in range(len(indices_in_chunk)-1)] # create new chunks
            chunks_with_roles = [((split[i][0] if j==0 else ''), chunk) for j,chunk in enumerate(new_chunks)] # add the role info
            split_final = split_final[:insert_point] + chunks_with_roles + split_final[insert_point+1:] # insert the new chunks

    # process each chunk
    for i in range(1,len(split_final)):
        raw_role, chunk = split_final[i]
        role = _clean_role(raw_role) if dtools.line_detagger(raw_role) else None # remove the tags that were added earlier

        # parse lawyers
        new_party = None
        if 'represented' in chunk:
            new_party = process_entity_and_lawyers(chunk)
        elif '<b>' in chunk: # edge case: defendant didn't have representation
            new_party = process_entity_without_lawyers(chunk)

        # parse info pertaining to this party's role
        if not new_party:
            print(f"WARNING: no party info found in '{role}' block")
        else:
            new_party['role'], new_party['party_type'] = (mappings[role]['title'], mappings[role]['type']) if role else (None, None)

            # parse info pertaining to criminal proceedings (e.g. criminal counts)
            new_party['pacer_id'] = None
            if is_cr:
                defendant_id = re.search('\((\d+)', raw_role)

                 # parse per-defendant header fields
                if new_party['role'] == 'Defendant':
                    def_prelim = split_final[i-1][1]
                    def_header = def_prelim.split('<tbody>' if '<tbody>' in def_prelim else '<table>')[-1] # pre-'10 dockets might not play nice
                    new_party['judge'], new_party['referred_judges'], new_party['appeals_case_ids'] = process_defendant_header_fields(def_header)
                else:
                    new_party['judge'], new_party['referred_judges'], new_party['appeals_case_ids'] = None, [], []

                if defendant_id:
                    new_party['pacer_id'] = int(defendant_id.groups()[0])
                if new_party['role'] == 'Defendant':
                    new_party['pending_counts'] = process_criminal_counts(chunk, 'Pending Counts')
                    new_party['terminated_counts'] = process_criminal_counts(chunk, 'Terminated Counts')
                    # this next for-loop has been awkwardly cobbled together over time; apologies to anyone reading this
                    for key, main_delim, width_pct, split_ind in [('highest_offense_level_opening','<u>Highest Offense Level (Opening)',40,1),
                                                ('highest_offense_level_terminated','<u>Highest Offense Level (Terminated)',40,1),
                                                ('complaints_text','Complaints',40,2),
                                                ('complaints_disposition','Complaints',60,1)]:
                        width_delim = f'width="{width_pct}%">'
                        new_party[key] = dtools.line_cleaner(dtools.line_detagger(chunk.split(main_delim)[1].split(width_delim)[split_ind].split(
                            '</td>')[0], prettify=True)) if main_delim in chunk and width_delim in chunk.split(main_delim)[1] else None
                        if new_party[key] == 'None':
                            new_party[key] = None
                else:
                    for k,v in {'pending_counts':[],
                                'terminated_counts':[],
                                'complaints_text':None,
                                'complaints_disposition':None,
                                'highest_offense_level_opening':None,
                                'highest_offense_level_terminated':None
                                }.items():
                        new_party[k] = v

            parties.append(new_party)

    return parties or []




##################################
### Functions for member cases ###
##################################

re_member_link = re.compile(r"""
    \<a[\s\S]+?         # start of the a tag <a
    href[\S]+?\.pl\?    # the href attribute
    (?P<pacer_id>\d+)   # the pacer id
    [\S]+?\>            # the rest of the opening atag
    (?P<case_id>[\S]+?) # the case id
    \<                  # the start of closing tag </a>
""",
re.X)

def get_member_cases(member_block_string, court):
    ''' Get the member cases list from the block "Member cases: .... " '''

    # Find all of the member links
    it = re_member_link.finditer(member_block_string)

    # Converter for groupdict
    _conv = lambda gd: {
        'member': dtools.ucid(court, ftools.clean_case_id(gd['case_id'])),
        'member_id': gd['pacer_id'],
    }

    mem_cases = [ _conv(match.groupdict()) for match in it]

    return mem_cases
    # return re.findall(r">(.{1,40})</a>",member_block_string)


def read_member_lead_df():
    '''
    Read the member-lead dataframe and map it to a dictionary

    Outputs:
        - member_cases (dict): a dictionary that maps leads to member list (all in ucids)
        e.x. {lead1: [mem1_1, mem1_2], lead2: [mem2_1], ...}
    '''
    # Load member case info
    df = pd.read_json(settings.MEMBER_LEAD_LINKS, lines=True)
    if not len(df):
        return {}

    member_cases_series = df.groupby('lead').member.agg(list)
    member_cases = dict(member_cases_series.iteritems())
    return member_cases


def update_member_cases(lead_case, lead_case_pacer_id, new_members_list, member_cases, court):
    ''' Update the in-memory and file-stored members lists for a new (lead, member_list) pair

    Inputs:
        - lead_case (str): ucid of lead case
        - lead_case_pacer_id (str): pacer id of lead case
        - new_members_list (list): a list of new member cases ucids
        - member_cases (dict): the in-memory mapping of leads to members
        - court (str): the current court
    '''
    # Update the member_cases dict
    member_cases[lead_case] = new_members_list

    print(f"Updating member cases for <lead:{lead_case}> with {len(new_members_list):,} member cases")

    # Add in extra details for writing out to jsonl
    it = (
        {**mem, 'court': court, 'lead':lead_case, 'lead_case_id': lead_case_pacer_id}
        for mem in new_members_list
    )
    # Append to the file
    with open(settings.MEMBER_LEAD_LINKS, 'a', encoding='utf-8') as rfile:
        rfile.writelines( json.dumps(mem)+'\n' for mem in it)


def tidy_member_cases_df():
    ''' Remove any duplicates from member cases file '''

    df = pd.read_json(settings.MEMBER_LEAD_LINKS, lines=True)
    if not len(df):
        return

    df.drop_duplicates(subset=('member', 'lead'), inplace=True)

    with open(settings.MEMBER_LEAD_LINKS, 'w') as wfile:
        for row_dict in df.fillna('').to_dict(orient='records'):
            wfile.write( json.dumps(row_dict)+'\n')


#############################
### Other major functions ###
#############################


def get_mdl_code(case_data):
    '''
    Get the mdl code if present
    Inputs:
        - case_data (dict): the rest of the case_data
    Outputs:
        - mdl_code (int): the case no for the mdl
        - mdl_id_source (str): the source of the identification of the code
    '''

    # Check lead case
    lead_case_id = case_data.get('lead_case_id')
    if lead_case_id:
        code = dei.mdl_code_from_casename(lead_case_id)
        if code:
            return (code, 'lead_case_id')

    # Check flags
    for flag in case_data.get('case_flags') or []:
        code = dei.mdl_code_from_string(flag)
        if code:
            return (code, 'flag')

    return (None,None)


def parse_docket(docket_table, reverse_docket=False):
    '''
    Get data from docket_table.
    NOTE: this method will modify docket_table in place in the process of creating the output data_rows.
    This is a useful efficiency for the parser but if you are using calling this method elsewhere and
    need docket_table to stay unmodified you should parse a copy of it (using `copy.copy(docket_table)``)
    Inputs:
        - docket_table (WebElement): the docket report main table
        - reverse_docket (bool): when True, reverse the order of docket entries (used for fixing backwards dockets from IASD)
    Output:
        data_rows (list): list of dicts with 5 entries (date_filed(str), ind(str), docket_text(str), documents(dict), edges(list of tuples))
    '''

    def _get_doc_id_(td):
        ''' Gets the id from an atag within a 2nd column td, returns None if td is empty'''
        atag = td.select_one('a')
        return atag.attrs.get('href').split('/')[-1] if atag else None

    def _encode_tags_(atag):
        '''
        Encode tag info into the string of a tag so it can pass through bs4 .text method.
        Solves the chicken-egg problem of:
        1) We need the spans of the links i.e. start and end indexes of text of the atags, RELATIVE to the whole docket text
        2) We must convert the td cell to text/string to see what these spans are
        3) Once we collapse to string we have lost the url information

        So this method: encodes info about the tag (most importantly the url) in the text,
        then iterates through to parse that info and decode it back to just the text of the atag e.g. "3"
        (There are other ways of iterating with bs4 through the td tag's children
        but it's hard to get the spans of these child tags relative to the 'docket text')

        Input:
            - atag (bs4 tag): a bs4 <a> tag
        Output:
            No output, this alters the atag.string property in place
        '''
        url = atag.attrs.get('href')
        doc_id = url.split('/')[-1]

        # Reference/Edge, encode the scales index
        if doc_id in line_doc_map.keys():
            scales_ind = line_doc_map[doc_id]
            encoded_info = scales_ind
            link_type = 'ref'

        # For attachment atags, encode the url
        else:
            encoded_info = url
            link_type = 'att'

        label = atag.string
        atag.string = f"###{link_type}_{encoded_info}_{label}###"

    # Get td tags from second column and map them to ids
    col2 =  docket_table.select('tr td:nth-of-type(2)')[1:]
    col2_ids = map(_get_doc_id_, col2)

    # Map document id to locational index
    line_doc_map = {doc_id:scales_ind for scales_ind,doc_id in enumerate(col2_ids) if doc_id }

    out_rows = []
    in_rows = docket_table.select('tr')[1:]
    if reverse_docket:
        in_rows.reverse()

    for scales_ind, row in enumerate(in_rows):
        documents, edges = {}, []

        cells = row.select('td')
        td_date, td_ind, td_entry = cells

        # Get line doc link
        if td_ind.select('a'):
            documents['0'] = {'url': td_ind.select_one('a').attrs.get('href'),'span': {} }

        # Get all atags
        atags = td_entry.select('a')
        # Filter out external links (by only using digit links)
        atags = [a for a in atags if a.text.strip().isdigit()]

        # Encode the tags
        list(map(_encode_tags_, atags))

        # Convert html to clean text, docket_text_pre is pre the decoding process (includes the ###..###)
        date_filed, ind, docket_text_pre = tuple(dtools.line_cleaner(x.text.strip()) for x in cells)

        # Buld docket_text iteratively through string addition, parse edges and documents on-the-fly
        # This will be the output clean json text
        docket_text_post = ''
        # Pointer to keep track of place within docket_text_pre
        pointer = 0

        # Pattern for regex to decode what was encoded in _encode_tags_
        re_encoded_tag = r"###(?P<link_type>ref|att)_(?P<encoded_info>[\s\S]+?)_(?P<label>\d+)###"

        # Iterate through all tags that have been encoded as ###...###
        for match in re.finditer(re_encoded_tag, docket_text_pre):
            # Get the start and end index of the match relative to docket_text_pre
            start, end = match.span()

            # Add text up until this match
            docket_text_post += docket_text_pre[pointer : start]

            # Decode info
            link_type, encoded_info, label = match.groups()
            # Build the span
            span = {'start':len(docket_text_post), 'end': len(docket_text_post) + len(label)}
            # Add the label to the text (what was previously inside the a tag)
            docket_text_post += label

            # Add to documents or edges
            if link_type=='ref':
                edges.append( [scales_ind, int(encoded_info), span] )
            elif link_type=='att':
                documents[label] = {'url':encoded_info, 'span': span}

            pointer = end

        # Make sure to get the last bit of text
        docket_text_post += docket_text_pre[pointer: ]

        out_row = {
            'date_filed': date_filed or None,
            'ind': ind or None,
            'docket_text': docket_text_post or None,
            'documents': documents,
            'edges': edges
        }
        out_rows.append(out_row)

    return out_rows


def get_city(html_text):
    '''
    Get the case city from the header (in parenthesis after court name)

    Example: Northern District of Illinois - CM/ECF LIVE, Ver 6.3.3 (Chicago)
            -> "Chicago"
    Inputs:
        - html_text (str) the html text for the page
    Output:
        (str)
    '''
    re_city = r"\((?P<city>[^\)]+)\)[^\(]+?DOCKET FOR CASE"
    match = re.search(re_city, html_text)
    return match.groupdict()['city'] if match else None


def get_lead_case(html_text):
    '''
    Get info on the lead case (if it exists)

    Output:
        - pacer_id (str): the pacer internal id for the lead case (from the href)
        - case_id (str): the case id
    '''
    match = re_lead_case_id.search(html_text)
    if match:
        case_id = match.groupdict().get('case_id').strip()
        href = match.groupdict().get('href')
        pacer_id_match = re.search('\.pl\?(\d+)', href)
        pacer_id = pacer_id_match.group(1) if pacer_id_match else None

        return pacer_id, case_id
    else:
        return None, None



##################################################################
### Main processing function (most of the action happens here) ###
##################################################################


def process_html_file(case, member_cases, court=None):
    '''
    Processes a html Pacer file, returns a dictionary object to be saved as JSON

    Inputs:
        - case (dict) - dict with 'docket_paths':tuple, 'summary_path':Path
        - member_cases (dict): map of lead cases to member lists (in ucids)
        - court (str): court abbrev, if none infers from filepath
    Output:
        case_data - dictionary
    '''
    # Use the first file to pull the case name etc
    fname = case['docket_paths'][0]

    #Get the basic case info
    case_data = {}
    case_data['case_id'] = ftools.colonize(fname.stem)
    case_data['case_type'] = ftools.decompose_caseno(case_data['case_id']).get('case_type')

    dlcourt = fname.parents[2].name
    if court:
        case_data['court'] = court
    else:
        case_data['court'] = dlcourt
    case_data['ucid'] = dtools.ucid(case_data['court'], case_data['case_id'])

    #Read the html page data
    if len(case['docket_paths']) == 1:
        extra_case_data = {}
        try:
            try:
                html_text = open(fname, 'r', encoding='utf-8').read()
            except:
                html_text = open(fname, 'r', encoding='windows-1252').read()

            # chop off the member cases list
            mem_beg,mem_end = ftools.get_member_list_span(html_text)
            if mem_beg:
                member_cases_found = True
                soup = bs(html_text[:mem_beg]+html_text[mem_end:], 'html.parser')
            else:
                member_cases_found = False
                soup = bs(html_text, 'html.parser')

        except: # this used to catch UnicodeDecodeErrors, but that was solved with the alternate windows-1252 encoding
            member_cases_found = False
            print(f"ERROR: couldn't read html from {fname} ({sys.exc_info()[0]})")
            return None

    else:
        # When there are case updates or recap input
        soup, extra_case_data = ftools.docket_aggregator(case['docket_paths'])
        html_text = soup.decode(formatter='html')
        if 'Member cases:' in html_text:
            member_cases_found = True
            mem_beg,mem_end = ftools.get_member_list_span(html_text)
        else:
            member_cases_found = False

    # prevent erroneous matches in the docket text
    html_non_docket = html_text.split('Docket Text')[0] if 'Docket Text' in html_text else html_text

    case_data['city'] = get_city(html_non_docket)
    case_data['header_case_id'] = re_existence_helper( re_header_case_id.search(html_non_docket) )
    case_data['filing_date'] = re_existence_helper( re_fdate.search(html_non_docket) )
    case_data['terminating_date'] = re_existence_helper( re_tdate.search(html_non_docket) )
    if case_data['terminating_date'] == None:
        case_data['case_status'] = 'open'
    else:
        case_data['case_status'] = 'closed'

    # Use nos_matcher to try to match nature of suit based on code (or fuzzy match text)
    nature_suit_raw = generic_re_existence_helper( re_nature.search(html_non_docket), 'Suit: ', -1, maxsplit=1)
    nature_suit_matched = dei.nos_matcher(nature_suit_raw, short_hand=True)
    # Use the matched code if found, otherwise keep the raw string
    case_data['nature_suit'] = nature_suit_matched or nature_suit_raw

    case_data['jury_demand'] = generic_re_existence_helper( re_jury.search(html_non_docket), 'Jury Demand: ', -1)
    case_data['cause'] = generic_re_existence_helper( re_cause.search(html_non_docket), 'Cause: ', -1)
    case_data['jurisdiction'] = generic_re_existence_helper( re_jurisdiction.search(html_non_docket), 'Jurisdiction: ', -1)
    case_data['monetary_demand'] = generic_re_existence_helper( re_demand.search(html_non_docket), 'Demand: ', -1)

    lead_case_pacer_id, lead_case_id = get_lead_case(html_non_docket)
    case_data['lead_case_pacer_id'] = lead_case_pacer_id
    case_data['lead_case_id'] = lead_case_id

    related_cases = generic_re_existence_helper( re_related.search(html_non_docket), ':', -1, maxsplit=1)
    case_data['related_cases'] = list(map(dtools.line_cleaner, related_cases.split('\n'))) if related_cases else []
    other_court = generic_re_existence_helper( re_other_court.search(html_non_docket), ':', -1, maxsplit=1)
    case_data['other_courts'] = list(map(dtools.line_cleaner, other_court.split('\n'))) if other_court else []
    mag_case_ids = generic_re_existence_helper( re_mag_judge.search(html_non_docket), ':', -1, maxsplit=1)
    case_data['magistrate_case_ids'] = list(map(dtools.line_cleaner, mag_case_ids.split('\n'))) if mag_case_ids else []
    # judge_panel = generic_re_existence_helper( re_judge_panel.search(html_non_docket), 'Panel:', -1)
    # case_data['judge_panel'] = list(map(dtools.line_cleaner, judge_panel.split('\n'))) if judge_panel else []
    filed_in_error_text = re_filed_in_error_text.search(html_non_docket)
    case_data['filed_in_error_text'] = dtools.line_detagger(filed_in_error_text.group()) if filed_in_error_text else None
    case_flags = dei.get_case_flags(html_non_docket)
    case_data['case_flags'] = case_flags.split(",") if case_flags else []

    # zero out these fields for criminal cases, since they appear on a per-defendant basis in those cases
    if case_data['case_type'] == 'cv':
        case_data['judge'], case_data['referred_judges'], case_data['appeals_case_ids'] = process_defendant_header_fields(html_non_docket)
    else:
        case_data['judge'], case_data['referred_judges'], case_data['appeals_case_ids'] = None, [], []

    # Other fields depend on case type (case name, parties, counts...)
    if case_data['case_type'] != 'cr' and case_data['case_type'] != 'cv':
        print(f"ERROR: unknown case type ({case_data['case_type']}) in {fname}")
        title_regex = re_cr_title.search(html_non_docket) or re_cv_title.search(html_non_docket) # try both methods at our disposal
        case_data['parties'] = [] # since we don't currently support HTMLs for types beyond cr/cv, don't bother trying to parse the parties
    else:
        is_cr = bool(case_data['case_type'] == 'cr')
        title_regex = re_cr_title.search(html_non_docket) if is_cr else re_cv_title.search(html_non_docket)
        try:
            tables = soup.select('div > table[cellspacing="5"]')
            party_table = ''.join([str(x) for x in tables[1:]]) if is_cr else str(tables[1]) # crim cases have more tables
        except: # when this happens, it's usually on a docket that was opened in error & that reads 'Sorry, no party found'
            if 'no party found' not in html_text: # but if that's not the case, print some info; otherwise, fail silently
                print(f"WARNING: couldn't parse party table in {fname} ({sys.exc_info()[0]})")
            party_table = None
        case_data['parties'] = [] if party_table is None else process_parties_and_counts(party_table, is_cr)
    case_data['case_name'] = dtools.line_cleaner(generic_re_existence_helper( title_regex, 'Case title: ', -1 ))

    # Now the docket
    case_data['docket'], case_data['docket_available'] = [], False
    no_docket_headers = [x for x in soup.find_all('h2') if re.search(ftools.re_no_docket, x.text)]
    if not no_docket_headers:
        docket_table = dei.identify_docket_table(soup)
        if docket_table:
            case_data['docket'] = parse_docket(docket_table, 'BACKWARDS_DOCKET' in html_text and len(case['docket_paths'])==1)
            case_data['docket_available'] = True

    ### Store member cases in a csv
    if member_cases_found:
        if case_data['lead_case_id']:
            case_data['member_case_key'] = dtools.ucid(case_data['court'], case_data['lead_case_id'])
        else:
            # If member case list but no lead case listed, this case must be the lead case
            case_data['member_case_key'] = case_data['ucid']

        if case_data['member_case_key'] not in member_cases.keys():
            # If we haven't seen this lead case before, we need to store it
            new_members_list = get_member_cases(html_text[mem_beg:mem_end], case_data['court'])
            if new_members_list:
                update_member_cases(case_data['member_case_key'],case_data['lead_case_id'], new_members_list, member_cases, case_data['court'])
            else:
                case_data['member_case_key'] = None
    else:
        case_data['member_case_key'] = None

    ### MDL/MULTI
    case_data['mdl_code'] , case_data['mdl_id_source'] = get_mdl_code(case_data)
    # Is an mdl if we have a code OR if an 'MDL' or 'MDL_<description>' flag exists
    case_data['is_mdl'] = bool(case_data['mdl_code']) or any(f.lower().startswith('mdl') for f in (case_data['case_flags'] or []))
    case_data['is_multi'] = any( (case_data['is_mdl'], case_data['lead_case_id'], member_cases_found, case_data['other_courts']) )

    # Transaction data
    transaction_data = ftools.parse_transaction_history(html_text)
    case_data['billable_pages'] = int(transaction_data['billable_pages']) if 'billable_pages' in transaction_data.keys() else None
    case_data['cost'] = float(transaction_data['cost']) if 'cost' in transaction_data.keys() else None
    case_data['download_timestamp'] = transaction_data.get('timestamp')

    # No of case dockets, will be 1 unless there are docket updates and it will be >1
    case_data['n_docket_reports'] = len(case['docket_paths'])

    # if we're parsing from an html, the source must be Pacer (the RECAP remapper will set this field to 'recap')
    case_data['source'] = 'pacer'
    case_data['recap_id'] = None

    # Scraper stamp data
    stamp_data = dtools.parse_stamp(html_text)
    case_data['download_url'] = stamp_data.get('download_url')
    case_data['case_pacer_id'] = stamp_data.get('pacer_id')
    slabels = stamp_data.get('slabels','').split(',')
    case_data['is_stub'] = 'stub' in slabels
    case_data['is_private'] = 'private' in slabels
    case_data['scraper_labels'] = slabels

    # Deal with extra_case_data
    if extra_case_data:

        # Deal with extra docket lines
        if len(extra_case_data.get('recap_docket',[])):
            case_data['docket'] = dtools.insert_extra_docketlines(case_data['docket'], extra_case_data['recap_docket'])

            case_data['source'] = 'pacer,recap'
            case_data['recap_id'] = extra_case_data.get('recap_id')

    # Get summary data if available
    if pd.isna(case['summary_path']):
        case_data['summary'] = {}
    else:
        summary_html = open(case['summary_path']).read()
        summary_data = {}
        d, summary_data = SummaryPipeline.process(summary_html, summary_data)
        case_data['summary'] = summary_data

    return case_data




####################
### Control flow ###
####################


def case_runner(case, output_dir, court, debug, force_rerun, count, member_df, log_parsed):
    '''
    Case parser management
    '''
    # Get the output path
    case_fname = Path(case['docket_paths'][0]).stem
    outname = ftools.get_expected_path(ucid=case['ucid'], manual_subdir_path=output_dir)

    if force_rerun or not outname.exists(): # Check whether the output file exists already
        case_data = process_html_file(case, member_df, court = court)
        try:
            outname.parent.mkdir(exist_ok=True)
            with open(Path(outname).resolve(), 'w+') as outfile:
                json.dump(case_data, outfile)
        except: # occasionally getting a permissions error while writing, although this should be fixed now
            print(f"ERROR: couldn't write json for case {case_fname} ({sys.exc_info()[0]})")
        count['parsed'] +=1
        print(f"Parsed: {outname}")

        if log_parsed:

            # Get the path of the new file relative to project root (if in project) else leave it as absolute path
            outpath = outname.resolve()
            try:
                outpath = outpath.relative_to(settings.PROJECT_ROOT)
            except ValueError:

                # Try and get it if it's relative to the resolved pacer path (i.e. /Volumes/scales_datastore/pacer)
                try:
                    relpath = outpath.relative_to(settings.PACER_PATH.resolve())
                    outpath = (settings.PACER_PATH / relpath).relative_to(settings.PROJECT_ROOT)

                except:
                    # Leave it as full path

                    pass

            log_line = [case_data['ucid'], outpath]
            with open(LOG_DIR/log_parsed, 'a', encoding='utf-8') as wfile:
                writer = csv.writer(wfile)
                writer.writerow(log_line)
    else:
        if debug:
            case_data = process_html_file(case, member_df, court = court)
        count['skipped'] +=1
        print(f"Skipped: {outname}")


async def parse_async(n_workers, cases, output_dir, court, debug, force_rerun, count, member_df, log_parsed):
    ''' Run parsing asynchronously'''

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        loop = asyncio.get_running_loop()
        tasks = (
            loop.run_in_executor(executor, case_runner, *(case, output_dir, court, debug, force_rerun, count, member_df, log_parsed))
            for case in cases
        )
        asyncio.gather(*tasks)


def parse(input_dir, output_dir, summaries_dir, court=None, all_courts=False, debug=False,
    force_rerun=False, force_ucids=None, n_workers=16, log_parsed=False, recap_file=None):

    if all_courts:
        print('\nAll-court mode enabled')
    for current_court in COURTS_94 if all_courts else [court]:
        if all_courts:
            print(f'\nRunning on {current_court}...')

        court_input_dir = Path(f'{input_dir}/{current_court}/html' if all_courts else input_dir).resolve()
        court_output_dir = Path(f'{output_dir}/{current_court}/json' if all_courts else output_dir).resolve() if output_dir else (
            court_input_dir.parent/'json').resolve()
        court_summ_dir = Path(f'{summaries_dir}/{current_court}/summaries' if all_courts else summaries_dir).resolve() if summaries_dir else (
            court_input_dir.parent/'summaries').resolve()

        hpaths = Path(court_input_dir).glob('*/*.html')
        spaths = Path(court_summ_dir).glob('*/*.html')
        recap_df = None # Recap is deprecated

        if force_ucids:
            force_rerun = True
            force_ucid_series = pd.read_csv(force_ucids, usecols=('ucid',), squeeze=True) # Filter cases by ucids of interest
            if all_courts:
                force_ucid_series = [x for x in list(force_ucid_series) if x.split(';;')[0]==current_court] # confine to ucids in current court
            cases = dtools.group_dockets(hpaths, court=current_court, use_ucids=force_ucid_series, summary_fpaths=spaths, recap_df=recap_df)
        else:
            cases = dtools.group_dockets(hpaths, court=current_court, summary_fpaths=spaths, recap_df=recap_df)

        count = {'skipped':0, 'parsed': 0}

        member_cases = read_member_lead_df()

        if log_parsed:
            # Inititate the file (will overwrite file with same name) and write the headers
            if all_courts:
                if '.' in log_parsed:
                    split = log_parsed.split('.')
                    logpath = LOG_DIR/(split[0]+f'_{current_court}.'+split[1])
                else:
                    logpath = LOG_DIR/(log_parsed+f'_{current_court}')
            else:
                logpath = LOG_DIR/log_parsed
            with open(logpath, 'w') as wfile:
                csv.writer(wfile).writerow(['ucid', 'fpath'])

        if debug:
            for case in cases:
                case_runner(case, court_output_dir, current_court, debug, force_rerun, count, member_cases, log_parsed)
        else:
            asyncio.run(parse_async(n_workers, cases, court_output_dir, current_court, debug, force_rerun, count, member_cases, log_parsed))

        n = sum(count.values())
        print(f"\nProcessed {n:,} cases in {Path(court_output_dir)}:")
        print(f" - Parsed: {count['parsed']:,}")
        print(f" - Skipped: {count['skipped']:,}")
        if log_parsed:
            print(f"Table of successfully parsed cases at: {logpath.resolve()}")

        tidy_member_cases_df()


@click.command()
@click.argument('input-dir')
@click.option('--output-dir', '-o', default=None, type=click.Path(exists=True, file_okay=False),
                help="Directory to place parsed output, if none provided defaults to INPUT_DIR/../json ")
@click.option('--summaries-dir', '-s', default=None, type=click.Path(exists=True, file_okay=False),
                help="Directory to place parsed output, if none provided defaults to INPUT_DIR/summaries ")
@click.option('--court', '-c', default=None,
                help ="Court abbrv, if none given infers from directory")
@click.option('--all-courts', '-a', is_flag=True, default=False,
               help='If true, iterates over all courts (and assumes that input_dir is the parent directory of all the courts')
@click.option('--debug', '-d', default=False, is_flag=True,
                help="Doesn't use multithreading")
@click.option('--force-rerun', '-f', default=False, is_flag=True,
                help='Parse case even if json already exists')
@click.option('--force-ucids', default=None, type=click.Path(exists=True),
                help='A list of ucids to force rerun on, expects a csv with a "ucid" column')
@click.option('--n-workers', '-nw', default=16, type=int, show_default=True,
                help='No. of simultaneous workers to run')
@click.option('--log-parsed', default=None,
                help='Name of file to log parsed in /parsers/log/{log-parsed} as csv with columns ucid, fpath')
@click.option('--recap-file', default=None, show_default=True,
                help='Path to csv with recap cases, with columns for ucid and fpath (relative path to recap file)')
def parser(**kwargs ):
    ''' Parses .html casefiles in INPUT_DIR and puts .json files into the output directory'''
    parse(**kwargs)

if __name__ == '__main__':
    parser()

