'''
File: parse_pacer.py
Author: Adam Pah, Greg Mangan, Scott Daniel
Description:
PACER document scraper
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
from bs4 import BeautifulSoup as bs
from pathlib import Path

# Non-standard imports
import click
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
sys.path.append(str(Path(__file__).resolve().parents[1]))
import support.data_tools as dtools
import support.docket_entry_identification as dei
import support.settings as settings
import support.fhandle_tools as ftools

# Global regex variables
re_fdate = re.compile('Date Filed: [0-9]{2}/[0-9]{2}/[0-9]{4}')
re_tdate = re.compile('Date Terminated: [0-9]{2}/[0-9]{2}/[0-9]{4}')
re_judge = re.compile('Assigned to: [ A-Za-z\'\.\,\-\\\\]{1,100}(\<|\()')
re_referred_judge = re.compile('Referred to: [ A-Za-z\'\.\,\-\\\\]{1,100}(\<|\()')
re_cr_title = re.compile('Case title: [ A-Za-z0-9#&;()\'\.\,\-\$\/\\\\]{1,100}(\<|\()')
re_cv_title = re.compile('(\<br( \/)?\>|\))([^(][ A-Za-z0-9#&;()\'\.\,\-\$\/\\\\]{1,100} v.? |(I|i)n (R|r)e:?)[ A-Za-z0-9#&;()\'\.\,\-\$\/\\\\]{1,100}(\<|\()')
re_nature = re.compile('Nature of Suit: [A-Za-z0-9 :()\.]{1,100}')
re_jury = re.compile('Jury Demand: [A-Za-z0-9 :(\.)]{1,100}')
re_cause = re.compile('Cause: [A-Za-z0-9 :(\.)]{1,100}')
re_jurisdiction = re.compile('Jurisdiction: [A-Za-z0-9 :(\.)]{1,100}')
re_lead_case_id = re.compile('Lead case: <a href=[^>]*>[A-Za-z0-9:-]{1,100}')
re_demand = re.compile('Demand: [0-9\,\$]{1,100}')
re_other_court = re.compile('Case&nbsp;in&nbsp;other&nbsp;court:</td><td>&nbsp;[A-Za-z0-9 :;()\.\,\-]{1,100}') # brittle but functional
re_party = re.compile('<b><u>([A-Za-z ]{1,100})(?:</u|\()')
re_pacer_case_id = re.compile('DOCKET FOR CASE #: [A-Za-z0-9 :\-]{1,100}')

WSPACE = string.whitespace+r'\xc2\xa0'


########################
### Helper functions ###
########################

def generic_re_existence_helper(obj, split_text, index):
    if obj != None:
        return line_detagger(obj.group()).split(split_text)[index]
    else:
        return None

def re_existence_helper(obj):
    if obj != None:
        return line_detagger(obj.group()).split(': ')[-1]
    else:
        return None

def line_detagger(obj):
    if obj != None:
        while '***' in obj: # sometimes Pacer does, e.g., "***DO NOT FILE IN THIS CASE***"
            obj = obj.split('***')[0]+obj.split('***')[2]
        return re.sub('\<[^<]+?>', '', obj).strip('<>?! ')
    else:
        return None

def line_cleaner(string):
    if string != None:
        string = string.replace('&amp;','&').replace('&nbsp;',' ').replace('\\\'','\'').replace('\\n','')
        string = string.lstrip(')').rstrip('(')
        string = ' '.join(string.split()).strip()
        return string
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

def fill_incomplete(dicti):
    '''
    Find lawyer fields marked 'See above for address' and fill them in with the actual address
    Input:
        - a dictionary with unresolved fields
    Output:
        - the dictionary with all fields resolved
    '''
    if 'See above' in str(dicti):

        # flatten the input dict
        lawyer_list = []
        for key in dicti.keys():
            dicti[key]['counsel'] = {'None':'None'} if dicti[key]['counsel'] is None else dicti[key]['counsel'] # make nulls safe to play with
            lawyer_list.extend([(k,v) for k,v in dicti[key]['counsel'].items()])

        # resolve 'see above' entries
        for i in range(1,len(lawyer_list)):
            if 'See above' in str(lawyer_list[i]):
                name,attributes = lawyer_list[i]
                for old_name,old_attr in lawyer_list[:i]:
                    if name == old_name:
                        attributes['office'] = old_attr['office']
                        if bool(old_attr['additional_info']) and 'trial_bar_status' in old_attr['additional_info'].keys():
                            new_tbs = {'trial_bar_status': old_attr['additional_info']['trial_bar_status']}
                            attributes['additional_info'] = new_tbs if not bool(attributes['additional_info']) else attributes['additional_info'].update(new_tbs)
                        break
                lawyer_list[i] = (name,attributes)

        # re-form the input dict
        new_dicti = dicti
        for key, num_items in [(key, len(dicti[key]['counsel'])) for key in dicti.keys()]:
            new_dicti[key]['counsel'] = dict([(k,v) for k,v in lawyer_list[0:num_items]])
            new_dicti[key]['counsel'] = None if new_dicti[key]['counsel'] == {'None':'None'} else new_dicti[key]['counsel'] # revert nulls
            lawyer_list = lawyer_list[num_items:]
        return new_dicti
    else:
        return dicti

def process_entity_and_lawyers(chunk, role):
    '''
    Parse information about a party's representation (e.g. lawyer name, office, lead attorney status...)
    Inputs:
        - chunk (str): some HTML pertaining to a particular party
        - role (str): that party's role in the case (e.g. Plaintiff, Defendant...)
    Outputs:
        - name (str): the name of the person/corporation/etc comprising this party
        - chunk_answer (dict): parsed info on this party and their representation
    '''
    #Get the defendant name set up
    split_chunk = chunk.split('represented')
    name = line_cleaner(line_detagger( split_chunk[0].split('<b>')[-1].split('</b>')[0] ))
    if 'represented' in chunk:
        lawyer_dict = {}
        is_pro_se = 'PRO SE' in chunk
        for lawyer_line in split_chunk[1].split('</tr>'):
            if 'by</td>' in lawyer_line:
                lineset = lawyer_line.split('by</td>')[-1].split('<b>') # split up into individual lawyers
                for line in lineset[1:]:
                    name_split = line.split('</b>')
                    lawyer_name = line_cleaner(name_split[0])

                    if not is_pro_se:
                        office = line_cleaner(name_split[1].split('<br')[1].split('</td>')[0].strip('/<> '))
                        is_lead = True if 'LEAD ATTORNEY' in line else False
                        is_pro_hac = True if 'PRO HAC VICE' in line else False
                        office = None if office == '.' or '@' in office else office # Pacer quirks

                        addtl_info = {} # fields in this dict will vary based on what sort of other info appears in the lawyer text block
                        if 'Designation' in line:
                            addtl_info['designation'] = line.split('Designation: ')[1].split('</i>')[0]
                        if any(x in line for x in ['Trial Bar Status', 'Trial bar Status']): # this seems to appear only in ILND
                            addtl_info['trial_bar_status'] = line_cleaner(line.split('<!--')[1].split('Status: ')[1].split('-->')[0].strip('<> '))
                        elif 'Bar Status' in line:
                            addtl_info['bar_status'] = line.split('Bar Status: ')[1].split('</i>')[0]
                        lawyer_dict[lawyer_name] = {'office':office,'is_lead_attorney':is_lead,'is_pro_hac_vice':is_pro_hac,'additional_info':(addtl_info or None)}

        #Now append the lawyer info
        chunk_answer = {name:{'counsel':(lawyer_dict or None), 'is_pro_se':is_pro_se, 'roles':[role]}}
    else:
        if name == '':
            name = chunk.split('</b><br />')[0].split('<b>')[-1]
        chunk_answer = {name:{'counsel':None, 'is_pro_se':False, 'roles':[role]}}
    return (name, chunk_answer)

def process_entity_without_lawyers(chunk, role): # Handle a party with no lawyers listed (same i/o as process_entity_and_lawyers)
    name = line_cleaner(line_detagger( chunk.split('<b>')[1].split('</b>')[0] ))
    chunk_answer = {name: {'counsel':None, 'is_pro_se':False, 'roles':[role]}}
    return (name, chunk_answer)

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
    pertinent_counts = chunk.split(count_type)[-1].split('Highest Offense Level')[0]
    try:
        count_table = pd.read_html('<table>' + pertinent_counts + '</table>')[0]
        for row in count_table.values:
            meaningful_row = [line_cleaner(x.replace('\\xc2\\xa7','§')) for x in row if type(x) == str]
            if len(meaningful_row) == 2:
                counts.append({'counts':meaningful_row[0], 'disposition':meaningful_row[1]})
            elif len(meaningful_row) == 1:
                counts.append({'counts':meaningful_row[0], 'disposition':None})
    except:
        pass
    if counts == [{'counts': 'None', 'disposition': None}]:
        counts = None
    return counts

def update_party(party_dict, name, chunk_answer):
    '''
    Update the parse structure with newly parsed info about a party, merging this info with existing info about the same party if necessary
    Inputs:
        - party_dict (dict): one of the five dictionaries (plaintiff, defendant, bk_party, other_party, misc) containing info on parties
        - name (str): the name of the person/corporation/etc comprising this party, to be used as a key into party_dict
        - chunk_answer (dict): the info we want to add, which (in this implementation) will be a dict returned from process_entity_and_lawyers
    Outputs:
        - party_dict (dict): the passed-in party dictionary updated with the new info
    '''
    if name not in party_dict.keys():
        party_dict.update(chunk_answer)
    else:
        if not bool(party_dict[name]['counsel']):
            party_dict[name]['counsel'] = chunk_answer[name]['counsel']
        else:
            party_dict[name]['counsel'].update(chunk_answer[name]['counsel'] or {})
        party_dict[name]['is_pro_se'] = (party_dict[name]['is_pro_se'] or chunk_answer[name]['is_pro_se'])
        party_dict[name]['roles'] += chunk_answer[name]['roles']
    return party_dict

def process_parties_and_counts(text, is_cr):
    '''
    Parse information about a party involved in this case, as well as any charges they may be facing
    Inputs:
        - text (str): the Pacer HTML table containing info on all the parties involved in a particular case
        - is_cr (bool): flag identifying this case as a criminal case
    Outputs:
        - plaintiff (dict): info on parties with plaintiff-type roles (Plaintiff, Petitioner...)
        - defendant (dict): info on parties with defendant-type roles (Defendant, Respondent...)
        - bk_party (dict): info on parties specifically involved in bankruptcy cases (Bankruptcy Judge...)
        - other_party (dict): info on parties with other roles (Amicus...)
        - misc (dict): info on participants who are not party to the litigation but are listed in the Pacer party table (Expert, ADR Provider...)
        - pending_counts (dict): for criminal cases only, info on pending charges against the defendants
        - terminated_counts (dict): for criminal cases only, info on terminated charges against the defendants
        - complaints (dict): for criminal cases only, info on the statutes specified as the basis for the charges
    '''
    parties = {'plaintiff':{}, 'defendant':{}, 'bk_party':{}, 'other_party':{}, 'misc':{}}
    pending_counts, terminated_counts, complaints = ({},{},{}) if is_cr else (None,None,None)

    # identify party roles (i.e. bolded/underlined words), throwing out words from the criminal-counts section
    roles = set()
    terms_to_exclude = ['Pending', 'Terminated', 'Offense', 'Disposition', 'Complaints', 'Judgment', 'Order']
    for role_candidate in [x.strip() for x in re_party.findall(text)]:
        if all(x not in role_candidate for x in terms_to_exclude):
            roles.add(role_candidate)

    # get additional info from the role-mappings JSON about the roles we found
    with open(settings.ROLE_MAPPINGS, 'r') as f:
        mappings = json.load(f)
    for role in roles:
        if role not in mappings.keys():
            mappings[role] = {'title':role, 'type':'misc'} # this role isn't in the dict, so spoof an entry for it

    # process parties
    split = split_on_multiple_separators(text, ['<u>'+x for x in roles]) # add HTML '<u>' to weed out, e.g., 'Defendant' in disposition text
    for (role, chunk) in [(line_detagger(r),c) for r,c in split[1:]]: # ...and now take the '<u>' out again
        party_title = mappings[role]['title']
        party_type = mappings[role]['type']
        if 'represented' in chunk:
            name, chunk_answer = process_entity_and_lawyers(chunk, party_title)
            while len(chunk.split('represented'))>2: # edge case: no "defendant" heading (should only happen in civil cases)
                chunk = chunk.split('represented',1)[1]
                new_name, new_answer = process_entity_and_lawyers(chunk, party_title)
                parties[party_type] = update_party(parties[party_type], new_name, new_answer)
        elif '<b>' in chunk: # edge case: defendant didn't have representation
            name, chunk_answer = process_entity_without_lawyers(chunk, party_title)

        # process counts
        parties[party_type] = update_party(parties[party_type], name, chunk_answer)
        if party_type == 'defendant' and is_cr:
            ipc = process_criminal_counts(chunk, 'Pending Counts')
            pending_counts[name] = ipc
            itc = process_criminal_counts(chunk, 'Terminated Counts')
            terminated_counts[name] = itc
            ic = line_cleaner(chunk.split('<u>Complaints')[1].split('width="40%">')[2].split('</td>')[0])
            complaints[name] = None if ic == 'None' else ic

    return tuple(v for k,v in parties.items()) + (pending_counts, terminated_counts, complaints)



##################################
### Functions for member cases ###
##################################

def get_member_cases(member_block_string):
    ''' Get the member cases list from the block "Member cases: .... " '''
    return re.findall(r">(.{1,40})</a>",member_block_string)

def read_member_lead_df():
    '''
    Read the member-lead dataframe and map it to a dictionary

    Outputs:
        - member_cases (dict): a dictionary that maps leads to member list (all in ucids)
        e.x. {lead1: [mem1_1, mem1_2], lead2: [mem2_1], ...}
    '''
    # Load member case info
    df = pd.read_csv(settings.MEMBER_LEAD_LINKS)
    member_cases_series = df.groupby('lead').member.agg(list)
    member_cases = dict(member_cases_series.iteritems())
    return member_cases

def update_member_cases(lead_case, new_members_list, member_cases):
    ''' Update the in-memory and file-stored members lists for a new (lead, member_list) pair

    Inputs:
        - lead_case (str): ucid of lead case
        - new_members_list (list): a list of new member cases ucids
        - member_cases (dict): the in-memory mapping of leads to
    '''
    # Update the member_cases dict
    member_cases[lead_case] = new_members_list

    print(f"Updating member cases for <lead:{lead_case}> with {len(new_members_list):,} member cases")

    # Append to the file
    with open(settings.MEMBER_LEAD_LINKS, 'a', encoding='utf-8') as rfile:
        csv.writer(rfile).writerows( (mem, lead_case) for mem in new_members_list )

def tidy_member_cases_df():
    ''' Remove any duplicates from member cases df '''
    df = pd.read_csv(settings.MEMBER_LEAD_LINKS)
    df.drop_duplicates(inplace=True)
    df.to_csv(settings.MEMBER_LEAD_LINKS, index=False)



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

def parse_docket(docket_table):
    '''
    Get data from docket_table
    Inputs:
        - docket_table (WebElement): the docket report main table
    Output:
        data_rows (list): list of tuples with 4 entries (date(str), ind(str), entry_text(str), links(dict))
    '''
    # Get all doc ids from line doc links (second column)
    line_doc_ids = [ x.attrs.get('href').split('/')[-1]
                     for x in docket_table.select('tr td:nth-of-type(2) a') ]
    data_rows = []
    for row in docket_table.select('tr')[1:]:
        links = {}
        cells = row.select('td')
        td_date, td_ind, td_entry = cells

        # Get line doc link
        if td_ind.select('a'):
            links['0'] = td_ind.select_one('a').attrs.get('href')

        # Get attachment links
        atags = td_entry.select('a')
        # Filter out external links (by only using digit links)
        atags = [a for a in atags if a.text.strip().isdigit()]
        # Filter out references to previous lines
        atags = [a for a in atags if a.attrs.get('href').split('/')[-1] not in line_doc_ids]

        for a in atags:
            links[a.text] = {'url':a.attrs.get('href')}

        date_filed, ind, docket_text = tuple(x.text.strip(WSPACE) for x in cells)
        data_row = {
            'date_filed': date_filed,
            'ind': ind,
            'docket_text': docket_text,
            'links': links
        }
        data_rows.append(data_row)

    return data_rows

def parse_stamp(html_text, llim=-400):
    '''
    Parse the stamp left by the scraper at the bottom of a html file

    Inputs:
        - html_text (str): the html text for the page
        - llim (int): left limit of where to search in the html
    Output:
        dict with whatever k-v pairs are present (user, time, download_ulr...)
    '''
    stamp_data = {}
    re_data_str = r"<!-- SCALESDOWNLOAD;([\s\S]+) -->"

    match = re.search(re_data_str,html_text[-llim:])
    if match:
        data_str = match.groups()[0]
        for pair in data_str.split(';'):
            k,v = re.split(':', pair, maxsplit=1)
            stamp_data[k] = v
    return stamp_data


##################################################################
### Main processing function (most of the action happens here) ###
##################################################################

def process_html_file(case_dockets, member_cases, court=None):
    '''
    Processes a html Pacer file, returns a dictionary object to be saved as JSON

    Inputs:
        - case_dockets (tuple of Path objects) - html filenames
        - member_cases (dict): map of lead cases to member lists (in ucids)
        - court (str): court abbrev, if none infers from filepath
    Output:
        case_data - dictionary
    '''
    # Use the first file to pull the case name etc
    fname = case_dockets[0]

    #Get the basic case info
    case_data = {}
    case_data['case_id'] = ftools.colonize(fname.stem)
    case_data['case_type'] = ftools.decompose_caseno(case_data['case_id']).get('case_type','')
    dlcourt = fname.parents[1].name
    if court:
        case_data['download_court'] = court
    else:
        case_data['download_court'] = dlcourt
    case_data['ucid'] = dtools.ucid(case_data['download_court'], case_data['case_id'])

    #Read the html page data
    if len(case_dockets) == 1:
        html_text = str( open(fname, 'rb').read() )
        try:
            if 'Member cases:' in html_text:
                member_cases_found = True
                mem_beg,mem_end = ftools.get_member_list_span(html_text)
                soup = bs(html_text[:mem_beg]+html_text[mem_end:], 'html.parser')
            else:
                member_cases_found = False
                soup = bs(html_text, 'html.parser')
        except: # a UnicodeDecodeError pops up once in a while
            member_cases_found = False
            print(f"html-reading error ({sys.exc_info()[0]}) in {fname}")
            return None
    else:
        # When there are case updates
        soup = ftools.docket_aggregator(case_dockets)
        html_text = str(soup.html)
        if 'Member cases:' in html_text:
            member_cases_found = True
            mem_beg,mem_end = ftools.get_member_list_span(html_text)
        else:
            member_cases_found = False

    # Some fields are universal (judge, filing date, terminating date...)
    case_data['pacer_case_id'] = re_existence_helper( re_pacer_case_id.search(html_text) )
    case_data['filing_date'] = re_existence_helper( re_fdate.search(html_text) )
    case_data['terminating_date'] = re_existence_helper( re_tdate.search(html_text) )
    if case_data['terminating_date'] == None:
        case_data['case_status'] = 'open'
    else:
        case_data['case_status'] = 'closed'
    case_data['judge'] = line_cleaner(re_existence_helper( re_judge.search(html_text) ))
    case_data['referred_judge'] = line_cleaner(re_existence_helper( re_referred_judge.search(html_text) ))
    case_data['nature_suit'] = generic_re_existence_helper( re_nature.search(html_text), 'Suit: ', -1)
    case_data['jury_demand'] = generic_re_existence_helper( re_jury.search(html_text), 'Jury Demand: ', -1)
    case_data['cause'] = generic_re_existence_helper( re_cause.search(html_text), 'Cause: ', -1)
    case_data['jurisdiction'] = generic_re_existence_helper( re_jurisdiction.search(html_text), 'Jurisdiction: ', -1)
    case_data['monetary_demand'] = generic_re_existence_helper( re_demand.search(html_text), 'Demand: ', -1)
    case_data['lead_case_id'] = generic_re_existence_helper( re_lead_case_id.search(html_text), 'Lead case: ', -1)
    case_data['other_court'] = generic_re_existence_helper( re_other_court.search(html_text), '&nbsp;', -1)
    case_flags = dei.get_case_flags(html_text)
    case_data['case_flags'] = case_flags.split(",") if case_flags is not None else []

    # Other fields depend on case type (case name, parties, counts...)
    if case_data['case_type'] != 'cr' and case_data['case_type'] != 'cv':
        print(f'Case type error (type = %s) with {fname}' % case_data['case_type'])
    else:
        is_cr = bool(case_data['case_type'] == 'cr')
        title_regex = re_cr_title.search(html_text) if is_cr else re_cv_title.search(html_text)
        case_data['case_name'] = line_cleaner(generic_re_existence_helper( title_regex, 'Case title: ', -1 ))
        try:
            party_table = str(soup.select('div > table[cellspacing="5"]')[(slice(1,None,None) if is_cr else 1)]) # crim cases have more tables
        except: # when this happens, it's usually on a docket that was opened in error & that reads 'Sorry, no party found'
            if 'no party found' not in html_text: # but just in case...
                print(f"error parsing party table ({sys.exc_info()[0]}) in {fname}")
            party_table = None
        p,d,bp,op,mp,pc,tc,c = (None,None,None,None,None,None,None,None) if party_table is None else process_parties_and_counts(party_table, is_cr)
        case_data['plaintiffs'] = fill_incomplete(p)
        case_data['defendants'] = fill_incomplete(d)
        case_data['bankruptcy_parties'] = fill_incomplete(bp)
        case_data['other_parties'] = fill_incomplete(op)
        case_data['misc_participants'] = fill_incomplete(mp)
        case_data['pending_counts'] = pc
        case_data['terminated_counts'] = tc
        case_data['complaints'] = c

    # Now the docket
    no_docket_headers = [x for x in soup.find_all('h2') if re.search(ftools.re_no_docket, x.text)]
    if len(no_docket_headers) > 0:
        case_data['docket'] = []
        case_data['docket_available'] = False
    else:
        docket_table = soup.select('table')[-2]
        case_data['docket'] = parse_docket(docket_table)
        case_data['docket_available'] = True

    ### Store member cases in a csv
    if member_cases_found:
        if bool(case_data['lead_case_id']):
            case_data['member_case_key'] = dtools.ucid(case_data['download_court'], case_data['lead_case_id'])
        else:
            # If member case list but no lead case listed, this case must be the lead case
            case_data['member_case_key'] = case_data['ucid']

        if case_data['member_case_key'] not in member_cases.keys():
            # If we haven't seen this lead case before, we need to store it
            new_members_list = get_member_cases(html_text[mem_beg:mem_end])
            update_member_cases(case_data['member_case_key'], new_members_list, member_cases)
    else:
        case_data['member_case_key'] = None

    ### MDL/MULTI
    case_data['mdl_code'] , case_data['mdl_id_source'] = get_mdl_code(case_data)
    # Is an mdl if we have a code OR if an 'MDL' or 'MDL_<description>' flag exists
    case_data['is_mdl'] = bool(case_data['mdl_code']) or any(f.lower().startswith('mdl') for f in case_data['case_flags'])
    case_data['is_multi'] = any( (case_data['is_mdl'], bool(case_data['lead_case_id']), bool(case_data['member_case_key']), bool(case_data['other_court'])) )

    # Transaction data
    transaction_data = ftools.parse_transaction_history(html_text)
    case_data['billable_pages'] = int(transaction_data['billable_pages']) if 'billable_pages' in transaction_data.keys() else None
    case_data['cost'] = float(transaction_data['cost']) if 'cost' in transaction_data.keys() else None
    case_data['download_timestamp'] = transaction_data.get('timestamp','')

    # No of case dockets, will be 1 unless there are docket updates and it will be >1
    case_data['n_docket_reports'] = len(case_dockets)

    # if we're parsing from an html, the source must be Pacer (the RECAP remapper will set this field to 'recap')
    case_data['source'] = 'pacer'

    # Scraper stamp data
    stamp_data = parse_stamp(html_text)
    case_data['download_url'] = stamp_data.get('download_url')

    return case_data



####################
### Control flow ###
####################

def case_runner(case_dockets, output_dir, court, debug, force_rerun, count, member_df):
    '''
    Case parser management
    '''
    #Get the output file and check for its existence
    case_fname = Path(case_dockets[0]).stem
    outname = Path(output_dir) / f"{case_fname}.json"
    if force_rerun or not outname.exists():
        case_data = process_html_file(case_dockets, member_df, court = court)
        try:
            with open(Path(outname).resolve(), 'w+') as outfile:
                json.dump(case_data, outfile)
        except: # occasionally getting a permissions error while writing, although this should be fixed now
            print(f"json-writing error ({sys.exc_info()[0]}) in {case_fname}")
        count['parsed'] +=1
        print(f"Parsed: {outname}")
    else:
        if debug:
            case_data = process_html_file(case_dockets, member_df, court = court)
        count['skipped'] +=1
        print(f"Skipped: {outname}")

async def parse_async(n_workers, dockets_by_case, output_dir, court, debug, force_rerun, count, member_df):
    ''' Run parsing asynchronously'''

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        loop = asyncio.get_running_loop()
        tasks = (
            loop.run_in_executor(executor, case_runner, *(case, output_dir, court, debug, force_rerun, count, member_df))
            for case in dockets_by_case
        )
        asyncio.gather(*tasks)

def parse(file_dir, output_dir, court=None, debug=False, force_rerun=False, n_workers=16):

    dockets_by_case = dtools.group_dockets(Path(file_dir).glob('*html'))
    count = {'skipped':0, 'parsed': 0}

    member_cases = read_member_lead_df()

    if debug:
        for case in dockets_by_case:
            case_runner(case, output_dir, court, debug, force_rerun, count, member_cases)
    else:
        asyncio.run(parse_async(n_workers, dockets_by_case, output_dir, court, debug, force_rerun, count, member_cases))

    n = sum(count.values())
    print(f"\nProcessed {n:,} cases in {Path(output_dir)}:")
    print(f" - Parsed: {count['parsed']:,}")
    print(f" - Skipped: {count['skipped']:,}")

    tidy_member_cases_df()

@click.command()
@click.argument('file_dir')
@click.argument('output_dir')
@click.option('--court', '-c', default=None,
                help ="Court abbrv, if none given infers from directory")
@click.option('--debug', '-d', default=False, is_flag=True,
                help="Doesn't use multithreading")
@click.option('--force-rerun', '-f', default=False, is_flag=True,
                help='Parse case even if json already exists')
@click.option('--n-workers', '-nw', default=16, type=int,
                help='No. of simultaneous workers to run')
def main(file_dir, output_dir, court, debug, force_rerun, n_workers):
    ''' Parses .html casefiles in FILE_DIR and puts .json files into OUTPUT_DIR'''
    parse(file_dir, output_dir, court, debug, force_rerun, n_workers)

if __name__ == '__main__':
    main()