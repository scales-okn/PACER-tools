import sys
import re
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import data_tools as dtools
from support import fhandle_tools as ftools

# Import the nature of suit Spreadsheet
nos_file = settings.DATAPATH / 'annotation' / 'nature_suit.csv'
df_nos = pd.read_csv(nos_file)
df_nos['composite'] = df_nos.name + ' ' + df_nos.major_type

re_mdl = r"MDL\s*(no.|[-_])?\s*(?P<code>\d{2,5})"

def date_transformer(x):
    import datetime
    try:
        return datetime.datetime.strptime(x, '%m/%d/%Y')
    except:
        return datetime.datetime.strptime('01/01/1900', '%m/%d/%Y')

def clean_nature_suit(x):
    import re
    try:
        result = re.search('[0-9]{2,3}', str(x))
        if result:
            return result.group()
        else:
            return None
    except TypeError:
        return None

def identify_judge(docket_block):
    import spacy
    nlp = spacy.load('en_core_web_sm')

    honorifics = ['Magistrate', 'Honorable', 'Judge']

    judge_names = []
    for dline in docket_block:
        doc = nlp(dline[-1])
        for ent in doc.ents:
            if ent.label_ == 'PERSON':
                if doc[ent.start-1].text in honorifics:
                    judge_names.append(ent.text)

    if len(judge_names)==0:
        judge_name = ' '
    elif len(judge_names)==1:
        judge_name =  judge_names[0]
    else:
        from collections import Counter
        counted = Counter(judge_names)
        #If names are all equal then pull the first name, since that typically comes first during a reassignment
        if len( set(counted.values()) ) == 1:
            judge_name = judge_names[0]
        else:
            judge_name = counted.most_common()[0][0]
    return judge_name


def check_docket(dlines):
    '''
    checks to see if there are transfers in the docket
    '''
    #handle the check
    if len(dlines)>0:
        entry_text = string_sanitizer(dlines[0][-1])
    else:
        entry_text = ''
    #Conditions on transfers
    if len(dlines)==0:
        return False
    elif 'EXECUTIVE COMMITTEE ORDER' in entry_text:
        return False
    elif len([x for x in dlines if type(x[-1])==float])>0:
        return False
    elif len([x for x in dlines if 'clerical error' in x[-1]])>0:
        return False
    elif 'in error' in entry_text:
        return False
    elif 'shall not be used for any other proceeding' in entry_text:
        return False
    else:
        return True


def identify_district(jfhandle):
    '''
    cleans the filename to identify the district
    '''
    state_trans = {'northern-georgia': 'gand', 'northern-illinois':'ilnd',
            'northern-indiana':'innd', 'southern-indiana':'insd',
            'southern-georgia':'gasd'}

    district = jfhandle.split('pacer/')[-1].split('/')[0]
    if district in state_trans:
        return state_trans[district]
    else:
        return district

def string_sanitizer(avar):
    '''
    Sanitizes a variable to a string
    '''
    avar = nonetype_sanitizer(avar)
    if type(avar) !='str':
        return str(avar)
    else:
        return avar

def nonetype_sanitizer(avar):
    '''
    Ensures that the variable is not a none type
    '''
    if type(avar) not in [str, int, float]:
        return ''
    else:
        return avar


def assign_entry_to_judge(dentry, judge_ents, dentry_index=None):
    '''
    Assigns a judge to a a single entry
    input:
        * dentry -- str, docket entry text
        * judge_ents -- list, list of spacy JUDGE entitites
        * dentry_index -- int (opt),  docket entry index for debugging purposes
    output:
        * clean_name -- str, cleaned judge name if found otherwise it is empty string ""
    '''
    import sys
    sys.path.append('..')
    import support.judge_functions as jof
    import support.language_tools as langtools

    #If we have one name, then we're effectively done otherwise -- handle the circumstances
    judge_lnames = [ent.text.strip(' .()') for ent in judge_ents]
    if len( list(set(judge_lnames)) ) > 1:
        #There is a (re)assignment of the case
        if 'assign' in dentry.lower():
            #Get the min index
            min_index = langtools.nearest_ent_index('assign', dentry.lower(), judge_ents)
            #My min index is the judge
            return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
        #Case is referred
        elif 'referr' in dentry.lower():
            #Get the min index
            min_index = langtools.nearest_ent_index('referr', dentry.lower(), judge_ents)
            #My min index is the judge
            return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
        #case is reference to
        elif 'reference' in dentry.lower():
            #Get the min index
            min_index = langtools.nearest_ent_index('reference', dentry.lower(), judge_ents)
            #My min index is the judge
            return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
        #Case is transferred
        elif 'transferr' in dentry.lower():
            #Get the min index
            min_index = langtools.nearest_ent_index('transferr', dentry.lower(), judge_ents)
            #My min index is the judge
            return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
        #Multiple judge names....what to do..base it off the text
        else:
            #Then there's multiple judges with before due to scheduling statements
            #Take the judge that signed the order scheduling
            if dentry.lower().count('before')>1 and 'signed' in dentry.lower():
                min_index = langtools.nearest_ent_index('signed', dentry.lower(), judge_ents)
                #My min index is the judge
                return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
            else:
                #If only one before or >1 before but with no sign, then we just want to
                #pull the first before, because it's the most likely one for the current entry
                if 'before' in dentry.lower():
                    min_index = langtools.nearest_ent_index('before', dentry.lower(), judge_ents)
                    #My min index is the judge
                    return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
                #If there wasn't a before, then we'll take a signatory
                elif re.search('si[gned ]{4,5}', dentry.lower()):
                    min_index = langtools.nearest_ent_index('si[gned ]{4,5}', dentry.lower(), judge_ents)
                    return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
                #Synonym for signatory
                elif 'order' in dentry.lower():
                    min_index = langtools.nearest_ent_index('order', dentry.lower(), judge_ents)
                    return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
                #Judgment by
                elif 'judgment by' in dentry.lower():
                    min_index = langtools.nearest_ent_index('judgment by', dentry.lower(), judge_ents)
                    return jof.clean_name(judge_ents[min_index].text, punc=False, lower=False, suffix=False, prefix=False)
                #Adding judges to the docket, no way to know who's in charge
                elif 'added' in dentry.lower():
                    return ';;;'.join([jof.clean_name(x.text, punc=False, lower=False, suffix=False, prefix=False) for x in judge_ents])
                #Conflict memo -- no real change
                elif 'conflict memo' in dentry.lower():
                    return ''
                else:
                    # print('eof', case_num, dentry_index)
                    return ''
    else:
        #Judge name found
        if judge_ents:
            return jof.clean_name(judge_ents[0].text, punc=False, lower=False, suffix=False, prefix=False)
        #No judge name found
        else:
            return ''

def backfill_judge_assignment(judge_track):
    '''
    Backfills a judge list
    input:
        judge_track: list, [judge name, '', '', ....]
    output:
        judge_track: list, [judge name, judge name, judge name, ....]
    '''
    def state_machine(jlist):
        cur_name = ''
        for i, name in enumerate(jlist):
            #Skip addition entries
            if ';;;' not in name:
                #Update the cur_name if name is not ''
                if name != '':
                    cur_name = name
                jlist[i] = cur_name
        return jlist

    #If there's only one judge name just save the processing time
    if len(set(judge_track))>1:
        #Forward pass
        judge_track = state_machine( judge_track )
        #Backwards pass
        judge_track = state_machine( list(reversed(judge_track)) )
        #Reverse the list back to normal orientation
        judge_track.reverse()
    return judge_track

def identify_judge_entriesv1(jfhandle = None, docket = None, djudge = ''):
    '''
    Attributes each docket entry to a judge.
    V1 -- Regex rules to identify docket 'chunks' and then do block attribution to chunk
          Returned [[district, case_id, judge_name, nos, clean_nos, entry number, entry text], ...]
    V2 -- Spacy langauge model to identify judge names on per entry basis. Entries without attribution are done via
          a forward and then backwards backfilling routine, assigning the closest attributed judge to the entry
          Returns [judge name, judge name, ... ] that will be the same length as the inputed docket
    input:
       * jfhandle -- str (opt), Filename for the json file
       * docket -- list (opt), default is None. Will supercede the json file
    output:
       * judge_ind_entries -- list, [judge name, judge name, ...]
    '''
    def _clean_name(judge_name, punc=True):
        #deletions
        titles = ['Judge ', 'Senior Judge ', 'Magistrate Judge ', 'Chief Judge ', 'Honorable ']
        puncs = ['\.', ',']
        #Clean the titles
        try:
            for title in titles:
                if title in judge_name:
                    judge_name = judge_name.split(title)[-1]
                if punc == True:
                    for punc in puncs:
                        judge_name = re.sub('.,', '', judge_name)
                judge_name = judge_name.strip(' ')
        except TypeError:
                pass
        return judge_name

    import json
    import re
    import spacy
    import sys
    sys.path.append('..')
    import support.data_tools as dtools
    import support.settings as settings

    #Input check
    if jfhandle == None and docket == None:
        print('INPUT ERROR: No docket or json filehandle provided. Returning None')
        return None

    #Load the data
    if not docket:
        case = dtools.load_case(jfhandle)
        docket = case['docket']
        djudge = case['judge']

    #Data and transfer phrase
    judge_ind_entries = []
    transfer_phrase = 'EXECUTIVE COMMITTEE ORDER: It appearing that cases previously assigned'

    #Check to see if there are transfers
    dcheck = check_docket(docket)
    transfer_indices = []
    if dcheck:
        for i, dline in enumerate(docket):
            if transfer_phrase in dline[-1]:
                transfer_indices.append(i)
    #If there are transfers, divide it up
    if transfer_indices != []:
        #need to check against the last index being the last entry
        if transfer_indices[-1] == len(docket)-1 and len(transfer_indices)==1:
            tindices = [0,-1]
        elif transfer_indices[-1] == len(docket)-1:
            tindices = [0] + transfer_indices
        #If not then iterate normally with the last entry
        else:
            tindices = [0] + transfer_indices + [-1]
        #Block iteration
        if tindices:
            #Iterate through the blocks
            for ti, block_index in enumerate(tindices[:-1]):
                end_index = tindices[ti+1]
                if end_index == -1:
                    docket_block = docket[block_index:]
                else:
                    docket_block = docket[block_index:end_index+1]
                pjudge = identify_judge(docket_block)
                if pjudge==' ':
                    if default_attr == True:
                        pjudge = djudge
                judge_ind_entries += [nonetype_sanitizer( _clean_name(pjudge,punc=False) ) for x in docket ]
    #Only one judge
    else:
        #Error case check
        wrong_assignment=False
        for dline in docket:
            try:
                if 'assigned in error' in dline[-1]:
                    wrong_assignment = True
                elif 'assigned' in dline[-1] and 'in error' in dline[-1]:
                    wrong_assignment = True
                elif 'clerical error' in dline[-1]:
                    wrong_assignment = True
                elif 'shall not be used for any other proceeding' in dline[-1]:
                    wrong_assignment = True
            except TypeError:
                #Type error is a suppressed case
                print(district, case_id)
        #If the wrong assignment is there, use the function to identify the judge
        if wrong_assignment == True:
            pjudge = identify_judge(docket)
        else:
            pjudge = djudge
        #Now add the lines
        judge_ind_entries += [nonetype_sanitizer( _clean_name(pjudge, punc=False) ) for x in docket]
    return judge_ind_entries


def identify_judge_entries(jfhandle = None, docket = None):
    '''
    Attributes each docket entry to a judge.
    V1 -- Regex rules to identify docket 'chunks' and then do block attribution to chunk
          Returned [[district, case_id, judge_name, nos, clean_nos, entry number, entry text], ...]
    V2 -- Spacy langauge model to identify judge names on per entry basis. Entries without attribution are done via
          a forward and then backwards backfilling routine, assigning the closest attributed judge to the entry
          Returns [judge name, judge name, ... ] that will be the same length as the inputed docket
    input:
       * jfhandle -- str (opt), Filename for the json file
       * docket -- list (opt), default is None. Will supercede the json file
    output:
       * judge_ind_entries -- list, [judge name, judge name, ...]
    '''
    import json
    import re
    import spacy
    import sys
    sys.path.append('..')
    import support.data_tools as dtools
    import support.settings as settings

    exclusions = ['EXECUTIVE COMMITTEE', 'Executive Committee', 'executive committee', 'GENERAL', 'General', 'general']

    #Input check
    if jfhandle == None and docket == None:
        print('INPUT ERROR: No docket or json filehandle provided. Returning None')
        return None

    #Load the data
    if not docket:
        case = dtools.load_case(jfhandle)
        docket = case['docket']

    #Load custom language model to recognize judge names and titles
    jnlp = spacy.load(settings.PROJECT_ROOT / 'code' / 'support' / 'language_models' / 'judgemodel/')

    judge_track = []
    for i, dline in enumerate(docket):
        # If it's a string and not odd formatting
        if type(dline[-1]) == str:
            #Find the names and titles
            doc = jnlp(dline[-1])
            #pull out the judge ents
            judge_ents = [ent for ent in doc.ents if ent.label_ == 'JUDGE' and ent.text not in exclusions]
            #Append the identified name
            judge_track.append( assign_entry_to_judge(dline[-1], judge_ents, dentry_index=i) )
    #Backfill code
    judge_track = backfill_judge_assignment(judge_track)
    return judge_track


def nos_matcher(nos, short_hand=False):
    '''
    Look up a 'nature of suit' string and map it to a row from the nature of suit spreadsheet
    inputs:
        - nos(str): the nature of suit string from the data
        - short_hand(bool): whether to return shorthand (Number: composite) or else the full row as a dictionary
    ouput:
        (dict): a dictionary corresponding to the row from the nature of suit spreadsheet
    '''
    # Shorthand representation to return
    nos_repr = lambda win_row: f"{win_row['number']}: {win_row['composite']}"

    if type(nos) not in (str,int) or nos in ['',' ']:
        return None

    # Try to find a code at the start of
    if type(nos) in [int,float]:
        code = str(int(nos))
    else:
        code = nos.split()[0].rstrip(':').lower()


    if code.isdigit():
        result = df_nos.query('number == @code')
        if len(result) == 1:
            win_row = result.iloc[0].to_dict()
            return nos_repr(win_row) if short_hand else win_row
        else:
            return None

    else:
        # If code is missing, try to match it against the nature of suit name and major_type text (composite column)
        nos_clean = "".join([x for x in nos if x not in '():-']).lower()
        nos_clean_words = nos_clean.split()

        # Give a match score for how close the nos is to each composite
        winning_score = -1
        for ind, row in df_nos.iterrows():
            category_words = row.composite.lower().split()

            #Calculate score by number of words in the nos that appear in the given composite
            match_score = sum([(word in category_words) for word in nos_clean_words]) / len(nos_clean_words)

            #Update the highest score for the given nos being matched
            if match_score > winning_score:
                winning_score = match_score
                win_row = row.to_dict()
                winning_category = nos_repr(win_row) if short_hand else win_row
            # Exit early if perfect match
            if match_score == 1:
                return winning_category

        # If the winning match is higher than 50%, this is the category
        # Except if two words in nos_clean_words, preventing a 1 out of 2 match
        if winning_score >= 0.5 and len(nos_clean_words)>2:
            return winning_category
        elif winning_score > 0.5:
            return winning_category

        return None

def get_case_flags(html_string):
    '''
    Get's the case flags for a case (top-right hand corner of case html)

    inputs:
        html_string: the case html as a string
    output:
        an comma-delimited string of the case flags e.g. 'CLOSED,SEALED'
    '''
    re_flag_line = r'''<table.+?<td align="right">.+?</table>'''
    flag_line = re.search(re_flag_line, html_string, re.DOTALL)
    if flag_line:
        results = re.findall(r'''<span.*?>([\w\d\s\-()]+)</span>''',flag_line.group())
        if results:
            return ",".join(results)

def mdl_code_from_string(string):
    ''' Search for mdl_code in a certain string'''
    mdl_match = re.search(re_mdl, string, re.IGNORECASE)
    if mdl_match:
        return int(mdl_match.groupdict()['code'])

def mdl_code_from_casename(casename):
    try:
        casename_data = ftools.decompose_caseno(casename)
    except ValueError:
        pass

    try:
        casename_data = ftools.decompose_caseno(casename, pattern=ftools.re_mdl_caseno_condensed)
    except ValueError:
        return None

    if casename_data['case_type'].lower() in ['md','ml', 'mdl']:
        return int(casename_data['case_no'])

def get_mdl_code(mdl_data, html_string=None, case=None):
    '''
    Identify the mdl_code for a case. Must supply either html_string or case

    Inputs:
        fpath (str or Path): filepath for the case
        mdl_data (dict): the output from mdl_check method
        html_string (str): the html string for the case
        case (dict): the case json data from dtools.load_case
    Output:
        code (int): the mdl code
        source_code (str): the source used to identify the code
    '''

    if not (html_string or case):
        raise ValueError('Must supply either html_string or case')


    # Check lead case
    lead_case_id = mdl_data.get('lead_case_id')
    if lead_case_id:
        code = mdl_code_from_casename(lead_case_id)
        if code:
            return (code, 'lead_case_id')

    # Check html for flags or other reference
    if html_string:
        flags = get_case_flags(html_string)
        if flags:
            code = mdl_code_from_string(flags)
            if code:
                return (code, 'flag')

        # Check rest of html for string
        code = mdl_code_from_string(html_string)
        if code:
            return  (code, 'html_string')

    elif case:
        # Check idb_mdl
        code = case.get('mdl_code')
        if code:
            return (code, 'idb_mdl_docket')
        # Check if it's an mdl case
        casename = case.get('case_id')
        code = mdl_code_from_casename(casename)
        if code:
            return (code, 'casename')

        # Search the docket
        docket = case.get('docket')

        if docket and len(docket[0])==3:
            try:
                for line in docket:
                    code = mdl_code_from_string(line[2])
                    if code:
                        return (code, 'docket')
            except:
                pass

    return None,None

def identify_mdl_from_json_data(case):
    ''' MDL identification based on idb fields in case json and from docket lines'''
    is_multi_idb = (case['origin'] in [6,13] or case['disposition']==10 or case['mdl_docket'])
    if is_multi_idb:
        mdl_data = {
            'is_multi': True,
            'origin': 'internal' if case['origin']==13 else \
                       'external' if case['origin']==6 else None,
            'disposition': (case['disposition']==10), # disposed of by mdl,
            'mdl_docket_no': case['mdl_docket'],
            'recap_mdl_status': case['mdl_status'],
            'source_identification': 'json'
        }
    else:
        mdl_data = {'is_multi': False}

    # Check for code regardless (may identify based on docket)
    mdl_data['mdl_code'], mdl_data['source_code']= get_mdl_code(mdl_data, case=case)
    # Coerce is_multi, make it true if mdl_code found
    mdl_data['is_multi'] = mdl_data['is_multi'] or  (mdl_data['mdl_code'] != None)

    return mdl_data

def identify_mdl_from_html(html):
    ''' MDL identification based on case html'''

    def id_from_atag(tag_string):
        ''' Get the case no. from within an <a> tag '''
        return tag_string.split('</a>')[0].split('>')[-1]


    idx_filed = html.index('date filed')
    re_mdl_flag = "(?<![a-z])mdl(?![a-z])"
    case_flags = get_case_flags(html)
    flag_found = bool(re.search(re_mdl_flag, get_case_flags(html), re.I)) if case_flags else False

    if not (flag_found or re.search('member cases?|case in other court', html[:idx_filed])):
        return {'is_multi': False}

    # Start from member cases link to avoid lead case
    try:
        idx_member = html.index('member case')
        truncated_member_list = bool(re.search(ftools.re_members_truncated, html[idx_member:idx_filed], re.I) )
        member_cases = [id_from_atag(atag) for atag in re.findall(ftools.re_case_link, html[idx_member:idx_filed], re.I)]
        has_member_cases = truncated_member_list or len(member_cases)

        lead_case_link = re.search(ftools.re_lead_link, html[:idx_filed], re.I)
        lead_case_id = id_from_atag(lead_case_link.group()) if lead_case_link else None
    except ValueError:
        member_cases, lead_case_link, lead_case_id = None,None,None
        has_member_cases = False

    # Get the "Case in other court: {court}, {case} data"
    other_court_line_match = re.search(ftools.re_other_court, html[:idx_filed], re.I)
    if other_court_line_match:
        try:
            # Extract the text
            other_court_line = other_court_line_match.group().split('</td>')[-2].split('<td>')[-1].strip()
            # Separate the court from the case no.
            vals = [x.strip() for x in other_court_line.split(',')[-2:]]
            other_court_court, other_court_case = vals
        except ValueError:
            other_court_court, other_court_case = None, vals[0]
    else:
        other_court_court, other_court_case = None, None

    mdl_data = {
        'is_multi': True,
        'is_lead_case': (lead_case_id==None and has_member_cases),
        'has_member_cases': has_member_cases,
        'member_cases': member_cases,
        'lead_case_id': lead_case_id,
        'in_other_courts': bool(other_court_line_match),
        'other_court_court': other_court_court,
        'other_court_case': other_court_case,
        'source_identification': 'html' if not flag_found else 'flag'
    }

    mdl_data['mdl_code'], mdl_data['source_code'] = get_mdl_code(mdl_data, html_string=html)
    return mdl_data

def mdl_check(fpath, case_jdata=None):
    '''
    Check if a case is an MDL case and return relevant data

    Inputs:
        fpath (str): the case fpath
    Outputs:
        mdl_data (dict) : data relating to the mdl
    '''

    def exclude(mdl_data):
        ''' Scenario where a multi case is excluded from being an mdl'''
        return  mdl_data.get('in_other_courts') \
                and not mdl_data.get('has_member_cases',False) \
                and not mdl_data.get('lead_case_id')

    # Recap
    if dtools.is_recap(fpath):
        case_jdata = dtools.load_case(fpath) if not case_jdata else case_jdata
        mdl_data = identify_mdl_from_json_data(case_jdata)

    # Pacer
    else:

        # Try html first
        case_html = dtools.load_case(fpath, html=True).replace('&nbsp;', ' ').lower()
        mdl_data_html = identify_mdl_from_html(case_html)
        if mdl_data_html['is_multi'] and mdl_data_html['mdl_code']:
            mdl_data = mdl_data_html
        else:
            # Then try idb from json
            case_jdata = dtools.load_case(fpath) if not case_jdata else case_jdata
            mdl_data_json = identify_mdl_from_json_data(case_jdata)
            if mdl_data_json['is_multi'] and mdl_data_json['mdl_code']:
                mdl_data = mdl_data_json

            # Otherwise: aggregate, prioritise html data
            else:
                mdl_data = mdl_data_html
                # Make sure is_multi returns true if either is true
                mdl_data['is_multi'] = mdl_data_html['is_multi'] or mdl_data_json['is_multi']
                mdl_data['source_identification'] = mdl_data_html.get('source_identification')  or mdl_data_json.get('source_identification')

    # is_mdl true if identification comes from a flag
    if mdl_data.get('source_identification')=='flag' or mdl_data.get('source_code')=='flag':
        mdl_data['is_mdl'] = True

    # Otherwise look at exclusion rule
    else:
        mdl_data['is_mdl'] = mdl_data['is_multi'] and not exclude(mdl_data)

    return mdl_data

def scrub_tags(html_string):
    ''' Remove all html tags from a string of html source string'''
    return re.sub(r"<[\s\S]+?>", '', html_string)