import re
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import data_tools as dtools
from support import court_functions as cfunc
from support import fhandle_tools as ftools

re_header_case_id = re.compile('DOCKET FOR CASE #: [A-Za-z0-9 :\-]{1,100}')

# Import the nature of suit Spreadsheet
df_nos = pd.read_csv(settings.NATURE_SUIT)
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
        doc = nlp(dline['docket_text'])
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
        entry_text = string_sanitizer(dlines[0]['docket_text'])
    else:
        entry_text = ''
    #Conditions on transfers
    if len(dlines)==0:
        return False
    elif 'EXECUTIVE COMMITTEE ORDER' in entry_text:
        return False
    elif len([x for x in dlines if type(x['docket_text'])==float])>0:
        return False
    elif len([x for x in dlines if 'clerical error' in x['docket_text']])>0:
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
                    judge_name = re.sub(punc, '', judge_name)
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
            if transfer_phrase in dline['docket_text']:
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
                    pjudge = djudge
                    # if default_attr == True:
                    #     pjudge = djudge
                judge_ind_entries += [nonetype_sanitizer( _clean_name(pjudge,punc=False) ) for x in docket ]
    #Only one judge
    else:
        #Error case check
        wrong_assignment=False
        for dline in docket:
            try:
                if 'assigned in error' in dline['docket_text']:
                    wrong_assignment = True
                elif 'assigned' in dline['docket_text'] and 'in error' in dline['docket_text']:
                    wrong_assignment = True
                elif 'clerical error' in dline['docket_text']:
                    wrong_assignment = True
                elif 'shall not be used for any other proceeding' in dline['docket_text']:
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


def identify_judge_entriesv2(jfhandle = None, docket = None):
    '''
    Attributes each docket entry to a judge.
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
        if type(dline['docket_text']) == str:
            #Find the names and titles
            doc = jnlp(dline['docket_text'])
            #pull out the judge ents
            judge_ents = [ent for ent in doc.ents if ent.label_ == 'JUDGE' and ent.text not in exclusions]
            #Append the identified name
            judge_track.append( assign_entry_to_judge(dline['docket_text'], judge_ents, dentry_index=i) )
    #Backfill code
    judge_track = backfill_judge_assignment(judge_track)
    return judge_track


def identify_judge_entries(jfhandle = None, docket = None, djudge = None):
    '''
    Attributes each docket entry to a judge.
    v3 - will eventually call the judge-entity Postgres database, but currently just falls back to v2
    input:
       * jfhandle -- str (opt), Filename for the json file
       * docket -- list (opt), default is None. Will supercede the json file
    output:
       * judge_ind_entries -- list, [judge name, judge name, ...]
    '''
    return identify_judge_entriesv1(jfhandle=jfhandle, docket=docket, djudge=djudge)


def nos_matcher(nos, short_hand=False):
    '''
    Look up a 'nature of suit' string and map it to a row from the nature of suit spreadsheet
    inputs:
        - nos (str): the nature of suit string from the data
        - short_hand (bool): whether to return shorthand ("{number} {name}" e.g. "830 Patent") or else the full row as a dictionary
    ouput:
        (dict or str): a dictionary corresponding to the row from the nature of suit spreadsheet if short_hand=False, else str
    '''
    # Shorthand representation to return
    nos_repr = lambda win_row: f"{win_row['number']} {win_row['name']}"

    if type(nos) not in (str,int) or nos in ['',' ']:
        return None

    # Try to find a code at the start of
    if type(nos) in [int,float]:
        code = str(int(nos))
    else:
        code = nos.split()[0].rstrip(':').lower()

    if code.isdigit():
        code = int(code) # not sure why this cast newly became necessary (new nos file?), but it seems to do the trick!
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
        win_row = None
        for ind, row in df_nos.iterrows():
            category_words = row.composite.lower().split()

            # Calculate score by number of words in the nos that appear in the given composite
            match_score = sum([(word in category_words) for word in nos_clean_words]) / len(nos_clean_words)

            # Update the highest score for the given nos being matched
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

def is_docket_table(table):
    '''
    Identify a table as being a docket entry table
    Inputs:
        - table (bs4.element.Tag)
    Output:
        (bool) True if it's a docket table, False otherwise
    '''
    try:
        first_cell_text = table.select_one('td').text.strip().lower()
        return (first_cell_text == 'date filed')
    except:
        return False

def extract_court_caseno(hstring, debug_name=''):
    '''
    Take the string of a docket report html and detect the caseno and court

    Inputs:
        - hstring (str): string of the docket report html
        - debug_name (str): a debug name for the case to print out if it can't find them in the header
    Output:
        - court (str): court abbreviation for the court (runs through court_functions.classify)
        - case_no (str): the uncleaned case_no for the case, can have judges initials e.g. "1:16-cv-00001-ABC-DEF"
    '''

    # Grab the "DOCKET FOR CASE: ......." text
    header_match = re.search(re_header_case_id, hstring)
    if not header_match:
        print(f"Couldn't find case header in {debug_name}")
        return

    # Extract the case_no from that
    case_no_match = re.search(ftools.re_case_name, header_match.group(0))
    if not case_no_match:
        print(f"Couldn't find case_no in header for {debug_name}")
        return
    else:
        case_no = case_no_match.group(0)


    # Grab the text from the start of the <h3> tag up until the "DOCKER FOR CASE" TEXT
    ind = header_match.span()[0]
    court_header_str = hstring[ind-200:ind].split('<h3',maxsplit=1)[1]

    # Remove anything in parenthesis because division can interfere otherwise e.g. "California Central District (Western Division)" -> "cawd"
    court_header_str = re.sub(r'\([^\)]+\)', '', court_header_str)
    # Run it through the classifier
    court = cfunc.classify(court_header_str)

    return court, case_no

def identify_docket_table(soup):
    '''
    Identify which table in a docket report is the docket sheet table.
    Uses identification on the first cell text equalling 'Date Filed'

    Inputs:
        - soup (bs4.BeautifulSoup): the soup of the entire page of a docket sheet html
    Output:
        (bs4.element.Tag) The tag that is the docket table.
        Returns None if no docket found (including if the matches the criteria of ftools.re_no_docket)
    '''
    # Return none if any of the 'no docket' language detected
    if any(x for x in soup.find_all('h2') if re.search(ftools.re_no_docket, x.text)):
        return None


    # Iterate in reverse over all tables
    tables = soup.select('table')
    for table in tables[::-1]:
        first_cell = table.select_one('td,th')

        # Catch for if table has no cells
        if not first_cell:
            continue
        # Strip and lower text of the first cell
        elif first_cell.text.lower().strip() == 'date filed':
            return table