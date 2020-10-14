import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import court_functions as cf

int_magistrate = 9999999
committee_names = {'Respondent':10000001, 'Unassigned':10000002, \
                   'Movant':10000003, 'Fugitive Calendar':10000004, \
                   'Executive Committee':10000005, 'Status':10000006, \
                   '':10000000}
judgey = ["judge", "magistrate", "mag", "chief", "ch", "honorable", "justice", \
             "district", "court", "us", "u.s.", "senior", "sr", \
            "appellate", "circuit", "visiting", "hon", "hon.",\
            "designated", 'unassigned', 'the']
judge_na = ["unassigned"]
suffixes = ['referred', 'unassigned']
suffixes_titles = ['i', 'ii', 'iii', 'iv', 'v', 'jr','jnr', 'snr', 'sr']

#Defaults
def generate_default_courtdf():
    courtdf_default = pd.read_csv(settings.COURTFILE)
    return courtdf

def generate_default_jdf():
    jdf_default = pd.read_csv(settings.JUDGEFILE, index_col=0)
    #Create fullName column
    if 'FullName' not in jdf_default.columns:
        print('Building FullName Column')
        FullName = jdf_default.apply(lambda row: ' '.join([str(x) for x in row[['First Name', 'Middle Name', 'Last Name','Suffix',]] if not pd.isnull(x)]), axis=1)
        FullName = FullName.apply(lambda row: row.replace('   ', ' '))
        jdf_default.insert(2, 'FullName', FullName)
        jdf_default.to_csv(settings.JUDGEFILE)

    #Convert columns to timestamp
    for i in range(1,7):
        for col in [f"Commission Date ({i})", f"Senior Status Date ({i})"]:
            jdf_default[col] = pd.to_datetime(jdf_default[col])
    return jdf

def clean_name_field(name_field):
    '''
    Cleans name from html and preserves ttile information
    input:
        * name field
    output:
        * name of judge (if valid) or None
    '''
    #Process the judge field
    if name_field != None:
        judge_name = name_field.split(' Demand:')[0].strip()
    else:
        judge_name =  ''
    #Return the none type if the judge name is valid
    if judge_name.strip() not in committee_names:
        return judge_name
    else:
        return None

def clean_name(judge_name, punc=True, lower=True, suffix=True, prefix=True):
    '''
    Clean up a judge name for better matching
    Inputs:
        - judge_name (str): Judge's name, including titles
        - punc (bool): whether to strip punctuation
    Output:
        Name is returned in lower case without titles and punctuation
        Example: 'District Judge Curtis V. Gomez' -> 'curtis v gomez'
    '''
    from string import punctuation

    puncs = ['.', ',', "'", '[',']']

    #Do the short stack of nothing
    if judge_name in ['', None, ' ']:
        return ''
    #Otherwise attempt a host of potential changes
    try:
        #Clean out a finding with a parenthetical
        if '(' in judge_name:
            judge_name = judge_name.split('(')[0]

        # Remove punctuation
        if punc == True:
            for p in puncs:
                judge_name = judge_name.replace(p, '')

        if lower==True:
            nlist = [x.lower() for x in judge_name.split()]
        else:
            nlist = judge_name.split()

        # Look for instances of non-listed judge
        if nlist[0] in judge_na:
            return ''

        # Remove prefixes
        if prefix==True:
            while nlist[0].lower() in judgey:
                nlist = nlist[1:]
                if len(nlist) == 0:
                    return ''

        # Remove suffixes
        if suffix==True:
            for suff in suffixes:
                if suff in nlist:
                    nlist = nlist[0 : nlist.index(suff)]

        # Return to a string
        judge_name = " ".join(nlist)
        judge_name = judge_name.strip(punctuation + ' ')

    except TypeError:
        print("TypeError with", judge_name, type(judge_name), len(judge_name))
        return ''
    except IndexError:
        print("IndexError with", judge_name, type(judge_name), len(judge_name))
        return ''
    return judge_name

def unique_mapping(judge_list, cleaned=True):
    '''
        Take a list of judges and return a unique mapping
        Maps names with middle intials back to "First Last" *if* that exists in list
        Example "John Reilly"=> "john reilly" and "John C. Reilly" => "john reilly"

        Inputs:
            - judge_list (list) - a list of strings of judge names
            - cleaned (bool) - if the list contains cleaned names
        Output:
            jmap (dict) - a mapping of names from judge_list to cleaned names
    '''
    jmap = {}

    # Create a clean judge list
    judges_list_clean = judge_list if cleaned else [clean_name(x) for x in judge_list]

    # Iterate through original input list
    for i, judge in enumerate(judge_list):

        jstr = judge if cleaned else judges_list_clean[i]
        jlist = jstr.split()

        if len(jlist) > 2:
            first_last = f"{jlist[0]} {jlist[-1]}"
            unique = first_last if first_last in judges_list_clean else judge
        else:
            unique = judge

        # Add to dictionary
        jmap[judge] = unique

    return jmap

def part_of_name_tagger(name):
    '''
    part of name tagger for a name
    Need to identify first, middle initial, last name, suffix
    '''
    pass

def identify_last_name(name):
    '''
    Identify the last name of a person
    '''
    #Has a jr, sr, numeral
    if ', ' in name:
        last_name = name.split(', ')[0].split(' ')[-1]
    else:
        last_name = name.split(' ')[-1]
    return last_name

def identify_last_name_clean(name):
    cname = clean_name(name)
    cname_parts = cname.split()
    if cname_parts[-1] in suffixes_titles:
        return cname_parts[-2]
    else:
        return cname_parts[-1]

def identify_judge_anno_pk(court, last_name, annodf):
    '''
    Using the court and last name, find the pk in the annotation dataframe
    '''
    court_format = 'U.S. District Court for the %s District of %s'
    cardinal, state = court.split('-')
    instantiated = court_format % (cardinal.capitalize(), state.capitalize())
    possible_nids = annodf[(annodf['Court Name (1)']==instantiated) & (annodf['Last Name']==last_name)].nid.values.tolist()
    if len(possible_nids) ==0:
        return None
    else:
        return possible_nids[0]

def name_similarity_matcher(name, jdf):
    '''
    inputs:
        * name - from docket, can be uncleaned with honorarium
        * jdf - shortened possible list of names matched on court and last name
    output:
        * bool - Match found
        * Series - the row of jdf that matches judge
    '''

    import support.text_functions as tfunc
    from collections import Counter
    #Clean our name
    cname = clean_name(name)
    source_parts = cname.split(' ')
    #Create the full judge name and clean
    fullnames = [x.strip().split(' ') for x in jdf.FullName.apply(clean_name)]
    firstnames = [x.strip() for x in jdf['First Name'].apply(clean_name)]
    for i, fname in enumerate(firstnames):
        # If first is just an initial, replace with middle name
        if len(fname) == 1:
            firstnames[i] = clean_name(jdf.iloc[i]['Middle Name'])

    lastnames = [x.strip() for x in jdf['Last Name'].apply(clean_name)]
    overlaps = []
    for fullname, firstname, lastname in zip(fullnames, firstnames, lastnames):
        if firstname in cname and lastname in cname:
            overlaps.append(1)
        else:
            overlaps.append(0)
    #Cosine similarity
    jdf.loc[:, 'SimScore'] = overlaps
    subdf = jdf[jdf.SimScore==1]
    if len(subdf)==0:
        return False, None
    else:
        return True, subdf[subdf.SimScore==subdf.SimScore.max()].iloc[0]


def district_or_magistrate(name, court_abbrev, date=None):
    '''
    Identify whether a (judge, court, date) triple is a district or magistrate judge.
    inputs:
        name - full name of the judge from the docket
        court_abbrev - four char abbreviation of court
        date - date of the event that the judge was involved in that we are checking, if null will return 'district' if the judge was ever a 'district' judge in their lifetime
    outputs:
        return 'district','other', 'Magistrate'

    ASSUMPTION: In a given district, if a judge is not a district judge then they're a Magistrate
    '''
    date = pd.to_datetime(date)

    nid = identify_judge_nid(name, court_abbrev, date)

    if nid < int_magistrate:
        return 'district'
    elif nid == int_magistrate:
        return 'magistrate'
    else:
        return None

def filter_by_court(df, court, date=None):
    '''
    Filter function to reduce to df to rows/judges with appointments in specified court
    inputs:
        df (DataFrame): the jdf or a subset
        court (str): the court in abbreviated fomat ex. ilsd
        date (timestamp): the date of the judge event you are checking
    output:
        DataFrame: subset of the original df that's filtered by court
    '''
    # Get the full court title
    court_full = cf.abbr2full(court)
    if not court_full:
        return pd.DataFrame([])

    def _filter_row(row, court_full, date):

        s_null = row.isnull()
        # Loop through all groupings of columns - ...(1),..(1), ... (2), ..(2), .... (6)
        for i in range(1,7):
            col_court = 'Court Name (%s)' % i
            col_comm = 'Commission Date (%s)' % i

            #If court name empty, then grouping is empty and ALL subsequent groupings empty
            if (s_null[col_court]):
                return False
            # If value in col_court matches court_format,
            elif(row[col_court] == court_full):
                #Extra filter layer if a date supplied
                if (date):
                    if (row[col_comm] < date):
                        return True
                else:
                    #If court matches and date not supplied
                    return True
        return False

    return df[df.apply(lambda row: _filter_row(row, court_full, date), axis=1)].copy()

def find_district_judge(name, court, date = None, jdf=None, courtdf=None):
    '''
        Locate a row in the judge_demographics spreadsheet for a given judge

        inputs:
         name - full name of the judge from the docket
         court - four char abbreviation of court

         outputs:
            bool - match found or not
            Series - the row from judge df
    '''
    # TODO remove?
    if type(name) is not str:
        return False, None
    if name.strip() in committee_names:
        return committee_names[name.strip()]

    #Load in the data if it isn't passed
    if type(courtdf)==type(None):
        courtdf = generate_default_courtdf()
    if type(jdf)==type(None):
        jdf = generate_default_jdf()

    # Filter just last name first
    last_name = identify_last_name_clean(name)
    jdf_matches = jdf[jdf['Last Name'].str.match(last_name, case=False)].copy()

    # Single match check name similarity (skip court filter)
    if len(jdf_matches) == 1:
        return name_similarity_matcher(name, jdf_matches)
    # No match
    elif len(jdf_matches) == 0:
        return False, None

    # Multiple matches
    else:
        # Filter by court
        jdf_matches = filter_by_court(jdf_matches, court, date)

        # Single match, return row
        if len(jdf_matches) == 1:
            return True, jdf_matches.iloc[0]
        # No match
        elif len(jdf_matches) == 0:
            return False, None
        else:
            return  name_similarity_matcher(name, filter_by_court(jdf, court, date))

def identify_judge_nid(name, court, date=None, jdf=None, fill_name_court=False):
    '''
        Find a judge's nid

        Inputs:
            name (str): judge's name
            court (str): court abbreviation
            date (str/DateTime): the date to check for
            jdf (DataFrame): judge dataframe or subset to look in
            fill_name_court (bool): whether to return a faux-id of "{name};;{court}"
                                    instead of 9999999 for magistrate judges
    '''
    # TODO- CHECK?
    if name.strip() in committee_names:
        return committee_names[name.strip()]

    date = pd.to_datetime(date)
    match_found, match = find_district_judge(name, court, date, jdf)

    if match_found:
        return match['nid']
    else:
        if fill_name_court:
            # Encoding name-court in place of an nid for magistrate judges
            return f"{name};;{court}"
        else:
            return int_magistrate

def is_senior(name, court, date=None):
    '''
        Identify if a judge has senior status, for a given (judge, court, date) triple
        inputs:
            name - full name of the judge from the docket
            court - four char abbreviation of court
            date - date of the event that the judge was involved in that we are checking,
                if null will return True if the judge was ever a senior judge in their lifetime
        outputs:
            bool - True if they are senior
    '''

    date = pd.to_datetime(date)
    match_found, match = find_district_judge(name, court, date)
    if not match_found:
        # Assumption: if not find, likely a magistrate judge so not senior
        return False

    # Look through result row in senior columns TODO: Generalise this?
    for i in range(1,7):
        col_court = f"Court Name ({i})"
        col_snr = f"Senior Status Date ({i})"

        # If court is emtpy then there are no subsequent appointments
        if pd.isnull(match[col_court]):
            return False

        elif match[col_court] == cf.abbr2full(court):
            # If date supplied, compare
            if date:
                if (match[col_snr] < date):
                    return True
            else:
                # If court matches and date not supplied just return true
                return True
        else:
            # Look at next appointment
            continue

    return False
