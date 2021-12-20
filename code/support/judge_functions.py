import pandas as pd
import numpy as np
from scipy import stats
import datetime
import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import court_functions as cf
from support import data_tools as dtools
from support import fhandle_tools as ftools

DF_SEL, DF_JEL = None, None
NAN_NAMES = {'not_found': 'XXNaNXX', 'committee': 'XXNon-IndividualXX'}
IGNORE_CASE = 'Void'

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

today = datetime.datetime.today()
timestamp = str(today.day) + str(today.month) + str(today.year)
DATAPATH = 'data/'



####################
### File getters ###
####################

def get_sel(court=None, reload_sel=False):
    '''
    Get the spacy entity lookup DataFrame, if it's first time calling it will read in the dframe

    Inputs:
        - court (str): optional, if supplied (and if first time calling getter),
        it will filter sel down to only that court
    Output:
        (pd.DataFrame) the sel dataframe

    '''
    global DF_SEL
    if type(DF_SEL) == type(None) or reload_sel:
        print("Reading in SEL...")
        DF_SEL = pd.read_csv(settings.DF_SEL,dtype={'SCALES_JID':str})
        print("...Complete")

        if court:
            print(f"Reducing SEL... to only {court}")
            DF_SEL= DF_SEL[DF_SEL.ucid.apply(lambda x: court in x)]

        # Mapping span and scales_ind of 'header' matches to -1
        DF_SEL['char_span_start'] = DF_SEL['char_span_start'].apply(lambda x: int(x) if x!='H' else -1)
        DF_SEL['char_span_end'] = DF_SEL['char_span_end'].apply(lambda x: int(x) if x!='H' else -1)
        DF_SEL['docket_index'] = DF_SEL['docket_index'].apply(lambda x: int(x) if x!='H' else -1)
    return DF_SEL

def get_jel():
    ''' Get the judge entity lookup DataFrame, if it's first time calling it will read in the dframe'''
    global DF_JEL
    if type(DF_JEL) == type(None):
        print("Reading in JEL...")
        DF_JEL = pd.read_json(settings.JEL_JSONL, lines=True)
        DF_JEL.loc[~DF_JEL.NID.isna(), "NID"] = DF_JEL.NID.apply(lambda x: str(x).split('.')[0])
        print("...Complete")
    return DF_JEL


def load_SEL(ucid = None, as_df = True):
    if not ucid:
        print("Please specify the ucid or list of ucids to load")

    if type(ucid) != list:
        ucid = [ucid]

    SEL_rows = []
    for each in ucid:
        # create filepath
        fname = ftools.build_sel_filename_from_ucid(each)
        # load file
        results = []
        if fname.exists():
            with open(fname, 'r') as json_file:
                json_list = list(json_file)
                for json_str in json_list:
                    results.append(json.loads(json_str))

        SEL_rows+=results
    
    # return dataframe
    if SEL_rows:
        if as_df:
            SEL = pd.DataFrame(SEL_rows)
        else:
            SEL = SEL_rows
        
        return SEL
    else:
        return "No data found"

######################################################
### Judge ID functions for build_judge_ifp_data.py ###
######################################################

def encrypt_judge(name):
    if name not in NAN_NAMES.values():
        return dtools.sign(name.encode('UTF-8'))
    else:
        return name

def jid_to_judge_name(jid):
    if jid not in NAN_NAMES.values():
        JEL = get_jel()
        subdf = JEL[JEL['SCALES_JID']==float(jid)]
        if len(subdf.index) == 1:
            return subdf.iloc[0]['Presentable_Name']
    return None

def check_neighborhood_exact_match(ucid, starting_point, order, statuses):

    preceding = [stat for stat in statuses if stat[0] < starting_point]
    succeeding = [stat for stat in statuses if stat[0] > starting_point]

    preceding = sorted(preceding, key = lambda tup: tup[0], reverse=True)
    succeeding = sorted(succeeding, key = lambda tup: tup[0], reverse=False)

    if order == 'preceding_first':
        run = [preceding, succeeding]
    else:
        run = [succeeding, preceding]

    for each in run:

        for i, line in enumerate(each):
            exact = check_if_direct_entry_match(ucid, line[0],line[2])
            if exact:
                return exact

    return None

def check_mode_between_lines(ucid, app_line, reso_line):
    SEL = get_sel()
    docket_lines = SEL[SEL.ucid ==ucid]

    relevant_lines = docket_lines[(docket_lines.docket_index <= reso_line) &
                                 (docket_lines.docket_index >= app_line)]
    relevant_lines = relevant_lines[~relevant_lines.SCALES_JID.isna()]

    if len(relevant_lines) == 0:
        #print("No Judges Between IFP Lines")
        return None

    mode_judges = relevant_lines[['SCALES_JID']].mode()
    if len(mode_judges) == 1:
        SJID = mode_judges.SCALES_JID.iloc[0]
        if pd.isnull(SJID):
            return None
        else:
            return SJID
    else:
        #print("Multi-Modal")
        return None


def check_only_one_span_overlap(span_of_interest, SEL_spans):
    SEL = get_sel()
    sois = span_of_interest['start']
    soie = span_of_interest['end']
    # if only 1 SEL span starts within the bounds of the ifp span or ends within the bounds
    keeps = []
    for span in SEL_spans:
        starter = span[0]
        ender = span[1]
        if starter <= soie and starter >=sois:
            keeps.append(span)
        elif ender >= sois and ender <= soie:
            keeps.append(span)

    if len(keeps) == 1:
        match = keeps[0]
        return (match[0],match[1])
    else:
        return None

def check_if_direct_entry_match(ucid, line_of_interest, span_of_interest):
    SEL = get_sel()
    docket_lines = SEL[SEL.ucid ==ucid]

    direct_line_match = docket_lines[docket_lines.docket_index == line_of_interest]
    # unique scales judges for this exact line
    dlm = direct_line_match.copy()
    dlm.drop_duplicates('SCALES_JID', inplace=True)
    dlm = dlm[~dlm.SCALES_JID.isna()].copy()

    # no direct match, use other logic
    if len(dlm) == 0:
        return None
    elif len(dlm) == 1:
        SJID = dlm.SCALES_JID.iloc[0]
        if pd.isnull(SJID):
                return None
        else:
            return SJID

    elif len(dlm)>1:
        #print("MORE THAN ONE MATCH")
        SEL_spans = [(i,j) for i, j in zip (direct_line_match.char_span_start.values, direct_line_match.char_span_end.values)]

        single_span_overlap = check_only_one_span_overlap(span_of_interest, SEL_spans)
        if single_span_overlap:
            new_dlm = direct_line_match[(direct_line_match.char_span_start == single_span_overlap[0]) &
                                        (direct_line_match.char_span_end == single_span_overlap[1])]

            SJID = new_dlm.SCALES_JID.iloc[0]
            if pd.isnull(SJID):
                    return None
            else:
                return SJID
        else:
            # sentences splitting logic
            #{'Mapping': 'Needs Additional Logic Multi-DLM'}
            return None

def number_judges_ucid(ucid):
    SEL = get_sel()
    docket_lines = SEL[SEL.ucid ==ucid]

    actual_judges = docket_lines[~docket_lines.SCALES_JID.isna()]
    if len(actual_judges)>0:
        total_docket_judges = len(actual_judges.SCALES_JID.unique())
        return total_docket_judges
    else:
        return 0

def single_judge_ucid_check(ucid):
    n_judges = number_judges_ucid(ucid)

    return n_judges == 1

def check_if_single_judge(ucid):
    SEL = get_sel()
    if single_judge_ucid_check(ucid):
        docket_lines = SEL[SEL.ucid ==ucid]
        the_judge = docket_lines[~docket_lines.SCALES_JID.isna()]
        SJID = the_judge.SCALES_JID.iloc[0]
        if pd.isnull(SJID):
            return None
        else:
            return SJID

    else:
        return None

def take_header_judge(ucid):
    SEL = get_sel()
    docket_lines = SEL[SEL.ucid==ucid]
    header = docket_lines[docket_lines.docket_index==-1]
    header = header[~header.SCALES_JID.isna()].copy()
    if len(header)>0:
        ## check for assigned judge first
        if 'Assigned_Judge' in header.Entity_Extraction_Method.values:
            this_judge = header[header.Entity_Extraction_Method == 'Assigned_Judge']
            SJID = this_judge.SCALES_JID.iloc[0]
            return SJID
        else:
            # take the referred_judge
            this_judge = header[header.Entity_Extraction_Method == 'Referred_Judge']
            SJID = this_judge.SCALES_JID.iloc[0]
            return SJID
    else:
        return None

def jed_sel_crosswalker(ucid, resolution, statuses, application_line, debug=False):

    if debug:
        return NAN_NAMES['not_found']

    # LOGIC: no resolution ucids
    if resolution == None: # no line flagged as the resolution line
        # IF OUR ENTITY LIST ONLY HAS A SINGLE JUDGE TIED TO THIS CASE, WIN EARLY AND ASSIGN THEM
        single_judge = check_if_single_judge(ucid)
        if single_judge:
            return single_judge

        app_line = [tup for tup in statuses if tup[1] == 'application' and tup[0] == application_line][0]
        spans_of_interest = app_line[2]
        direct_line = check_if_direct_entry_match(ucid, application_line, spans_of_interest)
        if direct_line:
            return direct_line

        neighbor = check_neighborhood_exact_match(ucid, application_line, 'succeeding_first', statuses)
        if neighbor:
            return neighbor

        # find mode of all IFP Lines
        min_line = min([t[0] for t in statuses])
        max_line = max([t[0] for t in statuses])
        modey = check_mode_between_lines(ucid, min_line, max_line)
        if modey:
            return modey

        header = take_header_judge(ucid)
        if header:
            return header

        return NAN_NAMES['not_found']

    # LOGIC: grant or deny
    else:
        # local vars
        line_of_interest = resolution[0] # index in scales
        endstate = resolution[1] # resolution label
        spans_of_interest = resolution[2] # ifp span relative to docket entry

        # LOGIC: resolution is on line 0, need to know if application came with it
        if line_of_interest == 0:
            # all line 0 statuses
            relevant = [stat for stat in statuses if stat[0]==0]

            # if there is an application, then we are good for attribution check
            if any(stat for stat in relevant if stat[1]=='application'):
                # good to go there is a pairing

                # IF OUR ENTITY LIST ONLY HAS A SINGLE JUDGE TIED TO THIS CASE, WIN EARLY AND ASSIGN THEM
                single_judge = check_if_single_judge(ucid)
                if single_judge:
                    return single_judge

                direct_line = check_if_direct_entry_match(ucid, line_of_interest, spans_of_interest)
                if direct_line:
                    return direct_line

                neighbor = check_neighborhood_exact_match(ucid, line_of_interest,'preceding_first', statuses)
                if neighbor:
                    return neighbor

                min_line = min([t[0] for t in statuses])
                max_line = max([t[0] for t in statuses])
                modey = check_mode_between_lines(ucid, min_line, max_line)
                if modey:
                    return modey


                header = take_header_judge(ucid)
                if header:
                    return header

                return NAN_NAMES['not_found']

            # no application, void ucid
            else:
                # bad, no pairing
                return IGNORE_CASE

        # LOGIC: RESOLUTION NOT ON LINE 0
        else:
            # attribution hierarchy
            # IF OUR ENTITY LIST ONLY HAS A SINGLE JUDGE TIED TO THIS CASE, WIN EARLY AND ASSIGN THEM
            single_judge = check_if_single_judge(ucid)
            if single_judge:
                return single_judge

            direct_line = check_if_direct_entry_match(ucid, line_of_interest, spans_of_interest)
            if direct_line:
                return direct_line

            neighbor = check_neighborhood_exact_match(ucid, line_of_interest, 'preceding_first', statuses)
            if neighbor:
                return neighbor

            modey = check_mode_between_lines(ucid, application_line, line_of_interest)
            if modey:
                return modey

            header = take_header_judge(ucid)
            if header:
                return header

            return NAN_NAMES['not_found']

        return NAN_NAMES['not_found']



####################################
### Judge significance functions ###
####################################

def bootstrap_ttest(df, min_cases=5, exclude_prisoner=False, outliers_prop=0):

    '''
    Check for statistical significance of variation of judge grant.
    Compares individual judge to average from all other judges within their district

    Inputs:
        - df (pd.DataFrame): data frame (expected columns: court, judge_name, case_id, resolution)
        - min_cases (int): skip judge if they have less that this many cases
        - outliers_prop(float): proportion (out of 1) of outliers to exclude based on decision_average e.g. 0.1 -> excludes <5% and >95%
    Output:
        (pd.DataFrame) results table with ['judge_name','court', 'diff', 'lb', 'ub']

    '''
    checkdf = df.copy()
    if outliers_prop > 0:
        out_lb = outliers_prop/2
        out_ub = 1 - out_lb
        checkdf = checkdf[(checkdf.decision_average>=out_lb) & (checkdf.decision_average<=out_ub)].copy()
    if exclude_prisoner:
        checkdf = checkdf[checkdf.nature_of_suit_prisoner==0].copy()

    checkdf = checkdf.loc[:, ['court', 'judge_name', 'case_id', 'resolution']].copy()
    checkdf.columns = ['court',  'judge', 'ucid', 'grant']

    judge_data = []
    courts = [x for x in checkdf.court.unique() if x!='nmid']
    for court in courts:
        #Just subset to keep the naming shorter
        cdf = checkdf[checkdf.court == court]
        #Get the judge list
        judges = cdf.judge.unique()
        #District differences
        for j in judges:
            jdf = cdf[cdf.judge==j]
            njdf = cdf[cdf.judge!=j]

            if len(jdf)>min_cases and len(njdf)>0:
                mu_1, var_1 = np.mean(jdf.grant), np.var(jdf.grant)
                mu_2, var_2 = np.mean(njdf.grant), np.var(njdf.grant)
                s_1 = np.std(jdf.grant)
                s_2 = np.std(njdf.grant)
                Ndf = len(cdf) - 2
                diff = (mu_1-mu_2)

                #even samples
                sp_2 =  (((len(jdf)-1)*s_1**2) + ((len(njdf)-1)*s_2**2))/ Ndf
                se = np.sqrt(s_1**2/len(jdf) + s_2**2/len(njdf))
                t = diff/se
                #se = np.sqrt(var_1/len(jdf.grant) + var_2/len(njdf.grant))
                lb = diff - stats.t.ppf(0.975, Ndf)*se
                ub = diff + stats.t.ppf(0.975, Ndf)*se

                #Uneven samples
                se = np.sqrt(s_1**2/len(jdf) + s_2**2/len(njdf))
                d = diff/se
                nndf = (se**2)**2/( (s_1**2/len(jdf))**2/(len(jdf)-1) + (s_2**2/len(njdf))**2/(len(njdf)-1) )
                if np.sign(diff) == -1:
                    lb = diff + stats.t.ppf(0.975, nndf)*se
                    ub = diff - stats.t.ppf(0.975, nndf)*se
                else:
                    lb = diff - stats.t.ppf(0.975, nndf)*se
                    ub = diff + stats.t.ppf(0.975, nndf)*se

                judge_data.append([j, court, diff, lb, ub])

    scidf = pd.DataFrame(judge_data, columns = ['judge_name','court', 'diff', 'lb', 'ub'])

    identify_sig = lambda row: int(np.sign(row['lb'])==np.sign(row['ub']) )

    scidf['sig'] = scidf.apply(identify_sig, axis=1)
    # print(f"Proportion significant: {scidf.sig.sum()/len(scidf)}")
    return scidf

def create_judge_var_sig_tables(dataset):
    '''
    Check for judge variance signifcance with boostrapping.
    Ouputs two tables (1) 'judge_var_sig.csv' and an aggregated (2) 'judge_var_sig_lookup.csv'

    Inputs:
        - dataset (pd.DataFrame or str/Path): dataframe formatted for living report
    '''

    if type(dataset) is not pd.DataFrame:
        # Read in dataset if argument is a string or path
        dataset = pd.read_csv(dataset)

    # Run separate bootstrap for excl_prisoner_outliers and not
    bt1 = bootstrap_ttest(dataset)
    bt2 = bootstrap_ttest(dataset, exclude_prisoner=True, outliers_prop=0.1)

    bt1['excl_prisoner_outliers'] = False
    bt2['excl_prisoner_outliers'] = True
    bt_both = pd.concat([bt1,bt2])
    bt_both.to_csv(DATAPATH + f'judge_var_sig_{timestamp}.csv', index=False)

    # Aggregate by court to produce a lookup table
    dfvar = bt_both.groupby(['excl_prisoner_outliers','court']).sig.agg(['sum','count','mean']).reset_index()
    # Reduce columns and rename
    dfvar = dfvar[ ['court','excl_prisoner_outliers','sum', 'count','mean'] ]
    dfvar.columns = ['court','excl_prisoner_outliers','jsum', 'jcount','jmean']

    dfvar = dfvar.sort_values('court').reset_index(drop=True)
    threshold = 0.05
    dfvar['isSig'] = dfvar.jmean > threshold
    dfvar.to_csv(DATAPATH + f'judge_var_sig_lookup_{timestamp}.csv',index=False)





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
