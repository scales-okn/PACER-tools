import re
import sys
import json
import numpy as np
from pathlib import Path
from itertools import chain
from datetime import datetime

import pandas as pd
from tqdm.autonotebook import tqdm
import zlib
import base64

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import docket_entry_identification as dei
from support import judge_functions as jf
from support import bundler as bundler
from support.core import std_path
from support import fhandle_tools as ftools

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

idb_remap = {
    'mdl_docket': 'multidistrict_litigation_docket_number',
    'origin': 'origin',
    'disposition': 'disposition',
    'mdl_status': 'mdl_status'
}

def remap_recap_data(recap_fpath=None, rjdata=None):
    '''
    Given a recap file, normalizes the process
    * recap_fpath
    output:
    *jdata
    '''

    def standardize_date(tdate):
        '''y-m-d to m/d/y'''
        if not tdate:
            return None
        try:
            y,m,d = tdate.split('-')
            return '/'.join([m, d, y])
        except AttributeError:
            return None

    def get_recap_docket(court, docket_entries):
        '''
        Remap the recap docket
        Inputs:
            - court (str): the court abbreviation
            - docket_entries (list): the value from the 'docket_entries' key in recap
        Output:
            - list of docket entries same as parsed format
        '''

        def get_doc_links(row):
            ''' Get links to documents (most rows don't have attachments, some do)'''
            links = {}
            for doc in row.get('recap_documents', []):
                # If no attachment number then it's the line document and use index of 0
                ind = int(doc.get('attachment_number') or 0)
                document_data = {
                    'url': ftools.get_pacer_url(court,'doc_link') + str(doc['pacer_doc_id']),
                    **{k: doc[k] for k in ('page_count','filepath_ia', 'description')}
                }
                links[ind] = document_data
            return links

        # rows = [
        #     {'date_filed': standardize_date(row['date_filed']),
        #      'ind': row['entry_number'],
        #      'docket_text': row['description'],
        #      'links': get_doc_links(row)
        #     }
        #     for row in docket_entries
        # ]
        rows = [
            [standardize_date(row['date_filed']),
             row['entry_number'],
             row['description'],
             get_doc_links(row)
            ]
            for row in docket_entries
        ]
        return rows

    #Load the data
    try:
        if not rjdata:
            recap_fpath = std_path(recap_fpath)
            jpath = settings.PROJECT_ROOT / recap_fpath
            rjdata = json.load(open(jpath), encoding="utf-8")
    except:
        print(f"Error loading file {recap_fpath}")
        return {}
    #Get the termiantion date
    tdate = standardize_date(rjdata['date_terminated'])
    case_status = 'closed' if tdate else 'open'

    # # Idb data
    # get_idb = lambda x: rjdata['idb_data'].get(x) if rjdata['idb_data'] else None
    # idb_data = {k: get_idb(v) for k,v in idb_remap.items()}

    # plaintiffs/defendants/criminal counts
    plaintiffs, defendants, pending_counts, terminated_counts = {}, {}, {}, {}
    has_petitioner_and_respondent = 'Petitioner' in str(rjdata['parties']) and 'Respondent' in str(rjdata['parties'])
    defendant_role, plaintiff_role = ('Respondent', 'Petitioner') if has_petitioner_and_respondent else ('Defendant', 'Plaintiff')

    for party in rjdata['parties']:
        name = party['name']

        lawyer_dict = {}
        is_pro_se = 'PRO SE' in str(party)
        if not is_pro_se and 'attorneys' in party.keys():
            for lawyer in party['attorneys']:
                office = lawyer['contact_raw'].split('\n')[0]
                is_lead = 'LEAD ATTORNEY' in str(lawyer)
                is_pro_hac = 'PRO HAC VICE' in str(lawyer)
                lawyer_dict[lawyer['name']] = {'office':office,'trial_bar_status':None,'is_lead_attorney':is_lead,'is_pro_hac_vice':is_pro_hac}

        dicti = {'counsel': (lawyer_dict or None), 'is_pro_se':is_pro_se}
        roles = [pt['name'] for pt in party['party_types']]
        if defendant_role in roles:
            dicti.update({'role':defendant_role})
            defendants[name] = dicti
        elif plaintiff_role in roles:
            dicti.update({'role':plaintiff_role})
            plaintiffs[name] = dicti
        # else: fill this in if we start caring about roles other than Plaintiff/Petitioner and Defendant/Respondent

        criminal_counts = [count for pt in party['party_types'] for count in pt['criminal_counts']]
        pending, terminated = [], []
        for cc in criminal_counts:
            cc_parsed = [cc['name'], cc['disposition']]
            if 'Count(s) remaining are dismissed' in cc['disposition']:
                terminated.append(cc_parsed)
            else:
                pending.append(cc_parsed)
        if len(pending) > 0:
            pending_counts[name] = pending
        if len(terminated) > 0:
            terminated_counts[name] = terminated

    #Convert the data
    fdata = {
        'case_flags': 'No flag data (RECAP download)',
        'case_id': ftools.clean_case_id(rjdata['docket_number']),
        'case_name': rjdata['case_name'],
        'case_status': case_status,
        'case_type': rjdata['docket_number'].split('-')[1],
        'cause': rjdata['cause'],
        'defendants': defendants,
        'demand':None,
        'docket': get_recap_docket(rjdata['court'], rjdata['docket_entries']) ,
        'download_court': rjdata['court'],
        'filing_date': standardize_date(rjdata['date_filed']),
        'has_petitioner_and_respondent': has_petitioner_and_respondent,
        'judge': rjdata['assigned_to_str'],
        'jurisdiction': rjdata['jurisdiction_type'],
        'jury_demand': rjdata['jury_demand'],
        'lead_case_id':None,
        'nature_suit': rjdata['nature_of_suit'],
        'other_court':None,
        'pending_counts':pending_counts,
        'plaintiffs': plaintiffs,
        'referred_judge': rjdata['referred_to_str'],
        'terminated_counts':terminated_counts,
        'terminating_date': tdate,
        'source':'recap',
        'ucid': ucid(rjdata['court'], ftools.clean_case_id(rjdata['docket_number'])),
        # MDL/Multi keys
        **{k:None for k in ['mdl_code', 'mdl_id_source','is_mdl', 'is_multi']},
        # Billing keys
        **{k:None for k in ['billable_pages', 'cost','n_docket_reports',]},
        # **idb_data
    }
    return fdata

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

def generate_unique_filepaths(outfile=None, to_file=False, n=None):
    '''
    Create a list of unique filepaths and export to txt
    Inputs:
        - outfile (str or Path) - the output file name (.csv) relative to the project root
        - to_file (bool) - whether or not to output the data to a .csv
        - n (int) - no. of cases to use (for testing)
    Outputs:
        DataFrame of file metadata (also output to outfile if output=True)
    '''
    import pandas as pd
    tqdm.pandas()

    pdset, ucidset = [], []

    # Check output specified before running
    if to_file and not outfile:
        raise ValueError("Argument 'outfile' cannot be empty if 'to_file' is True")

    pacer_jsons = [court_dir.glob('json/*.json') for court_dir in settings.PACER_PATH.glob('*')
                    if court_dir.is_dir()]
    recap_jsons = settings.RECAP_PATH.glob('*.json')

    #Iterate through all the json_paths
    for i, fpath in enumerate(chain(*pacer_jsons, recap_jsons)):
        print(fpath)
        #Get the temp df object
        tdf = convert_filepaths_list(file_list=[fpath])
        ucid = tdf.index[0]
        #check to make sure the ucid is not in ucidset, then append both
        if ucid not in ucidset:
            ucidset.append(ucid)
            pdset.append(tdf)

        if n and i>=n-1:
            break

    #Concatenate the pdset
    df = pd.concat(pdset)

    #Write the file
    if to_file:
        outfile = std_path(outfile)
        df.to_csv(outfile)

    return df

def convert_filepaths_list(infile=None, outfile=None, to_file=False, file_list=None, nrows=None):
    '''
    Convert the list of unique filepaths into a DataFrame with metadata and exports to csv

    Inputs:
        - infile (str or Path) - the input file, relative to the project root
        - outfile (str or Path) - the output file name (.csv) relative to the project root
        - to_file (bool) - whether or not to output the data to a .csv
        - file_list (list) - list of filepaths, bypasses infile and reads list directly
        - nrows (int) - number of rows, if none then all
    Outputs:
        DataFrame of file metadata (also output to outfile if output=True)
    '''
    import pandas as pd
    tqdm.pandas()

    # Map of keys to functions that extract their values (avoids keeping separate list of keys/property names)
    #c: case json, f: filepath
    dmap = {
        'court': lambda c,f: c['download_court'] if is_recap(f) else Path(f).parent.parent.name,
        'year': lambda c,f: c['filing_date'].split('/')[-1],
        'filing_date': lambda c,f: c['filing_date'],
        'terminating_date': lambda c,f: c.get('terminating_date'),
        'case_id': lambda c,f: ftools.clean_case_id(c['case_id']),
        'case_type': lambda c,f: c['case_type'],
        'nature_suit': lambda c,f: dei.nos_matcher(c['nature_suit'], short_hand=True) or '',
        'judge': lambda c,f: jf.clean_name(c.get('judge')),
        'recap': lambda c,f: is_recap(f),
        'is_multi': lambda c,f: c['is_multi'],
        'is_mdl': lambda c,f: c['is_mdl'],
        'mdl_code': lambda c,f: c['mdl_code']

    }

    properties = list(dmap.keys())

    def get_properties(fpath):
        ''' Get the year, court and type for the case'''
        case = load_case(fpath)
        return tuple(dmap[key](case,fpath) for key in properties)

    # Check output specified before running
    if to_file and not outfile:
        raise ValueError("Argument 'outfile' cannot be empty if 'to_file' is True")

    # Load fpaths from list or else from infile
    if file_list is not None:
        df = pd.DataFrame(list(file_list[:nrows]), columns=['fpath'])
    elif infile:
        # Read in text file of filepath names
        df = pd.read_csv(std_path(infile), names=['fpath'], nrows=nrows)
    else:
        raise ValueError("Must provide either 'infile' or 'file_list'")

    # Convert filepath to relative format
    def _clean_fpath_(x):
        p = std_path(x)
        if 'scales_data_dev' in p.parts:
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
    prop_cols = zip(*properties_vector)

    # Insert new columns, taking names from ordering of properties
    for i, new_col in enumerate(prop_cols):
        df[properties[i]] = new_col

    # Set UCID index
    df['ucid'] = ucid(df.court, df.case_id)#, series=True) #Not sure why this was here
    df = df.set_index('ucid')

    # Judge matching
    jmap = jf.unique_mapping(df.judge.unique())
    df.judge = df.judge.map(jmap)

    columns = properties.copy()
    columns.insert(2,'fpath')

    if to_file:
        outfile = std_path(outfile)
        df[columns].to_csv(outfile)
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
    import pandas as pd
    # datecols = ['filing_date', 'terminating_date']

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

def load_case(fpath, html=False, recap_orig=False):
    '''
    Loads the case given its filepath

    input:
        fpath (string/Path): a path relative to the project roots
        html (bool): whether to return the html (only works for Pacer, not Recap)
        recap_orig (bool): whether to return the original recap file, rather than the mapped

    output:
        the json of the case (or html if html is True)
    '''
    # Standardise across Windows/OSX and make it a Path object
    fpath = std_path(fpath)

    # # Check filepath is relative to project root
    # if fpath.parts[0] != 'data':
    #     raise ValueError('File should be in the data folder and filepath should be relative to project folder (ex. data\pacer\ilnd\json...)')

    # Absolute path to json file
    jpath = settings.PROJECT_ROOT / fpath

    # Recap
    if 'recap' in jpath.parts:
        if html:
            raise ValueError('HTML files do not exist for Recap files')
        if recap_orig:
            return json.load( open(settings.PROJECT_ROOT / jpath, encoding="utf-8") )
        else:
            return remap_recap_data(settings.PROJECT_ROOT / jpath)
    # Pacer
    else:
        if html:
            hpath = get_pacer_html(jpath)
            if hpath:
                return str( open(settings.PROJECT_ROOT / hpath, 'rb').read() )
            else:
                raise FileNotFoundError('HTML file not found')
        else:
            jdata = json.load( open(settings.PROJECT_ROOT / jpath, encoding="utf-8") )
            jdata['case_id'] = ftools.clean_case_id(jdata['case_id'])

            # Idb data
            get_idb = lambda x: jdata['idb_data'].get(x) if jdata.get('idb_data') else None
            idb_data = {k: get_idb(v) for k,v in idb_remap.items()}
            jdata.update(idb_data)

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
def ucid(court, case_id, clean=False):
    '''
    Generate a unique case id (ucid)

    Inputs:
        - court (str or Series): court abbreviation
        - case_id (str or Series): either colon or hyphen format, will be standardised
        - clean (bool): whether the case_id is already clean (speeds up calculation)
    Output:
        (str) like 'nced;;5:16-cv-00843'
    '''
    if type(case_id)==pd.Series:
        if not clean:
            return court + ';;' + case_id.map(ftools.clean_case_id)
        else:
            return court + ';;' + case_id
    else:
        if not clean:
            return f"{court};;{ftools.clean_case_id(case_id)}"
        else:
            return f"{court};;{case_id}"

get_ucid = ucid

def ucid_from_scratch(court, office, year, case_type, case_no):
    ''' Generate a ucid from all base elements (year is 2-digit)'''
    if type(court)==pd.Series and type(case_id)==pd.Series:
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
        return ucid.str.replace(rf"{ftools.re_com['office']}:", r':')
    else:
        return re.sub(rf"{ftools.re_com['office']}:", r':', ucid)

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

def group_dockets(docket_fpaths):
    '''
    Group dockets by case

    Inputs:
        - docket_fpaths (list of str): list of docket filepaths
    Outputs:
        - a list of tuples of docket filepaths (one tuple per case)
    '''

    df = pd.DataFrame(docket_fpaths, columns=['fpath'])
    df['case_no'] = df.fpath.apply(lambda x:Path(x).stem).map(ftools.colonize)

    # Sort on filename, assumes that this builds the correct order for updated filenames
    df.sort_values('fpath', inplace=True)

    df['decomposed'] = df.case_no.map(ftools.decompose_caseno)
    df['case_id'] = df.decomposed.map(ftools.build_case_id)

    cases = df.groupby('case_id')['fpath'].apply(tuple).to_list()
    return cases

# n.b.: compress_data and decompress_data may no longer be needed after updates to parse_pacer.py (11/2/20)
def compress_data(data):
    '''Compress complex data into a JSON-serializable format (written to generate the 'member_cases' field in JSONs from parse_pacer.py)'''
    return base64.b64encode(zlib.compress(json.dumps(data).encode('utf-8'),9)).decode('utf-8')

def decompress_data(data):
    '''Reverse the effects of compress_data'''
    return json.loads(zlib.decompress(base64.b64decode(data.encode('utf-8'))).decode('utf-8'))

def parse_transaction_history(html_text):
    '''
    Parse the text of the transaction history receipt table

    Inputs:
        - html_text (str): text of the page
    Output:
        - a dict of results, keys from re named groups (timestamp, user, ... etc)
    '''

    # Srub the html tags from the text
    html_text = dei.scrub_tags(html_text)

    re_transaction_history = \
        r"Transaction Receipt (?P<timestamp>[\s\S]+?)\s+"\
        r"Pacer Login:\s*(?P<user>[a-zA-Z0-9]+)[\s\S]+?"\
        r"Description:\s+(?P<description>[\s\S]+?)"\
        r"\s+[a-zA-Z\s]+:\s+(?P<search_criteria>[\s\S]+?)"\
        r"\s+Billable Pages:\s+(?P<billable_pages>\d+)"\
        r"\s+Cost:\s+(?P<cost>\d+\.\d+)"

    match =  re.search(re_transaction_history, html_text, re.I)
    return match.groupdict() if match else {}