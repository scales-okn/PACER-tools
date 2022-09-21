import re
import sys
import json
import simplejson
import csv
import numpy as np
from pathlib import Path

import click
import pandas as pd
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import court_functions as cf
from support import data_tools as dtools

# District codes
with open(settings.DATAPATH/'annotation'/'fjc_district_codes.json', 'r') as rfile:
    code2abbr = json.load(rfile)

# Constants
FJC_PERIOD_10TO20 = "10to20"
DATE_NA = "1900-01-01"

# Dictionary that maps idb column to dict with datatypes and recap key name
IDB_COLS = {
    'AMTREC': {'dtype': np.int16, 'recap_key': 'amount_received'},
    'ARBIT': {'dtype': str, 'recap_key': 'arbitration_at_filing'},
    'CIRCUIT': {'dtype': np.int16, 'recap_key': 'circuit'},
    'CLASSACT': {'dtype': np.int16, 'recap_key': 'class_action'},
    'COUNTY': {'dtype': np.int64, 'recap_key': 'county_of_residence'},
    'DEF': {'dtype': str, 'recap_key': 'defendant'},
    'DEMANDED': {'dtype': np.int16, 'recap_key': 'monetary_demand'},
    'DISP': {'dtype': np.int16, 'recap_key': 'disposition'},
    'DISTRICT': {'conv': lambda x: code2abbr[x], 'recap_key': 'district'},
    'DOCKET': {'dtype': str, 'recap_key': 'docket_number'},
    'FILEDATE': {'conv': pd.Timestamp,'recap_key': 'date_filed'},
    'JUDGMENT': {'dtype': np.int16, 'recap_key': 'judgment'},
    'JURIS': {'dtype': np.int16, 'recap_key': 'jurisdiction'},
    'MDLDOCK': {'dtype': str, 'recap_key': 'multidistrict_litigation_docket_number'},
    'NAME': {'dtype':str, 'recap_key':'defendant'},
    'NOJ': {'dtype': np.int16, 'recap_key': 'nature_of_judgement'},
    'NOS': {'conv': lambda x: str(int(float(x))), 'recap_key': 'nature_of_suit'},
    'OFFICE': {'dtype': str, 'recap_key': 'office'},
    'ORIGIN': {'dtype': np.int16, 'recap_key': 'origin'},
    'PLT': {'dtype': str, 'recap_key': 'plaintiff'},
    'PROCPROG': {'dtype': np.int16, 'recap_key': 'procedural_progress'},
    'PROSE': {'dtype': np.int16, 'recap_key': 'pro_se'},
    'RESIDENC': {'dtype': np.int16, 'recap_key': 'diversity_of_residence'},
    'SECTION': {'dtype': str, 'recap_key': 'section'},
    'SUBSECT': {'dtype': str, 'recap_key': 'subsection'},
    'TAPEYEAR': {'dtype': np.int16, 'recap_key': 'year_of_tape'},
    'TERMDATE': {'conv': pd.Timestamp, 'recap_key': 'date_terminated'},
    'TITL': {'dtype': str, 'recap_key': 'title'},
    'TRANSDAT': {'dtype': str, 'recap_key': 'date_transfer'},
    'TRANSDOC': {'dtype': np.int64, 'recap_key': 'transfer_docket_number'},
    'TRANSOFF': {'dtype': str, 'recap_key': 'transfer_office'},
    'TRANSORG': {'dtype': str, 'recap_key': 'transfer_origin'},
    'TRCLACT': {'dtype': np.int16,'recap_key': 'termination_class_action_status'},
    'TRMARB': {'dtype': str, 'recap_key': 'arbitration_at_termination'}
}
BARE_MIN_COLS = ['DOCKET','OFFICE', 'DISTRICT', 'FILEDATE']

def make_ucid_and_weak(docket, office, district, case_type):
    '''
    Make the ucid and weak_ucid from idb data. Can take str values for a single row,
    or can take series and output as Series.

    Inputs:
        - docket (str or Series): idb 'docket' looks like 1600123 for year=16, case_no=00123
        - office (str or Series): idb office #
        - district (str or Series): court abbreviation e.g. 'ilnd'
        - case_type (str): 'cv' or 'cr'

    Outputs:
        - ucid (str or Series): looks like 'ilnd;;1:16-cv-00123'
        - ucid_weak (str or Series): ucid with office removed, looks like 'ilnd;;16-cv-00001'
    '''
    # Find the ucid and weak_ucid
    # data = {k:row[columns.index(k)] for k in ['DOCKET', 'DISTRICT', 'OFFICE']}
    if type(docket) is pd.Series:
        case_year = docket.str.slice(0,2)
        case_no = docket.str.slice(2,)
    else:
        case_year = docket[:2]
        case_no = docket[2:]

    # court = IDB_COLS['DISTRICT']['conv'](data['DISTRICT'])

    ucid = dtools.ucid_from_scratch(district, office, case_year, case_type, case_no)
    ucid_weak = dtools.get_ucid_weak(ucid)
    return ucid, ucid_weak

def load_idb_csv(fpath, case_type, nrows=None, all_cols=False, cols=[]):
    '''
    Load fjc data from a .csv (a sas file that's been converted by fjc.convert_sas7).
    Converts the district to abbreviation, manages datatypes according to IDB_COLS,
    puts all column names in lowercase, fixes na dates encoded as 1900,
    creates ucid and weak_ucid columns

    Inputs:
        - fpath (str or Path): the path to load e.g. settings.FJC/'cv10to20.csv'
        - case_type (str): must specify 'cr' or 'cv' (because of different datasets/columns)
        - nrows (int): nrows to load from the csv if not all
        - all_cols (bool): whether to load all columns, if false loads recap columns
        - cols (list): specific list of columns to load if all_cols=False
    Output:
        DataFrame
    '''
    if all_cols:
        usecols = None # Pandas returns all cols
    elif cols:
        usecols = cols
    else:
        # Get all the recap columns
        usecols =  [x.upper() for x in get_recap_idb_cols(case_type)]

    # csvs = [settings.IDB/f"{case_type}{year}.csv" for year in years]
    # for file in csvs:
    #     if not file.exists():
    #         print(f"The file {file.name} does not exist, skipping")
    #         csvs.remove(file)
    #
    # df_list = []
    #
    # for file in csvs:
    #     df_year = pd.read_csv(file, nrows=nrows, encoding="utf-8", usecols=usecols,
    #                  converters={k:v['conv'] for k,v in IDB_COLS.items() if v.get('conv')},
    #                  dtype={k:v['dtype'] for k,v in IDB_COLS.items() if v.get('dtype')},
    #               )
    #
    #     df_list.append(df_year)
    #
    # df = pd.concat(df_list, ignore_index=True)
    df = pd.read_csv(fpath, nrows=nrows, encoding="utf-8", usecols=usecols,
                 converters={k:v['conv'] for k,v in IDB_COLS.items() if v.get('conv')},
                 dtype={k:v['dtype'] for k,v in IDB_COLS.items() if v.get('dtype')},
    )

    # Ensuring column ordering
    if usecols:
        df = df[usecols]

    # Lowercase column names
    df.columns = [col_name.lower() for col_name in df.columns]

    # Fix missing data encoded as 1900
    for date_col in ['filedate', 'termdate']:
        if date_col in df.columns:
            idx_na = df[df[date_col].eq(DATE_NA)].index
            df.loc[idx_na, date_col] = None

    # Make ucid and weak_ucid columns and insert at the front
    if 'ucid' not in df.columns:
        ucid, ucid_weak = make_ucid_and_weak(df.docket, df.office, df.district, case_type)
        df.insert(0, 'ucid', ucid)
        df.insert(1, 'ucid_weak', ucid_weak)

    return df

def get_recap_idb_cols(case_type):
    '''
    Get the list of recap idb_data columns relevant to case_type

    Inputs:
        - case_type: ('cv' or 'cr')
    Output:
        list of idb columns
    '''

    # For cv, the column list includes all idb recap columns
    if case_type == 'cv':
        # cols = recap_idb_mapping().values()
        return list(k for k in IDB_COLS.keys() if k!="NAME")

    # For cr, only a few relevant columns
    elif case_type == 'cr':
        return ['docket', 'filedate', 'termdate', 'office', 'circuit', 'district',\
                'county', 'tapeyear', 'name']

def extract_recap_idb_data(row, case_type):
    '''
    Extract the equivalent of the idb_data object in Recap data for a single
    case/row, used when outputting to json

    Inputs:
        - row (Series or dict-like): the idb data for a single case (lowercase columns)
        - case_type (str): 'cr' or 'cv'
    Output:
        dict

    '''
    def conv(value):
        ''' Converion function for idb data, get around serialising issues with json/dicts '''
        # Nonetypes
        if value in [-8, "-8","-8.0", None]:
            return None
        if pd.isna(value):
            return None
        # Numerics as strings
        elif type(value) is str:
            if re.match(r"\d+\.\d+\s?$",value):
                return int(float(value))
            elif value.isdecimal():
                return int(value)
        # Floats
        elif type(value) in [float,np.float64] and value.is_integer():
            return int(value)
        elif type(value) in [np.int64, np.int16]:
            return int(value)

        return value

    if type(row) is not dict:
        case = row.fillna('').to_dict()

    extra =  ['ucid', 'ucid_weak']
    data = {v['recap_key']: conv(case.get(k.lower())) for k,v in IDB_COLS().items() if k not in extra},
    data['plaintiff'] =  case.get('plt') if case_type=='cv' else 'USA'
    data['defendant'] = case.get('def') if case_type=='cv' else case.get('name')

    # Convert timestamp to str
    for key in ['date_filed', 'date_terminated']:
        val = data[key]
        if val:
            data[key] = str(data[key].date())
    return data

def convert_sas7(infile, outfile=None, dir=None, n_rows=None):
    '''
    Convert a SAS7BDAT file to csv
    Inputs:
        - infile (str): name of input file
        - outfile (str): name for output file, else takes same name and changes extension to .csv
        - dir (str or Path): directory that infile is in, if none specified uses FJC folder
        - n_rows (str): no. of rows to convert, if none it converts all
    '''
    from sas7bdat import SAS7BDAT

    dir = dir or settings.FJC
    infile_str = str(Path(dir)/infile)
    if not outfile:
        outfile = infile.rstrip('.sas7bdat') + '.csv'
    outfile_str = str(Path(dir)/outfile)

    with SAS7BDAT(infile_str, skip_header=False) as reader:
            with open(outfile_str, 'w+', newline="\n", encoding="utf-8") as wfile:
                for i, row in tqdm(enumerate(reader),total=n_rows):
                    csv.writer(wfile).writerow(row)
                    if n_rows and i>=n_rows:
                        break


def split_txt(old_file, out_dir, case_type, year_lb=0, nrows=None, year_var='DOCKET'):
    '''
    Cut one of the large .txt tab-delimited IDB datasets into multiple csv files, by year.

    Inputs:
        - old_file(str or Path): the .txt file to be split
        - out_dir (str or Path): the output directory for new csv files
        - case_type ('cv' or 'cr')
        - year_lb (int): lower bound on year, to filter out rows with filedate below
        - nrows (int): max number of rows to write (for testing small samples)
        - year_var ('DOCKET', 'FILEDATE'): which IDB varibale to get the year from (for file splitting)
    '''

    # Create directory if it doesn't exist
    out_dir = Path(out_dir).resolve()
    if not out_dir.exists():
        out_dir.mkdir()

    with open(old_file, 'r+', encoding='ISO-8859-1') as rfile:
        # Get the column headers from the first line
        columns = rfile.readline().rstrip('\n').split('\t')
        ind_filedate = columns.index('FILEDATE')
        write_count = 0

        # Session dictionary to map year to open csv writers
        session = {}

        for line in rfile.readlines():
            # Extract the data in the line
            row = line.rstrip('\n').split('\t')
            if len(row) != len(columns):
                # Error, skip row
                continue

            # Filter by year lower bound
            file_year = int(row[ind_filedate].split('/')[-1])
            if file_year < year_lb:
                continue

            if year_var=='FILEDATE':
                split_year = file_year

            elif year_var=='DOCKET':
                # Use the year from the DOCKET variable e.g.g 1600001 -> 16
                ind_docket = columns.index('DOCKET')
                split_year = row[ind_docket][:2]
            else:
                raise ValueError("`year_var` must be in ('FILEDATE','DOCKET')")


            # Check if we have a csv for 'year' and if not, start it up
            if split_year not in session.keys():
                filepath = out_dir/f"{case_type}{split_year}.csv"
                session[split_year] = {'file': open(filepath, 'w', encoding="utf-8",
                                                newline='\n')}
                session[split_year]['writer'] = csv.writer(session[split_year]['file'])
                # Write the header row for this new file
                session[split_year]['writer'].writerow(['ucid','ucid_weak', *columns])

            # Find the ucid and weak_ucid
            data = {k:row[columns.index(k)] for k in ['DOCKET', 'DISTRICT', 'OFFICE']}
            case_year = data['DOCKET'][:2]
            case_no = data['DOCKET'][2:]
            court = IDB_COLS['DISTRICT']['conv'](data['DISTRICT'])
            ucid = dtools.ucid_from_scratch(court, data['OFFICE'], case_year, case_type, case_no)
            ucid_weak = dtools.get_ucid_weak(ucid)

            # Write the new row, which is (ucid, ucid_weak, <<original row data>>)
            session[split_year]['writer'].writerow([ucid,ucid_weak,*row])

            write_count += 1
            if nrows:
                if write_count >= nrows:
                    break

    for v in session.values():
        v['file'].close()

def idb_merge(idb_data_file, case_type, preloaded_idb_data_file=None, dframe=None, cols='bare_min'):
    '''
    Merge dataframe of cases with idb data

    Inputs
        - idb_data_file (str or Path): the idb csv file to use e.g. 'cv10to19.csv'
        - case_type (str): the case type ('cv' or 'cr') of the cases in the idb file provided
        - preloaded_idb_data_file (DataFrame): specify a preloaded IDB dataframe, e.g. if the consumer has already called load_idb_csv
        - dframe (DataFrame): specify table of case files, instead of using all of unique files table
        - cols (str, either 'bare_min' or 'all'): the set of IDB columns to output in the merged table
    Outputs
        - final (DataFrame): the merged table
        - match_rate (float): the no. of original casefiles matched against idb
    '''
    if dframe is None:
        dff = dtools.load_unique_files_df()
        dff = dff[dff.case_type.eq(case_type)].copy()
    else:
        dff = dframe.copy()
    N = dff.shape[0]
    print(f"\n{N:,} SCALES cases provided")

    # Make sure there's a ucid column
    dff.reset_index(inplace=True)
    dff['ucid_copy'] = dff['ucid'].copy()

    if preloaded_idb_data_file is not None:
        df_idb = preloaded_idb_data_file
    else:
        print(f'Loading idb file: {idb_data_file}...')
        df_idb = load_idb_csv(idb_data_file, case_type=case_type,
            all_cols = False if cols=='bare_min' else True, cols=BARE_MIN_COLS if cols=='bare_min' else None)
    df_idb.sort_values(['ucid', 'filedate'], inplace=True)
    df_idb.drop_duplicates('ucid', keep='first', inplace=True)

    #Stage 1 (matching on ucid)
    print(f'STAGE 1: matching on ucid...')
    matched_mask = dff.ucid.isin(df_idb.ucid)
    matched_ucids = dff.ucid[matched_mask]
    if cols == 'bare_min':
        keepcols = ['fpath', 'case_type', 'filing_date', 'terminating_date', 'source',
                    *[x.lower() for x in BARE_MIN_COLS]]
                     # *[x.lower() for x in get_recap_idb_cols(case_type)] ]
        if 'nos_subtype' in dff.columns:
            keepcols.append('nos_subtype')

    # Make table of data merged on ucid
    print(f'STAGE 1: merging...')
    merged_ucid = dff[matched_mask].merge(df_idb, how='inner', left_on='ucid', right_on='ucid').set_index('ucid_copy')
    if cols == 'bare_min':
        merged_ucid = merged_ucid[keepcols]
    print(f'STAGE 1: {{matched:{sum(matched_mask):,}, unmatched:{sum(~matched_mask):,} }}')

    # Reduce dff to unmatched
    dff = dff[~matched_mask].copy()
    # Create weak ucid
    dff['ucid_weak'] = dtools.get_ucid_weak(dff.ucid)

    # Remove matched from df_idb and reduce to weak_ucid match
    print(f'STAGE 2: matching on weak_ucid...')
    df_idb = df_idb[~df_idb.ucid.isin(matched_ucids) & df_idb.ucid_weak.isin(dff.ucid_weak)]

    # Stage 2 (matching on ucid_weak and filing date)
    print(f'STAGE 2: merging...')
    merged_weak = dff.merge(df_idb, how="inner", left_on=['ucid_weak','filing_date'],
                             right_on=['ucid_weak', 'filedate']).set_index('ucid_copy')
    if cols == 'bare_min':
        merged_ucid = merged_ucid[keepcols]
    matched_stage2 = merged_weak.shape[0]
    print(f"STAGE 2 {{matched:{matched_stage2:,}, unmatched:{sum(~matched_mask) -matched_stage2 :,} }}")

    final = pd.concat([merged_ucid, merged_weak])
    del dff, df_idb

    match_rate = final.shape[0]/N
    print(f"Overall match rate: {match_rate :.2%}")

    return final, match_rate

def _update_case_(row, indent):
    '''
    Update a single case file json with idb data

    Inputs:
        - row (Series or dict): row of the merged dataframe that contains data on
                    the case (fpath,case_type and all idb_recap columns needed)
        - indent (int): size of indent if pretty printing
    '''
    # Actually fix incorrect data
    if row.recap:
        # Get the case and update the idb_data key
        case = dtools.load_case(row.fpath, recap_orig=True)
        case['idb_data'] = extract_recap_idb_data(row, row.case_type)

        # Update the outer json with idb_data
        for key in ['date_filed', 'date_terminated', 'nature_of_suit']:
            case[key] = case['idb_data'][key]

    else:
        # Pacer: just add in data
        case = dtools.load_case(row.fpath)
        case['idb_data'] = extract_recap_idb_data(row, row.case_type)

    with open(settings.PROJECT_ROOT/row.fpath, 'w+', encoding='utf-8') as wfile:
        simplejson.dump(case, wfile, ignore_nan=True, indent=indent)

def execute_idb_merge(merged_df):
    '''
    Update casefiles from an idb merge

    Inputs
        - merged_df (DataFrame): a merged dataframe, output from idb_merge
    '''

    for i, row in tqdm( merged_df,
                        total=merged_df.shape[0],
                        desc=f"Updating {ct_name} case files.."
                        ):
        _update_case_(row, indent)

##########
### FJC MDL Panel Database stuff
##########

def pull_mdl_terminated(infile, outfile, only_data=True, dir=None):
    '''
    Pull data from MDL panel terminated cases file
    Inputs:
        - infile (str/Path): the input html file
        - outfile (str): the name of the output file to be placed in data/fjc/mdl
        - only_data (bool): if true, only scrape the lines with observation rows (exclude headers and totals etc.)
        - dir (str or Path): directory that infile is in, if none specified uses FJC/mdl
    '''

    def is_court_row(row):
        ''' Checks if this is a row indicating a new court'''
        return row.find_all('td')[0].text[0].isdecimal()

    def is_data_row(row):
        ''' Checks if a row is a data row (contains an mdl observation)'''
        tds = row.find_all('td')
        return tds[0].text.strip() == '' and tds[1].text.strip().isdecimal()

    def is_headers(row):
        ''' Check if a row is a column headers row'''
        tds = row.find_all('td')
        if tds[0].text.strip().lower() in['court code', 'district']:
            return True
        elif tds[-2].text.strip().lower() == 'actions terminated':
            return True
        else:
            return False

    from bs4 import BeautifulSoup

    dir = dir or settings.FJC/'mdl'
    with open(Path(dir)/infile, encoding="utf-8") as rfile:
        soup = BeautifulSoup(rfile, 'html.parser')
    tables = soup.find_all('table')

    columns = ['district', 'mdl_no', 'caption', 'judge', 'transferred', 'filed', 'closed', 'remanded', 'year_termination']

    with open(Path(dir)/outfile, 'w+', encoding="utf-8", newline='') as wfile:
        writer = csv.writer(wfile)
        writer.writerow(columns)

        district = None
        for table in tables:
            for row in table.find_all('tr'):
                # Skip headers
                if is_headers(row):
                    continue
                # Update the district
                if is_court_row(row):
                    district = row.find_all('td')[2].text.strip().lower()

                data = [cell.text.strip() for cell in row.find_all('td')]

                # If we only want data: fill in the district if it's a data row else skip
                if only_data:
                    if is_data_row(row):
                        data[0] = district
                    else:
                        continue

                writer.writerow(data)

def pull_mdl_pending(infile, outfile, dir=None):
    '''
    Pull data from MDL panel pending cases file
    Inputs:
        - infile (str/Path): the input html file
        - outfile (str): the name of the output file to be placed in data/fjc/mdl
        - dir (str or Path): directory that infile is in, if none specified uses FJC/mdl
     '''

    def first_line(row):
        ''' Determine if this is the start line of the data '''
        return row.find_all('td')[1].text.strip().isupper()

    def is_totals_row(row):
        ''' Determine if this is the totals row (end of data)'''
        return row.find_all('td')[0].text.strip().lower().startswith('report totals')

    def new_district(row):
        ''' Determine if there is a new district in this row'''
        first_two = [cell.text.strip() for cell in row.find_all('td')[:2] ]
        for val in first_two:
            if val and len(val) in range(2,5):
                return val

    from bs4 import BeautifulSoup

    columns = ['district', 'judge', 'mdl_no', 'caption', 'actions_pending', 'actions_total']
    x2x = lambda x:x
    conv = {'district': lambda x: (x+'d').lower(),
                  'judge': x2x,
                  'mdl_no': lambda x: x.split('MDL -')[-1].strip(),
                  'caption': lambda x: x.split("IN RE: ")[-1].strip(),
                  'actions_pending': x2x,
                  'actions_total': x2x }


    dir = dir or settings.FJC/'mdl'
    with open(Path(dir)/infile, encoding="utf-8") as rfile:
        soup = BeautifulSoup(rfile, 'html.parser')
    tables = [t for t in soup.find_all('table')[:-3] if len(t.find_all('tr')) > 1]

    with open(Path(dir)/outfile, 'w+', encoding="utf-8", newline='') as wfile:
        writer = csv.writer(wfile)
        writer.writerow(columns)

        district, start = None, False
        for table in tables:
            for row in table.find_all('tr'):
                # Check if data has started yet
                if not start:
                    if first_line(row):
                        start = True
                    else:
                        continue
                # Stopping point
                if is_totals_row(row):
                    return
                # Update current district
                if new_district(row):
                    district = new_district(row)

                data = [cell.text for cell in row.find_all('td') if cell.text.strip()]
                # If blank row
                if not len(data):
                    continue
                # If row needs district filled in
                elif len(data) == (len(columns) - 1):
                    data.insert(0, district)

                out_data = out_line = [conv[col](data[i]) for (i,col) in enumerate(columns)]
                writer.writerow(out_data)

def coerce_int(val):
    if val in ['', None]:
        return None
    else:
        return int(val.replace(',',''))

def load_mdl_terminated(infile='mdls_terminated_2019.csv'):
    ''' Load data from the terminated mdl table'''
    int_cols = ['mdl_no','transferred', 'filed', 'closed', 'remanded', 'year_termination']
    conv = {'district': lambda x: cf.name2abbr(x, ordinal_first=False),
            **{col: coerce_int for col in int_cols}
            }

    df = pd.read_csv(settings.FJC/'mdl'/infile, converters=conv) \
           .drop_duplicates('mdl_no').set_index('mdl_no')

    return df

def load_mdl_pending(infile='mdls_pending_2020.csv'):
    ''' Load data from the pending mdl table'''
    int_cols = ['mdl_no','actions_pending', 'actions_total']
    conv = {col: coerce_int for col in int_cols}
    return pd.read_csv(settings.FJC/'mdl'/infile, index_col='mdl_no', converters=conv)

def load_mdl_all():
    ''' Load all mdl data: terminated, pending AND augmented (manually input in 'augmented.csv')'''
    term = load_mdl_terminated()
    pend = load_mdl_pending()
    augmented = pd.read_csv(settings.FJC/'mdl'/'augmented.csv', index_col='mdl_no')

    for dframe, is_term in [(term,'terminated'), (pend,'pending'), (augmented,'other')]:
        dframe['status'] = is_term


    # If in both, take data from terminated?
    pend_unique = pend[~pend.index.isin(term.index)]
    df = term.append(pend_unique)
    df = df.append(augmented)
    return df
