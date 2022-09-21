'''
File: settings.py
Author: Adam Pah
Description: Settings file
'''
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


# Root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data
DATAPATH = PROJECT_ROOT / 'data'
CORE_DATA = PROJECT_ROOT / 'code'/ 'support' / 'core_data'
BUNDLES = DATAPATH / 'bundles'
PACER_PATH = DATAPATH / 'pacer'
RECAP_PATH =  DATAPATH / 'recap'
FJC =  DATAPATH / 'fjc'
IDB = FJC / 'idb'
SENTENCING_COMMISSION = DATAPATH / 'sentencing-commission'
EDGAR = DATAPATH / 'annotation' / 'edgar_ciks.csv'
CENSUS_CITIES = DATAPATH / 'annotation' / 'census_SUB-EST2020_ALL.csv'
UNIQUE_FILES_TABLE = DATAPATH / 'unique_docket_filepaths_table.csv'

# Scraper Files
MEM_DF = PROJECT_ROOT / 'code' / 'downloader' / 'member_cases.csv'
LOG_DIR = PROJECT_ROOT / 'code' / 'downloader' / 'logs'

# Parser Files
MEMBER_LEAD_LINKS = DATAPATH / 'annotation' / 'member_lead_links.jsonl'
ROLE_MAPPINGS = DATAPATH / 'annotation' / 'role_mappings.json'

# Annotation Files
COURTFILE = CORE_DATA / 'district_courts.csv'
JUDGEFILE = CORE_DATA / 'judge_demographics.csv'
JUDGEFILES_DIR = DATAPATH / 'annotation' / 'fjc_article_iii_biographical_directory'
STATEY2CODE = CORE_DATA / 'statey2code.json'
DISTRICT_COURTS_94 = CORE_DATA / 'district_courts_94.csv'
NATURE_SUIT = CORE_DATA / 'nature_suit.csv'
EXCLUDE_CASES = DATAPATH / 'exclude.csv'
FLAGS_DF = DATAPATH / 'annotation' / 'case_flags.csv'
RECAP_ID_DF = DATAPATH / 'annotation' / 'recap_id2ucid.csv'

DIR_SEL = DATAPATH / 'annotation' / 'SEL_DIR'
JEL_JSONL = DATAPATH / 'annotation' / 'JEL_Nov21.jsonl'

# Counsel/Firm Disambiguations
COUNSEL_DIS_DIR = DATAPATH / 'annotation' / 'Counsel_Disambiguations'
COUNSEL_DIS_CLUSTS = DATAPATH / 'annotation' / 'Counsel_Clusters.jsonl'

FIRM_DIS_DIR = DATAPATH / 'annotation' / 'Firm_Disambiguations'
FIRM_DIS_CLUSTS = DATAPATH / 'annotation' / 'Firm_Clusters.jsonl'

AMLAW_100 = DATAPATH / 'annotation' / 'amlaw_top_100.csv'
HYBRID_FIRMS = DATAPATH / 'annotation' / 'hybrid_firm_list.csv'

# Party Disambiguations
PARTY_DIS_DIR = DATAPATH / 'annotation' / 'Party_Disambiguations'
PARTY_DIS_CLUSTS = DATAPATH / 'annotation' / 'Party_Clusters.jsonl'

BAMAG_JUDGES = CORE_DATA / 'brmag_judges.csv'
BAMAG_POSITIONS = CORE_DATA / 'brmag_positions.csv'

# Misc
CTYPES = {'cv':'civil', 'cr':'criminal' }
STYLE = PROJECT_ROOT / 'code' / 'support' / 'style'

# Dev
SETTINGS_DEV = Path(__file__).parent / 'settings_dev.py'
if SETTINGS_DEV.exists():
    from support.settings_dev import *
