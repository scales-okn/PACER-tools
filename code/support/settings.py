'''
File: settings.py
Author: Adam Pah
Description:
Settings file
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
LOG_DIR = PROJECT_ROOT / 'code' / 'downloader' / 'logs'
SENTENCING_COMMISSION = DATAPATH / 'sentencing-commission'

UNIQUE_FILES_TABLE = DATAPATH / 'unique_docket_filepaths_table.csv'

# Parser Files
MEMBER_LEAD_LINKS = DATAPATH / 'annotation' / 'member_lead_links.jsonl'
ROLE_MAPPINGS = DATAPATH / 'annotation' / 'role_mappings.json'

# Annotation Files
COURTFILE = CORE_DATA / 'district_courts.csv'
JUDGEFILE = CORE_DATA / 'judge_demographics.csv'
STATEY2CODE = CORE_DATA / 'statey2code.json'
NATURE_SUIT = CORE_DATA / 'nature_suit.csv'
EXCLUDE_CASES = DATAPATH / 'exclude.csv'
MEM_DF = PROJECT_ROOT / 'code' / 'downloader' / 'member_cases.csv'
FLAGS_DF = DATAPATH / 'annotation' / 'case_flags.csv'
RECAP_ID_DF = DATAPATH / 'annotation' / 'recap_id2ucid.csv'

DIR_SEL = DATAPATH / 'annotation' / 'SEL_DIR'
JEL_JSONL = DATAPATH / 'annotation' / 'JEL_Nov21.jsonl'

BAMAG_JUDGES = CORE_DATA / 'brmag_judges.csv'
BAMAG_POSITIONS = CORE_DATA / 'brmag_positions.csv'

# Misc
CTYPES = {'cv':'civil', 'cr':'criminal' }
STYLE = PROJECT_ROOT / 'code' / 'support' / 'style'

# Dev
SETTINGS_DEV = Path(__file__).parent / 'settings_dev.py'
if SETTINGS_DEV.exists():
    from support.settings_dev import *
