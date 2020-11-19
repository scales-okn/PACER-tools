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
BUNDLES = DATAPATH / 'bundles'
PACER_PATH = DATAPATH / 'pacer'
RECAP_PATH =  DATAPATH / 'recap'
FJC =  DATAPATH / 'fjc'
IDB = FJC / 'idb'
LOG_DIR = PROJECT_ROOT / 'code' / 'downloader' / 'logs'

UNIQUE_FILES_TABLE = DATAPATH / 'unique_docket_filepaths_table.csv'

# Parser Files
MEMBER_LEAD_LINKS = DATAPATH / 'annotation' / 'member_lead_links.csv'
ROLE_MAPPINGS = DATAPATH / 'annotation' / 'role_mappings.json'

# Annotation Files
COURTFILE = DATAPATH / 'annotation' / 'district_courts.csv'
JUDGEFILE = DATAPATH / 'annotation' / 'judge_demographics.csv'
EXCLUDE_CASES = DATAPATH / 'exclude.csv'
MEM_DF = PROJECT_ROOT / 'code' / 'downloader' / 'member_cases.csv'
STATEY2CODE = DATAPATH / 'annotation' / 'statey2code.json'
FLAGS_DF = DATAPATH / 'annotation' / 'case_flags.csv'

# Misc
CTYPES = {'cv':'civil', 'cr':'criminal' }

# Dev
SETTINGS_DEV = Path(__file__).parent / 'settings_dev.py'
if SETTINGS_DEV.exists():
    from support.settings_dev import *
