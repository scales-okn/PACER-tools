import sys
import importlib
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

reload = importlib.reload
import_dict = {
    'support.fhandle_tools': 'ftools',
    'support.settings': 'settings',
    'support.data_tools': 'dtools',
    'support.docket_entry_identification':'dei',
    'support.court_functions':'cf',
    'support.judge_functions':'jf',
}

print('')
for mod, alias in import_dict.items():
    globals().update({alias:importlib.import_module(mod)})
    print(f"Imported {mod} as {alias}")

dff = dtools.load_unique_files_df()
print(f"Imported unique files df as dff")
