import pandas as pd

import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from support import fhandle_tools as ftools
from support import settings


def load_counsel_clusters():
    ''' Simple Loader File'''
    return pd.read_json(settings.COUNSEL_DIS_CLUSTS, lines=True)

def load_disambiguated_counsels(ucid, as_df=True):
    '''
    Load Counsel data (from relevant .jsonl files in the COUNSEL_DIS_DIR)

    Inputs:
        - ucid (str or iterable): can be a single ucid (str) or any iterable (list / pd.Series)
        - as_df (bool): if true returns as type pd.DataFrame, otherwise list of dicts

    Output:
        (pd.DataFrame or list of dicts) Disambiguated counsel data for the given ucid(s)
    '''

    # Coerce to an iterable
    if type(ucid) is str:
        ucid = [ucid]

    ROW_DAT = []
    for each in ucid:
        # create filepath
        fname = ftools.build_counsel_filename_from_ucid(each)
        # load file
        results = []
        if fname.exists():
            with open(fname, 'r') as json_file:
                json_list = list(json_file)
                for json_str in json_list:
                    results.append(json.loads(json_str))

        ROW_DAT+=results

    # return dataframe
    if ROW_DAT:
        if as_df:
            COUNSELS = pd.DataFrame(ROW_DAT)
        else:
            COUNSELS = ROW_DAT

        return COUNSELS
    else:
        return None


    return

def load_firm_clusters():
    return

def load_disambiguated_firms(ucid, as_df=True):
    return