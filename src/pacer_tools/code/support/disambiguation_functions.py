import pandas as pd

import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from support import fhandle_tools as ftools
from support import settings


def load_CENSUS_cities():
    ''' Simple Loader Function'''
    return pd.read_csv(settings.CENSUS_CITIES, encoding="ISO-8859-1")

def load_AMLAW_100():
    ''' Simple Loader Function'''
    return pd.read_csv(settings.AMLAW_100)

def load_hybrid_350():
    ''' Simple Loader Function'''
    return pd.read_csv(settings.HYBRID_FIRMS)

def load_counsel_clusters():
    ''' Simple Loader Function'''
    return pd.read_json(settings.COUNSEL_DIS_CLUSTS, lines=True)

def load_firm_clusters():
    ''' Simple Loader Function'''
    return pd.read_json(settings.FIRM_DIS_CLUSTS, lines=True)

def load_party_clusters():
    ''' Simple Loader Function'''
    return pd.read_json(settings.PARTY_DIS_CLUSTS, lines=True)

def load_disambiguated_counsels(ucid, as_df=True, collection_location=None):
    '''
    Load Counsel data (from relevant .jsonl files in the COUNSEL_DIS_DIR)

    Inputs:
        - ucid (str or iterable): can be a single ucid (str) or any iterable (list / pd.Series)
        - as_df (bool): if true returns as type pd.DataFrame, otherwise list of dicts

    Output:
        (pd.DataFrame or list of dicts) Disambiguated counsel data for the given ucid(s) if the counsel appeared multiple times in the corpus
    '''

    # Coerce to an iterable
    if type(ucid) is str:
        ucid = [ucid]

    ROW_DAT = []
    for each in ucid:
        # create filepath
        fname = ftools.build_counsel_filename_from_ucid(each, collection_location)
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

def load_disambiguated_firms(ucid, as_df=True, collection_location=None):
    '''
    Load Firm data (from relevant .jsonl files in the FIRM_DIS_DIR)

    Inputs:
        - ucid (str or iterable): can be a single ucid (str) or any iterable (list / pd.Series)
        - as_df (bool): if true returns as type pd.DataFrame, otherwise list of dicts

    Output:
        (pd.DataFrame or list of dicts) Disambiguated firm data for the given ucid(s) if the firm appeared multiple times in the corpus
    '''

    # Coerce to an iterable
    if type(ucid) is str:
        ucid = [ucid]

    ROW_DAT = []
    for each in ucid:
        # create filepath
        fname = ftools.build_firm_filename_from_ucid(each, collection_location)
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
            FIRMS = pd.DataFrame(ROW_DAT)
        else:
            FIRMS = ROW_DAT

        return FIRMS
    else:
        return None


def load_disambiguated_parties(ucid, as_df=True, collection_location=None):
    '''
    Load Party data (from relevant .jsonl files in the FIRM_DIS_DIR)

    Inputs:
        - ucid (str or iterable): can be a single ucid (str) or any iterable (list / pd.Series)
        - as_df (bool): if true returns as type pd.DataFrame, otherwise list of dicts

    Output:
        (pd.DataFrame or list of dicts) Disambiguated party data for the given ucid(s) if the party appeared multiple times in the corpus
    '''

    # Coerce to an iterable
    if type(ucid) is str:
        ucid = [ucid]

    ROW_DAT = []
    for each in ucid:
        # create filepath
        fname = ftools.build_party_filename_from_ucid(each, collection_location)
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
            PARTIES = pd.DataFrame(ROW_DAT)
        else:
            PARTIES = ROW_DAT

        return PARTIES
    else:
        return None
    