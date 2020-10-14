from pathlib import Path

def std_path(fpath):
    ''' Standardise a filepath, returns a Path object'''
    if type(fpath) is str:
        fpath = Path(fpath.replace('\\','/'))
    return fpath
