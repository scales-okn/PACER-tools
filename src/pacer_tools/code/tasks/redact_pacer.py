import os
import glob
import json
import spacy
import click
import pandas as pd

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import data_tools as dtools
nlp = spacy.load("en_core_web_trf")



def _redact_file(fpath, outdir_replacement_target, outdir_replacement_text):
    is_html = 'html' in fpath
    fpath_new = fpath.replace(outdir_replacement_target, outdir_replacement_text)
    data = dtools.load_case(fpath=fpath, html=is_html)
    
    try:
        data_redacted = dtools.redact_private_individual_names(data, is_html=is_html, elective_nlp=nlp)
        os.makedirs(os.path.dirname(fpath_new), exist_ok=True)
        with open(fpath_new, 'w') as f:
            if is_html:
                f.write(data_redacted)
            else:
                json.dump(data_redacted, f)
        
        print(f'Created {fpath_new}')
    except Exception as e:
        print(f'Error while creating {fpath_new}: {e}')

    

@click.command()
@click.argument('file_pattern')
@click.argument('outdir_replacement_target')
@click.argument('outdir_replacement_text')
def main(file_pattern, outdir_replacement_target, outdir_replacement_text):

    fpaths = glob.glob(file_pattern)
    print(f'Compiled list of {len(fpaths)} files to redact')
    for fpath in fpaths:
        _redact_file(str(Path(fpath).resolve()), outdir_replacement_target, outdir_replacement_text)
    print('Finished redacting')

if __name__ == '__main__':
    main()