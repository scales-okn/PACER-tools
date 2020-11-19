'''
Tool for bundling files together
'''
import sys
import shutil
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import data_tools as dtools

def style(additional=''):
    base_style = '''
            html{
                font-family:monospace;
                font-size:16px;
            }
            table.maintable{
                width:100%;
                border-collapse:collapse;
                font-size: 1rem;
            }
            .maintable th{
                text-align:left;
            }
            .maintable th, .maintable td{
                padding: 5px;
            }
            .maintable tr:hover td{
                background-color: aliceblue;
                cursor: pointer;
            }
            .maintable td{
                    border: 1px solid lightgrey;
            }
    '''
    return f"<style>{base_style}{additional}</style>"

def bundler(df, name):
    '''
    Bundle up a collection of files
    Inputs:
        - df (DataFrame): any dataframe with an fpath column to identify files
        - name: name of directory to bundle into (will be put in /data/{name})
    '''
    # Want to include the index if it's ucid
    if df.index.name == 'ucid':
        df = df.reset_index()

    # Columns needed to generate
    if 'fpath' not in df.columns:
        raise ValueError('DataFrame must include fpath column to point to file locations')
    elif 'ucid' not in df.columns:
        raise ValueError('DataFrame must include ucid to identify case')



    bundle_dir = settings.BUNDLES/name
    if bundle_dir.exists():
        raise ValueError(f'The directory {str(bundle_dir)} already exists')
    else:
        bundle_dir.mkdir()


    heading = f"<h1 class='heading'>Data Dump: {name}</h1>"

    opening = f"<html>{style()}<body>{heading}"
    rows = []
    header = [f"<th>{val}</th>" for val in df.columns]
    rows.append("".join(header))

    for i,row in df.iterrows():
        # Get filepath
        rel_path = row.fpath
        if type(rel_path) is str:
            rel_path = Path(rel_path.replace('\\','/'))
        abs_path = settings.PROJECT_ROOT / rel_path

        # If it's a pacer file, get the html
        if 'pacer' in abs_path.parts:
            abs_path = dtools.get_pacer_html(abs_path)

        # Copy file
        new_name = row.ucid.replace(':', '-')
        fname = copy_file(abs_path, bundle_dir, new_name)

        cells = [f"<td>{val}</td>" for val in row.values]
        row_string = f'''<tr onclick="window.open('{fname}')">''' + "".join(cells) + "</tr>"
        rows.append(row_string)

    table = f"<table class='maintable'>{''.join(rows)}</table>"
    closing = f"</body></html>"

    html = opening + table + closing

    with open(bundle_dir/'_index.html', 'w+') as wfile:
        wfile.write(html)

    print(f"Files Succesfully bundled into /data/bundles/{name}")

def copy_file(old, new_dir, new_name=None):
    '''
        Copy a file
        Inputs:
            - old (pathlib.Path): old directory
            - new_idr (pathlib.Path): new directory
            - new_name (str): new name for file (without extension), if none given, old name is used
        Outputs:
            -new_full_name: the new full name with extension
    '''
    if new_name:
        ext = old.suffix
        new_full_name = new_name + ext
    else:
        new_full_name = old.name
    shutil.copyfile(old, new_dir/new_full_name)
    return new_full_name
