'''
Tool for bundling files together
'''
import re
import sys
import json
import shutil
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from tqdm.autonotebook import tqdm

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import data_tools as dtools

def index_style(additional=''):
    '''
    Build css style tag
    Inputs:
        - additional (str): additional str to be inserted on-the-fly
    Output:
        (str) a valid html-style tag
    '''
    base_style = open(settings.STYLE/'bundler_index.css').read().replace('\n','')
    return f"<style>{base_style}{additional}</style>"

def bundler(indf, name, notes=None, overwrite=False, anno_col=None):
    '''
    Bundle up a collection of files
    Inputs:
        - indf (DataFrame): any dataframe with an fpath column to identify files
        - name (str): name of directory to bundle into (will be put in /data/{name})
        - notes (str): notes to be injected under the header (html string)
        - anno_col (str): name of annotations column if any, column should be valid json string
    '''
    df = indf.copy()
    # Want to include the index if it's ucid
    if df.index.name == 'ucid':
        df = df.reset_index()

    if anno_col:
        # import pdb;pdb.set_trace()
        df[anno_col] = df[anno_col].map(json.loads)

    # Columns needed to generate
    if 'fpath' not in df.columns:
        raise ValueError('DataFrame must include fpath column to point to file locations')
    elif 'ucid' not in df.columns:
        raise ValueError('DataFrame must include ucid to identify case')

    # Handle directory
    bundle_dir = settings.BUNDLES/name
    if bundle_dir.exists():
        if overwrite:
            # Delete all files in the directory
            for file in bundle_dir.iterdir():
                file.unlink()
        else:
            raise ValueError(f'The directory {str(bundle_dir)} already exists')
    else:
        bundle_dir.mkdir(parents=True)

    # Start building html index page with strings
    heading = f"<h1 class='heading'>Data Dump: {name}</h1>"
    notes = f'''<div class="notes">NOTES: {notes}</div>''' if notes else ''
    opening = f"<html>{index_style()}<body>{heading}{notes}"

    # Start building table rows
    table_rows = []
    header = [f"<th>{val}</th>" for val in df.columns if val!=anno_col]
    table_rows.append("".join(header))

    for i,row in tqdm(df.iterrows(), total=len(df)):
        # Get filepath
        rel_path = row.fpath
        if type(rel_path) is str:
            rel_path = Path(rel_path.replace('\\','/'))
        abs_path = settings.PROJECT_ROOT / rel_path

        # Annotation scenario
        if 'pacer' in abs_path.parts and anno_col and row[anno_col]:
            # Load the html text and json data to make the annotated docket
            hpath = dtools.get_pacer_html(abs_path)
            html_text = open(hpath, 'r', encoding='utf-8').read()
            json_data = dtools.load_case(row.fpath)
            new_html = make_annotated_docket(html_text, json_data, row[anno_col])

            # Copy the new (annotated) html into the bundle directory
            tqdm.write(f"Annotating {row.ucid}")
            new_name = row.ucid.replace(':', '-') + '.html'
            with open(bundle_dir/new_name, 'w', encoding='utf-8') as wfile:
                wfile.write(new_html)

        else:
            if 'pacer' in abs_path.parts:
                # Get the path to the html file
                abs_path = dtools.get_pacer_html(abs_path)

            # Copy the file
            tqdm.write(f"Copying {row.ucid}")
            new_name = row.ucid.replace(':', '-') + abs_path.suffix
            shutil.copyfile(abs_path, bundle_dir/new_name)


        cells = [f"<td>{v}</td>" for k,v in row.iteritems() if k!=anno_col]
        row_string = f'''<tr onclick="window.open('{new_name}')">''' + "".join(cells) + "</tr>"
        table_rows.append(row_string)

    # Finish out the html string for the index
    table = f"<table class='maintable'>{''.join(table_rows)}</table>"
    closing = f"</body></html>"
    html = opening + table + closing

    with open(bundle_dir/'_index.html', 'w+') as wfile:
        wfile.write(html)

    print(f"\nFiles Succesfully bundled into {bundle_dir}")

def build_new_td(json_text, row_annotations, soup=None, inner_html=False):
    '''
    Make a new td cell to replace current docket text td cell, for a single docket entry/row

    Inputs:
        - json_text (str): the cleaned docket text from the saved json
        - row_annotations (list): a list of dicts of annotation spans for a single docket line
                                 e.x. [{'start': 0, 'end':10, 'label':"SOMETHING"}]
        - soup (bs4 instance): soup needed to make a tag, if None will create an empty soup
        - inner_html (bool): if true returns inner html as string
    Output
        new_td (bs4 object or str): new td cell to be inserted
    '''
    if not soup:
        soup = BeautifulSoup('','html.parser')

    new_td = soup.new_tag('td')

    # Index pointer to current place in original json
    og_pointer = 0

    # Sort annotation by 'start'
    row_annotations.sort(key=lambda x: x['start'])

    # Iterate through each annotation and 'swap out' original text for new span
    for annot in row_annotations:

        # Get all the text up until this annotation
        new_td.append( json_text[ og_pointer: annot['start'] ] )

        # Build the span html tag, add attributes that allow for styling/highlighting
        span_tag = soup.new_tag('span', attrs={'class':"annotation", 'data-label':annot['label']})
        span_tag.string = json_text[annot['start']:annot['end']]
        new_td.append(span_tag)

        # Set the pointer to the end index of the annotation
        og_pointer = annot['end']

    # Get the last bit of the docket
    new_td.append( json_text[ og_pointer: ] )

    if inner_html:
        return new_td.decode_contents()
    else:
        return new_td

def make_annotated_docket(html_text, json_data, case_annotations):
    '''
    Main function to build annotated html for a PACER docket

    Inputs:
        - html_text (str)
        - json_data (dict)
        - case_annotations (dict): mapping from row index (int, ordinal index) -> annotation data list of dicts e.g. {2: [ {span1},...], 5: [ {span2}, ...]}

    Output:
        (str) html source text for annotated html
    '''

    # Make the soup
    soup = BeautifulSoup(html_text, 'html.parser')

    docket_table = soup.select('table')[-2]

    for row_index, tr in enumerate(docket_table.select('tr')[1:]):

        # Skip row if no annotation
        if row_index not in case_annotations.keys():
            continue

        tr.attrs['class'] = tr.attrs.get('class', '') + ' annotated'

        #Isolate the original td
        docket_entry_td = tr.select('td')[2]

        # Gather info for new td
        jdata_text = json_data['docket'][row_index]['docket_text']
        row_annotations = case_annotations[row_index]

        # Build and inject new td
        new_cell = build_new_td(jdata_text, row_annotations, soup)
        docket_entry_td.replace_with(new_cell)


    # Inject the style.css file into the header
    style_tag = soup.new_tag('style')
    style_tag.string = open(settings.STYLE/'pacer_docket.css').read().replace('\n','')
    soup.head.append(style_tag)

    return re.sub(r"b'|\\n|\\t",'',str(soup))

def make_annotated_docket_for_dash(html_text, json_data, case_annotations, range_to_keep):
    '''
    Annotate a Pacer docket and return only a specified range of docket lines in JSON format - for use with make_excerpts()

    Inputs:
        - html_text (str)
        - json_data (dict)
        - case_annotations (dict): mapping from row index (within case) -> annotation data dict e.g. {'2': [ {span1}, ... ], '5': [ {span2}, ... ]}
        - range_to_keep (range): the range of docket lines (SCALES-indexed) to be returned

    Output:
        (dict) the docket excerpt as a JSON
    '''

    # make the preliminary JSON and the soup
    new_json = {"case_id": json_data['case_id'], "docket": []}
    soup = BeautifulSoup(html_text, 'html.parser')
    docket_table = soup.select('table')[-2]

    # check whether each row needs to be inserted into the final JSON
    for row_index, tr in enumerate(docket_table.select('tr')[1:]):
        if row_index in range_to_keep:

            # check whether this row needs to be annotated
            if str(row_index) in case_annotations.keys():
                row_annotations = case_annotations[str(row_index)]
                tr.attrs['class'] = tr.attrs.get('class', '') + ' annotated'
            else:
                row_annotations = {}

            # gather remaining info needed for build_new_td()
            old_entry = json_data['docket'][row_index]
            new_docket_text = old_entry['docket_text']

            # build & insert new docket entry
            new_docket_html = build_new_td(new_docket_text, row_annotations, inner_html=True)
            new_entry = {
                "date_filed": old_entry['date_filed'],
                "ind": old_entry['ind'],
                "docket_text": new_docket_text,
                "docket_html": new_docket_html
                }
            new_json['docket'].append(new_entry)

    return new_json