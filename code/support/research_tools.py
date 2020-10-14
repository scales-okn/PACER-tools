import re
import csv
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import data_tools as dt

# Case-level metadata for results
case_metadata = {
    'ucid': lambda case: dt.ucid(case['download_court'], case['case_id']),
    'court': lambda case: case['download_court'],
    'judge': lambda case: case['judge'],
}

def wide_net_match_line(docket_line, case, wide_net=[], match_fn=None):
    '''
    Check single docket line for wide net match
    Inputs:
        - docket_line (list): a single docket line
        - case (json): The case json
        - wide_net (list): a list of regex patterns co ca
        - match_fn (function): a match function to run if no docket_patterns supplied
    '''
    if len(wide_net):
        ## TODO: Verify this full pattern works
        full_pattern = '|'.join(f"({pat})" for pat in wide_net)
        return bool(re.search(full_pattern, docket_line[2], re.I))
    else:
        return match_fn(docket_line, case)

def row_builder(docket_line, ind, case, fpath, patterns, computed_attrs={},  rlim=None):
    '''
    Function to build observation row of result set.

    Inputs:
        - docket_line (tuple): The docket entry (date, #, docket text)
        - ind (int): index of docket_line (relative to dockets list in json)
        - case (json): The case json
        - fpath (str): file path
        - patterns (dict): a dictionary of pattern names and regex patterns
        - computed_attrs (dict): A dictionary with attribute names as keys,
                        and functions taking docket_line and case as values
                        e.g.{'is2020': lambda dl, c: dl[0].year==2020}
        - rlim (int): right limit to search text
    Output:
        row (dict)
    '''
    row = {
        # Case-level metadata
        **{k: fn(case) for k,fn in case_metadata.items()},
        'fpath': fpath,
        'date': docket_line[0],
        'ind':ind,
        'text': docket_line[2][:100],
        # Computed attribues
        **{k: fn(docket_line, case) for k,fn in computed_attrs.items()},
        # Pattern matches
        **{k: int(bool(re.search(v, docket_line[2][:rlim], re.I))) for k,v in patterns.items()},
    }
    return row

def get_case_matches(fpath, patterns, wide_net, computed_attrs={}, rlim=None, line_match_fn=None):
    '''
    Process a case and return observation rows

    Output:
    A list of obersvation rows
    '''
    case_rows = []
    case = dt.load_case(fpath)

    for ind, line in enumerate(case.get('docket', [])):
        if not(type(line)==list and len(line)==3):
            continue

        if wide_net_match_line(line, case, wide_net, line_match_fn):
            # Use row builder
            row = row_builder(docket_line=line, ind=ind, case=case, fpath=fpath,
                    patterns=patterns, computed_attrs=computed_attrs, rlim=rlim)
            case_rows.append(row)
    return case_rows

def docket_searcher(case_paths, outfile, wide_net, patterns, computed_attrs={}, rlim=None, line_match_fn=None):
    '''
    Main function to build results set from criteria

    Inputs:
        - case_paths (list): list of filepaths
        - outfile (str or Path): path to output file (.csv)
        - patterns (dict): a dictionary of patterns
        - wide_net (list): a list of wide regex patterns to match on docket lines
        - computed_attrs (dict): a dictionary of computed attributes
                        (named functions that take (docket_line, case) inputs)
        - rlim (int): right limit on docket line to analyze
    '''
    # Get table column headers
    headers = [*case_metadata.keys(), 'fpath', 'date','ind', 'text', *computed_attrs.keys(), *patterns.keys()]

    # Open outfile for writing
    with open(outfile, 'w', encoding='utf-8') as rfile:
        writer = csv.writer(rfile)
        writer.writerow(headers)

        for fpath in case_paths:
            case_rows = get_case_matches(fpath, patterns, wide_net, computed_attrs, rlim, line_match_fn)
            print(f"<case:fpath> found {len(case_rows)} matches")

            if len(case_rows):
                # Write to file
                for row_dict in case_rows:
                    # Ensure ordered printing by headers
                    writer.writerow(row_dict[k] for k in headers)

    print(f'Docket Searcher complete, results located at {outfile}')
