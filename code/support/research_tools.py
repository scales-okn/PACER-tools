import re
import csv
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import data_tools as dtools

# Case-level metadata for results
case_metadata = {
    'ucid': lambda case: dtools.ucid(case['download_court'], case['case_id']),
    'court': lambda case: case['download_court'],
    'judge': lambda case: case['judge'],
}

def pattern_matcher(patterns, text_str):
    '''
    Search for a group of patterns in the same string, return spans of matches

    Inputs:
        - patterns (dict): key-value pairs of pattern name to pattern value (regex pattern)
        - text_str (str): the text to search in
    Output
        matches(dict): key-value pairs of (pattern name, match span),
                if there is a match the span is a tuple of integers, otherwise it is None
    '''
    _get_span_ = lambda match: match.span() if match else None

    return {name: _get_span_(re.search(pattern, text_str,re.I)) for name,pattern in patterns.items()}

def wide_net_match_line(docket_line, case, wide_net=[], wide_net_fn=None):
    '''
    Check single docket line for wide net match, or uses wide_net_fn if supplied
    Inputs:
        - docket_line (list): a single docket line
        - case (json): The case json
        - wide_net (list): a list of regex patterns co ca
        - match_fn (function): a match function to run if no docket_patterns supplied
    '''
    if wide_net_fn is not None:
        return wide_net_fn(docket_line, case)
    else:
        full_pattern = '|'.join(f"({pat})" for pat in wide_net)
        return bool(re.search(full_pattern, docket_line['docket_text'], re.I))

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
        'date': docket_line['date_filed'],
        'ind': ind,
        'text': docket_line['docket_text'][:100],
        # Computed attribues
        **{k: fn(docket_line, case) for k,fn in computed_attrs.items()},
        # Pattern matches
        **pattern_matcher(patterns, docket_line['docket_text'][:rlim]),
    }
    return row

def get_case_matches(fpath, patterns, wide_net,
                    computed_attrs={}, rlim=None, wide_net_fn=None, skip_non_matches=False):
    '''
    Process a case and return observation rows

    Output:
    (list) of obersvation rows (dicts)
    '''

    case_rows = []
    case = dtools.load_case(fpath)

    for ind, line in enumerate(case['docket']):

        if wide_net_match_line(line, case, wide_net, wide_net_fn):
            # Use row builder
            row = row_builder(docket_line=line, ind=ind, case=case, fpath=fpath,
                    patterns=patterns, computed_attrs=computed_attrs, rlim=rlim)

            if skip_non_matches:
                # Only add row if at least one pattern match
                if not any(v for k,v in row.items() if k in patterns):
                    continue

            case_rows.append(row)

    return case_rows

def docket_searcher(case_paths, outfile, patterns, wide_net=[], computed_attrs={},
                     rlim=None, wide_net_fn=None, skip_non_matches=False):
    '''
    Main function to build results set from criteria

    Inputs:
        - case_paths (iterable): list of filepaths
        - outfile (str or Path): path to output file (.csv)
        - patterns (dict): a dictionary of patterns
        - wide_net (list): a list of wide regex patterns to match on docket lines
        - computed_attrs (dict): a dictionary of computed attributes
                        (named functions that take (docket_line, case) inputs)
        - rlim (int): right limit on characters in docket text to analyze
        - wide_net_fn (function): a function that takes (docket_line, case) arguments,
           where docket_line is dict (from the case['docket'] array) and case is the case dict,
           and maps to a boolean, if supplied will be used to decide on a row match instead of wide_net
        - skip_non_matches (bool): Useful for debugging/exploring, if true then
          rows that match the wide net but have no pattern matches are not written to outfile
    '''
    if (not len(wide_net)) and (wide_net_fn is None):
        raise ValueError('Must supply either wide_net or wide_net_fn')

    # Get table column headers
    headers = [*case_metadata.keys(), 'fpath', 'date','ind', 'text', *computed_attrs.keys(), *patterns.keys()]

    # Open outfile for writing
    with open(outfile, 'w', encoding='utf-8') as rfile:
        writer = csv.writer(rfile)
        writer.writerow(headers)

        for fpath in case_paths:
            case_rows = get_case_matches(fpath, patterns, wide_net, computed_attrs, rlim, wide_net_fn, skip_non_matches)
            print(f"<case:{fpath}> found {len(case_rows)} rows with matches")

            if len(case_rows):
                # Write to file
                for row_dict in case_rows:
                    # Ensure ordered printing by headers

                    #TODO: make this an append
                    writer.writerow(row_dict[k] for k in headers)

    print(f'Docket Searcher complete, results located at {outfile}')

def make_spacy_spans(row_series, pat_cols):
    ''' Convert a row from docket searcher output to a spaCy span-like output
    Inputs:
        - row_series(pd.Series): a pandas series/row
        - pat_cols (list): list of str of column names in row_series that are pattern columns
    Output:
        (list of dicts) with start, end, label keys

    Example:

        row_series =
            ucid #####
            year ######
            pat1 (10,15)
            pat2 (30,40)

        pat_cols = ['pat1', 'pat2']

        output: [
                    {'start':10, 'end':15, 'label':'pat1'},
                    {'start':30, 'end':40, 'label':'pat2'}
                ]


    '''
    return [{'start':int(v[0]), 'end':int(v[1]), 'label':k} for k,v in row_series[pat_cols].iteritems() if v]
