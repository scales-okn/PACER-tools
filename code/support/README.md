# Overview
Collection of common tools and functions for SCALES project

# Filehandle Tools 
In `fhandle_tools` there are several functions to simplify and unify common transformations of filenames and case names. For full function argument documentation see the docstrings. Below is a summary of some of the most useful methods with usage examples.

## Quick lookup
### Dockets/Cases
| method |input  | output|
|--|--|--|
| *decompose_caseno* | `"1:16-cv-12345-2-ABC-DEF"` |`{'office': '1', 'year': '16',...}`  |
| *clean_case_id* | `"1:16-cv-12345-2-ABC-DEF"` | `"1:16-cv-12345"` |
|*generate_docket_filename*|`"1:16-cv-12345",def_no=3, ind=2`|`"1:16-cv-12345-3_2.html"`|


### Documents
| method |input  | output|
|--|--|--|
|*generate_document_id*|`"ilnd;;1:16-cv-12345", index=3, att_index=10`|`"ilnd;;1-16-cv-12345_3_10"`|
|*generate_document_fname*|`"ilnd;;1-16-cv-12345_3_10", user_hash="12345678"`|`"ilnd;;1-16-cv-12345_3_10_u12345678_t210106.pdf"`|
|*parse_document_fname*|`"ilnd;;1-16-cv-12345_3_10_u12345678_t210106.pdf"`| `{'index':3, 'att_index':10, ...`|

## Dockets/Cases
**decompose_caseno**(*case_no, pattern=re_case_no_gr*)

*Takes a PACER-style case no. and returns a dictionary of its decomposed parts.*
```python
decompose_caseno("1:16-cv-12345-2-ABC-DEF")
>> {'office': '1',
 'year': '16',
 'case_type': 'cv',
 'case_no': '12345',
 'judge_names': ['ABC', 'DEF'],
 'def_no': '2',
 'update_ind': ''}
```

**clean_case_id**(*case_no, allow_def_stub=False)*

*Takes a case id and cleans off anything that isn't the office, year, case type, and case no. Can handle filenames also.*
```python
clean_case_id("1:16-cv-12345-2-ABC-DEF")
>> "1:16-cv-12345"
clean_case_id("1-16-cv-12345_1.html")
>> "1:16-cv-12345"
```

**generate_docket_filename**(*case_name, def_no=None, ind=None, ext='html'*)

*Generate the filename for a docket*
```python
generate_docket_filename("1:16-cv-12345")
>> "1:16-cv-12345.html"
generate_docket_filename("1:16-cv-12345",def_no=3, ind=2)
>> "1:16-cv-12345-3_2.html"
```

## Documents

**generate_document_id**(*ucid, index, att_index=None*)

*Generate a unique id name for case document download*
```python
generate_document_id("ilnd;;1:16-cv-12345", 3)
>> "ilnd;;1-16-cv-12345_3"
generate_document_id("ilnd;;1:16-cv-12345", 3, 10)
>> "ilnd;;1-16-cv-12345_3_10"
```

**generate_document_fname**(*doc_id, user_hash, ext='pdf'*)

*Generate a unique file name for case document download*
```python
generate_document_fname("ilnd;;1-16-cv-12345_3_10",user_hash=12345678 )
>> "ilnd;;1-16-cv-12345_3_10_u12345678_t210106.pdf"
```
**parse_document_fname**(*fname'*)

*Parse a document filename, return the component parts as a dict*
```python
parse_document_fname("ilnd;;1-16-cv-12345_3_10_u12345678_t210106.pdf" )
>> {'doc_id': 'ilnd;;1-16-cv-12345_3_10',
 'index': '3',
 'att_index': '10',
 'user_hash': '12345678',
 'download_time': '210106',
 'ext': 'pdf',
 'ucid': 'ilnd;;1:16-cv-12345'}
```

## Other
**get_expected_path**(*ucid, ext='json', pacer_path=settings.PACER_PATH, def_no=None*)

*Find the expected path of the json file for the case*
```python
get_expected_path("ilnd;;1:16-cv-12345")
>> "{{abs}}/data/pacer/ilnd/json/1-16-cv-12345.json"
get_expected_path("ilnd;;1:16-cv-12345", ext="html", def_no=2)
>> "{{abs}}/data/pacer/html/json/1-16-cv-12345_2.html"
```
**get_pacer_url**(*court, page*)

*Get a court-specific pacer url for various pages: query, login, logout, docket, document link, possible case*

```python
get_pacer_url("ilnd", "query")
>>> "https://ecf.ilnd.uscourts.gov/cgi-bin/iquery.pl"
get_pacer_url("txed", "logout")
>>> "https://ecf.txed.uscourts.gov/cgi-bin/login.pl?logout"
```


# Research Tools (`research_tools.py`)
## Docket Searcher
### Description
The docket searcher is a tool to analyze case dockets for events/patterns and build a table of observations.
The tool takes a collection of docket reports, and for each line of each docket report it does the following:
 1. Checks if the text of the docket line matches **basic criteria**. This can be one of two ways:
	 - The docket line matches the *wide_net*
	 - The docket line matches the *docket_line_fn* function
 2. If so, checks the line for a variety of patterns (patterns, computed_attrs) and use this to build a row for the result set.

### Usage

```python
docket_searcher(case_paths, outfile, wide_net, patterns,
	        computed_attrs={}, rlim=None, line_match_fn=None)
```
- **case_paths** (list): a list of filepaths to case data (.json files) that are relative to the project root
- **outfile** (str): the output file (.csv)
- **wide_net** (list): a list of regex patterns
- **patterns** (dict): a dictionary of regex patterns with (variable_name, pattern) pairs
- **computed_attrs** (dict): a dictionary of (variable_name, function) pairs. The functions take two arguments (*docket_line*, *case*) where the *docket_line* is a list and *case* is a parsed case json
- **rlim** (int): a right limit to narrow search within docket entry text
- **line_match_fn** (function): a function to use to instead to check if a line matches the basic criteria. The function takes two arguments (docket_line, case) as above. If line_match_fn is supplied it is used instead of *wide_net* to check basic criteria.


### Example
```python
import research_tools as rt

case_paths = [...]
outfile = 'results_table.csv'
wide_net = ['seal', 'protective']

patterns = {
	'seal_motion':'(motion|order)( to)? seal',
	'grant_part': 'granting in part motion to seal',
	'deny_part' : 'denying in part motion to seal'
}

def date_diff(x,y):
	return (pd.Timestamp(x) - pd.TimeStamp(y)).days

computed_attrs = {
	'case_type': lambda dl,c: c['case_type'],
	'days_from_filing': lambda dl,c: date_diff(dl[0], c['filing_date'])
}

rt.docket_searcher(case_paths, 'res_tab.csv', wide_net,
				patterns, computed_attrs)
```

*/res_tab.csv*
```
fpath,ucid,court,judge,case_type,case_type,days_from_filing,seal_motion,grant_part,deny_part
<fpath_caseA>,ilnd;;<caseno_caseA>>,ilnd,Judge A,cr,0,1,0,0
<fpath_caseA>,ilnd;;<caseno_caseA>>,ilnd,Judge A,cr,12,0,1,1
<fpath_caseB>,ilnd;;<caseno_caseB>>,ilnd,Judge B,cr,2,1,0,0
<fpath_caseB>,ilnd;;<caseno_caseB>>,ilnd,Judge B,cr,7,0,1,0
<fpath_caseB>,ilnd;;<caseno_caseB>>,ilnd,Judge B,cr,8,0,0,1

```

### Output
The output file has a row for each docket line that meets the basic criteria.
The output columns are (in the following order):

 - *ucid*
 - *court*
 - *judge*
 - *fpath*
 - *date*: the docket line date
 - *ind*: the index of docket line, relative to docket list in case json
 - *text*: the first 100 characters of the docket line text

Following the above are:
 - all columns generated by **computed_attrs** keys
 - all columns from **patterns** keys

