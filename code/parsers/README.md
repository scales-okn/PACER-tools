# Description
A parser that reads HTMLs downloaded from Pacer.gov and breaks them up into JSON format.

# Usage
To run the parser:

    python scrapers.py [OPTIONS] INPATH OUTPATH
    
### Arguments
 - `INPATH`: Relative path to the folder where HTMLs will be read, e.g.   `../../data/pacer/ilnd/html`.
 - `OUTPATH`: Relative path to the folder where JSONs will be written, e.g.   `../../data/pacer/ilnd/json`.
 
If you are using the parser in conjunction with SCALES's Pacer scraper, you will likely want your input and output directories to be the scraper-generated `html` and `json` folders within your chosen court directory, as outlined [here](../downloader/README.md#directory-structure).

### Options
- `-c, --court TEXT` *(defaults to none)* The standard abbreviation for the district court being parsed, e.g. `ilnd`. If not specified, and if using the directory structure mentioned above, the parser will inference the court abbreviation from the parent folder.
- `-d, --debug` *(flag)* Turns off concurrency in the parser. Useful for ensuring that error traces are printed properly.
- `-f, --force-rerun` *(flag)* Tells the parser to process HTMLs even when their corresponding JSONs already exist. Useful for obtaining fresh parses after scraping updates to existing dockets.
- `-nw, --n-workers INTEGER` *(defaults to 16)* Number of concurrent workers to run simultaneously - i.e., no. of simultaneous parses running.

### Shell scripts
Two shell scripts, `parse_all.sh` and `parse_subset.sh`, are provided for batch runs across multiple court directories. To run them:

    sh parse_all.sh INPATH [OPTIONS]
    sh parse_subset.sh INPATH -s STARTDIR -e ENDDIR [OPTIONS]
    
where `INPATH` is the relative path to a parent folder containing multiple court directories, `STARTDIR` and `ENDDIR` define the inclusive alphabetical range of court directories to parse (e.g. `nyed` through `nywd`), and `OPTIONS` are any command-line options you would like to pass through to `parse_pacer.py` (e.g. `--debug`, `--force-rerun`, `--n-workers`).

*Note: each court directory in the batch must include an HTML folder for input and a JSON folder for output, as occurs in the scraper-generated directory structure.*

# What's in the JSONs?
The parser output will always consist of the following fields:
- `case_id` *(string)*
- `case_type` *(string)*
- `download_court` *(string)*
- `ucid` *(string)*
- `filing_date` *(string)*
- `terminating_date` *(string)*
- `case_status` *(string)*
- `judge` *(string)*
- `referred_judge` *(string)*
- `nature_suit` *(string)*
- `jury_demand` *(string)*
- `cause` *(string)*
- `jurisdiction` *(string)*
- `demand` *(string)*
- `lead_case_id` *(string)*
- `other_court` *(string)*
- `case_flags` *(list of strings)*
- `case_name` *(string)*
- `plaintiffs` *(dictionary)*
- `defendants` *(dictionary)*
- `bankruptcy_parties` *(dictionary)*
- `other_parties` *(dictionary)*
- `misc_participants` *(dictionary)*
- `pending_counts` *(dictionary)*
- `terminated_counts` *(dictionary)*
- `docket` *(list of dictionaries)*
- `docket_available` *(boolean)*
- `member_case_key` *(string)*
- `mdl_code` *(integer)*
- `mdl_id_source` *(string)*
- `is_mdl` *(boolean)*
- `is_multi` *(boolean)*
- `billable_pages` *(integer)*
- `cost` *(float)*
- `download_timestamp` *(string)*
- `n_docket_reports` *(integer)*
- `source` *(string)*
- `download_url` *(string)*