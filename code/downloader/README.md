
# Description
A collection of web scrapers to download data from Pacer.gov.
The `scraper.py` script contains five scraper modules:

 1. Query Scraper
 2. Docket Scraper
 3. Summary Scraper
 4. Member Scraper
 5. Document Scraper



|  |Purpose|Input|Output
|--|--|--|--|
|  *Query Scraper* | Pull case query results | Query parameters | Query results page (*html*)
|  *Docket Scraper* | Pull case dockets | Query html/ csv | Case dockets (*html*)
|  *Summary Scraper* | Pull case summaries| Query html / csv | Case summaries (*html*)
|  *Member Scraper* | Pull MDL member case pages| Query html / csv | Member cases pages (*html*)
|  *Document Scraper* | Pull case documents + attachments | Case dockets | Case documents (*pdf*)



# Getting Started
## Setup
To run this scraper you will need the following:

 - Python 3.7+
 - [Selenium](https://selenium-python.readthedocs.io/index.html) 3.12+
 - [Firefox](https://www.mozilla.org/en-US/firefox/new/) 80.0+
 - [GeckoDriver](https://github.com/mozilla/geckodriver)

## Login Details
Before running the scraper you will need to have an account on [Pacer.gov](Pacer.gov). You will need to create an auth file (in .json format) with your login details as below:

```json
{
    "user": "<your_username>",
    "pass": "<your_password>"
}
```

## Directory Structure
As the scraper is run on a single district court at a time, it is recommended that Pacer downloads should be separated into different directories by court. An example of a data folder is the following:

    /data
    |-- pacer
    |    |-- ilnd
    |    |-- nyed
    |    |-- txsd
    |    |-- ...

When running the scraper, a court directory will have an imposed structure as below ( the necessary sub-directories will be created).

    /ilnd
    |-- html   			# Orginal case dockets
    |   |-- 1-16-cv-00001.html
    |   |-- ...
    |   
    |-- json			# Parsed case dockets
    |   |-- 1-16-cv-00001.json
    |   |-- ...
    |
    |-- queries			# Downloaded queries and saved configs
    |   |-- 2016cv_result.html
    |   |-- 2016cv_config.json
    |   |-- ...
    |
    |-- summaries		# Downloaded case summaries
    |   |-- 1-16-cv-00001.html
    |   |-- ...
    |
    |-- docs			# Downloaded documents and attachments
    |   |-- ilnd;;1-16-cv-00001_1_2_u7905a347_t200916.pdf
    |   |-- ...
    |
    |-- _temp_			# Temporary download folder for scraper
    |   |-- 0
    |       | ...
    |   |-- 1
    |       | ...
    |   |-- ...

## UCIDs (unique case identifiers)
To uniquely identify cases, the project uses its own identifier called UCIDs which are constructed with the following two components:

    <court abbreviation>;;<case id>
For example, the case `1:16-cv-00001` in the Northern District of Illinois would be identified as `ilnd;;1:16-cv-00001`.

*Note: In some districts it is common to include judge initials at the end of a case id e.g. `2:15-cr-11112-ABC-DE` . These initials are always excluded from a UCID*.

## Runtime
The scraper is designed to run at night to reduce its impact on server load. By default it will only run between 6pm and 6am (CDT). These parameters can be altered and overridden through the `-rts,` `-rte` and `--override-time` options, see below for details.

## $$$
Pacer fees can rack up quickly! Running this scraper will incur costs to your own Pacer account.  There are a number of options for the scraper that exist to limit the potential for accidentally incurring large charges:

 - Docket limit - A maximum no. of dockets to be downloaded can be specified, see `--docket-limit` below.
 - Document limit - A maximum can be specified so as to exclude certain dockets from the Document Scraper that have large amounts of documents, see `--document-limit` below.

# Usage
To run the scraper:

    python scrapers.py [OPTIONS] INPATH

## Arguments

 - `inpath`: Relative path to the court directory folder e.g.   `../../data/pacer/ilnd`. This is the directory that will have the imposed structure as outlined above.

## Options
The options passed to the scraper can be grouped into the following four categories:

*General* *(apply to all three modules)*
 - `-m, --mode` *[query|docket|summary|member|document]*
Which scraper mode to run.

 - `-a, --auth-path`
 Relative path to login details auth file (see above)

 - `-c, --court`
The standard abbreviation for district court being scraped e.g. `ilnd`


 - `-nw, --n-workers INTEGER`
No. of workers to run simultaneously (for docket/document scrapers), i.e. no. of simultaneous browsers running.

 - `-ct, --case-type TEXT`
Specify a single case type to filter query results. If none given, scraper will pull  '*cv*' and '*cr*' cases.

 - `-rts, --runtime-start INTEGER` *(default:20)*
The start runtime hour (in 24hr, CDT). The scraper will not run if the current hour is before this hour.

 - `-rte, --runtime-end INTEGER` *(default:4)*
The end runtime hour (in 24hr, CDT). The scraper stop running when the current hour reaches this hour.

 - `--override-time`
Override the time restrictions and run scraper regardless of current time.

 - `--case-limit INTEGER`
Sets limit on maximum no. of cases to process (enter 'false' or 'f' for no limit). This will be applied to limit:
	  - the no. of case dockets the docket scraper pulls
	  - the no. of case dockets the document scraper takes as an input

- `--headless`
Selenium will run in headless mode i.e. no Firefox window will appear, useful if running on a server that does not have a display.

- `--verbose`
Give slightly more verbose logging output

*Query Scraper*

 - `-qc, --query-conf TEXT` 
 Configuration file (.json) for the query that will be used to populate the query form on Pacer. If none is specified the query builder will run in the terminal. (The query config format is fully described in the TEMPLATE_QUERY object in [forms.py](./forms.py), the most used fields are "filed_from", "filed_to", "nature_suit" and "case_status")

  - `--query-prefix TEXT`
  A prefix for the filenames of output query HTMLs. If date range of the query is greater than 180 days, the query will be split into chunks of 31 days to prevent PACER crashing while serving a large query results page. Multiple files will be created that follow the pattern `{query_prefix}__i.html` where `i` enumerates over the date range chunks.

*Docket Scraper*

 - `--docket-input TEXT`
A relative path that is the input for the Docket Scraper module: this can be a single query result page (.html), a directory of query html files or a csv with UCIDs

 - `-mem, --docket-mem-list` *[always|avoid|never] (default: never)*
 How to deal with member lists in docket reports (affects costs particularly with class actions/ MDLs)

	 - `always`: Always include them in reports
	 - `avoid`: Do not include them in a report if the current case was previously seen listed as a member case in a previously downloaded docket
	 - `never`: Never include them in reports

- `--docket-exclude-parties`
If True, 'Parties and counsel' and 'Terminated parties' will be excluded from docket reports (this reduces the page count for the docket report so can reduce costs).

 - `-ex, --docket-exclusions TEXT`
Relative path to a csv file with a column of UCIDs that are cases to be excluded from the Docket Scraper.

- `--docket-update`
Check for new docket lines in existing cases.  A `--docket-input` must also be provided. If the docket input is a csv, a `latest_date` column *can* be provided to give the latest date across docket lines for each case. This date (+1) is passed to the "date filed from" field in Pacer when the docket report is generated. If no `latest_date` column provided for a case that has been previously downloaded, the date is calculated from the case json.

*Summary Scraper*
- `--summary-input TEXT`
Similar to `--docket-input`. A relative path that is the input for the Summary Scraper module: this can be a single query result page (.html), a directory of query html files or a csv with UCIDs.

*Member Scraper*
- `--member-input TEXT`
A relative path to a csv that has at least one of the following columns: *pacer_id, case_no, ucid*

*Document Scraper*

 - `--document-input TEXT`
A relative path that is the Document Scraper module: a csv file that contains a *ucid* column. These will be the cases that the Document Scraper will run on. If a *doc_no* column is provided, then the specific cases specified will be downloaded, see [Downloading specific documents](#downloading-specific-documents) below. Otherwise an error will appear warning the user to use the --document-all-docs option, if they want to download all documents for a case. See below.

- `--document-all-docs`
This will force the scraper to download **all** documents for each of the cases supplied in *document-input*. Warning: this can be very expensive!
 - `--document-att / --no-document-att` *(default: True)*
Whether or not to get document attachments from docket lines.

 - `--document-skip-seen / --no-document-skip-seen` *(default:True)*
Whether to skip seen cases. If true, documents will only be downloaded for cases that have not previously had documents downloaded. That is, if `CaseA` is in the input for the Document Scraper, it will be excluded and not have any documents downloaded in this session if there are any documents associated with `CaseA` that have previously been downloaded (i.e. that are in the */docs* subdirectory).

 - `--document-limit INTEGER` *(default: 1000)*
A limit on the no. of documents to download **within** in a case. Cases that have more documents that the limit (i.e. extremely long dockets) will be excluded from the Document Scraper step.

## Notes
### Downloading specific documents
When giving the Document Scraper specific dockets to download, you can specify specific documents to download from each docket. If you need to download **every** document in each case you have supplied then you need to use the `--document-all-docs` flag. 

There are two types of documents that can be downloaded:

 1. Line documents: these are documents that relate to the whole docket entry line in the docket report, the links for these documents appear in the # column of the docket report table.
 2. Attachments: these are attachments or exhibits included in the line, they are referenced in-line in the docket entry text.

*Note: Many docket entries contain links with references to documents from previous lines. These are ignored and not treated as attachments. To download these, refer to their original line.*

To specify specific documents to be downloaded, give the `--document-input` argument a csv that has both a *ucid* and a *doc_no* column. The *doc_no* column is a column where you can give a comma delimited list of documents to download. The following are valid individual values:

 - *x* -  just the line document *x*
 - *x:y* - the line documents from *x* to *y*, inclusive
 - *x_z* - the *z*'th attachment on line *x*
 - *x_a:b* -  attachments *a* through *b*, inclusive, from line *x*

These values are combined into a comma-delimited list, so for example for a given case you could specify: *"2,3:5,6_1,7_1:4"*. See Common tasks below for a full example of this.

Notes:

 - If *doc_no* column is **not** present in the csv and the `--document-all-docs` flag has not been supplied, the scraper will give an error message. You need to either supply a `doc_no` column or specify that you want to download all documents for each case, by using the `--document-all-docs` flag.
 - If *doc_no* column **is** present and there is a row with a case that has no value (empty string)  specified for doc_no, **all** documents will be downloaded for that case. Note: this may be very expensive.
 - The no. or index of the document corresponds to the # column in the docket table on PACER. These are not necessarily displayed in sequential order due to PACER filing peculiarities.

### Specific defendant dockets
For criminal cases, there may be separate dockets/stubs for defendants if there are multiple defendants. To download a docket for a specific defendant you can supply a `def_no` column in the docket input csv. In this column, any blank value will be interpreted as getting the main docket. If the `def_no` column is excluded, the scraper will pull the main docket for every case.

For example

*/docket_update.csv*
```
ucid,def_no
ilnd;;1:16-cr-12345,2
ilnd;;1:16-cr-12345,3
ilnd;;1:16-cr-12346,
ilnd;;1:16-cr-12347,4
```
Running the following

    python scrapers.py -m docket
    --docket-input <path_to_file>/docket_update.csv --docket-update <path_to_ilnd_folder>

Will pull the following dockets:

 - *ilnd;;1:16-cr-12345*: The docket for defendants 2 and 3
 - *ilnd;;1:16-cr-12346*: The main docket
 - *ilnd;;1:16-cr-12347*: The docket for defendant 4

## Common tasks
  ### 1. Run a search query 
 Suppose you want to run a search query, for example, all cases opened in Northern Illinois in the first week of 2020.
 To do this:

    python scrapers.py -m query -a <path_to_auth_file> --query-prefix "first_week_2020"
       -c ilnd <path_to_ilnd_folder>

Since the Query Scraper module will run and no query config file has been specified, the query config builder will run in the terminal, allowing you to enter search parameters for the Pacer query form. The Query Scraper will then run the relevant query, download all relevant dockets from the query report and then download all documents from those case dockets.

### 2. Downloading Dockets
Suppose you had run the above search query, and it created a file at `pacer/ilnd/queries/first_week_2020.html`. To now download all civil and criminal cases included in that search result you would run 

    python scrapers.py -m docket -a <path_to_auth_file> --document-input <path_to_first_week_2020.html>
       -c ilnd <path_to_ilnd_folder>

The dockets will be downloaded into `pacer/ilnd/html/<year>/html/`, depending on the year code in the case id (note, this may differ from the actual filing date e.g. a case `ilnd;;1:20-cv-XXXX` may have a filing date from 2019 in PACER.

Alternatively if you had the list of cases either from that query html file or just an adhoc/manual list you could put them in a csv file (that has a `ucid` column) and pass that as the argument in for `--document-input` instead of the query html file.

### 3. Run Document Scraper on a subset of dockets
If you have have previously downloaded a bunch of case dockets and you want to download the documents for just a subset of these cases, you first need to create a file with the subset of interest. This can be any csv file that has a UCID column and a doc_no column, which we will create and call *subset.csv*, as below:

```
ucid,doc_no
ilnd;;1:16-cv-03630,2
ilnd;;1:16-cv-03631,"4,5"
```

To run the document scraper on just this subset you could do the following:

```
python scraper.py -m document -a <path_to_auth_file> -c ilnd --document-input <path_to_subset.csv> <path_to_ilnd_folder>
```

*Notes:*
- *The dockets for these cases must have been downloaded and must be in the /html folder for the Document Scraper to detect them.*
- *The `doc_no` column will download specific documents (see more in [Download specific documents](#download-specific-documents) below)*
- *If you need to download all documents in each case, you can forgo the `doc_no` column and supply the `--document-all-docs` flag, see above*.

### 4. Update dockets

To run a docket update, you need to give a csv file to the  `--docket-input` argument and also use the
`--docket-update` flag. For example, the following csv:

*/docket_update.csv*
```
ucid,latest_date
ilnd;;1:16-cv-03630,1/31/2016
ilnd;;1:16-cv-03631
ilnd;;1:16-cv-03632
```
To run the scraper:

    python scrapers.py -m docket
    --docket-input <path_to_file>/docket_update.csv --docket-update
     <path_to_ilnd_folder>


Suppose that ..630 and ..631 are cases that have previously been downloaded, but ...632 has not been. The following will occur when the Docket Scraper runs:

 - For ..630: the date 2/1/2016 will be passed to the date_from field in Pacer when the docket report is generated. A new docket will be downloaded and saved as ..630_1.html (or _2, _3 etc depending on if previous updates exist).
 - For ..631: as it has previously been downloaded but no date has been given in the *latest_date* column, the date of the latest docket entry will be retrieved from the case json and filled in as the*latest_date*, the rest proceeds as above
 - For ...632: since this case has not previously been downloaded, the whole docket report will be downloaded (i.e. it will proceed as normal for this case)

### 5. Download specific documents
When running the Document Scraper, you can specify a list of specific documents to download (see above for valid values). For example, suppose the following file is given:

*document_downloads.csv*
```
ucid,doc_no
ilnd;;1:16-cv-03630,"1,3:5"
ilnd;;1:16-cv-03631,"7_6, 7_9:11,"
ilnd;;1:16-cv-03632
```

To run this

    python scrapers.py -m document
    --document-input <path_to_file>/document_downloads.csv
     <path_to_ilnd_folder>
When it runs the document downloader will download the following:

 - For case ...630: line documents 1,3,4 and 5
 - For case ...631: attachments 6,9,10 and 11 from line 7
 - For case ...632: all documents

