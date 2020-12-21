



# Description
A collection of web scrapers to download data from Pacer.gov.
The `scraper.py` script contains three scraper modules:

 1. Query Scraper
 2. Docket Scraper
 3. Document Scraper

These three steps are sequential and naturally lead from one to the next. When run all together (in `all` mode) the output from each step forms the input to the next step, as summarized below:

|  |Purpose|Input|Output
|--|--|--|--|
|  *Query Scraper* | Pull case query results | Query parameters | Query results page (*html*)
|  *Docket Scraper* | Pull case dockets | Query results page | Case dockets (*html*)
|  *Document Scraper* | Pull case documents + attachments | Case dockets | Case documents (*pdf*)

Each scraper module can also be run independently (`query`, `docket` and `document` modes respectively) with user specified inputs.


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
The scraper is designed to run at night to reduce its impact on server load. By default it will only run between 8pm and 4am (CDT). These parameters can be altered and overridden through the `-rts,` `-rte` and `--override-time` options, see below for details.

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
 - `-m, --mode` *[all|query|docket|document]*
 Whether to run a single scraper module are all three in sequence.

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

*Query Scraper*

 - `-qc, --query-conf`
 Configuration file (.json) for the query that will be used to populate the query form on Pacer. If none is specified the query builder will run in the terminal.

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

 - `-ex, --docket-exclusions`
Relative path to a csv file with a column of UCIDs that are cases to be excluded from the Docket Scraper.

- `--docket-update`
Check for new docket lines in existing cases.  A `--docket-input` must also be provided. If the docket input is a csv, a `latest_date` column can be provided to give the latest date across docket lines for each case. This date (+1) is passed to the "date filed from" field in Pacer when the docket report is generated. If no `latest_date` column provided for a case that has been previously downloaded, the date is calculated from the case json.

*Document Scraper*

 - `--document-input TEXT`
A relative path that is the Document Scraper module: a csv file that contains a column of UCIDs. These will be the cases that the Document Scraper will run on. If none given, behavior defaults to the following depending on which mode the scraper is running in:

	 - `document` - documents will be downloaded for all cases whose dockets are in the */html* subdirectory for the given court
	 - `all` - documents will be downloaded for all new dockets downloaded in the previous step of the scraper (the Docket Scraper)

 - `--document-att / --no-document-att` *(default: True)*
Whether or not to get document attachments from docket lines.

 - `--document-skip-seen / --no-document-skip-seen` *(default:True)*
Whether to skip seen cases. If true, documents will only be downloaded for cases that have not previously had documents downloaded. That is, if `CaseA` is in the input for the Document Scraper, it will be excluded and not have any documents downloaded in this session if there are any documents associated with `CaseA` that have previously been downloaded (i.e. that are in the */docs* subdirectory).

 - `--document-limit INTEGER` *(default: 1000)*
A limit on the no. of documents in a case. Cases that have more documents that the limit (i.e. extremely long dockets) will be excluded from the Document Scraper step.

## Notes
### Downloading specific documents
When giving the Document Scraper specific dockets to download, you can also specify specific documents to download from each docket, rather than downloading every document. There are two types of documents that can be downloaded:

 1. Line documents: these are documents that relate to the whole line in the docket report, the links for these documents appear in the # column of the report table.
 2. Attachments: these are attachments or exhibits included in the line, they are referenced in-line in the docket entry text.

*Note: Many docket entries contain links with references to documents from previous lines. These are ignored and not treated as attachments. To download these, refer to their original line.*

To specify specific documents to be downloaded, give the `--document-input` argument a csv that has both a *ucid* and a *doc_no* column. The *doc_no* column is a column where you can give a comma delimited list of documents to download. The following are valid individual values:

 - *x* -  just the line document *x*
 - *x:y* - the line documents from *x* to *y*, inclusive
 - *x_z* - the *z*'th attachment on line *x*
 - *x_a:b* -  attachments *a* through *b*, inclusive, from line *x*

These values are combined into a comma-delimited list, so for example for a given case you could specify: *"2,3:5,6_1,7_1:4"*. See Common tasks below for a full example of this.

Notes:

 - If no *doc_no* column present in the csv, all documents will be download for each of the given cases
 - If *doc_no* column present and there is a row with a case that has no value (empty string)  specified for doc_no, **all** documents will be downloaded for that case.
 - The no. or index corresponds to the # column in the docket table, which may be out of order.

### Specific defendants
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
  ### 1. Run a search query from start to finish
 Suppose you want to run a search query, get all the dockets and get all associated documents. For example, all cases opened in Northern Illinois in the first week of 2020.
 To do this:

    python scrapers.py -m all -a <path_to_auth_file> -c ilnd <path_to_ilnd_folder>

Since the Query Scraper module will run and no query config file has been specified, the query config builder will run in the terminal, allowing you to enter search parameters for the Pacer query form. The Query Scraper will then run the relevant query, download all relevant dockets from the query report and then download all documents from those case dockets.

### 2. Run Document Scraper on a subset of dockets
If you have have previously downloaded a bunch of case dockets and you want to download the documents for just a subset of these cases, you first need to create a file with the subset of interest. This can be any csv file that has a UCID column, the simplest example is just a one-column csv that we will create and call *subset.csv*, as below:

```
ucid
ilnd;;1:16-cv-03630
ilnd;;1:16-cv-03631
ilnd;;1:16-cv-03632
ilnd;;1:16-cv-03633
ilnd;;1:16-cv-03634
```

To run the document scraper on just this subset you could do the following:

```
python scraper.py -m document -a <path_to_auth_file> -c ilnd --document-input <path_to_subset.csv> <path_to_ilnd_folder>
```

*Note: the dockets for these cases must have been downloaded and must be in the /html folder for the Document Scraper to detect them.*

### 3. Update dockets

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

### 4. Download specific documents
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

