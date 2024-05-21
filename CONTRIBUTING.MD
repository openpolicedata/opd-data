# Contributer's Guide to Recommending or Adding Datasets
Thank you for considering contributing to OpenPoliceData (OPD). Below is a guide to helping identify data to add to [OPD](https://github.com/openpolicedata/openpolicedata) Python package. Although the main goal is to add datasets to OPD, the spreadsheets developed here serves an additional purpose as a way to track which police departments in the United States are releasing incident-level data to the public. 

OpenPoliceData is community-driven project. It's people like you that make OPD useful and successful. Your contributions are necessary to make both OPD and the overall data tracking as complete and thorough as possible. Anyone can help. We have tasks available for both developers and non-developers. This guide will help you get started.

Adding or recommending datasets to be included in OPD is a great way to contribute to the OPD project. Help is needed with one or more of the following:
* Identifying previously untracked police departments and/or datasets
* Adding new datasets to the OPD package
* Testing new datasets added to the OPD package
* Developing and testing any new code required to incorporate new datasets

This guide provides suggestions for:
* How to find police data
* What types of data should be included in OPD
* How to recommend a dataset to the team (if you are recommending)
* How to add the dataset to OpenPoliceData (if you are helping with development)

Note that OPD does not provide access to all types of police datasets. The types of data that OPD includes are referred to as [qualifying datasets](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata) and are described further below. 

> ``Developers``: Please review the [OpenPoliceData Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) before or after reviewing this document.

**If you need any help after reading this guide, feel free to ask for it on our [Discussions page](https://github.com/openpolicedata/opd-data/discussions) or by [email](mailto:openpolicedata@gmail.com).**

## Table of Contents
- [Contributer's Guide to Recommending or Adding Datasets](#contributers-guide-to-recommending-or-adding-datasets)
  - [Table of Contents](#table-of-contents)
  - [Ground Rules](#ground-rules)
  - [Freedom of Information Act (FOIA) Requests](#freedom-of-information-act-foia-requests)
  - [Steps for Contributing](#steps-for-contributing)
    - [Finding Untracked Police Departments and Their Data](#finding-untracked-police-departments-and-their-data)
      - [`Which Departments/Sources to Check`](#which-departmentssources-to-check)
      - [`Searching for a Police Department's Data`](#searching-for-a-police-departments-data)
    - [Create an Issue for New Datasets and Police Departments with No Available Data](#create-an-issue-for-new-datasets-and-police-departments-with-no-available-data)
    - [Clone the opd-data Repository](#clone-the-opd-data-repository)
    - [Update the Police Data Tracking Spreadsheet](#update-the-police-data-tracking-spreadsheet)
  - [Adding a Dataset to OpenPoliceData](#adding-a-dataset-to-openpolicedata)
    - [Testing That The Dataset Works in OpenPoliceData](#testing-that-the-dataset-works-in-openpolicedata)
  - [Pull Requests](#pull-requests)
  - [Qualifying Data - What Types of Datasets Should be Included in OpenPoliceData?](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata)
  - [What if I find a dataset in OpenPoliceData that should not be included?](#what-if-i-find-a-dataset-in-openpolicedata-that-should-not-be-included)
  - [Still Have Questions?](#still-have-questions)

## Ground Rules

The goal is to maintain a diverse community that's pleasant for everyone. Please be considerate and respectful of others by following our
[code of conduct](https://github.com/openpolicedata/openpolicedata/blob/main/CODE_OF_CONDUCT.md).

## Freedom of Information Act (FOIA) Requests
The below mainly discusses datasets that are already available. However, many departments do not release data or do not release certain types of data. Another great way to contribute to OPD (and to transparency more generally) is to make FOIA requests of police departments or states for data that is currently unavailable. 

If you obtain a dataset through FOIA, we would love to include it in OPD. If the data is already posted online, please share the link by creating an [issue](https://github.com/openpolicedata/openpolicedata/issues) or [emailing us](mailto:openpolicedata@gmail.com).

## Steps for Contributing
The below sections describe:
1. Steps for finding a new dataset
2. Adding a new datasets to the [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv)
3. Adding a new dataset to the [OPD Source Table](https://github.com/openpolicedata/opd-data/blob/main/opd_source_table.csv) 
4. Testing a new dataset to see if it works in the [OPD Python library](https://github.com/openpolicedata)
5. If tests fail above, updating the OPD code to incorporate the new dataset

There are many ways to contribute, and you can complete whichever steps you want and as many as you want. Ways to contribute include but are not limited to:
* You can tell us about a dataset from your local police department that is not currently in our tracking spreadsheet
* You can search across many police departments for data and notify us what you've found
* You can take a single dataset that you have found and complete all the steps above
* You can take a dataset that someone else found and add it to OpenPoliceData and/or develop the code necessary to add it

### Finding Untracked Police Departments and Their Data
To find untracked police departments, you first need to know what is being tracked. Check out the [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv). It tracks:
* Datasets already added (Scroll horizontally to find data columns with the value marked `Added`)
* Datasets recommended for inclusion but that have yet to be added (Marked with an `X`)
* Datasets requiring code updates to add (Marked by issue #'s referencing [OpenPoliceData data issues](https://github.com/openpolicedata/openpolicedata/issues))
* Datasets recommended for inclusion but who have reliability or other issues with the download URL or website (Marked `URL Issue`)
* Police departments or data sources that do not currently contain any data (Marked with an `X in the NO DATA column`)

The [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv) also includes information on:
* Name of the source (typically a police department)
* State
* Open data website URL (if any)
* Date last checked for open data
* User name of who last checked for open data (optional but useful if there are questions)
* What open data sets are available including if no data is available

#### `Which Departments/Sources to Check`
Next, you'll need to search the internet for data. First of all, where police departments and other sources share their open data can vary so if you're looking for open data for a specific department, be prepared to search a bit. That being said, there are some general guidelines for finding data in most cases.

Some states have laws that require all departments in the state to collect data and report it to the state for public release. Where these laws exist, the state-level sources are often the best sources since they include data for all departments in the state.

Another option is to just select any police department that is either not on the spreadsheet or hasn't been checked recently. Then, follow the instructions below for finding data on either their site or their municipality's (the county, city, or town) site.

Finally, muckrock is a website where FOIA requests are published. Very few datasets available on muckrock are currently included in OPD so this is a great place to look as anything that you find is likely not already in OPD.

#### `Searching for a Police Department's Data`
The most common location of police open data is on the municipality's Open Data site. This is also the location mostly likely to contain [qualifying data](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata). The following steps generally work for this case:
* Internet search for ``{town/county/city name} open data``. If it exists, the first link typically will be the Open Data Portal. Click on it.
![image](https://user-images.githubusercontent.com/67804485/156833142-87bc9c84-11a9-47de-b566-8c7b28c24750.png)
* Often, the data portal has a category called ``Public Safety`` or ``Community Safety``. If so, click on that as all police data will fall under this category.
* Scan the results for [qualifying datasets](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata)

If there is no Open Data Portal or if the Open Data Portal does not contain qualifying police data, data may exist on the department's website. In this case, try:
* Searching for ``{town/county/city name} Police Department open data``
* Searching for ``{town/county/city name} Police Department data transparency``
* Searching for ``{town/county/city name} Police Department data``
* Going to the department/source's page and looking for a data link.

**Remember that if you can't find anything for a police department, we want to know about that too!**

### Create an Issue for New Datasets and Police Departments with No Available Data
Notify the community about what you found.
* Go to our [issues](https://github.com/openpolicedata/openpolicedata/issues) page
* Select `New Issue`
* Select `Get Started` next to the `Issue template for new datasets or police departments that have no data` template
* Give the issue a title and fill in the details.
* If you plan on adding this dataset to OpenPoliceData yourself, click `assign yourself` under Assignees to let us know that you will be adding this dataset yourself.
* Click `Submit New Issue`

### Clone the opd-data Repository
To complete all of the following steps, you will need to have [git](https://git-scm.com/downloads) (link has instructions) installed. To check if you already have Git, run the following from the command line:
```sh
git --version
```

(If you don't want to install Git or are unable to (feel free to ask for help on our [Discussions page](https://github.com/openpolicedata/opd-data/discussions)), that's OK! If you are able to help us by adding issues (as described in the above step) that notify us of which departments have data and which don't, that is already a big help!)

Login to your [GitHub](https://github.com) account and make a fork of the
[opd-data repository](https://github.com/openpolicedata/opd-data/) by clicking the "Fork" button.
Clone your fork (in terminal on Mac/Linux or Git Bash/GUI on Windows) to the location you'd like to keep it.
We are partial to creating a ``repos`` or ``projects`` directory in our home folder.

```sh
git clone https://github.com/<your-user-name>/opd-data.git
```

### Update the Police Data Tracking Spreadsheet
The [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv) is how we keep track of which sources have been searched and which haven't and which departments have data and which don't.

The spreadsheet can be found in the opd-data folder that was created on your machine when you cloned the opd-data repo.

For each police department or data source that you research, start by updating the spreadsheet with the following information:
* Name of the source (typically a police department)
* State
* Open data website URL (if any)
* Date last checked for open data
* (Optional) Your user name

Then, fill in information indicating what data is available from the source:
* If the police department that you researched has no [qualifying data](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata), put an `X` in the `NO DATA` column.
* If qualifying data is available, mark the categories of data that are available with an `X`. If you are not sure, feel free to ask for help. If no category matches and you think the data should be made available in OpenPoliceData, submit an [issue](https://github.com/openpolicedata/opd-data/issues) and provide details about the dataset and why you think a category needs to be added.

If you are complete and will not be completing any of the following steps, submit a [Pull Request](#pull-requests) after you've completed adding all your research to the spreadsheet.

If you have any questions at anytime in the process, please contact us on the [Discussions page](https://github.com/openpolicedata/opd-data/discussions) or by [email](mailto:openpolicedata@gmail.com).

## Adding a Dataset to OpenPoliceData
In most cases, adding data to OPD will be as easy as adding a row to a spreadsheet. OpenPoliceData's data catalog is determined by the [OPD Source Table](https://github.com/openpolicedata/opd-data/blob/main/opd_source_table.csv) in the opd-data repository.

Add a row with as much of the following information as you can. Some columns may not be aplicable for certain datasets, and we can fill in any required information that you don't add. Feel free to skip columns that you don't know even if they are marked required below. Required indicates that they are required in order for the OPD Python library to load a dataset. They are not required for you to help us when adding all the information that you can to the Source Table.
* **State** (Required): State where agency(s) are
* **SourceName** (Required): In most cases, this is the location of the police department (i.e. Denver for the Denver Police Department). For state-level data, this should be the name of state.
* **Agency** (Required): In most cases, this is the same as the Source Name (i.e. the police department name). However, if the source includes multiple police departments such as a dataset that contains information on all stops across the state, this should be `MULTI`.
* **AgencyFull** (Required): This is the full name of the police department (i.e. Phoenix Police Department or Los Angeles County Sheriff's Department). Leave blank if it's not applicable (i.e. the source contains data for multiple agencies). This column differences from SourceName in that SourceName is typically just the location (i.e. Phoenix or Los Angeles County) that is used as shorthand when accessing data.
* **TableType** (Required): This defines the type of data (i.e. `TRAFFIC STOPS`, `USE OF FORCE`, etc.) Typically, you'll want to use a label that has been used in a previous dataset. If you think that the dataset that you found is of a new type, you can fill this out with your own type that best describes the data. You only need to try your best here. We will review and correct as needed.
* **coverage_start** (Required): Earliest date available in the dataset
* **coverage_end** (Required): Most recent date available in the dataset
* **last_coverage_check**: The date that you recorded coverage_start and coverage_end. Some datasets are updated periodically so it is valuable to know when someone last checked the coverage.
* **Description**: Description of the dataset. You can often use text from the dataset's website.
* **source_url**: This is the main website (or landing page for the dataset). Try out the links for other datasets for examples.
* **readme**: Link to website or downloadable file containing readme (i.e. descriptions of fields and/or data values) information for a dataset
* **URL** (Required): This is the URL where OpenPoliceData can access the data. 
    * For ArcGIS data, it is usually found by clicking `View Full Details`, then `View API Resources`, and copying the GeoService URL that includes the string `FeatureServer` or `MapServer` followed by a forward slash (/) and a number (i.e. https://opendata.baltimorecity.gov/egis/rest/services/Hosted/911_Calls_For_Service_2018_csv/FeatureServer/0). 
    * For Socrata data, it is the top-level website. For example, if the data is found at https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q, the URL is simply data.montgomerycountymd.gov. In most cases, the initial "https://" should not be include.
    * For CKAN data,  it is the top-level website. For example, if the data is found at https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q, the URL is simply https://www.phoenixopendata.com. 
    * For all other file types (CSV, Excel, etc.), it is the link that downloads the file when you click on it in your internet browser. If you go to the RIPA Stop Data at the [California DOJ's Open Justice site](https://openjustice.doj.ca.gov/data), you can right click on the link to download the CSV and copy the link URL. Sometimes, this doesn't work. In these cases, download the data. Then, go to `Downloads` in your browser, right click on the downloaded file in the Downloads list, and select `Copy Download Link`.
* **Year** (Required): If the dataset contains data for a single year (usually indicated in the title of the dataset on the website), enter the year. If the data contains multiple years, enter `MULTI`. If the data does not correspond to a specific date (typically employee demographics data), enter `None`.
* **DataType** (Required): This tells OpenPoliceData how to access and load the data. Formats currently suppported by OPD are Excel, CSV, Arcgis, Socrata, Carto, and CKAN. We prefer options that allow access by a REST API (all current options except CSV and Excel) because we can request that the website filter the data before we download it. If these options are not available, downloadable file types can be used (CSV or Excel). If the type of data does not match any of these types, open an [issue](https://github.com/openpolicedata/openpolicedata/issues) in the OpenPoliceData repo and indicate that a new data loader needs added for your dataset.
* **date_field** (Required IF Year is `MULTI` AND DataType is not CSV or Excel): This is the field or column header in the data where the date is stored.
* **dataset_id** (See below for which data types require this): Information telling OPD which dataset(s) to read.
    * Socrata (Required): Consists of 2 4-digit values separated by a hyphen(-). For example, if the data is found at https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q, the dataset_id is 4mse-ku6q.
    * CKAN (Required): 
    * Carto (Required):
    * Excel (Required in *very rare* instances): For Excel files, dataset_id should almost always be left blank. The following instances require a dataset_id:
        * If there are multiple sheets in the Excel file and only one contains data, this is the name of the sheet containing data.
        * If a year's worth of data (minimum time duration for an OPD dataset) is spread across multiple Excel files, put the common part of the Excel file URL in the URL column and put the remaining part of the URL in a semi-colon separated list in the dataset_id column. For example, Wallkill PD's pedestrian stops data is broken up into quarters. The URLs for 2017 for the first 2 quarters are https://wallkillpd.org/document-center/data/vehicle-a-pedestrian-stops/2017-vehicle-pedestrian-stops/182-2017-1st-quarter-vehicle-pedestrian-stops/file.html and https://wallkillpd.org/document-center/data/vehicle-a-pedestrian-stops/2017-vehicle-pedestrian-stops/192-2017-2nd-quarter-vehicle-pedestrian-stops/file.html. The URL column should be https://wallkillpd.org/document-center/data/vehicle-a-pedestrian-stops/2017-vehicle-pedestrian-stops and the dataset_id column should be 182-2017-1st-quarter-vehicle-pedestrian-stops/file.html; 192-2017-2nd-quarter-vehicle-pedestrian-stops/file.html; ... (actual dataset_id also contains 3rd and 4th quarter URL segments)
* **agency_field** (Required IF Agency is `MULTI`): This is the field or column containing the agency corresponding to each row of data.
* **min_version**: Minimum version of OPD that dataset is compatible with. Leave blank unless you ran tests on this dataset and those tests failed.
* **query**: Query to apply when loading data such that the **TableType** is accurate. This only currently applies to 1 dataset, and it can be assumed that this can be left blank. The one current usage involves data that is read from a dataset containing all shootings in Philadelphia where the dataset is filtered to only include officer-involved shooting to create a OFFICER-INVOLVED SHOOTINGS table type.

### Testing That The Dataset Works in OpenPoliceData
See the [Contributing Guide for the OpenPoliceData repository](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD/) for how to test that the added datasets in OpenPoliceData work properly.

## Pull Requests
The changes to opd-data code, documentation, and spreadsheets should be made via GitHub pull requests against ``main``, even for those with administration rights. While it's tempting to make changes directly to ``main`` and push them up, it is better to make a pull request so
that others can give feedback. 

**Working on your first Pull Request?** You can learn how from this *free* video series
[How to Contribute to an Open Source Project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github),
Aaron Meurer's [tutorial on the git workflow](https://www.asmeurer.com/git-workflow/), or the
guide [“How to Contribute to Open Source"](https://opensource.guide/how-to-contribute/).

Commit the changes you made. Chris Beams has written a [guide](https://cbea.ms/git-commit/) on how to write good commit messages.

Push to your fork and [submit a pull request](https://github.com/openpolicedata/openpolicedata/compare).

## Qualifying Data - What Types of Datasets Should be Included in OpenPoliceData?
Here are some guidelines for identifying datasets to add to the OpenPoliceData package:
* We are mainly looking for incident-level data. This means that the dataset is not a summary and is a table that has a row for every police action of a particular type. For example, a summarized dataset might be the number of traffic stops in 2019 for different groups (races, ethnicities, ages, etc.). Incident-level data for traffic stops would be a table with a row for each traffic stop that contains details about that specific stop.
* The one current exception to the above incident-level rule is police employee data (i.e. officer demographics). This data can be included in OpenPoliceData.
* Data in OpenPoliceData should be anonymized (with the exception of officer-involved shootings). That means that data that contains community member's personal information (such as names or home addresses) should not be included. It is a privacy issue if police departments are sharing personal information online, and we do not want to spread it further.
    * Addresses are sometimes OK. If a dataset includes *specific* addresses AND there is a reasonable possibility that the incident occurred at a personal residence (such as for (crime) incidents, calls for service, and arrests), the dataset should not be included. Addresses in traffic and pedestrian stop data data are OK if they refer to the location of the incident and not a community member's personal address.
    * Names of community members are NOT OK except for officer-involved shootings. Officer-involved shootings are routinely reported on by the press, and therefore the names can be assumed to be already publicly available.
    * Since police departments are the original source of all data, the personal information of the departments' officers is considered OK.
    * If you have any questions about a piece of information, please ask!
* The data needs to be in a format that can be accessed and extracted easily. For example, PDFs are complicated and difficult to automatically parse (although we have a long term goal of extracting data from PDFs and HTML if you are interested). Data listed in HTML pages are also difficult to automatically extract. Most other formats are likely to be fine although some formats may need to have code added to OpenPoliceData to allow them to be imported.

## What if I find a dataset in OpenPoliceData that should not be included?
Have you found a dataset in OpenPoliceData that has people's personal information? Is there data that does not satisfy one of the other constraints, such as not being incident-level? Please let us know on the [Discussions page](https://github.com/openpolicedata/opd-data/discussions) or by [email](mailto:openpolicedata@gmail.com) so that it can be reviewed!

## Still Have Questions?
Check out our [Discussions page](https://github.com/openpolicedata/openpolicedata/discussions) or [send us an email](mailto:openpolicedata@gmail.com)
