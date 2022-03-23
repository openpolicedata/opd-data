# Contributer's Guide to Recommending or Adding Datasets
Thank you for considering contributing to OpenPoliceData. Below is a guide to helping identify data to include in the [OpenPoliceData](https://github.com/openpolicedata/openpolicedata) Python package. Although the main goal is to add datasets to OpenPoliceData, the spreadsheets developed here serve an additional purpose as a way to track which police departments in the United States are releasing incident-level data to the public. 

OpenPoliceData is community-driven project. It's people like you that make OpenPoliceData useful and successful. Your contributions are necessary to make both OpenPoliceData and the overall data tracking as complete and thorough as possible. Anyone can help. We have tasks available for both developers and non-developers. This guide will help you get started.

Adding or recommending datasets to be included in OpenPoliceData is a great way to contribute to the OpenPoliceData project. Help is needed with one or more of the following:
* Identifying previously untracked police departments and/or datasets
* Adding new datasets to the OpenPoliceData package
* Developing and testing any new code required to incorporate new datasets

This guide provides suggestions for how to find police data, what types of data should be included, how to recommend a dataset to the team (if you are recommending), and how to add the dataset to OpenPoliceData (if you are helping with development). Note that OpenPoliceData does not provide access to all types of police datasets. The types of data that can be added to OpenPoliceData are referred to as [qualifying datasets](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata) and are described further below. 

``Developers``: Please review the [OpenPoliceData Contributing Guide](https://github.com/openpolicedata/openpolicedata/blob/main/CONTRIBUTING.MD) before or after reviewing this document.

If you need any help after reading this guide, feel free to ask for it on our [Discussions page](https://github.com/openpolicedata/opd-data/discussions).

## Table of Contents
* [Ground Rules](#ground-rules)
* [Steps for Contributing](#steps-for-contributing)
    * [Finding Untracked Police Departments and Their Data](#finding-untracked-police-departments-and-their-data)
    * [Create an Issue for Untracked Police Departments and New Datasets](#create-an-issue-for-untracked-police-departments-and-new-datasets)
    * [Clone the opd-data Repository](#clone-the-opd-data-repository)
    * [Update the Police Data Tracking Spreadsheet](#update-the-police-data-tracking-spreadsheet)
    * [Adding a Dataset to OpenPoliceData](#adding-a-dataset-to-openpolicedata)
    * [Testing That The Dataset Works in OpenPoliceData](#testing-that-the-dataset-works-in-openpolicedata)
* [Pull Requests](#pull-requests)
* [What Types of Datasets Should be Included in OpenPoliceData?](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata)
* [What if I find a dataset in OpenPoliceData that should not be included?](#what-if-i-find-a-dataset-in-openpolicedata-that-should-not-be-included)

## Ground Rules

The goal is to maintain a diverse community that's pleasant for everyone. Please be considerate and respectful of others by following our
[code of conduct](https://github.com/openpolicedata/openpolicedata/blob/main/CODE_OF_CONDUCT.md).

## Steps for Contributing
The below sections describe, in order, the steps for finding a new dataset, adding it to the [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv), adding them to the OpenPoliceData package, updating the OpenPoliceData code to incorporate new datasets (if necessary), and testing that the new dataset works properly in the OpenPoliceData package. There are many ways to contribute, and you can complete whichever steps you want and as many as you want. Ways to contribute include but are not limited to:
* You can tell us about a dataset from your local police department that is not currently in our tracking spreadsheet
* You can search across many police departments for data and notify us what you've found
* You can take a single dataset that you have found and follow all the below steps
* You can take a dataset that someone else found and add it to OpenPoliceData and/or develop the code necessary to add it

### Finding Untracked Police Departments and Their Data
To find untracked police departments, you first need to know what is being tracked. Check out the [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv). It tracks:
* Datasets already added (Marked `Added`)
* Datasets recommended for inclusion but that have yet to be added (Marked with an `X`)
* Datasets requiring code updates to add (Marked by issue #'s referencing [OpenPoliceData data issues](https://github.com/openpolicedata/openpolicedata/issues?q=is%3Aopen+is%3Aissue+label%3Adata+))
* Datasets recommended for inclusion but who have reliability or other issues with the download URL or website (Marked `URL Issue`)
* Police departments or data sources that do not currently contain any data (Marked with an `X in the NO DATA column`)

The [police data tracking spreadsheet](https://github.com/openpolicedata/opd-data/blob/main/police_data_source_tracking.csv) also includes information on:
* Name of the source (typically a police department)
* State
* Open data website URL (if any)
* Date last checked for open data
* User name of who last checked for open data
* What open data sets are available including if no data is available
* Comments

#### `Which Departments/Sources to Check`
Next, you'll need to search the internet for data. First of all, where police departments and other sources share their open data can vary so if you're looking for open data for a specific department, be prepared to search a bit. That being said, there are some general guidelines for finding data in most cases.

The [Police Data Initiative](https://www.policedatainitiative.org/datasets) is a good place to start. As of the time of writing this document, not all department in this list have been checked for data.

The [Stanford Open Policing Project](https://openpolicing.stanford.edu/data/) is another place to check. The data available at this site is available through OpenPoliceData. However, many of these datasets have not been updated recently. Where it exists, we would like to replace the data that we are currently getting from Stanford with data that is made available by the departments themselves as these datasets may be more up-to-date.

Some states have laws that require all departments in the state to collect data and report it to the state for public release. Where these laws exist, the state-level sources are often the best sources since they include data for all departments in the state.

Otherwise, just select any police department that is either not on the spreadsheet or hasn't been checked recently. Then, follow the instructions below for finding data on either their site or their municipality's (the county, city, or town) site.

#### `Searching for a Police Department's Data`
The most common location of police open data is on the municipality's Open Data site. This is also the location mostly likely to contain [qualifying data](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata). The following steps generally work for this case:
* Internet search for ``{town/county/city name} open data``. If it exists, the first link typically will be the Open Data Portal. Click on it.
![image](https://user-images.githubusercontent.com/67804485/156833142-87bc9c84-11a9-47de-b566-8c7b28c24750.png)
* Often, the data portal has a category called ``Public Safety`` or ``Community Safety``. If so, click on that as all police data will fall under this category.
* Scan the results for [qualifying datasets](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata)

If there is no Open Data Portal or if the Open Data Portal does not contain qualifying police data, data may exist on the department's website. In this case, try searching for ``{town/county/city name} Police Department data`` or going to the department/source's page and looking for a data link.

Remember that if you can't find anything for a police department, we want to know about that too!

### Create an Issue for Untracked Police Departments and New Datasets
Notify the community about what you found.
* Go to our [opd-data issues](https://github.com/openpolicedata/opd-data/issues) page
* Select `New Issue`
* Select `Get Started` next to the `Issue template for new datasets or police departments that have no data` template
* Give the issue a title and fill in the details.
* If you plan on adding this dataset to OpenPoliceData yourself, click `assign yourself` under Assignees to let us know that you will be working this all the way through.
* Click `Submit New Issue`

### Clone the opd-data Repository
To complete all of the following steps, you will need to have [git](https://git-scm.com/downloads) (link has instructions) installed. To check if you already have Git, run the following from the command line:
```
git --version
```

(If you don't want to install Git or are unable to (feel free to ask for help on our [Discussions page](https://github.com/openpolicedata/opd-data/discussions)), that's OK! If you are able to help us by adding issues (as described in the above step) that notify us of which departments have data and which don't, that is already a big help!)

Login to your [GitHub](https://github.com) account and make a fork of the
[opd-data repository](https://github.com/openpolicedata/opd-data/) by clicking the "Fork" button.
Clone your fork of the opd-data repository (in terminal on Mac/Linux or git shell/GUI on Windows) to the location you'd like to keep it.
We are partial to creating a ``git_repos`` or ``projects`` directory in our home folder.

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
* Your user name
* Any other information that you think is importnat

Then, fill in information indicating what data is available from the source:
* If the police department that you researched has no [qualifying data](#qualifying-data---what-types-of-datasets-should-be-included-in-openpolicedata), put an `X` in the `NO DATA` column.
* If qualifying data is available, mark the categories of data that are available with an `X`. If you are not sure, feel free to ask for help. If no category matches and you think the data should be made available in OpenPoliceData, submit an [issue](https://github.com/openpolicedata/opd-data/issues) and provide details about the dataset and why you think a category needs to be added.

If you are complete and will not be completing any of the following steps, submit a [Pull Request](#pull-requests) after you've completed adding all your research to the spreadsheet.

If you have any questions at anytime in the process, please contact us on the [Discussions page](https://github.com/openpolicedata/opd-data/discussions).

## Adding a Dataset to OpenPoliceData
Adding data to the OpenPoliceData package in most cases will be as easy as adding a row to a spreadsheet. OpenPoliceData's data catalog is determined by the [OPD Source Table](https://github.com/openpolicedata/opd-data/blob/main/opd_source_table.csv) in the opd-data repository.

Add a row with the following information:
* State
* Source Name: In most cases, this is the name of the police department (i.e. Denver for the Denver Police Department). For some cases (typically state-level data), this should be the name of who owns the data (i.e. California Department of Justice).
* Jurisdiction: In most cases, this is the same as the Source Name (i.e. the police department name). However, if the source includes multiple police departments such as a dataset that contains information on all stops across the state, this should be `MULTI`.
* TableType: This defines the type of data (i.e. `TRAFFIC STOPS`, `USE OF FORCE`, etc.) This should match one of the table types in the TableTypes enum defined [here](https://github.com/openpolicedata/openpolicedata/blob/main/openpolicedata/_datasets.py) (HINT: search for "class TableTypes"). Use the string values to the right of the equal sign.
* Year: If the dataset contains data for a single year (usually indicated in the title of the dataset on the website), enter the year. If the data contains multiple years, enter `MULTI`. If the data does not correspond to a specific date (typically employee demographics data), enter `None`
* Description: Optional description of the dataset. You can often use some text from the dataset's website.
* DataType: This tells OpenPoliceData how to access and load the data. At the time of the writing of this document, acceptable values are CSV for comma separated values files, GeoJSON for GeoJSON files, ArcGIS for data available through an ArcGIS API, and Socrata for data available through a Socrata API. (For up-to-date data types, see the DataTypes enum defined [here](https://github.com/openpolicedata/openpolicedata/blob/main/openpolicedata/_datasets.py). We prefer options that allow access by a REST API (such as ArcGIS or Socrata) because we can request that the website filter the data before we download it. If these options are not available, downloadable file types can be used (such as CSV or GeoJSON). If the type of data does not match any of these types, open an [issue](https://github.com/openpolicedata/openpolicedata/issues) in the OpenPoliceData repo and indicate that a new data loader needs added for your dataset.
* URL: This is the URL where OpenPoliceData can access the data. For ArcGIS data, it is usually found by clicking `View Full Details`, then `View API Resources`, and copying the GeoServic URL that includes the string `FeatureServer` or `MapServer` followed by a forward slash (/) and a number (i.e. https://opendata.baltimorecity.gov/egis/rest/services/Hosted/911_Calls_For_Service_2018_csv/FeatureServer/0). For Socrata data, it is the top-level website. For example, if the data is found at https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q, the URL is simply data.montgomerycountymd.gov. For CSV and GeoJSON files, it is the link that downloads the file when you click on it in your internet browser. If you go to the RIPA Stop Data at the [California DOJ's Open Justice site](https://openjustice.doj.ca.gov/data), you can right click on the link to download the CSV and copy the link URL. Sometimes, this doesn't work. In these cases, download the data. Then, go to `Downloads` in your browser, right click on the downloaded file in the Downloads list, and select `Copy Download Link`.
* date_field: This is optional unless the Year is MULTI. This is the field or column header in the data where the date is stored.
* dataset_id: This is only for Socrata data where it is used to access the correct dataset. It consists of 2 4-digit values separated by a hyphen(-). For example, if the data is found at https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q, the dataset_id is 4mse-ku6q.
* jurisdiction_field: This is unnecessary unless the Jurisdiction is MULTI. This is the field or column header in the data where the jurisdiction is stored.

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
* Crime data should not be included
* Data in OpenPoliceData should be anonymized. That means that data that contains community member's personal information (such as names or addresses) should not be included. We believe that it is a privacy issue if police departments are sharing personal information online and do not want to spread it further.
    * Arrests data should not be included. This data has been found to be particularly likely to have personal information, and therefore, we believe it is safest to exclude it.
    * Currently, we include Calls for Service (i.e. information about 911 calls), but please carefully check that it does not contain personal info. It sometimes does, and we may exclude this category entirley in the future if it's found to too frequently contain such info.
* The data needs to be in a format that can be accessed and extracted easily. For example, PDFs are complicated difficult to parse. Most other formats are likely to be fine although some formats may need to have code added to OpenPoliceData to allow them to be imported.

## What if I find a dataset in OpenPoliceData that should not be included?
Have you found a dataset in OpenPoliceData that has people's personal information? Is there data that does not satisfy one of the other constraints, such as not being incident-level? Please let us know on the [Discussions page](https://github.com/openpolicedata/opd-data/discussions) so that it can be removed!