import re
import pandas as pd
from datetime import datetime
from pathlib import Path

from openpolicedata.data_loaders import Arcgis, Carto, Ckan, Csv, Excel, Html, Socrata

OPD_SOURCE_TABLE = Path(__file__).parent.parent.parent / "opd_source_table.csv"
DELETED_TABLE = Path(__file__).parent.parent.parent / "datasets_deleted_by_publisher.csv"

# Map DataType to loader class and required spreadsheet_fields
LOADER_MAP = {  
    "arcgis": {
        "loader": Arcgis,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType"],  # date_field required if Year is MULTI, but unlikely if URL contains year 
        "constructor": lambda url, sf: (url, sf.get("date_field"), sf.get("query"))
    },
    "carto": {
        "loader": Carto,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType", "dataset_id"],  # dataset_id required
        "constructor": lambda url, sf: (url, sf["dataset_id"], sf.get("date_field"), sf.get("query"))
    },
    "ckan": {
        "loader": Ckan,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType", "dataset_id"],  # dataset_id required
        "constructor": lambda url, sf: (url, sf["dataset_id"], sf.get("date_field"), sf.get("query"))
    },
    "csv": {
        "loader": Csv,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType"],
        "constructor": lambda url, sf: (url, sf.get("date_field"), sf.get("agency_field"), sf.get("dataset_id"), sf.get("query"))
    },
    "excel": {
        "loader": Excel,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType"],
        "constructor": lambda url, sf: (url, sf.get("dataset_id"), sf.get("date_field"), sf.get("agency_field"))
    },
    "html": {
        "loader": Html,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType"],
        "constructor": lambda url, sf: (url, sf.get("date_field"), sf.get("agency_field"))
    },
    "socrata": {
        "loader": Socrata,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType", "dataset_id"],  # dataset_id required
        "constructor": lambda url, sf: (url, sf["dataset_id"], sf.get("date_field"))
    }
}

def is_data_available(data_type, url, spreadsheet_fields):
    """
    Returns True if the endpoint is available and has at least one record.
    Returns False if not available, not accessible, or has no data.
    """
    loader_info = LOADER_MAP.get(data_type.lower())
    if not loader_info:
        raise ValueError(f"Unsupported DataType: {data_type}")
    # Check required fields
    for field in loader_info["required_fields"]:
        if field not in spreadsheet_fields or (spreadsheet_fields[field] in [None, ""] and field != "date_field"):
            raise ValueError(f"Missing required field '{field}' for DataType '{data_type}'")
    try:
        args = loader_info["constructor"](url, spreadsheet_fields)
        loader = loader_info["loader"](*args)
        count = loader.get_count()
        return count > 0
    except Exception as e:
        return False

def find_valid_url_for_year(url, year, year_str, data_type, spreadsheet_fields):
    """
    Returns (is_valid, new_url, coverage_start, coverage_end)
    """
    new_url = url.replace(year_str, str(year), 1)
    try:
        valid = is_data_available(data_type, new_url, spreadsheet_fields)
    except Exception as e:
        valid = False
    return valid, new_url

def try_url_years(
    url: str,
    spreadsheet_fields: dict,
    n_years = 5,
    forward: bool = None,
    year_slice: tuple = None,
    verbose: bool = False
):
    """
    Try to find valid URLs by replacing a 4-digit year in the URL.
    Args:
    url: str. The URL to modify.
    spreadsheet_fields: dict with fields to use for checking validity and adding to OPD_Source_table.
    n_years: int or range. If int, tries n_years forward (or backward if forward=False). If range, uses as years to try.
    forward: If n_years is int, direction to try. If None, defaults to True.
    year_slice: tuple of (start, end) to slice the URL for the year. If None, finds the first 4-digit year in the URL.
    verbose: If True, prints progress messages.
    
    Attempts to find valid URLs by incrementing or decrementing the year in the URL and checking if the resulting URL is valid.
    Updates OPD_SOURCE_TABLE and DELETED_TABLE as appropriate.
    """
    if spreadsheet_fields is None or "DataType" not in spreadsheet_fields:
        raise ValueError("spreadsheet_fields must include 'DataType' key")
    data_type = spreadsheet_fields["DataType"].lower()

    # 1. Find 4-digit year in URL
    if year_slice:
        year_str = url[year_slice[0]:year_slice[1]]
        if not re.fullmatch(r"\d{4}", year_str):
            raise ValueError("Specified slice does not contain a 4-digit year.")
    else:
        matches = re.findall(r"\d{4}", url)
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one 4-digit year in URL, found {len(matches)}: {matches}")
        year_str = matches[0]

    year = int(year_str)

    # Determine years_to_try
    current_year = datetime.now().year
    if isinstance(n_years, range):
        years_to_try = [y for y in n_years if y <= current_year]
    elif isinstance(n_years, int):
        if forward is None:
            forward = True
        if forward:
            years_to_try = [y for y in range(year + 1, year + n_years + 1) if y <= current_year]
        else:
            years_to_try = [y for y in range(year - 1, year - n_years - 1, -1) if y <= current_year]
    else:
        raise ValueError("n_years must be an int or a range.")
    
    df = pd.read_csv(OPD_SOURCE_TABLE)
    # deleted_df = pd.read_csv(DELETED_TABLE)
    current_year = datetime.now().year    
    
    for y in years_to_try:
        new_url = url.replace(year_str, str(y), 1)
        # Check if URL is already in the spreadsheet
        in_spreadsheet = (df["URL"] == new_url).any()
        if in_spreadsheet:
            if verbose:
                print(f"{new_url} already in spreadsheet. Skipping.")
            continue
            # idx = df.index[df["URL"] == new_url][0]
            # row = df.loc[idx]
            # # Test if still valid
            # valid, _ = find_valid_url_for_year(url, y, year_str, data_type, spreadsheet_fields)
            # if valid:
            #     # Update last_coverage_check and coverage dates
            #     df.at[idx, "last_coverage_check"] = datetime.now().strftime("%m/%d/%Y")
            #     df.to_csv(OPD_SOURCE_TABLE, index=False)
            #     if verbose:
            #         print(f"{new_url} already in spreadsheet and valid. Updated last_coverage_check.")
            #     continue
            # else:
            #     # Remove from spreadsheet, add to deleted
            #     if verbose:
            #         print(f"{new_url} in spreadsheet but no longer valid. Removing and logging as deleted.")
            #     removed_row = df.loc[[idx]].copy()
            #     deleted_fields = [
            #         "OPD Status","State","SourceName","Agency","AgencyFull","TableType","Year","Error",
            #         "Date Outage Started","Last Outage Confirmation","Date Outage Ended","Date Last Contacted",
            #         "Contact Details","source-url","URL","dataset-id"
            #     ]
            #     # Prepare the deleted row
            #     deleted_row = {f: "" for f in deleted_fields}
            #     deleted_row["OPD Status"] = f"Deleted from OPD {datetime.now().strftime('%m/%d/%Y')}"
            #     deleted_row["Error"] = "URL no longer valid via prediction_funcs test."
            #     for col in deleted_fields:
            #         if col in removed_row.columns:
            #             deleted_row[col] = removed_row.iloc[0].get(col, "")

            #     # Append to deleted_df and update files
            #     deleted_df = pd.concat([deleted_df, pd.DataFrame([deleted_row])], ignore_index=True)
            #     df = df.drop(idx)
            #     df.to_csv(OPD_SOURCE_TABLE, index=False)
            #     deleted_df.to_csv(DELETED_TABLE, index=False)
            #     continue
        else:
            valid, new_url = find_valid_url_for_year(url, y, year_str, data_type, spreadsheet_fields)
            if valid:
                if verbose:
                    print(f"Valid URL found: {new_url}. Adding to OPD_Source_table.")
                # Prepare row for OPD_Source_table
                fields = [
                    "State", "SourceName", "Agency", "AgencyFull", "TableType",
                    "coverage_start", "coverage_end", "last_coverage_check", "Description",
                    "source_url", "readme", "URL", "Year", "DataType", "date_field",
                    "dataset_id", "agency_field", "min_version", "query"
                ]
                row = {f: "" for f in fields}
                row["URL"] = new_url
                row["Year"] = str(y)
                row["last_coverage_check"] = datetime.now().strftime("%m/%d/%Y")
                row["coverage_start"] = f"01/01/{y}"
                if y == current_year:
                    row["coverage_end"] = datetime.now().strftime("%m/%d/%Y")
                else:
                    row["coverage_end"] = f"12/31/{y}"
                if spreadsheet_fields:
                    for k, v in spreadsheet_fields.items():
                        if k in row:
                            row[k] = v
                # Append to OPD_Source_table.csv
                df = pd.read_csv(OPD_SOURCE_TABLE)
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
                df.to_csv(OPD_SOURCE_TABLE, index=False)
                if verbose:
                    print(f"Added to OPD_Source_table: {new_url}")
                continue
            else:
                if verbose:
                    print(f"{new_url}: not valid. Skipping.")
    return None

def auto_update_sources(
    outdated_days=None,
    verbose=False
):
    """
    Automatically check for new data by incrementing year in URLs for testable sources. Adds valid URLs to OPD_Source_table.csv.

    Args:
        outdated_days (int): How many days before a source is considered outdated.
        verbose (bool): Print progress.
    """
    # 1. Load table and parse last_coverage_check as datetime
    df = pd.read_csv(OPD_SOURCE_TABLE)
    df["last_coverage_check_dt"] = pd.to_datetime(df["last_coverage_check"], errors="coerce")
    # 2. Group by composite key and get max last checked date
    composite_key = ['State', 'SourceName', 'AgencyFull', 'TableType']
    max_df = df.loc[df.groupby(composite_key)["Year"].idxmax()]
    # 3. Filter URLs with exactly one 4-digit year
    max_df = max_df[max_df["URL"].str.contains(r"\d{4}", regex=True)]
    max_df = max_df[max_df["URL"].str.count(r"\d{4}") == 1]
    # Keep rows where last_coverage_check_dt is NaT or older than outdated_days
    current_date = datetime.now()
    if outdated_days is not None:
        cutoff_date = current_date - pd.Timedelta(days=outdated_days)
        to_test = max_df[max_df["last_coverage_check_dt"] <= cutoff_date]
    else:
        last_year = current_date.year - 1
        to_test = max_df[max_df["last_coverage_check_dt"].dt.year <= last_year]
    to_test = to_test.drop(columns=["last_coverage_check_dt"])
    # 6. For each outdated row, try to find new URLs by incrementing year using try_url_years
    current_year = datetime.now().year
    count = 0
    for row in to_test.itertuples(index=False):
        url = row.URL
        data_type = row.DataType.lower()
        year_match = re.search(r"\d{4}", url)
        if not year_match:
            continue
        year_str = year_match.group()
        year = int(year_str)
        spreadsheet_fields = row._asdict()
        if year < current_year:
            for y in range(year + 1, current_year + 1):
                valid, new_url = find_valid_url_for_year(url, y, year_str, data_type, spreadsheet_fields)
                if valid:
                    new_row = spreadsheet_fields.copy()
                    new_row["URL"] = new_url
                    new_row["Year"] = str(y)
                    new_row["last_coverage_check"] = datetime.now().strftime("%m/%d/%Y")
                    new_row["coverage_start"] = f"01/01/{y}"
                    new_row["coverage_end"] = f"12/31/{y}"
                    df = pd.read_csv(OPD_SOURCE_TABLE)
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    df.to_csv(OPD_SOURCE_TABLE, index=False)
                    count += 1
                    if verbose:
                        print(f"Added to OPD_Source_table: {new_url}") 
                else:
                    if verbose:
                        print(f"{new_url}: not valid. Skipping.")
    return print(f"Checked {len(to_test)} sources for new URLs, found {count} new sources.")