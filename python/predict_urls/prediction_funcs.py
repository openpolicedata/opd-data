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

def try_url_years(
    url: str,
    n_years = 5,
    forward: bool = None,
    year_slice: tuple = None,
    spreadsheet_fields: dict = None,
    verbose: bool = False,
):
    """
    Try to find valid URLs by replacing a 4-digit year in the URL.
    n_years: int or range. If int, tries n_years forward (or backward if forward=False). If range, uses as years to try.
    forward: If n_years is int, direction to try. If None, defaults to True.
    
    *Assumes date fields are related to the year in the URL.
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
    if isinstance(n_years, range):
        years_to_try = n_years
    elif isinstance(n_years, int):
        if forward is None:
            forward = True
        if forward:
            years_to_try = range(year + 1, year + n_years + 1)
        else:
            years_to_try = range(year - 1, year - n_years - 1, -1)
    else:
        raise ValueError("n_years must be an int or a range.")

    for y in years_to_try:
        df = pd.read_csv(OPD_SOURCE_TABLE)
        deleted_df = pd.read_csv(DELETED_TABLE)
        new_url = url.replace(year_str, str(y), 1)
         # Check if URL is already in the spreadsheet
        in_spreadsheet = (df["URL"] == new_url).any()
        if in_spreadsheet:
            idx = df.index[df["URL"] == new_url][0]
            row = df.loc[idx]
            # Test if still valid
            try:
                valid = is_data_available(data_type, new_url, spreadsheet_fields)
            except Exception as e:
                if verbose:
                    print(f"Error fetching {new_url}: \n {e}")
                continue
            if valid:
                # Update last_coverage_check and coverage dates
                df.at[idx, "last_coverage_check"] = datetime.now().strftime("%m/%d/%Y")
                df.to_csv(OPD_SOURCE_TABLE, index=False)
                if verbose:
                    print(f"{new_url} already in spreadsheet and valid. Updated last_coverage_check.")
                continue
            else:
                # Remove from spreadsheet, add to deleted
                if verbose:
                    print(f"{new_url} in spreadsheet but no longer valid. Removing and logging as deleted.")
                # TODO - fix fields for deleted_table
                removed_row = df.loc[[idx]].copy()
                deleted_fields = [
                    "OPD Status","State","SourceName","Agency","AgencyFull","TableType","Year","Error",
                    "Date Outage Started","Last Outage Confirmation","Date Outage Ended","Date Last Contacted",
                    "Contact Details","source-url","URL","dataset-id"
                ]
                # Prepare the deleted row
                deleted_row = {f: "" for f in deleted_fields}
                deleted_row["OPD Status"] = f"Deleted from OPD {datetime.now().strftime('%m/%d/%Y')}"
                deleted_row["Error"] = "URL no longer valid via prediction_funcs test."
                for col in deleted_fields:
                    if col in removed_row.columns:
                        deleted_row[col] = removed_row.iloc[0].get(col, "")

                # Append to deleted_df and update files
                deleted_df = pd.concat([deleted_df, pd.DataFrame([deleted_row])], ignore_index=True)
                df = df.drop(idx)
                df.to_csv(OPD_SOURCE_TABLE, index=False)
                deleted_df.to_csv(DELETED_TABLE, index=False)
                continue
        else:
            try:
                valid = is_data_available(data_type, new_url, spreadsheet_fields)
            except Exception as e:
                if verbose:
                    print(f"Error fetching {new_url}: \n {e}")
                continue
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
    outdated_days=30,
    verbose=False
):
    """
    Automatically check for new data by incrementing year in URLs for testable sources.

    Args:
        source_table_path (str or Path): Path to OPD_Source_table.csv.
        tracking_table_path (str or Path): Path to police_data_source_tracking.csv.
        outdated_days (int): How many days before a source is considered outdated.
        verbose (bool): Print progress and results.
    Returns:
        list: List of new URLs added.
    """
    # 1. Load tables
    df = pd.read_csv(OPD_SOURCE_TABLE)
    # 2. Filter by DataType (use all testable types in LOADER_MAP)
    testable_types = list(LOADER_MAP.keys())
    df = df[df["DataType"].str.lower().isin([t.lower() for t in testable_types])]
    # 3. Filter URLs with exactly one 4-digit year
    df = df[df["URL"].str.contains(r"\d{4}", regex=True)]
    df = df[df["URL"].str.count(r"\d{4}") == 1]
    # 4. Group by composite key and get max last checked date
    composite_key = ['State', 'SourceName', 'AgencyFull', 'TableType']
    max_df = df.loc[df.groupby(composite_key)["last_coverage_check"].idxmax()]
    # 5. Check for outdated sources 
    # Parse last_coverage_check as datetime
    max_df["last_coverage_check_dt"] = pd.to_datetime(max_df["last_coverage_check"], errors="coerce")
    last_year = datetime.now().year - 1
    # Keep rows where last_coverage_check is before Jan 1 of this year
    to_test = max_df[max_df["last_coverage_check_dt"].dt.year <= last_year]
   
   
    ## STOPPED HERE TODO
    # 6. Test URLs for 
    new_urls = []
    for idx in outdated:
        row = df.loc[idx]
        url = row["URL"]
        data_type = row["DataType"].lower()
        year_match = re.search(r"\d{4}", url)
        if not year_match:
            continue
        year_str = year_match.group()
        year = int(year_str)
        # Try next year
        new_year = year + 1
        new_url = url.replace(year_str, str(new_year), 1)
        # Prepare spreadsheet_fields/extra_fields for loader
        spreadsheet_fields = row.to_dict()
        spreadsheet_fields["Year"] = str(new_year)
        spreadsheet_fields["URL"] = new_url
        try:
            valid = is_data_available(data_type, new_url, spreadsheet_fields)
            resp_status = "OPD loader check"
        except Exception as e:
            if verbose:
                print(f"Error fetching {new_url}: {e}")
            continue
        if valid:
            if verbose:
                print(f"Valid new URL found: {new_url} (status: {resp_status})")
            # Add to OPD_Source_table
            new_row = row.copy()
            new_row["URL"] = new_url
            new_row["Year"] = str(new_year)
            new_row["last_coverage_check"] = datetime.now().strftime("%m/%d/%Y")
            opd_df = pd.read_csv(source_table_path)
            opd_df = pd.concat([opd_df, pd.DataFrame([new_row])], ignore_index=True)
            opd_df.to_csv(source_table_path, index=False)
            if verbose:
                print(f"Added to OPD_Source_table: {new_url}")
            new_urls.append(new_url)
        else:
            if verbose:
                print(f"{new_url}: not valid (status: {resp_status})")
    return new_urls