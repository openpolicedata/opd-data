import re
import pandas as pd
from datetime import datetime
from pathlib import Path

from openpolicedata.data_loaders import Arcgis, Carto, Ckan, Csv, Excel, Html, Socrata

OPD_SOURCE_TABLE = Path(__file__).parent.parent.parent / "opd_source_table.csv"
TRACKING_TABLE = Path(__file__).parent.parent.parent / "police_data_source_tracking.csv"

# Map DataType to loader class and required spreadsheet_fields
LOADER_MAP = {
    "arcgis": {
        "loader": Arcgis,
        "required_fields": ["State", "SourceName", "Agency", "AgencyFull", "TableType", "DataType", "date_field"],  # date_field required if Year is MULTI
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
    years_to_try: range = None,
    forward: bool = False,
    n_years: int = 5,
    year_slice: tuple = None,
    spreadsheet_fields: dict = None,
    verbose: bool = True
):
    """
    Try to find valid URLs by replacing a 4-digit year in the URL.
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
    if years_to_try is None:
        if forward:
            years_to_try = range(year + 1, year + n_years + 1)
        else:
            years_to_try = range(year - 1, year - n_years - 1, -1)

    for y in years_to_try:
        new_url = url.replace(year_str, str(y), 1)
        try:
            valid = is_data_available(data_type, new_url, spreadsheet_fields)
            resp_status = "OPD loader check"
        except Exception as e:
            if verbose:
                print(f"Error fetching {new_url}: {e}")
            continue
        if valid:
            if verbose:
                print(f"Valid URL found: {new_url} (status: {resp_status})")
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
            return new_url
        else:
            if verbose:
                print(f"{new_url}: not valid (status: {resp_status})")
    if verbose:
        print("No valid URL found.")
    return None

def auto_update_sources(
    source_table_path=OPD_SOURCE_TABLE,
    tracking_table_path=TRACKING_TABLE,
    outdated_days=30,
    verbose=True
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
    df = pd.read_csv(source_table_path)
    tracking = pd.read_csv(tracking_table_path)
    # 2. Filter by DataType (use all testable types in LOADER_MAP)
    testable_types = list(LOADER_MAP.keys())
    df = df[df["DataType"].str.lower().isin([t.lower() for t in testable_types])]
    # 3. Filter URLs with exactly one 4-digit year
    df = df[df["URL"].str.contains(r"\d{4}", regex=True)]
    df = df[df["URL"].str.count(r"\d{4}") == 1]
    # 4. Check last checked date
    columns_to_match = ['State', 'SourceName', 'AgencyFull', 'TableType']
    outdated = []
    for idx, row in df.iterrows():
        # Find matching tracking row
        mask = (tracking[columns_to_match] == row[columns_to_match]).all(axis=1)
        last_checked = None
        if mask.any():
            last_checked = tracking.loc[mask, "last_coverage_check"].values[0]
        if last_checked:
            try:
                last_checked_dt = pd.to_datetime(last_checked)
            except Exception:
                last_checked_dt = datetime.min
        else:
            last_checked_dt = datetime.min
        if (datetime.now() - last_checked_dt).days >= outdated_days:
            outdated.append(idx)
    if not outdated:
        if verbose:
            print("No outdated sources found.")
        return []
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