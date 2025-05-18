import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

OPD_SOURCE_TABLE = Path(__file__).parent.parent.parent / "opd_source_table.csv"
TRACKING_TABLE = Path(__file__).parent.parent.parent / "police_data_source_tracking.csv"

from openpolicedata.data_loaders import Arcgis

def is_arcgis_data_available(url):
    """
    Returns True if the ArcGIS endpoint is available and has at least one record.
    Returns False if not available, not accessible, or has no data.
    """
    try:
        ags = Arcgis(url)
        count = ags.get_count()
        return count > 0
    except Exception:
        return False

def try_url_years(
    url: str,
    years_to_try: range = None,
    forward: bool = False,
    n_years: int = 5,
    year_slice: tuple = None,
    extra_fields: dict = None,
    verbose: bool = True
):
    """
    Try to find valid URLs by replacing a 4-digit year in the URL.

    Args:
        url (str): The URL containing a 4-digit year.
        years_to_try (range, optional): Range of years to try. If None, uses +/- n_years from found year.
        forward (bool, optional): If True, test forward in time. Default is False (backward).
        n_years (int, optional): How many years to test. Default is 5.
        year_slice (tuple, optional): (start, end) indices to extract year substring.
        extra_fields (dict, optional): Extra fields for OPD_Source_table row.
        verbose (bool, optional): Print progress and results.
    Returns:
        str or None: The first valid URL found, or None.
    """
    # 1. Find 4-digit year in URL
    if year_slice:
        year_str = url[year_slice[0]:year_slice[1]]
        if not re.fullmatch(r"\d{4}", year_str):
            raise ValueError("Specified slice does not contain a 4-digit year.")
        matches = [year_str]
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
        is_arcgis = "featureserver" in new_url.lower() or "mapserver" in new_url.lower() or (extra_fields and extra_fields.get("DataType", "").lower() == "arcgis")
        try:
            if is_arcgis:
                valid = is_arcgis_data_available(new_url)
                resp_status = "ArcGIS check"
            else:
                resp = requests.head(new_url, allow_redirects=False, timeout=10)
                valid = resp.status_code == 200
                resp_status = resp.status_code
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
            if extra_fields:
                for k, v in extra_fields.items():
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
    # 2. Filter by DataType
    testable_types = ["CSV", "Excel", "Arcgis"]
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
        year_match = re.search(r"\d{4}", url)
        if not year_match:
            continue
        year_str = year_match.group()
        year = int(year_str)
        # Try next year
        new_year = year + 1
        new_url = url.replace(year_str, str(new_year), 1)
        try:
            resp = requests.head(new_url, allow_redirects=False, timeout=10)
        except Exception as e:
            if verbose:
                print(f"Error fetching {new_url}: {e}")
            continue
        content_type = resp.headers.get("Content-Type", "")
        if resp.status_code == 200 and (
            "csv" in content_type.lower() or
            "excel" in content_type.lower() or
            "arcgis" in new_url.lower()
        ):
            if verbose:
                print(f"Valid new URL found: {new_url}")
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
                print(f"{new_url}: status {resp.status_code}, content-type {content_type}")
    return new_urls