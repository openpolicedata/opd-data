"""Adds datasets from Stanford Open Policing Project
The Stanford Open Policing Project contains datasets from many cities.
This methods adds all the datasets from Stanford that are not already added and updates dates
of previously added datasets.
"""

import pandas as pd
import requests
from datetime import datetime

plot_flag = False

new_cases = {'New Jersey Camden':'Camden County Police Department',
             'Virginia State Patrol':"Virginia State Police"}

_us_state_abbrev = {
    'AL' : 'Alabama', 
    'AK' : 'Alaska',
    'AS' : 'American Samoa',
    'AZ' : 'Arizona',
    'AR' : 'Arkansas',
    'CA' : 'California',
    'CO' : 'Colorado',
    'CT' : 'Connecticut',
    'DE' : 'Delaware',
    'DC' : 'District of Columbia',
    'FL' : 'Florida',
    'GA' : 'Georgia',
    'GU' : 'Guam',
    'HI' : 'Hawaii',
    'ID' : 'Idaho',
    'IL' : 'Illinois',
    'IN' : 'Indiana',
    'IA' : 'Iowa',
    'KS' : 'Kansas',
    'KY' : 'Kentucky',
    'LA' : 'Louisiana',
    'ME' : 'Maine',
    'MD' : 'Maryland',
    'MA' : 'Massachusetts',
    'MI' : 'Michigan',
    'MN' : 'Minnesota',
    'MS' : 'Mississippi',
    'MO' : 'Missouri',
    'MT' : 'Montana',
    'NE' : 'Nebraska',
    'NV' : 'Nevada',
    'NH' : 'New Hampshire',
    'NJ' : 'New Jersey',
    'NM' : 'New Mexico',
    'NY' : 'New York',
    'NC' : 'North Carolina',
    'ND' : 'North Dakota',
    'MP' : 'Northern Mariana Islands',
    'OH' : 'Ohio',
    'OK' : 'Oklahoma',
    'OR' : 'Oregon',
    'PA' : 'Pennsylvania',
    'PR' : 'Puerto Rico',
    'RI' : 'Rhode Island',
    'SC' : 'South Carolina',
    'SD' : 'South Dakota',
    'TN' : 'Tennessee',
    'TX' : 'Texas',
    'UT' : 'Utah',
    'VT' : 'Vermont',
    'VI' : 'Virgin Islands',
    'VA' : 'Virginia',
    'WA' : 'Washington',
    'WV' : 'West Virginia',
    'WI' : 'Wisconsin',
    'WY' : 'Wyoming'
}

def find_next(r, string, last_loc):
    new_loc = r.text[last_loc+1:].find(string)
    if new_loc >= 0:
        new_loc += last_loc+1
    return new_loc

def find_next_state(r, last_loc):
    new_loc = find_next(r, '<tr class="state-title">', last_loc)
    if new_loc < 0:
        return new_loc, None
    td_loc = find_next(r, '<td', new_loc)
    start = find_next(r, '>', td_loc)+1
    end = find_next(r, '<', start)
    name = r.text[start:end].strip()
    name = _us_state_abbrev[name]
    return new_loc, name

def find_next_pd(r, last_loc):
    new_loc = find_next(r, '<td class="state text-left" data-title="State">', last_loc)
    if new_loc < 0:
        return new_loc, None, None
    span_loc = find_next(r, '<span', new_loc)
    start = find_next(r, '>', span_loc)+1
    end = find_next(r, '<', start)
    name = r.text[start:end].strip()
    local_str = "<sup>1</sup>"
    is_multi = r.text[end:end+len(local_str)] == local_str
    return new_loc, name, is_multi

def find_next_csv(r, start, end):
    open_loc = start
    while open_loc < end:
        open_loc = find_next(r, '<a href', open_loc+1)
        if open_loc >= end:
            raise ValueError("unable to find CSV")
        close_loc = find_next(r, '</a>', open_loc)
        if close_loc >= end:
            raise ValueError("unable to find CSV")

        if "Download data as CSV" in r.text[open_loc:close_loc]:
            first_quote = find_next(r, '"', open_loc)
            last_quote = find_next(r, '"', first_quote+1)
            return r.text[first_quote+1:last_quote]

    raise ValueError("unable to find CSV")

def includes_pedestrian_stops(r, start, end):
    open_loc = find_next(r, '<td class="text-right" data-title="Stops">', start)
    if open_loc >= end:
        raise ValueError("Unable to find # of stops")

    close_loc = find_next(r, '</td>', open_loc)
    if close_loc >= end:
        raise ValueError("Unable to find # of stops")

    return '<sup>2</sup>' in r.text[open_loc:close_loc]

def find_time_range(r, start, end):
    open_loc = find_next(r, '<td class="text-right" data-title="Time range">', start)
    if open_loc >= end:
        raise ValueError("Unable to find time range")

    close_loc = find_next(r, '</span></td>', open_loc)
    if close_loc >= end:
        raise ValueError("Unable to find time range end")

    date_start = r.text[close_loc-10-45:close_loc-45]
    date_stop = r.text[close_loc-10:close_loc]
    return datetime.strptime(date_start, "%Y-%m-%d"), datetime.strptime(date_stop, "%Y-%m-%d")


opd_csv = "opd_source_table.csv"
df = pd.read_csv(opd_csv)
stanford_desc = "Standardized stop data from the Stanford Open Policing Project"
readme = 'https://github.com/stanford-policylab/opp/blob/master/data_readme.md'
source_url = 'https://openpolicing.stanford.edu/data/'

not_stanford = ~df["URL"].str.lower().str.contains('stanford.edu')
df_stanford_old = df[~not_stanford]
df = df[not_stanford]

url = "https://openpolicing.stanford.edu/data/"

r = requests.get(url)

row_states = df["State"].to_list()
row_pds = ["Charlotte" if x == "Charlotte-Mecklenburg" else x for x in df["Agency"].to_list()]
row_pds = [x.replace("St.","Saint") if x.startswith('St.') else x for x in row_pds]
row_types = df["TableType"].to_list()

st_loc, state = find_next_state(r, -1)
next_st_loc, next_state = find_next_state(r, st_loc)
pd_loc, pd_name, is_multi = find_next_pd(r, -1)
num_datasets = 0
end_dates = []
while pd_loc >= 0 and pd_loc != len(r.text):
    next_pd_loc, next_pd_name, next_is_multi = find_next_pd(r, pd_loc+1)
    if next_pd_loc < 0:
        next_pd_loc = len(r.text)

    num_datasets += 1
    csv_file = find_next_csv(r, pd_loc, next_pd_loc)

    if includes_pedestrian_stops(r, pd_loc, next_pd_loc):
        table_type = "STOPS"
    else:
        table_type = "TRAFFIC STOPS"

    start_date, stop_date = find_time_range(r, pd_loc, next_pd_loc)
    end_dates.append(stop_date)

    if is_multi:
        source_name = state
        jurisdiction = "MULTI"
        jurisdiction_field = "department_name"
    else:
        source_name = pd_name
        jurisdiction = pd_name
        jurisdiction_field = ""

    already_added = False
    for k in range(len(row_states)):
        if (jurisdiction == row_pds[k] or (jurisdiction=='State Patrol' and row_pds[k]=='State Police')) and \
            state == row_states[k] and table_type == row_types[k]:
            already_added = True
            break

    date_field = "date"

    matches = (df_stanford_old['Agency']==jurisdiction) & \
        (df_stanford_old['State']==state)
    if already_added:
        if jurisdiction=='Charlotte':
            jurisdiction = source_name = "Charlotte-Mecklenburg" 
        elif jurisdiction.startswith('Saint'):
            jurisdiction = source_name = source_name.replace('Saint', 'St.')
        matches = (df['Agency']==jurisdiction) & \
            (df['State']==state) & \
            (df['SourceName']==source_name)
        if jurisdiction=='State Patrol' and matches.sum()==0:
            jurisdiction = source_name = 'State Police'
        matches = (df['Agency']==jurisdiction) & \
            (df['State']==state) & \
            (df['SourceName']==source_name)
        
        assert matches.sum()>0
        if jurisdiction == 'MULTI':
            assert df.loc[matches, 'AgencyFull'].isnull().all()
        else:
            assert (df.loc[matches, 'AgencyFull'] == df.loc[matches, 'AgencyFull'].iloc[0]).all()
        agency_full = df.loc[matches, 'AgencyFull'].iloc[0]
    elif matches.sum()!=1:
        if matches.sum()==0 and f"{state} {jurisdiction}" in new_cases.keys():
            agency_full = new_cases[f"{state} {jurisdiction}"]
            jurisdiction = source_name = "State Police" if source_name=="State Patrol" and "Police" in agency_full else source_name
        else:
            raise NotImplementedError()
    else:
        agency_full = df_stanford_old[matches].iloc[0]['AgencyFull']

    dict_append = {
        'State':state,
        'SourceName':source_name,
        'Agency':jurisdiction,
        'AgencyFull':agency_full,
        'TableType':table_type,
        'coverage_start': start_date.strftime(r"%m/%d/%Y"),
        'coverage_end': stop_date.strftime(r"%m/%d/%Y"),
        'last_coverage_check':datetime.now().strftime(r"%m/%d/%Y"),
        'Description': stanford_desc,
        'source_url': source_url,
        'readme': readme,
        'URL': csv_file,
        'Year': 'MULTI', 
        'DataType': 'CSV',
        'date_field': date_field,
        'agency_field': jurisdiction_field
    }

    if already_added:
        dict_append['min_version'] = 0.7

    assert all([x in df.columns for x in dict_append.keys()])

    dict_append = {k:[v] for k,v in dict_append.items()}
    df_append = pd.DataFrame(dict_append, columns=df.columns)

    matches = pd.Series(True, index=df_stanford_old.index)
    for c in df_stanford_old.columns:
        if c=='last_coverage_check':
            continue
        cur_val = df_append.loc[0, c]
        is_null_cur = pd.isnull(cur_val) or (isinstance(cur_val, str) and len(cur_val)==0)
        matches = matches & \
            (
                (df_stanford_old[c]==df_append.loc[0, c]) | \
                (df_stanford_old[c].isnull() & is_null_cur)
            )
        
    if not matches.any():
        df = pd.concat([df, df_append])
    else:
        # Re-insert original
        df = pd.concat([df, df_stanford_old[matches]])

    pd_loc = next_pd_loc
    pd_name = next_pd_name
    is_multi = next_is_multi

    if pd_loc > next_st_loc:
        st_loc = next_st_loc
        state = next_state
        next_st_loc, next_state = find_next_state(r, st_loc)

if plot_flag:
    import matplotlib.pyplot as plt
    import pandas as pd
    fig, ax = plt.subplots()
    s = pd.Series(end_dates)
    s.hist(ax=ax)
    plt.show()

# Reorder columns so columns most useful to user are up front
start_cols = ["State","SourceName","Agency","AgencyFull","TableType","coverage_start","coverage_end","last_coverage_check","Description","source_url","readme","URL"]
cols = start_cols.copy()
cols.extend([x for x in df.columns if x not in start_cols])

# TODO: Convert date columns to dates so that they sort properly
df = df.sort_values(by=cols)

df.to_csv(opd_csv,index=False)