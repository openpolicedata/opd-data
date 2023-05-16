"""Adds datasets from Stanford Open Policing Project
The Stanford Open Policing Project contains datasets from many cities.
However, it appears to no longer be updated.
This methods adds all the datasets from Stanford that are not already added.
It is assumed that ones that already added are from other open data sets
and therefore, have more up-to-date data.
"""

import pandas as pd
import requests
from datetime import datetime

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

    close_loc = find_next(r, '</span>', open_loc)
    if close_loc >= end:
        raise ValueError("Unable to find time range")

    start_date_str = r.text[close_loc-10:close_loc]

    close_loc = find_next(r, '</span></td>', open_loc)
    if close_loc >= end:
        raise ValueError("Unable to find time range end")

    stop_date_str = r.text[close_loc-10:close_loc]
    return datetime.strptime(start_date_str, "%Y-%m-%d"), datetime.strptime(stop_date_str, "%Y-%m-%d")

def get_stanford():
    url = "https://openpolicing.stanford.edu/data/"

    r = requests.get(url)

    states = []
    sources = []
    agencies = []
    start_dates = []
    end_dates = []
    table_types = []

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

        if includes_pedestrian_stops(r, pd_loc, next_pd_loc):
            table_type = "STOPS"
        else:
            table_type = "TRAFFIC STOPS"

        s,e = find_time_range(r, pd_loc, next_pd_loc)
        start_dates.append(s)
        end_dates.append(e)
        table_types.append(table_type)
        
        if is_multi:
            source_name = state
            jurisdiction = "MULTI"
        else:
            source_name = pd_name
            jurisdiction = pd_name

        states.append(state)
        sources.append(source_name)
        agencies.append(jurisdiction)

        pd_loc = next_pd_loc
        pd_name = next_pd_name
        is_multi = next_is_multi

        if pd_loc > next_st_loc:
            st_loc = next_st_loc
            state = next_state
            next_st_loc, next_state = find_next_state(r, st_loc)

    return pd.DataFrame({"state":states, "source":sources, "agency":agencies, "start_date":start_dates, "end_date":end_dates})
