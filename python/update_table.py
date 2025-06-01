try:
    import openpolicedata as opd
except:
    import sys
    sys.path.append('../openpolicedata')
    import openpolicedata as opd

import os
import pandas as pd
from datetime import datetime
import urllib
from io import BytesIO
import json
import re
import warnings
from zipfile import ZipFile

def compare_tables():
    old_file = r"opd_source_table.csv"
    new_file = r"opd_source_table w source url.csv"

    df1 = pd.read_csv(old_file)
    df2 = pd.read_csv(new_file)

    df1["which"] = 0
    df2["which"] = 1

    df = pd.concat([df2,df1])

    cols = []
    for c in df.columns:
        if c=='readme':
            break
        else:
            cols.append(c)

    df = df.sort_values(by=cols)
    df = df.drop_duplicates(subset=cols, keep=False, ignore_index=True)

def update_dates(kstart=0):
    import stanford

    skip = []
    run = None

    src_file = r"opd_source_table.csv"
    if src_file is not None:
        opd.datasets.reload(src_file)
    df = opd.datasets.query()
    df_stanford = stanford.get_stanford()

    downloadable_file = "Downloadable File"
    data_type_to_access_type = {"Socrata":"API", "ArcGIS":"API","CSV":downloadable_file,"Excel":downloadable_file,"Carto":"API",
                                'CKAN':'API','Opendatasoft':'API'}

    df["date_field"] = df["date_field"].apply(lambda x: x.strip() if isinstance(x,str) else x)

    # Reorder columns so columns most useful to user are up front
    start_cols = ["State","SourceName","Agency","AgencyFull","TableType","coverage_start","coverage_end",
                  "last_coverage_check",'Year','agency_originated','supplying_entity',"Description","source_url","readme","URL"]
    cols = start_cols.copy()
    cols.extend([x for x in df.columns if x not in start_cols])
    df = df[cols]

    df['coverage_start'] = pd.to_datetime(df['coverage_start'])
    df['coverage_end'] = pd.to_datetime(df['coverage_end'])

    sort_cols = cols.copy()
    sort_cols.remove('dataset_id')
    df = df.sort_values(by=sort_cols)

    df['coverage_start'] = df['coverage_start'].dt.strftime('%m/%d/%Y')
    df['coverage_end'] = df['coverage_end'].dt.strftime('%m/%d/%Y')

    min_year = 1990
    for k in df.index:
        if k<kstart:
            continue
        cur_row = df.loc[k]

        if (cur_row["SourceName"], cur_row["TableType"]) in skip:
            continue
        if run is not None and (cur_row["SourceName"], cur_row["TableType"]) not in run:
            continue

        print("{}: {} {} for year {}".format(k, cur_row["SourceName"], cur_row["TableType"], cur_row["Year"]))

        if "stanford.edu" in df.loc[k,"URL"]:
            match = (df_stanford["state"]==cur_row["State"]) & \
                (df_stanford["source"].isin([cur_row["SourceName"],cur_row["SourceName"].replace("Police",'Patrol')])) & \
                (df_stanford["agency"].isin([cur_row["Agency"], cur_row["Agency"].replace("Police",'Patrol')]))
            if match.sum()==0 and cur_row["SourceName"]=='Charlotte-Mecklenburg':
                match = df_stanford["source"] == 'Charlotte'
            if match.sum()!=1:
                raise ValueError("Unable to find the correct # of Stanford matches")
            coverage_start = df_stanford[match]["start_date"].iloc[0].strftime('%m/%d/%Y')
            coverage_end = df_stanford[match]["end_date"].iloc[0].strftime('%m/%d/%Y')

        else:
            src = opd.Source(cur_row["SourceName"], cur_row["State"], agency=cur_row["Agency"])

            if cur_row["Year"] == opd.defs.NA:
                continue
            elif cur_row["Year"] == opd.defs.MULTI:
                # Manually get years since get years gets years for all datasets
                try:
                    loader = src._Source__get_loader(opd.defs.DataType(cur_row["DataType"]), cur_row["URL"], cur_row['query'], 
                                            dataset=cur_row["dataset_id"],
                                            date_field=cur_row["date_field"], agency_field=cur_row["agency_field"])
                except opd.exceptions.OPD_DataUnavailableError:
                    continue
                
                if cur_row['DataType'] in ["Excel",'CSV']:
                    years = [opd.defs.MULTI]
                else:
                    try:
                        years = loader.get_years()
                    except (opd.exceptions.OPD_SocrataHTTPError, opd.exceptions.OPD_DataUnavailableError):
                        continue
                    years.sort()
                    years = [x for x in years if x >= min_year]

                if len(years)==0:
                    warnings.warn(f'No years found for {cur_row["SourceName"]}, {cur_row["State"]} {cur_row["TableType"]}')
                    continue

                nrows = 1 if data_type_to_access_type[cur_row["DataType"]]=="API" else None

                # For if case, assuming 1st year might be a mistake
                years_req = years[:2] if len(years)>1 and years[1]-years[0]>5 else years[0]

                if nrows==1:
                    assert pd.notnull(cur_row["date_field"])

                table = src.load(year=years_req, table_type=cur_row["TableType"], nrows=nrows, url=cur_row['URL'], id=cur_row['dataset_id'],
                                 sortby='date')
                if len(table.table)==0:
                    raise ValueError("No records found in first year")
                
                if pd.notnull(cur_row["date_field"]):
                    date_field = cur_row["date_field"]
                    min_val = table.table[date_field].min()
                    if isinstance(min_val, pd.Timestamp) and min_val.year<2000 and len(table.table)>1 and \
                        table.table[date_field].nsmallest(2).iloc[1].year - min_val.year > 5:
                        min_val = table.table[date_field].nsmallest(2).iloc[1]
                    if not isinstance(min_val, str):
                        min_val = min_val.strftime('%m/%d/%Y')
                    coverage_start = min_val

                    if years!=[opd.defs.MULTI]:
                        table = src.load(year=years[-1], table_type=cur_row["TableType"], url=cur_row['URL'], id=cur_row['dataset_id'])

                    col = table.table[date_field][table.table[date_field].apply(lambda x: not isinstance(x,str))]
                    if len(col)==0:
                        col = pd.to_datetime(table.table[date_field])
                    try:
                        max_val = col.max()
                    except TypeError:
                        col = col[col.apply(lambda x: isinstance(x, pd.Timestamp))]
                        max_val = col.max()

                    if not isinstance(max_val, str):
                        max_val = max_val.strftime('%m/%d/%Y')
                    coverage_end = max_val
                else:
                    # Attempt to find date column
                    dt_col = [x for x in table.table.columns if "date" in x.lower()]
                    if len(dt_col)>1:
                        raise NotImplementedError()
                    elif len(dt_col)==0:
                        dt_col = [x for x in table.table.columns if "year" in x.lower()]
                        if len(dt_col)>1:
                            raise NotImplementedError()
                        elif len(dt_col)==0:
                            continue
                        else:
                            dt_col = dt_col[0]
                            min_year = table.table[dt_col].min()
                            max_year = table.table[dt_col].max()
                            coverage_start = "01/01/{}".format(min_year)
                            coverage_end = "12/31/{}".format(max_year)
                    else:
                        dt_col = dt_col[0]
                        if isinstance(table.table[dt_col],str):
                            raise NotImplementedError()
                        else:
                            table.table[dt_col] = pd.to_datetime(table.table[dt_col], format='mixed')
                            min_val = table.table[dt_col].min()
                            if not isinstance(min_val, str):
                                min_val = min_val.strftime('%m/%d/%Y')
                            coverage_start = min_val

                            if years[-1]!=years_req:
                                table = src.load(year=years[-1], table_type=cur_row["TableType"], url=cur_row['URL'], id=cur_row['dataset_id'])
                            max_val = table.table[dt_col].max()
                            if not isinstance(max_val, str):
                                max_val = max_val.strftime('%m/%d/%Y')
                            coverage_end = max_val
            else:
                coverage_start = "01/01/{}".format(cur_row["Year"])
                coverage_end = "12/31/{}".format(cur_row["Year"])

        start_changed = False
        if pd.to_datetime(coverage_start) < pd.to_datetime(df.loc[k,"coverage_start"]):
            start_changed = True
            df.loc[k,"coverage_start"] = coverage_start
        elif pd.to_datetime(df.loc[k,"coverage_start"]) == pd.to_datetime(coverage_start):
            pass
        elif pd.to_datetime(coverage_start) > pd.to_datetime(df.loc[k,"coverage_start"]):
            start_changed = True
            warnings.warn(f'Coverage start increased from {df.loc[k,"coverage_start"]} to {coverage_start}')
            df.loc[k,"coverage_start"] = coverage_start
        else:
            throw_error = True
            if throw_error:
                raise ValueError("Start")

        end_changed = False
        if pd.to_datetime(coverage_end) > pd.to_datetime(df.loc[k,"coverage_end"]) or \
            (pd.isnull(pd.to_datetime(df.loc[k,"coverage_end"])) and not pd.isnull(pd.to_datetime(coverage_end))):
            end_changed = True
            df.loc[k,"coverage_end"] = coverage_end
        elif  pd.to_datetime(df.loc[k,"coverage_end"]) == pd.to_datetime(coverage_end):
            pass
        elif pd.to_datetime(coverage_end) < pd.to_datetime(df.loc[k,"coverage_end"]):
            end_changed = True
            warnings.warn(f'Coverage end decreased from {df.loc[k,"coverage_end"]} to {coverage_end}')
            df.loc[k,"coverage_end"] = coverage_end
        else:
            raise ValueError("Stop")

        if start_changed or end_changed:
            df.loc[k, "last_coverage_check"] = datetime.now().strftime('%m/%d/%Y')

            assert df['coverage_start'].apply(lambda x: pd.isnull(x) or isinstance(x,str)).all()
            assert df['coverage_end'].apply(lambda x: pd.isnull(x) or isinstance(x,str)).all()

            # df['coverage_start'] = df['coverage_start'].dt.strftime('%m/%d/%Y')
            # df['coverage_end'] = df['coverage_end'].dt.strftime('%m/%d/%Y')

            df_save = df.copy()
            df_save['dataset_id'] = df_save['dataset_id'].apply(lambda x: json.dumps(x) if type(x) in [list, dict] else x)
            df_save.to_csv(src_file, index=False)

agency_types = ['Police',"Sheriffs", 'St Prison for Women',"St Prison",
                "St Hospital", "Probation", "Department of Corrections",'Health Care Facility','Medical Facility',
                'Department of Public Safety','Community Correctional Facility', 'Prison', 
                'Department of Forestry & Fire Protection','Department of Parks & Recreation','Transit',
                'Unified School District Police','Sheriff','Forest Preserve Police',
                'Park District Police','University Police','Illinois University Police',
                'Transit Administration Police', 'District Attorney']
agency_types.sort(key=len, reverse=True)
agency_types = [x.title() for x in agency_types]
agency_types = [re.sub(r"s\s",' ',x) for x in agency_types]

p = re.compile(r"\s("+"|".join(agency_types)+r").*$", re.IGNORECASE)
def count_agencies():
    from rapidfuzz import fuzz
    ca_state_prison = "California St Prison"
    st_univ_police = 'St University Police'

    def clean(x):
        return p.sub("", x).strip()
    
    def add_agency(agency_name_orig, state, agencies):
        agency_name = agency_name_orig.strip().title()\
                                 .replace("Allegany","Alleghany")\
                                 .replace(" Co. ", " County ")\
                                 .replace(" So"," Sheriff").title()
        agency_name = re.sub('(.+) \\1', '\\1', agency_name)

        agency_name = re.sub(r"['’]?s?\s*(Department|Office|dept\.?)", "", agency_name, flags=re.IGNORECASE)
        agency_name = re.sub(r'\sPD\b',r' Police',agency_name, flags=re.IGNORECASE)
        agency_name = re.sub(r"(\w+)pd",'\\1 Police', agency_name.lower(), flags=re.IGNORECASE)
        agency_name = re.sub(r"\sSd$",r" Sheriff",agency_name, flags=re.IGNORECASE)
        agency_name = re.sub(r"\sDa$",r" District Attorney",agency_name, flags=re.IGNORECASE)
        agency_name = re.sub(r"P(oli|rin)$",r"Police",agency_name)
        agency_name = re.sub('Csp Troop [A-Z]','Connecticut St Police',agency_name)
        agency_name = re.sub(r'\s+',' ',agency_name)
        agency_name = re.sub(r'\buniv\.?\b','university',agency_name, flags=re.IGNORECASE)
        # agency_name = re.sub(r"('s|s|')\s",' ',agency_name.lower())
        agency_name = re.sub(r"\sco\.?(?=\s|$)",' county',agency_name)
        # Both saint and state can be abbreviated st so just convert to abbreviation
        agency_name = re.sub(r"\b(state|saint)\b", "st", agency_name,  flags=re.IGNORECASE)
        agency_name = re.sub(r"\s*\#?\s*\d+$", "", agency_name)  # Remove any numbers at the end that may indicate parts of a larger org
        agency_name = agency_name.replace("-",' ').replace(',','')
        agency_name = agency_name.title()
        agency = clean(agency_name)
        reduced_agencies = [x[0] for x in agencies if x[1]==state]
        cleaned_agencies = [clean(x) for x in reduced_agencies]
        cur_type = [x for x in agency_types if x in agency_name]
        cur_type = cur_type[0] if len(cur_type)>0 else None
        matches = [x.replace('-',' ')==agency.replace('-',' ') for x in cleaned_agencies]
        if len([m.start() for m in re.finditer('department', agency_name, re.IGNORECASE)])>1:
            # Word department is repeated. String likely contains multiple departments or same one repeated
            return
        if any(matches):
            full_names = [x for x,y in zip(reduced_agencies, matches) if y]
            match_types = []
            for y in full_names:
                full_type = [x for x in agency_types if x in y]
                match_types.append(full_type[0] if len(full_type)>0 else None)
            if any([x.replace('-',' ') == agency_name.replace('-',' ') for x in full_names]) or \
                full_names[0].lower().replace(" ", "").startswith(agency_name.replace(" ", "").lower()):
                return
            elif cur_type is not None and all([x is not None and (w.startswith(ca_state_prison) or x!=cur_type) for w,x in zip(full_names,match_types)]):
                agencies.append((agency_name,state))
            elif agency_name.startswith(st_univ_police) and all([x.startswith(st_univ_police) and \
                    x.replace(st_univ_police,'').strip() != agency_name.replace(st_univ_police,'').strip() for x in full_names]):
                agencies.append((agency_name,state))
            elif len(full_names) != 1:
                if agency_name.startswith(ca_state_prison) and \
                    all([x.startswith(ca_state_prison) and x.split(',')[1] != agency_name.split(',')[1] for x in full_names]):
                    agencies.append((agency_name,state))
                # elif any(['Departmentuthern' in x for x in full_names]):
                #     return
                else:
                    return
            else:
                full_type = [x for x in agency_types if x in full_names[0]]
                if cur_type is not None and len(full_type)>=1 and cur_type!=full_type[0] and \
                    full_names[0].replace(full_type[0],cur_type) == agency_name:
                    agencies.append((agency_name,state))
                elif (full_names[0].startswith(ca_state_prison) and agency_name.startswith(ca_state_prison) and \
                    full_names[0].split(',')[1] != agency_name.split(',')[1]) or \
                        full_names[0].startswith(ca_state_prison) + agency_name.startswith(ca_state_prison)==1:
                    agencies.append((agency_name,state))
                elif full_names[0].startswith(st_univ_police) and agency_name.startswith(st_univ_police) and \
                    full_names[0].replace(st_univ_police,'').strip() != agency_name.replace(st_univ_police,'').strip():
                    agencies.append((agency_name,state))
                elif agency_name.split('-')[0].strip() == full_names[0]:
                    pass
                elif (full_names[0].startswith(agency_name) and cur_type is None):
                    pass
                elif agency_name.startswith(full_names[0]) and len(full_type)==0:
                    k = [k for k,x in enumerate(agencies) if x==(full_names[0],state)][0]
                    agencies[k] = (agency_name,state)
                elif agency_name.startswith(full_names[0]) and '-' in agency_name_orig:
                    # This was found when 2 departments were concatenated with a -
                    pass
                else:
                    return
        else:
            def fuzzclean(x):
                return x.replace("County","").replace("University Of","")
            ratios = [fuzz.ratio(fuzzclean(agency), fuzzclean(x)) for x in cleaned_agencies]
            if len(ratios)> 0 and max(ratios)>86:
                high_scoring = [x for x,y in zip(cleaned_agencies, ratios) if y==max(ratios)]
                if high_scoring[0]=="Chico" and agency=="Chino":
                    return
                r = [x for x,y in zip(reduced_agencies,cleaned_agencies) if y in high_scoring]
                match_types = []
                for y in r:
                    full_type = [x for x in agency_types if x in y]
                    match_types.append(full_type[0] if len(full_type)>0 else None)
                keep = [cur_type is None or x is None or x==cur_type for x in match_types]
                match_types = [x for x,y in zip(match_types, keep) if y]
                r = [x for x,y in zip(r, keep) if y]
                if len(r)==0:
                    agencies.append((agency_name,state))
                elif len(r)>0 and cur_type is not None and \
                    any([y is not None and y==cur_type and fuzz.ratio(agency_name, x)>98 for x,y in zip(r,match_types)]):
                    return
                elif len(r)==1:
                    if agency.replace("Women","Men")==r[0] or\
                        agency_name.replace("Park","Beach")==r[0] or\
                        agency.replace("Center","Institution")==r[0] or \
                        agency_name.replace("County ","")==r[0] or \
                        ((a:=re.match(r"Sant?a?\s([A-Z][a-z]+)", agency_name)) and (b:=re.match(r"Sant?a?\s([A-Z][a-z]+)", r[0])) and a.group(1)!=b.group(1)) or \
                        (len(agency_name) > len(r[0]) and r[0]==agency_name[-len(r[0]):]):
                        agencies.append((agency_name,state))
                    elif (agency_name.startswith('Willisville') and len(r)==1 and r[0].startswith('Williamsville')):
                        agencies.append((agency_name,state))
                    elif (agency_name.startswith(r[0]) and len(r[0])>=35) or \
                        agency_name in ["Towsonu",] or \
                        " " not in agency_name and r[0].startswith(agency_name+" ") or \
                        r[0].lower().replace(" ", "").startswith(agency_name.lower()):
                        return
                    else:
                        words1 = agency_name.split()
                        words2 = r[0].split()
                        k1 = k2 = 0
                        while k1<len(words1) and k2<len(words2):
                            if words1[k1]==words2[k2]:
                                words1.pop(k1)
                                words2.pop(k2)
                            else:
                                break
                        k1 = len(words1)-1
                        k2 = len(words2)-1
                        while k1>=0 and k2>=0:
                            if words1[k1]==words2[k2]:
                                words1.pop(k1)
                                words2.pop(k2)
                                k1-=1
                                k2-=1
                            else:
                                break
                        w1 = " ".join(words1)
                        w2 = " ".join(words2)
                        score = fuzz.ratio(w1, w2)
                        if score==0:
                            if agency_name.split()==1:
                                raise NotImplementedError()
                            else:
                                agencies.append((agency_name,state))
                        elif score<90:
                            agencies.append((agency_name,state))
                        else:
                            return
                elif (cur_type is not None and all([x is not None and (w.startswith(ca_state_prison) or x!=cur_type) for w,x in zip(r,match_types)])) or \
                    (a:=re.match(r"Lo\s([A-Z][a-z]+)", agency_name)) and all([(b:=re.match(r"Lo\s([A-Z][a-z]+)", d)) and a.group(1)!=b.group(1) for d in r]):
                    agencies.append((agency_name,state))
                elif agency_name=="Prince George Police" or \
                    " " not in agency_name and r[0].startswith(agency_name+" ") or \
                    r[0].lower().replace(" ", "").startswith(agency_name.lower()):
                    # Should be County PD
                    return
                elif 'forest ranger' in agency_name.lower():
                    return
                else:
                    print(f"{agency_name_orig} is unknown")
                    return
                # elif all([("county" in agency_name.lower())+("county" in x.lower())==1 for x in high_scoring]):
                #     agencies.append((agency_name,state))
            else:
                agencies.append((agency_name,state))

    src_file = r"opd_source_table.csv"
    if src_file is not None:
        opd.datasets.datasets =opd. datasets._build(src_file)
    datasets = opd.datasets.query()

    output_dir = os.path.join('.','data')

    agencies = []
    for k in range(len(datasets)):
        if datasets['Agency'][k] not in [opd.defs.MULTI, opd.defs.NA]:
            if (datasets['AgencyFull'][k],datasets['State'][k]) not in agencies:
                add_agency(datasets['AgencyFull'][k], datasets['State'][k], agencies)
                # def clean(x):
                #     return p.sub("", x)
                
                # agency = clean(datasets['AgencyFull'][k])
                # cleaned_agencies = [clean(x[0]) for x in agencies if x[1]==datasets['State'][k]]
                # matches = [x==agency for x in cleaned_agencies]
                # if any(matches):
                #     raise NotImplementedError()
                # else:
                #     ratios = [fuzz.ratio(agency, x) for x in cleaned_agencies]
                #     if len(ratios)> 0 and max(ratios)>77:
                #         high_scoring = [x for x,y in zip(cleaned_agencies, ratios) if y==max(ratios)]
                #         raise NotImplementedError()
                #     else:
                #         agencies.append((datasets['AgencyFull'][k],datasets['State'][k]))

    for k in range(len(datasets)):
        if datasets['Agency'][k] == opd.defs.MULTI and datasets['State'][k] != opd.defs.MULTI:
            now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
            print(f"{now} Testing {k} of {len(datasets)-1}: {datasets.iloc[k]['SourceName']} {datasets.iloc[k]['TableType']} table")

            src = opd.Source(datasets['SourceName'][k], datasets['State'][k], agency=datasets["Agency"][k])
            csv_filename = src.get_csv_filename(datasets['Year'][k], output_dir, datasets.iloc[k]["TableType"], 
                     url=datasets.iloc[k]['URL'], id=datasets.iloc[k]['dataset_id'])
            output_file = csv_filename.replace('.csv','.txt')
            if datasets['Year'][k]!=opd.defs.MULTI and datasets['Year'][k]!=opd.defs.NA and datasets['Year'][k] < datetime.now().year and \
                os.path.exists(output_file):
                with open(output_file) as f:
                    new_agencies = [x.strip() for x in f.readline().split(',')]

            elif datasets['DataType'][k] in ["CSV"]:
                t = src.load(datasets['TableType'][k], datasets['Year'][k], url=datasets['URL'][k], id=datasets['dataset_id'][k])
                new_agencies = t.table[datasets['agency_field'][k]].unique()
                if datasets['agency_field'][k] == "ORI":
                    if datasets['Year'][k]<=2020:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/URSUS_ORI-Agency_Names_20210902.xlsx","Agency","ORI_Number",pd.read_excel)
                    elif datasets['Year'][k]==2021:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/UseofForce_ORI-Agency_Names_2021.csv","AGENCY_NAME","ORI", pd.read_csv)
                    elif datasets['Year'][k]==2022:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-06/UseofForce_ORI-Agency_Names_2022f.csv","AGENCY_NAME","ORI", pd.read_csv)
                    elif datasets['Year'][k]==2023:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-07/UseofForce_ORI-Agency_Names_2023.csv","AGENCY_NAME","ORI", pd.read_csv)
                    else:
                        raise ValueError("Unknown dataset")
                    try:
                        ori_df = data[3](data[0])
                    except urllib.error.URLError as e:
                        with opd.data_loaders.get_legacy_session() as session:
                            r = session.get(data[0])
                            
                        r.raise_for_status()
                        file_like = BytesIO(r.content)
                        ori_df = data[3](file_like)
                    except:
                        raise
                    for j in range(len(new_agencies)):
                        match = ori_df[data[1]][ori_df[data[2]] == new_agencies[j]]
                        if len(match)!=1:
                            raise NotImplementedError()
                        else:
                            new_agencies[j] = match.iloc[0]
            else:
                # ds_filter, _ = src._Source__filter_for_source(datasets['TableType'][k], datasets.iloc[k]["Year"], None, None, errors=False)
                # url_contains = datasets.iloc[k]['URL'] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None
                # id_contains = datasets.iloc[k]['dataset_id'] if isinstance(ds_filter,pd.DataFrame) and len(ds_filter)>1 else None
                new_agencies = src.get_agencies(datasets['TableType'][k], year=datasets.iloc[k]["Year"])

            with open(output_file, "w") as f:
                f.write(','.join(new_agencies))
            
            for agency in new_agencies:
                if pd.isnull(agency) or len(agency)==1:
                    continue
                add_agency(agency, datasets['State'][k], agencies)
    print(f"OPD contains data for {len(agencies)} police agencies")

def update_ripa(url, dict_url, year):
    src_file = r"opd_source_table.csv"
    if src_file is not None:
        opd.datasets.reload(src_file)
    df = opd.datasets.query()

    df['dataset_id'] = df['dataset_id'].apply(lambda x: json.dumps(x) if type(x) in [list, dict] else x)

    df['coverage_start'] = pd.to_datetime(df['coverage_start'], errors='ignore')
    df['coverage_end'] = pd.to_datetime(df['coverage_end'], errors='ignore')
    df['last_coverage_check'] = pd.to_datetime(df['last_coverage_check'])

    match = (df['SourceName']=='Alameda County') & (df['TableType']=='STOPS') & (df['Year']==year-1)
    assert match.sum()==1

    base = df[match].iloc[0]
    base['coverage_start'] = f'01/01/{year}'
    base['coverage_end'] = f'12/31/{year}'
    base['last_coverage_check'] = datetime.now().strftime('%m/%d/%Y')
    base['Year'] = year
    base['readme'] = dict_url
    base['URL'] = url

    q1_data = []

    with opd.httpio.open(url, block_size=2**20) as fp:
        with ZipFile(fp, 'r') as z:
            for name in z.namelist():
                new_entry = base.copy()

                m = re.search(rf'Data\s\_\s(?P<loc>[\w\s]+)\s{year}\sQ?(?P<Q>\d)?', name)
                assert m

                if m.group('Q')!=None:
                    if m.group('Q')=='1':
                        assert m.group('loc') not in q1_data
                        q1_data.append(m.group('loc'))

                        new_entry['dataset_id'] = json.dumps({'files': [name.replace(' Q1 ', f' Q{x} ') for x in range(1,5)]})
                    else:
                        assert m.group('loc') in q1_data
                        continue
                else:
                    new_entry['dataset_id'] = name

                match = (df['URL']==url) & (df['dataset_id']==new_entry['dataset_id'])
                assert match.sum()<2
                if match.sum()>0:
                    continue

                if m.group('loc')=='CHP':
                    new_entry['SourceName'] = 'State Patrol'
                    new_entry['Agency'] = 'State Patrol'
                    new_entry['AgencyFull'] = 'California Highway Patrol'
                else:
                    data = pd.read_excel(BytesIO(z.read(name)))

                    if data[base['agency_field']].nunique()>1:
                        county = m.group('loc') + " County"

                        assert ((df['SourceName']==county) & (df['Agency']=="MULTIPLE")).any()

                        new_entry['SourceName'] = county
                        new_entry['Agency'] = opd.defs.MULTI
                        new_entry['AgencyFull'] = pd.NA
                    else:
                        agency = data[base['agency_field']].iloc[0]

                        agency = re.sub(r' SO$', " SHERIFF'S OFFICE", agency).replace(' CO ',' COUNTY ')
                        agency = re.sub(r' SHERIFF$', " SHERIFF'S OFFICE", agency)

                        agency_types = ["SHERIFF'S OFFICE", 'POLICE DEPARTMENT']
                        assert any(x in agency for x in agency_types)
                        city = agency
                        for x in agency_types:
                            city = city.replace(x, '').strip()

                        city = city.title()
                        agency = agency.title().replace("'S","'s")

                        assert ((df['SourceName']==city) & (df['Agency']==city) & (df['AgencyFull'].isin([agency]))).any()

                        new_entry['SourceName'] = city
                        new_entry['Agency'] = city
                        new_entry['AgencyFull'] = agency

                print(name)
                
                df = pd.concat([df,new_entry.to_frame().T], ignore_index=True)
                sort_cols = df.columns.to_list().copy()
                sort_cols.remove('dataset_id')
                df = df.sort_values(by=sort_cols)

                df_save = df.copy()
                df_save['dataset_id'] = df_save['dataset_id'].apply(lambda x: json.dumps(x) if type(x) in [list, dict] else x)

                df_save['coverage_start'] = pd.to_datetime(df_save['coverage_start']).dt.strftime('%m/%d/%Y')
                df_save['coverage_end'] = pd.to_datetime(df_save['coverage_end']).dt.strftime('%m/%d/%Y')
                df_save['last_coverage_check'] = pd.to_datetime(df_save['last_coverage_check']).dt.strftime('%m/%d/%Y')
                df_save.to_csv(src_file, index=False)


        
update_dates(kstart=941)
# count_agencies()

# update_ripa('https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-12/RIPA-Stop-Data-2023.zip',
#             'https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2024-12/RIPA%20Dataset%20Read%20Me%202023%20Final.pdf',
#             2023)