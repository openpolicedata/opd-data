try:
    import openpolicedata as opd
except:
    import sys
    sys.path.append('../openpolicedata')
    import openpolicedata as opd
import pandas as pd
from datetime import datetime
import urllib
from io import BytesIO
import re

def compare_tables():
    old_file = r"C:\Users\matth\repos\opd-data\opd_source_table.csv"
    new_file = r"C:\Users\matth\repos\opd-data\opd_source_table w source url.csv"

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

def add_dates():
    import stanford

    kstart = 0

    skip = []
    run = None

    src_file = r"C:\Users\matth\repos\opd-data\opd_source_table.csv"
    df = pd.read_csv(src_file)
    if src_file is not None:
        opd.datasets.datasets =opd. datasets._build(src_file)
    df_stanford = stanford.get_stanford()

    downloadable_file = "Downloadable File"
    data_type_to_access_type = {"Socrata":"API", "ArcGIS":"API","CSV":downloadable_file,"Excel":downloadable_file,"Carto":"API"}

    if "coverage_start" not in df:
        df["coverage_start"] = pd.NaT
        df["coverage_end"] = pd.NaT
        df["last_coverage_check"] = pd.NaT

    df["date_field"] = df["date_field"].apply(lambda x: x.strip() if isinstance(x,str) else x)
    df["SourceName"] = df["SourceName"].str.replace("Police Department", "").str.strip()

    # Reorder columns so columns most useful to user are up front
    start_cols = ["State","SourceName","Agency","AgencyFull","TableType","coverage_start","coverage_end","last_coverage_check","Description","source_url","readme","URL"]
    cols = start_cols.copy()
    cols.extend([x for x in df.columns if x not in start_cols])
    df = df[cols]

    df = df.sort_values(by=cols)

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
            match = (df_stanford["state"]==cur_row["State"]) & (df_stanford["source"]==cur_row["SourceName"]) & (df_stanford["agency"]==cur_row["Agency"])
            if match.sum()!=1:
                raise ValueError("Unable to find the correct # of Stanford matches")
            coverage_start = df_stanford[match]["start_date"].iloc[0].strftime('%m/%d/%Y')
            coverage_end = df_stanford[match]["end_date"].iloc[0].strftime('%m/%d/%Y')

        else:
            src = opd.Source(cur_row["SourceName"], cur_row["State"])

            if cur_row["Year"] == opd.defs.NA:
                coverage_start = "N/A"
                coverage_end = "N/A"
            elif cur_row["Year"] == "MULTI":
                # Manually get years since get years gets years for all datasets
                loader = src._Source__get_loader(opd.defs.DataType(cur_row["DataType"]), cur_row["URL"], dataset_id=cur_row["dataset_id"],
                                        date_field=cur_row["date_field"], agency_field=cur_row["agency_field"])
                years = loader.get_years(force=True)
                years.sort()
                years = [x for x in years if x >= min_year]

                nrows = 1 if data_type_to_access_type[cur_row["DataType"]]=="API" else None

                table = src.load_from_url(year=years[0], table_type=cur_row["TableType"], nrows=nrows)
                if len(table.table)==0:
                    raise ValueError("No records found in first year")
                
                if pd.notnull(cur_row["date_field"]):
                    min_val = table.table[cur_row["date_field"]].min()
                    if "year" in cur_row["date_field"].lower() and "month" not in cur_row["date_field"].lower():
                        coverage_start = "1/1/{}".format(min_val.year)
                    else:
                        if not isinstance(min_val, str):
                            min_val = min_val.strftime('%m/%d/%Y')
                        coverage_start = min_val

                    table = src.load_from_url(years[-1], table_type=cur_row["TableType"])
                    col = table.table[cur_row["date_field"]][table.table[cur_row["date_field"]].apply(lambda x: not isinstance(x,str))]
                    max_val = col.max()
                    if "year" in cur_row["date_field"].lower() and "month" not in cur_row["date_field"].lower():
                        coverage_end = "12/31/{}".format(max_val.year)
                    else:
                        if not isinstance(max_val, str):
                            max_val = max_val.strftime('%m/%d/%Y')
                        coverage_end = max_val
                else:
                    # Attempt to find date column
                    dt_col = [x for x in table.table.columns if "date" in x.lower()]
                    if len(dt_col)>1:
                        raise NotImplementedError()
                    elif len(dt_col)==0:
                        pass
                    else:
                        dt_col = dt_col[0]
                        if isinstance(table.table[dt_col],str):
                            raise NotImplementedError()
                        else:
                            min_val = table.table[dt_col].min()
                            if not isinstance(min_val, str):
                                min_val = min_val.strftime('%m/%d/%Y')
                            coverage_start = min_val

                            table = src.load_from_url(years[-1], table_type=cur_row["TableType"])
                            max_val = table.table[dt_col].max()
                            if not isinstance(max_val, str):
                                max_val = max_val.strftime('%m/%d/%Y')
                            coverage_end = max_val
            else:
                coverage_start = "1/1/{}".format(cur_row["Year"])
                coverage_end = "12/31/{}".format(cur_row["Year"])

        if pd.isnull(df.loc[k,"coverage_start"]) or \
            (df.loc[k,"coverage_start"][:-4]=="01/01/" and coverage_start[:-4]=='1/1/'):
            df.loc[k,"coverage_start"] = coverage_start
        elif df.loc[k,"coverage_start"] == coverage_start:
            pass
        else:
            throw_error = True
            if throw_error:
                raise ValueError("Start")

        if pd.isnull(df.loc[k,"coverage_end"]):
            df.loc[k,"coverage_end"] = coverage_end
        elif df.loc[k,"coverage_end"] == coverage_end:
            pass
        elif pd.to_datetime(df.loc[k,"coverage_end"]) < pd.to_datetime(coverage_end):
            df.loc[k,"coverage_end"] = coverage_end
        else:
            raise ValueError("Stop")

        df.loc[k, "last_coverage_check"] = datetime.now().strftime('%m/%d/%Y')

        df.to_csv(src_file, index=False)

agency_types = ['Police Department',"Sheriffs Department", 'State Prison for Women',"State Prison",
                "State Hospital", "Probation Department", "Department of Corrections",'Health Care Facility','Medical Facility',
                'Department of Public Safety','Community Correctional Facility', 'Prison', 
                'Department of Forestry & Fire Protection','Department of Parks & Recreation','Transit',
                'Unified School District Police Department','Sheriff','Forest Preserve Police Department',
                'Park District Police Department','University Police Department','Illinois University Police Department',
                'Transit Administration Police Department']
agency_types.sort(key=len, reverse=True)
agency_types = [x.title() for x in agency_types]
agency_types = [re.sub(r"s\s",' ',x) for x in agency_types]

p = re.compile(r"\s("+"|".join(agency_types)+r").*$", re.IGNORECASE)
def count_agencies():
    from rapidfuzz import fuzz
    ca_state_prison = "California State Prison"

    def clean(x):
        return p.sub("", x).strip()
    
    def add_agency(agency_name_orig, state, agencies):
        agency_name = agency_name_orig.strip().title()\
                                 .replace("Allegany","Alleghany")\
                                 .replace(" Co. ", " County ")\
                                 .replace(" So"," Sheriff's Department").title()
        agency_name = re.sub('(.+) \\1', '\\1', agency_name)
        agency_name = re.sub('Probation$','Probation Department',agency_name)
        agency_name = re.sub(r'\sP(d|olice)$',r' Police Department',agency_name)
        agency_name = re.sub(r"Sheriff'?[s|S]?(\sOffice?)?$",r"Sheriff's Department",agency_name)
        agency_name = re.sub(r"P(oli|rin)$",r"Police",agency_name)
        agency_name = re.sub('Csp Troop [A-Z]','Connecticut State Police',agency_name)
        agency_name = re.sub(r'\s+',' ',agency_name)
        agency_name = re.sub(r"('s|s|')\s",' ',agency_name.lower())
        agency_name = re.sub(r"(\w+)pd",'\\1 Police Department', agency_name.lower())
        agency_name = re.sub(r"\sco\.?(?=\s|$)",' county',agency_name)
        agency_name = re.sub(r"^st\.?\s", "saint ", agency_name)
        agency_name = re.sub(r"dept\.?$", "department", agency_name)
        agency_name = agency_name.title()
        agency = clean(agency_name)
        reduced_agencies = [x[0] for x in agencies if x[1]==state]
        cleaned_agencies = [clean(x) for x in reduced_agencies]
        cur_type = [x for x in agency_types if x in agency_name]
        cur_type = cur_type[0] if len(cur_type)>0 else None
        matches = [x.replace('-',' ')==agency.replace('-',' ') for x in cleaned_agencies]
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
            elif len(full_names) != 1:
                if agency_name.startswith(ca_state_prison) and \
                    all([x.startswith(ca_state_prison) and x.split(',')[1] != agency_name.split(',')[1] for x in full_names]):
                    agencies.append((agency_name,state))
                elif any(['Departmentuthern' in x for x in full_names]):
                    return
                else:
                    raise NotImplementedError()
            else:
                full_type = [x for x in agency_types if x in full_names[0]]
                if cur_type is not None and len(full_type)>=1 and cur_type!=full_type[0] and \
                    full_names[0].replace(full_type[0],cur_type) == agency_name:
                    agencies.append((agency_name,state))
                elif (full_names[0].startswith(ca_state_prison) and agency_name.startswith(ca_state_prison) and \
                    full_names[0].split(',')[1] != agency_name.split(',')[1]) or \
                        full_names[0].startswith(ca_state_prison) + agency_name.startswith(ca_state_prison)==1:
                    agencies.append((agency_name,state))
                elif agency_name.split('-')[0].strip() == full_names[0]:
                    pass
                elif (full_names[0].startswith(agency_name) and cur_type is None):
                    pass
                elif agency_name.startswith(full_names[0]) and len(full_type)==0:
                    k = [k for k,x in enumerate(agencies) if x==(full_names[0],state)][0]
                    agencies[k] = (agency_name,state)
                else:
                    raise NotImplementedError()
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
                        ((a:=re.match("Sant?a?\s([A-Z][a-z]+)", agency_name)) and (b:=re.match("Sant?a?\s([A-Z][a-z]+)", r[0])) and a.group(1)!=b.group(1)) or \
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
                    (a:=re.match("Lo\s([A-Z][a-z]+)", agency_name)) and all([(b:=re.match("Lo\s([A-Z][a-z]+)", d)) and a.group(1)!=b.group(1) for d in r]):
                    agencies.append((agency_name,state))
                elif agency_name=="Prince George Police Department" or \
                    " " not in agency_name and r[0].startswith(agency_name+" ") or \
                    r[0].lower().replace(" ", "").startswith(agency_name.lower()):
                    # Should be County PD
                    return
                else:
                    raise NotImplementedError()
                # elif all([("county" in agency_name.lower())+("county" in x.lower())==1 for x in high_scoring]):
                #     agencies.append((agency_name,state))
            else:
                agencies.append((agency_name,state))

    src_file = r"C:\Users\matth\repos\opd-data\opd_source_table.csv"
    if src_file is not None:
        opd.datasets.datasets =opd. datasets._build(src_file)
    datasets = opd.datasets.query()

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
        if datasets['Agency'][k] == opd.defs.MULTI:
            src = opd.Source(datasets['SourceName'][k], datasets['State'][k])
            if datasets['DataType'][k] in ["CSV"]:
                t = src.load_from_url(datasets['Year'][k], datasets['TableType'][k])
                new_agencies = t.table[datasets['agency_field'][k]].unique()
                if datasets['agency_field'][k] == "ORI":
                    if datasets['Year'][k]<=2020:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/URSUS_ORI-Agency_Names_20210902.xlsx","Agency","ORI_Number",pd.read_excel)
                    elif datasets['Year'][k]==2021:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2022-08/UseofForce_ORI-Agency_Names_2021.csv","AGENCY_NAME","ORI", pd.read_csv)
                    elif datasets['Year'][k]==2022:
                        data = (r"https://data-openjustice.doj.ca.gov/sites/default/files/dataset/2023-06/UseofForce_ORI-Agency_Names_2022f.csv","AGENCY_NAME","ORI", pd.read_csv)
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
                new_agencies = src.get_agencies(datasets['TableType'][k])
            
            for agency in new_agencies:
                if pd.isnull(agency) or len(agency)==1:
                    continue
                add_agency(agency, datasets['State'][k], agencies)
    print(f"OPD contains data for {len(agencies)} police agencies")

count_agencies()