import openpolicedata as opd
import pandas as pd
from datetime import datetime

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

def count_agencies():
    from rapidfuzz import fuzz
    import re

    def clean(x):
        return p.sub("", x)
    
    def add_agency(agency_name, state, agencies):
        agency = clean(agency_name)
        reduced_agencies = [x[0] for x in agencies if x[1]==state]
        cleaned_agencies = [clean(x[0]) for x in reduced_agencies]
        matches = [x==agency for x in cleaned_agencies]
        if any(matches):
            full_names = [x for x,y in zip(reduced_agencies, matches) if y]
            if len(full_names) != 1:
                raise NotImplementedError()
            else:
                if full_names[0] == agency_name:
                    pass
                else:
                    raise NotImplementedError()
        else:
            ratios = [fuzz.ratio(agency, x) for x in cleaned_agencies]
            if len(ratios)> 0 and max(ratios)>77:
                high_scoring = [x for x,y in zip(cleaned_agencies, ratios) if y==max(ratios)]
                raise NotImplementedError()
            else:
                agencies.append((agency_name,state))

    src_file = r"C:\Users\matth\repos\opd-data\opd_source_table.csv"
    if src_file is not None:
        opd.datasets.datasets =opd. datasets._build(src_file)
    datasets = opd.datasets.query()

    agencies = []
    p = re.compile(r"\s(Police|Sheriff).+$", re.IGNORECASE)
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
                    ori_df = data[3](data[0])
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