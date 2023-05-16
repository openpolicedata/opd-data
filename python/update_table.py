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

    skip = [("Seattle", "CALLS FOR SERVICE"),("Seattle", "COMPLAINTS")]

    src_file = r"C:\Users\matth\repos\opd-data\opd_source_table.csv"
    df = pd.read_csv(src_file)
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
    run_all = False
    for k in df.index:
        cur_row = df.loc[k]
        if not run_all and pd.notnull(cur_row["coverage_start"]):
            continue
        if (cur_row["SourceName"], cur_row["TableType"]) in skip:
            continue
        print("{}: {} {} for year {}".format(k, cur_row["SourceName"], cur_row["TableType"], cur_row["Year"]))

        if "stanford.edu" in df.loc[k,"URL"]:
            match = (df_stanford["state"]==cur_row["State"]) & (df_stanford["source"]==cur_row["SourceName"]) & (df_stanford["agency"]==cur_row["Agency"])
            if match.sum()!=1:
                raise ValueError("Unable to find the correct # of Stanford matches")
            df.loc[k, "coverage_start"] = df_stanford[match]["start_date"].iloc[0].strftime('%m/%d/%Y')
            df.loc[k, "coverage_end"] = df_stanford[match]["end_date"].iloc[0].strftime('%m/%d/%Y')

        else:
            src = opd.Source(cur_row["SourceName"], cur_row["State"])

            if cur_row["Year"] == opd.defs.NA:
                df.loc[k,"coverage_start"] = "N/A"
                df.loc[k,"coverage_end"] = "N/A"
            elif cur_row["Year"] == opd.defs.MULTI:
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
                    if not isinstance(min_val, str):
                        min_val = min_val.strftime('%m/%d/%Y')
                    df.loc[k,"coverage_start"] = min_val

                    table = src.load_from_url(years[-1], table_type=cur_row["TableType"])
                    max_val = table.table[cur_row["date_field"]].max()
                    if not isinstance(max_val, str):
                        max_val = max_val.strftime('%m/%d/%Y')
                    df.loc[k,"coverage_end"] = max_val
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
                            df.loc[k,"coverage_start"] = min_val

                            table = src.load_from_url(years[-1], table_type=cur_row["TableType"])
                            max_val = table.table[dt_col].max()
                            if not isinstance(max_val, str):
                                max_val = max_val.strftime('%m/%d/%Y')
                            df.loc[k,"coverage_end"] = max_val
            else:
                df.loc[k,"coverage_start"] = "1/1/{}".format(cur_row["Year"])
                df.loc[k,"coverage_end"] = "12/31/{}".format(cur_row["Year"])

        df.loc[k, "last_coverage_check"] = datetime.now().strftime('%m/%d/%Y')

        df.to_csv(src_file, index=False)
        
add_dates()