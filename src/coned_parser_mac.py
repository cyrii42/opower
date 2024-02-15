from datetime import datetime
import pandas as pd

def convert_coned_csv_to_df(coned_file: str = None) -> pd.DataFrame:
    df = pd.read_csv(coned_file, header=4, parse_dates=[[1, 2], [1, 3]], keep_date_col=True, skip_blank_lines=True)
    df = df.convert_dtypes()
    df = df.drop(columns=["DATE_END TIME", "NOTES"])

    start_times_naive = pd.Index(df["DATE_START TIME"])
    start_times_aware = start_times_naive.tz_localize("America/New_York")
    start_times_aware_iso = start_times_aware.strftime("%Y-%m-%dT%H:%M:%S%z")  ## Doesn't work as of 4/9/23; UTC offset has no colon
    start_times_unix = (start_times_aware - pd.Timestamp("1970-01-01T00:00:00Z")) // pd.Timedelta("1ns")

    #df_influx = df.replace(start_times_naive, start_times_aware_iso) ## and change column names below to "time|dateTime:RFC3339"
    # OR
    df_influx = df.replace(start_times_naive, start_times_unix)  ## and change column names below to "time|dateTime:number"
    df_influx = df_influx.rename(columns={"UNITS": "kWh|measurement", "TYPE": "type|tag", "USAGE": "usage|double", "DATE_START TIME": "time|dateTime:number"}) 
    df_influx = df_influx.replace("Electric usage", "Electric")
    df_influx = df_influx[["kWh|measurement", "type|tag", "usage|double", "time|dateTime:number"]]

    filename_out_influx = str(datetime.now().strftime("%Y-%m-%d_%H-%M")) + "_coned_electric_influxdb.csv"
    df_influx.to_csv(filename_out_influx, index=False)
    
    return df_influx


def df_influx_to_csv(df_influx: pd.DataFrame) -> str:
    filename_out_influx = str(datetime.now().strftime("%Y-%m-%d_%H-%M")) + "_coned_electric_influxdb.csv"

    df_influx.to_csv(filename_out_influx, index=False)
    
    return filename_out_influx


def main():
    coned_filename = input("Please enter Con Ed CSV filename: ")

    df_influx = convert_coned_csv_to_df(coned_filename)
    filename_out_influx = df_influx_to_csv(df_influx)

    print("Use this Command: ")
    print("influx write dryrun --bucket conedison --file " + filename_out_influx)
    print("OR")
    print("influx write --bucket conedison --file " + filename_out_influx)


if __name__ == "__main__":
    main()






# # TRANSFORMATIONS FOR EXCEL
# dates_raw = pd.Index(df["DATE"])
# dates_parsed = pd.Index(df["DATE_START TIME"])
# dates_excel = dates_parsed.strftime("%m/%d/%y")
# df_excel = df.replace(dates_raw, dates_excel)
# df_excel = df.rename(columns={"UNITS": "Unit", "TYPE": "Type", "USAGE": "Usage", "DATE": "Date", "START TIME": "Start Time", "END TIME": "End Time"})
# df_excel = df_excel[['Type', 'Date', 'Start Time', 'End Time', 'Usage', 'Unit']]

# filename_out_excel = str(datetime.now().strftime("%Y-%m-%d_%H-%M")) + "_coned_electric_excel.csv"
# df_excel.to_csv(filename_out_excel, index=False)