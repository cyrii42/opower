import asyncio
from datetime import date, datetime, timedelta

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings
import pandas as pd

import zmv_const as const
import zmv_opower as opower


def get_range_start_str(days:int=7) -> str:
    today = date.today()
    today_at_midnight = datetime(today.year, today.month, today.day, hour=0, minute=0, second=0, tzinfo=const.EASTERN_TIME) 
    
    days_to_subtract = days
    range_start_time = today_at_midnight - timedelta(days=int(days_to_subtract))  # int() only really necessary if user provides str input
    range_start_time_str = range_start_time.isoformat(sep='T', timespec='seconds')
    
    return range_start_time_str


def influx_query_electric(range_start_time_str: str, range_end_time_str: str = None) -> pd.DataFrame:
    if range_end_time_str:
        range_start_time_str = f"{range_start_time_str}, stop: {range_end_time_str}"
    
    influx_query = (
        f"import \"timezone\"\n"
        f"option location = timezone.location(name: \"America/New_York\")\n"
        f"from(bucket: \"conedison\")\n"
        f"  |> range(start: {range_start_time_str})\n"
        f"  |> filter(fn: (r) => r[\"_measurement\"] == \"kWh\")\n"
        f"  |> filter(fn: (r) => exists r._value)\n"
        f"  |> aggregateWindow(every: 1h, timeSrc: \"_start\", fn: sum)\n"
        f"  |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
    )
    
    with InfluxDBClient(url=const.INFLUX_URL, token=const.INFLUX_TOKEN_CONED, org=const.INFLUX_ORG) as client:
        df_influx = client.query_api().query_data_frame(influx_query)
        
    return df_influx


def df_filter_electric(df_opower: pd.DataFrame, df_influx: pd.DataFrame) -> pd.DataFrame:
    # OPOWER columns:   'start_time', 'end_time', 'consumption'
    df_opower = df_opower.rename(columns={'start_time': 'time', 'consumption': 'kWh_opower'})
    # df_opower['time'] = [x.tz_localize(tz='UTC') for x in df_opower['time'].tolist()]  # opower dataframe already tz-aware
    
    # INFLUX columns: 'result', 'table', '_time', '_start', '_stop', '_measurement', 'type', 'usage'
    df_influx = df_influx.drop(columns=['result', 'table', '_start', '_stop', '_measurement', 'type'])
    df_influx = df_influx.rename(columns={'_time': 'time', 'usage': 'kWh_influx'})
    
    df_merged = df_opower.merge(df_influx)
    print(df_merged.to_string())
    df_filtered = df_merged[(df_merged['kWh_influx'].isnull())]
    df_output = df_filtered.drop(columns=['kWh_influx', 'type', 'end_time']).rename(columns={'time': '_time', 'kWh_opower': 'usage'}).set_index(['_time'])
    
    return df_output


def df_filter_test(df_opower: pd.DataFrame, df_influx: pd.DataFrame) -> pd.DataFrame:
    # OPOWER columns:   'start_time', 'end_time', 'consumption'
    df_opower = df_opower.rename(columns={'start_time': '_time', 'consumption': 'usage'})
    df_opower = df_opower.drop(columns=['end_time', 'type'])
    
    # INFLUX columns: 'result', 'table', '_time', '_start', '_stop', '_measurement', 'type', 'usage'
    df_influx = df_influx.drop(columns=['result', 'table', '_start', '_stop', '_measurement', 'type'])
    df_influx['_time'] = [x.tz_convert(tz='America/New_York') for x in df_influx['_time'].tolist()]

    df_merged = pd.merge(left=df_influx, right=df_opower, how='right', indicator=True)

    df_filtered = df_merged.where(df_merged['_merge'] == "right_only").dropna().drop(columns=['_merge'])
    return df_filtered



def influx_write_electric(df: pd.DataFrame) -> None:
    with InfluxDBClient(url=const.INFLUX_URL, token=const.INFLUX_TOKEN_CONED, org=const.INFLUX_ORG) as client:
        point_settings = PointSettings()
        point_settings.add_default_tag("type", "Electric")

        with client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings) as write_client:
            write_client.write(
                record=df, 
                bucket=f"conedison", 
                data_frame_measurement_name="kWh"
            )


def main() -> None:

    # ############# OPTION 1:  Raw number of days
    # # # Query OPower
    NUM_DAYS = 7    #### hourly data probably doesn't go back further than 30 days
    df_opower = asyncio.run(opower.get_opower_electric_data(NUM_DAYS))
    print(df_opower)
    df_opower.to_csv('asdf.csv')
    
    # # # # Query InfluxDB
    # range_start_time_str = get_range_start_str(NUM_DAYS)
    # df_influx = influx_query_electric(range_start_time_str)

    ############## OPTION 2: Custom datetimes
    # # Query OPower
    # start_date = datetime.fromisoformat("2022-09-20T00:00:00-00:00")
    # end_date = datetime.fromisoformat("2022-09-25T00:00:00-00:00")
    # df_opower = asyncio.run(opower.get_opower_electric_data_custom_dates(start_date, end_date))
    # print(df_opower.to_string())

    # # Query InfluxDB
    # range_start_time_str = start_date.isoformat()
    # range_end_time_str = end_date.isoformat()
    # df_influx = influx_query_electric(range_start_time_str, range_end_time_str)
    # print(df_influx)


    


    # Merge & filter DataFrames
    # df_filtered = df_filter_electric(df_opower, df_influx)
    # df_filtered = df_filter_test(df_opower, df_influx)

############## PROBLEM STILL APPEARS TO BE THAT THIS ISN'T OVERWRITNG EXISTING DATA IN INFLUXDB



    # # If there's new data, write it to InfluxDB
    # if df_filtered.shape[0] > 0:
    #     influx_write_electric(df_filtered)
    #     print(df_filtered.to_string())
    #     print(f"Added {df_filtered.shape[0]} new entries to InfluxDB.")
    # else:
    #     print("No new entries!")


    # # Prep OPower dataframe for export to InfluxDB & write to InfluxDB
    # NUM_DAYS = 3    #### hourly data probably doesn't go back further than 30 days
    # df_opower = asyncio.run(opower.get_opower_electric_data(NUM_DAYS))
    # df_opower.rename(columns={'start_time': '_time', 'consumption': 'usage'}).set_index('_time', inplace=True)
    # df_opower.index = pd.to_datetime(df_opower.index, unit='s')
    # # influx_write_electric(df_opower)
    # print(df_opower.to_string())
    # print(f"Added {df_opower.shape[0]} new entries to InfluxDB.")


if __name__ == "__main__":
    main()



