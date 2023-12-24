import asyncio
from datetime import date, datetime, timedelta

from gspread_pandas import Spread
import pandas as pd

import zmv_const as const
import zmv_opower as opower

spread = Spread(const.CONED_SPREADSHEET)

def get_last_datetime_from_gsheet() -> pd.DataFrame:
    spread.open_sheet('Electric usage')
    df = spread.sheet_to_df(index=None)
    last_existing_row_start_date_split_str = (df.iloc[-1]['Date']).split(sep='/')
    start_date_str = (f"{last_existing_row_start_date_split_str[2].zfill(2)}-" + 
                f"{last_existing_row_start_date_split_str[0].zfill(2)}-" + 
                f"{last_existing_row_start_date_split_str[1].zfill(2)}")
    start_time_str = df.iloc[-1]['Start']
    # last_existing_row_date = date.fromisoformat(start_date_str)
    last_existing_row_datetime = datetime.fromisoformat(start_date_str + "T" + start_time_str).astimezone(const.EASTERN_TIME)
    return last_existing_row_datetime
    # days_to_pull = int((date.today() - last_existing_row_date).total_seconds() // 86400)    
    # return days_to_pull

    # df = spread.sheet_to_df(index=None)

    # last_existing_row_start_date_str = df.iloc[-1]['Date']
    # last_existing_row_start_date_split_str = last_existing_row_start_date_str.split(sep='/')
    # last_existing_row_start_time_str = df.iloc[-1]['Start']
    # last_existing_row_datetime_str = f"{last_existing_row_start_date_split_str[2].zfill(2)}-{last_existing_row_start_date_split_str[0].zfill(2)}-{last_existing_row_start_date_split_str[1].zfill(2)}T{last_existing_row_start_time_str}"
    # last_existing_row_start_time_dt = datetime.fromisoformat(last_existing_row_datetime_str).replace(tzinfo=const.EASTERN_TIME)

    # today = date.today()
    # today_at_midnight = datetime(today.year, today.month, today.day, hour=0, minute=0, second=0, tzinfo=const.EASTERN_TIME) 
    # days_to_pull = int(((datetime.now(tz=const.EASTERN_TIME) - last_existing_row_start_time_dt).total_seconds()) // 86400)
    
    # return days_to_pull


def process_df_opower_for_gsheet(df_opower: pd.DataFrame) -> pd.DataFrame:
    df_opower['Type'] = 'Electric usage'
    df_opower['Date'] = [x.tz_convert(tz=const.EASTERN_TIME).strftime('%m/%d/%Y') for x in df_opower['start_time']]
    df_opower['Start'] = [x.tz_convert(tz=const.EASTERN_TIME).strftime('%H:%M') for x in df_opower['start_time']]
    df_opower['End'] = [x.tz_convert(tz=const.EASTERN_TIME).strftime('%H:%M') for x in df_opower['end_time']]
    df_opower['Unit'] = 'kWh'
    df_opower['Month'] = [x.tz_convert(tz=const.EASTERN_TIME).strftime('%b %Y') for x in df_opower['start_time']]

    # Rename "consumption" column & change the column order
    df_opower = df_opower.rename(columns={'consumption': 'Usage'})
    df_opower = df_opower.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Unit', 'Month'])  # 'days', 

    return df_opower


def write_to_google_sheet(df_opower: pd.DataFrame) -> None:
    spread.open_sheet('Electric usage')

    last_existing_row = spread.get_sheet_dims()[0]
    starting_row = last_existing_row + 1

    if df_opower.shape[0] > 0:
        print(f"Writing {df_opower.shape[0]} rows of Con Edison data...")
        df_opower = process_df_opower_for_gsheet(df_opower)
        print(df_opower)
        spread.df_to_sheet(df_opower, index=False, headers=False, start=(starting_row, 1))
    else:
        print("Google spreadsheet is all caught up!")


def main() -> None:
    last_datetime_from_gsheet = get_last_datetime_from_gsheet()
    num_days = int((date.today() - datetime.date(last_datetime_from_gsheet)).total_seconds() // 86400)    

    df_opower = asyncio.run(opower.get_opower_electric_data(num_days))

    df_opower = df_opower.drop(df_opower.loc[df_opower['start_time'] <= last_datetime_from_gsheet].index)
    write_to_google_sheet(df_opower)
    

if __name__ == "__main__":
    main()



