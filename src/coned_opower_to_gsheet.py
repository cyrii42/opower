# import asyncio
# from datetime import date, datetime, timedelta

from gspread_pandas import Spread

from zmv_const import CONED_SPREADSHEET  # , EASTERN_TIME
from zmv_opower_gsheets import ElectricityUsage

# import pandas as pd


# def get_last_datetime_from_gsheet(spread: Spread) -> datetime:
#     spread.open_sheet('Electric usage')
#     df = spread.sheet_to_df(index=None)
#     df['dt_start_str'] = df['Date'] + ' ' + df['Start']
#     df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
#     return df.iloc[-1]['dt_start']


# def process_df_opower_for_gsheet(df_opower: pd.DataFrame) -> pd.DataFrame:
#     df_opower['Type'] = 'Electric usage'
#     df_opower['Date'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%m/%d/%Y') for x in df_opower['start_time']]
#     df_opower['Start'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%H:%M') for x in df_opower['start_time']]
#     df_opower['End'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%H:%M') for x in df_opower['end_time']]
#     df_opower['Unit'] = 'kWh'
#     df_opower['Month'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%b %Y') for x in df_opower['start_time']]

#     # Rename "consumption" column & change the column order
#     df_opower = df_opower.rename(columns={'consumption': 'Usage'})
#     df_opower = df_opower.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Unit', 'Month'])  # 'days', 

#     return df_opower


# def write_to_google_sheet(spread: Spread, df_opower: pd.DataFrame) -> None:
#     spread.open_sheet('Electric usage')

#     last_existing_row = spread.get_sheet_dims()[0]
#     starting_row = last_existing_row + 1

#     if df_opower.shape[0] > 0:
#         print(f"Writing {df_opower.shape[0]} rows of Con Edison data...")
#         df_opower = process_df_opower_for_gsheet(df_opower)
#         print(df_opower)
#         spread.df_to_sheet(df_opower, index=False, headers=False, start=(starting_row, 1))
#     else:
#         print("Google spreadsheet is all caught up!")


def main() -> None:
    spread = Spread(CONED_SPREADSHEET)
    usage = ElectricityUsage(spread)

    usage.pull_new_data_from_opower()
    
    # last_datetime_from_gsheet = get_last_datetime_from_gsheet(spread)
    # num_days = int((date.today() - datetime.date(last_datetime_from_gsheet)).total_seconds() // 86400)    

    # df_opower = asyncio.run(opower.get_opower_electric_data(num_days))

    # df_opower = df_opower.drop(df_opower.loc[df_opower['start_time'] <= last_datetime_from_gsheet].index)
    # write_to_google_sheet(spread, df_opower)
    

if __name__ == "__main__":
    main()



