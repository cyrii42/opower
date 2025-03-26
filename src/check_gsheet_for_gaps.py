# import asyncio
# from datetime import datetime, timedelta
# import time
# from pprint import pprint

from gspread_pandas import Spread
import pandas as pd

# from zmv_const import CONED_SPREADSHEET
# import zmv_opower as opower
from zmv_opower_gsheets import ConEdUsage

# WEEKS_TO_CHECK = 4

# def get_last_datetime_from_gsheet(spread: Spread) -> pd.Timestamp:
#     spread.open_sheet('Electric usage')
#     df = spread.sheet_to_df(index=None)
#     df['dt_start_str'] = df['Date'] + ' ' + df['Start']
#     df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
#     return df.iloc[-1]['dt_start']


# def check_gsheet_for_gaps(spread: Spread):
#     spread.open_sheet('Electric usage')
#     df = spread.sheet_to_df(index=None)

#     df['dt_start_str'] = df['Date'] + ' ' + df['Start']
#     df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
#     df = df.drop(columns=['dt_start_str'])

#     df = df[df['dt_start'] >= pd.Timestamp.now().tz_localize(tz='America/New_York') - timedelta(weeks=WEEKS_TO_CHECK)]

#     df['gap'] = df['dt_start'].diff()
#     df['gap_start_dt'] = df['dt_start'] - df['gap'] + timedelta(minutes=15)

#     # return df
#     return df[df['gap'] > timedelta(minutes=15)]


# def pull_gaps_from_opower(input_df: pd.DataFrame) -> pd.DataFrame:
#     if input_df.shape[0] == 1:
#         input_df = input_df.reset_index()
#         dt_tuple = (input_df.loc[0, 'gap_start_dt'].to_pydatetime(), input_df.loc[0, 'dt_start'].to_pydatetime())
#         df_opower = asyncio.run(opower.get_opower_electric_data_custom_dates(dt_tuple[0], dt_tuple[1]))
        
#     elif input_df.shape[0] > 1:
#         dt_tuple_list = []
#         for row in input_df.itertuples():
#             dt_tuple_list.append((row.gap_start_dt.to_pydatetime(), row.dt_start.to_pydatetime()))
            
#         dataframe_list = []
#         for dt_tuple in dt_tuple_list:
#             new_df = asyncio.run(opower.get_opower_electric_data_custom_dates(dt_tuple[0], dt_tuple[1]))
#             print(new_df)
#             dataframe_list.append(new_df)
#             print(f"Waiting 30 seconds...")
#             time.sleep(30)
            
#         df_opower = pd.concat(dataframe_list, ignore_index=True)        
   
#     return df_opower


# def process_df_opower_for_appending_to_gsheet(df_opower: pd.DataFrame) -> pd.DataFrame:
#     # df_opower['start_time'] = df_opower['start_time'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
#     # df_opower['end_time'] = df_opower['end_time'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
    
#     df_opower['Type'] = 'Electric usage'
#     df_opower['Date'] = [x.tz_convert(tz='America/New_York').strftime('%m/%d/%Y') for x in df_opower['start_time']]
#     df_opower['Start'] = [x.tz_convert(tz='America/New_York').strftime('%H:%M') for x in df_opower['start_time']]
#     df_opower['End'] = [x.tz_convert(tz='America/New_York').strftime('%H:%M') for x in df_opower['end_time']]
#     df_opower['Unit'] = 'kWh'
#     df_opower['Month'] = [x.tz_convert(tz='America/New_York').strftime('%b %Y') for x in df_opower['start_time']]

#     # Rename "consumption" column & change the column order
#     df_opower = df_opower.rename(columns={'consumption': 'Usage'})
#     df_opower = df_opower.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Unit', 'Month'])  # 'days', 

#     return df_opower


# def append_new_rows_to_gsheet(spread: Spread, input_df: pd.DataFrame) -> None:
#     spread.open_sheet('Electric usage')
#     last_existing_row = spread.get_sheet_dims()[0]
#     starting_row = last_existing_row + 1

#     # print(input_df.info())
#     df = process_df_opower_for_appending_to_gsheet(input_df)
    
#     spread.df_to_sheet(df, index=False, headers=False, start=(starting_row, 1))


# def sort_gsheet_by_date(spread: Spread) -> None:
#     spread.open_sheet('Electric usage')

#     df = spread.sheet_to_df(index=None)
    
#     df['dt_start_str'] = df['Date'] + ' ' + df['Start']
#     df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))

#     df = df.sort_values(by=['dt_start']).drop_duplicates(subset=['dt_start'], ignore_index=True)
#     df = df.drop(columns=['dt_start', 'dt_start_str'])

#     if df.shape[0] > 0:
#         spread.clear_sheet(rows=0, cols=11)
#         spread.df_to_sheet(df, replace=True, index=False, headers=True, freeze_headers=True, start='A1') #sheet='New Test Sheet', 

#         # Re-format the worksheet
#         # spread.open_sheet('New Test Sheet')
        
#         spread.sheet.format('A:K', {'textFormat': {'bold': False}})
#         spread.sheet.format("A1:K1",
#                             {
#                                 "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
#                                 "horizontalAlignment": "CENTER",
#                                 "textFormat": {"bold": True},
#                             })
#         spread.sheet.format('J:K', {"horizontalAlignment": "CENTER"})
#     else:
#         print("\nUnable to fill gap using Con Edison data.  Exiting now.")
    


def main() -> None:
    usage = ConEdUsage()

    usage.check_gsheet_for_gaps()

    # gaps_df = pd.read_csv('df_opower_2024_07_22-14_11_08.csv')
    # usage.append_new_opower_rows_to_gsheet(gaps_df)
    # usage.sort_gsheet_by_date()

    # gaps_df = pd.read_csv('df_opower_2024_07_22-10_52_15.csv')
    # processed_df = usage.process_df_opower_for_appending_to_gsheet(gaps_df)
    # print(processed_df)
    # print(processed_df.dtypes)
    # processed_df.to_csv('df_opower_2024_07_22-10_52_15_PROCESSED.csv')

    # processed_df = pd.read_csv('df_opower_2024_07_22-10_52_15_PROCESSED.csv', index_col=0)
    # print(processed_df)
    # print(processed_df.dtypes)


    

    # df_gaps = check_gsheet_for_gaps(spread)
    # if df_gaps.shape[0] > 0:
    #     print(f"Found {df_gaps.shape[0]} gaps in dataset:")
    #     print(df_gaps)

    #     if input("Attempt to fill the gaps from Con Edison data? ") in ['Y', 'y', 'yes', 'Yes', 'YES']:
    #         print("Attempting now...")
    #         df_opower = pull_gaps_from_opower(df_gaps)
    #         print(df_opower)
    #         df_opower.to_csv(f"df_opower_{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.csv")

    #         # df_csv = pd.read_csv('df_opower_2024_01_31-22_18_16.csv', index_col=0)
    #         # print(df_csv)

    #         append_new_rows_to_gsheet(spread, df_opower)

    #         sort_gsheet_by_date(spread)
    #     else:
    #         print("Oh, ok.  Bye!")

    # else:
    #     print("No gaps!")

    

    
if __name__ == "__main__":
    main()



