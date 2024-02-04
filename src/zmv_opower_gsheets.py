import asyncio
from datetime import date, datetime, timedelta
import time

from gspread import Cell
from gspread.utils import ValueInputOption
from gspread_pandas import Spread
from gspread_formatting import batch_updater, CellFormat, Color
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from zmv_const import CONED_SPREADSHEET
import zmv_opower as opower

WEEKS_TO_CHECK = 4
USAGE_WORKSHEET = "Electric Usage"
BILLS_WORKSHEET = "Electric Bills"


class ElectricityUsage():
    
    def __init__(self, spread: Spread):
        self.spread = spread
        self.df_usage = None
        self.df_bills = None


    def ingest_worksheets(self) -> None:
        self.df_usage = self.spread.sheet_to_df(sheet=USAGE_WORKSHEET, index=None)
        self.df_bills = self.spread.sheet_to_df(sheet=BILLS_WORKSHEET, index=None).astype({
            'START DATE': 'datetime64[ns, America/New_York]',
            'END DATE': 'datetime64[ns, America/New_York]',
        }).drop(columns=['NOTES', ''])


    def get_last_datetime_from_gsheet(self) -> pd.Timestamp:
        if self.df_usage is None:
            self.ingest_worksheets()
        df = self.df_usage            
        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
        return df.iloc[-1]['dt_start']


    def pull_new_data_from_opower(self) -> None:
        last_datetime_from_gsheet = self.get_last_datetime_from_gsheet()
        num_days = int((date.today() - datetime.date(last_datetime_from_gsheet)).total_seconds() // 86400)    

        df_opower = asyncio.run(opower.get_opower_electric_data(num_days))

        df_opower = df_opower.drop(df_opower.loc[df_opower['start_time'] <= last_datetime_from_gsheet].index)

        if df_opower.shape[0] > 0:
            print(f"Found {df_opower.shape[0]} new rows of Con Edison data!  Writing to Google Sheets now...")
            self.append_new_rows_to_gsheet(df_opower)
        else:
            print("No new Con Edison data.")


    def append_new_rows_to_gsheet(self, input_df: pd.DataFrame) -> None:
        if self.df_usage is None:
            self.ingest_worksheets()
        last_existing_row = self.df_usage.shape[0] + 1
        starting_row = last_existing_row + 1

        output_df = self.process_df_opower_for_appending_to_gsheet(input_df)
        self.spread.df_to_sheet(output_df, sheet=USAGE_WORKSHEET, index=False, headers=False, start=(starting_row, 1))
        print(output_df)


    def check_gsheet_for_gaps(self) -> None:
        if self.df_usage is None:
            self.ingest_worksheets()
        df = self.df_usage 

        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
        df = df.drop(columns=['dt_start_str'])

        df = df[df['dt_start'] >= pd.Timestamp.now().tz_localize(tz='America/New_York') - timedelta(weeks=WEEKS_TO_CHECK)]

        df['gap'] = df['dt_start'].diff()
        df['gap_start_dt'] = df['dt_start'] - df['gap'] + timedelta(minutes=15)

        df_gaps = df[df['gap'] > timedelta(minutes=15)]
        
        if df_gaps.shape[0] > 0:
            print(f"Found {df_gaps.shape[0]} gaps in dataset:")
            print(df_gaps)

            if input("Attempt to fill the gaps from Con Edison data? ") in ['Y', 'y', 'yes', 'Yes', 'YES']:
                print("Attempting now...")
                df_opower = self.pull_gaps_from_opower(df_gaps)
                print(df_opower)
                df_opower.to_csv(f"df_opower_{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.csv")

                self.append_new_rows_to_gsheet(df_opower)

                self.sort_gsheet_by_date()
            else:
                print("Oh, ok.  Bye!")
        else:
            print("No gaps!")


    @staticmethod
    def pull_gaps_from_opower(input_df: pd.DataFrame) -> pd.DataFrame:
        if input_df.shape[0] == 1:
            input_df = input_df.reset_index()
            dt_tuple = (input_df.loc[0, 'gap_start_dt'].to_pydatetime(), input_df.loc[0, 'dt_start'].to_pydatetime())
            df_opower = asyncio.run(opower.get_opower_electric_data_custom_dates(dt_tuple[0], dt_tuple[1]))
            
        elif input_df.shape[0] > 1:
            dt_tuple_list = []
            for row in input_df.itertuples():
                dt_tuple_list.append((row.gap_start_dt.to_pydatetime(), row.dt_start.to_pydatetime()))
                
            dataframe_list = []
            for dt_tuple in dt_tuple_list:
                new_df = asyncio.run(opower.get_opower_electric_data_custom_dates(dt_tuple[0], dt_tuple[1]))
                print(new_df)
                dataframe_list.append(new_df)
                print(f"Waiting 30 seconds...")
                time.sleep(30)
                
            df_opower = pd.concat(dataframe_list, ignore_index=True)        
    
        return df_opower

    
    def process_df_opower_for_appending_to_gsheet(self, df_opower: pd.DataFrame) -> pd.DataFrame:
        if not is_datetime64_any_dtype(df_opower['start_time'].dtype):
            df_opower['start_time'] = df_opower['start_time'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
            
        if not is_datetime64_any_dtype(df_opower['end_time'].dtype):
            df_opower['end_time'] = df_opower['end_time'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
        
        df_opower['Type'] = 'Electric usage'
        df_opower['Date'] = [x.tz_convert(tz='America/New_York').strftime('%m/%d/%Y') for x in df_opower['start_time']]
        df_opower['Start'] = [x.tz_convert(tz='America/New_York').strftime('%H:%M') for x in df_opower['start_time']]
        df_opower['End'] = [x.tz_convert(tz='America/New_York').strftime('%H:%M') for x in df_opower['end_time']]
        df_opower['Unit'] = 'kWh'
        df_opower['Month'] = [x.tz_convert(tz='America/New_York').strftime('%b %Y') for x in df_opower['start_time']]
        df_opower['Price/kWh'] = [self.lookup_monthly_price_per_kWh(ts) for ts in df_opower['start_time']] # [f"=VLOOKUP(B{x + starting_row},'Electric Bills'!$B:$J,8)" for x in df_opower.index]
        df_opower['Cost/15min'] = [self.get_15min_cost_column(x, y) for x, y in zip(df_opower['consumption'].tolist(), df_opower['Price/kWh'].tolist())] # [f"=E{x + starting_row}*H{x + starting_row}" for x in df_opower.index]
        df_opower['Summer?'] = ['Yes' if x.month in [6, 7, 8, 9] else 'No' for x in df_opower['start_time']]
        df_opower['Period'] = [self.get_period_column(x) for x in df_opower['start_time']]

        # Rename "consumption" column & change the column order
        df_opower = df_opower.rename(columns={'consumption': 'Usage'})
        df_opower = df_opower.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Unit', 'Month', 'Price/kWh', 'Cost/15min', 'Summer?', 'Period'])

        return df_opower


    def sort_gsheet_by_date(self, df: pd.DataFrame = None) -> None:
        if df is None:
            if self.df_usage is None:
                self.ingest_worksheets()
            df = self.df_usage
        
        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))

        df = df.sort_values(by=['dt_start']).drop_duplicates(subset=['dt_start'], ignore_index=True)
        df = df.drop(columns=['dt_start', 'dt_start_str'])

        if df.shape[0] > 0:
            self.spread.open_sheet(USAGE_WORKSHEET)

            self.spread.df_to_sheet(df, replace=True, index=False, headers=True, freeze_headers=True, start='A1')

            self.reformat_electric_usage_worksheet()
            time.sleep(5)
            self.fix_sealed_invoices_worksheet()
        else:
            print("\nUnable to fill gap using Con Edison data.  Exiting now.")


    def lookup_monthly_price_per_kWh(self, ts: pd.Timestamp) -> str:
        ''' Using the "Electric Bills" worksheet, looks up the per-kWh price for a given `pd.Timestamp`.

        Previous Excel formula was:  `=VLOOKUP(B{row_num},'Electric Bills'!$B:$J,8)`'''
        
        # filtered_df = self.df_bills[self.df_bills['MONTH'] == ts.strftime('%b %Y')].reset_index()
        filtered_df = self.df_bills[(self.df_bills['START DATE'] <= ts) & 
                                    (self.df_bills['END DATE'] >= ts)].reset_index()
        
        if filtered_df.shape[0] > 0:
            return filtered_df.loc[0, '$/kWh']
        else:
            # year = ts.year - 1 if ts.month == 1 else ts.year
            # last_month = 12 if ts.month == 1 else ts.month - 1
            # new_ts = pd.Timestamp(year=year, month=last_month, day=1, tz='America/New_York')

            new_ts = ts - timedelta(weeks=2)
            return self.lookup_monthly_price_per_kWh(new_ts)

        
    @staticmethod
    def get_period_column(ts: pd.Timestamp) -> str:
        ''' Determines the Con Edison time-of-use period for a given `pd.Timestamp`. 

        - `Peak` is 8 AM to midnight during the non-summer months
        - `Summer Peak` is 8 AM to midnight during the summer months (June 1 to Sept 30)
        - `Super Peak` is on weekdays between 2 PM and 6 PM during the summer months
        
        See https://www.coned.com/en/accounts-billing/your-bill/time-of-use for details.

        Previous Excel formula was:
        `=IF(J{row_num}="No",IF(HOUR(C{row_num})<8,"Non-Peak","Peak"),IF(HOUR(C{row_num})<8,
        "Non-Peak",IF(AND(HOUR(C{row_num})>13,HOUR(C{row_num})<18),"Super Peak","Peak")))`'''
        
        if ts.month in [6, 7, 8, 9]:
            if (ts.dayofweek < 5) and (14 <= ts.hour < 18):  # 'dayofweek' is zero-indexed and begins w/ Monday
                return 'Super Peak'
            elif ts.hour >= 8:
                return 'Summer Peak'
            else:
                return 'Non-Peak'
        elif ts.hour >= 8:
            return 'Peak'
        else:
            return 'Non-Peak'


    @staticmethod
    def get_15min_cost_column(kWh: float, price_str: str) -> str:
        ''' Calculates the price of electricity for a given time period.

        Previous Excel formula was:  `=E{row_num}*H{row_num}` '''
        if not price_str or price_str == '':
            return ''
        else:
            try:
                price = float(price_str.strip('$'))
                result = kWh * price
                return f"${result:,.3f}"
            except TypeError:
                return 'TypeError'


    def reformat_electric_usage_worksheet(self) -> None:
        worksheet = self.spread.find_sheet(USAGE_WORKSHEET)
        with batch_updater(worksheet.spreadsheet) as batch:
            batch.format_cell_range(worksheet, 'A:K', CellFormat(
                backgroundColor=Color(1,1,1),   
                textFormat={"bold": False}
            ))
            batch.format_cell_range(worksheet, 'A1:K1', CellFormat(
                backgroundColor=Color(0.85, 0.85, 0.85),
                horizontalAlignment='CENTER',
                textFormat={"bold": True}
            ))
            batch.format_cell_range(worksheet, 'F:G', CellFormat(
                horizontalAlignment='CENTER'
            ))
            batch.format_cell_range(worksheet, 'J:K', CellFormat(
                horizontalAlignment='CENTER'
            ))
            batch.format_cell_range(worksheet, 'J:K', CellFormat(
                numberFormat={"type": "CURRENCY"}
            ))
            batch.set_column_width(worksheet, 'B', 75)
            batch.set_column_width(worksheet, 'C:F', 70)
            batch.set_column_width(worksheet, 'G:J', 85)
            batch.set_column_width(worksheet, 'K', 100)


    def reset_electric_usage_worksheet(self) -> None:
        if self.df_usage is None:
            self.ingest_worksheets()
        df = self.df_usage
        df = df.astype({'Usage': 'float64'})

        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tz='America/New_York'))
        
        df['Price/kWh'] = [self.lookup_monthly_price_per_kWh(ts) for ts in df['dt_start']]
        df['Cost/15min'] = [self.get_15min_cost_column(x, y) for x, y in zip(df['Usage'].tolist(), df['Price/kWh'].tolist())] 
        df['Summer?'] = ['Yes' if x.month in [6, 7, 8, 9] else 'No' for x in df['dt_start']]
        df['Period'] = [self.get_period_column(x) for x in df['dt_start']]

        df = df.sort_values(by=['dt_start']).drop_duplicates(subset=['dt_start'], ignore_index=True)
        df = df.drop(columns=['dt_start', 'dt_start_str'])
        df = df.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Unit', 'Month', 'Price/kWh', 'Cost/15min', 'Summer?', 'Period'])

        if df.shape[0] > 0:
            self.spread.open_sheet(USAGE_WORKSHEET)

            self.spread.df_to_sheet(df, replace=True, index=False, headers=True, freeze_headers=True, start='A1')

            self.reformat_electric_usage_worksheet()
            time.sleep(5)
            self.fix_sealed_invoices_worksheet()
        else:
            print("\nUnable to fill gap using Con Edison data.  Exiting now.")


    def fix_sealed_invoices_worksheet(self) -> None:
        worksheet = self.spread.find_sheet('Sealed Invoices')

        energy_list = [Cell(row_num, 17, 
            f"=SUMIFS('Electric Usage'!E:E, 'Electric Usage'!B:B, \">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})")
                             for row_num in range(2, 57) if row_num not in [21, 22, 29, 30, 43, 44]]
        price_list = [Cell(row_num, 25, 
            f"=AVERAGEIFS('Electric Usage'!H:H, 'Electric Usage'!B:B, \">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})")
                             for row_num in range(2, 57) if row_num not in [21, 22, 29, 30, 43, 44]]
        
        combined_list = energy_list + price_list

        worksheet.update_cells(combined_list, ValueInputOption.user_entered)
        

def main():  
    pass

    # spread = Spread(CONED_SPREADSHEET)
    # usage = ElectricityUsage(spread)

    # usage.reset_electric_usage_worksheet()
    
    # usage.ingest_worksheets()
    # print(usage.df_bills.info())
    # print(usage.df_bills)
    # usage.fix_sealed_invoices_worksheet()

    
if __name__ == "__main__":
    main()



