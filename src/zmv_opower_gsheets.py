import asyncio
from datetime import date, datetime, timedelta
import time

from gspread import Cell
from gspread.utils import ValueInputOption
from gspread_formatting import CellFormat, Color, batch_updater
from gspread_pandas import Spread
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from zmv_const import CONED_SPREADSHEET, EASTERN_TIME
import zmv_opower as opower

WEEKS_TO_CHECK = 4
USAGE_WORKSHEET = "Electric Usage"
BILLS_WORKSHEET = "Electric Bills"


class ConEdUsage():
    
    def __init__(self, spreadsheet_id: str = CONED_SPREADSHEET):
        self.spread = Spread(spreadsheet_id)
        self.df_usage = None
        self.df_bills = None

    def ingest_usage_worksheet(self) -> None:
        self.df_usage = self.spread.sheet_to_df(sheet=USAGE_WORKSHEET, index=None)

    def ingest_bills_worksheet(self) -> None:
        self.df_bills = self.spread.sheet_to_df(sheet=BILLS_WORKSHEET, index=None).astype({
            'START DATE': 'datetime64[ns, America/New_York]',
            'END DATE': 'datetime64[ns, America/New_York]',
        }).drop(columns=['NOTES', ''])

    def ingest_worksheets(self) -> None:
        self.ingest_usage_worksheet()
        self.ingest_bills_worksheet()

    def get_last_datetime_from_gsheet(self) -> pd.Timestamp:
        if self.df_usage is None:
            self.ingest_usage_worksheet()
        df = self.df_usage.copy()
        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tzinfo=EASTERN_TIME))
        return df.iloc[-1]['dt_start']

    def pull_new_data_from_opower(self) -> None:
        last_datetime_from_gsheet = self.get_last_datetime_from_gsheet()
        num_days = int((date.today() - datetime.date(last_datetime_from_gsheet)).total_seconds() // 86400)    

        df_opower = asyncio.run(opower.get_opower_electric_data(num_days))

        df_opower = df_opower.drop(df_opower.loc[df_opower['start_time'] <= last_datetime_from_gsheet].index)

        if df_opower.shape[0] > 0:
            print(f"Found {df_opower.shape[0]} new rows of Con Edison data!  Writing to Google Sheets now...")
            self.append_new_opower_rows_to_gsheet(df_opower)
        else:
            print("No new Con Edison data.")

    def append_new_opower_rows_to_gsheet(self, input_df: pd.DataFrame) -> None:
        if self.df_usage is None:
            self.ingest_usage_worksheet()
        last_existing_row = self.df_usage.shape[0] + 1
        starting_row = last_existing_row + 1

        print(f"Appending {input_df.shape[0]:,} rows of electric-usage data to Google Sheets...")
        output_df = self.process_df_opower_for_appending_to_gsheet(input_df)
        self.spread.df_to_sheet(output_df, sheet=USAGE_WORKSHEET, index=False, headers=False, start=(starting_row, 1))
        self.ingest_usage_worksheet()  # refresh this object's usage worksheet data before you re-sort!
        print(output_df)

    def find_gsheet_gaps(self, weeks_to_check: int = WEEKS_TO_CHECK) -> pd.DataFrame | None:
        if self.df_usage is None:
            self.ingest_usage_worksheet()
        df = self.df_usage 

        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tzinfo=EASTERN_TIME))
        df = df.drop(columns=['dt_start_str'])

        df = df[df['dt_start'] >= pd.Timestamp.now().tz_localize(tz=EASTERN_TIME) - timedelta(weeks=weeks_to_check)]

        df['gap'] = df['dt_start'].diff()
        df['gap_start_dt'] = df['dt_start'] - df['gap'] + timedelta(minutes=15)

        df_gaps = df[df['gap'] > timedelta(minutes=15)]
        
        if df_gaps.shape[0] > 0:
            return df_gaps
        else:
            return None

    def check_gsheet_for_gaps(self, weeks_to_check: int = WEEKS_TO_CHECK) -> None:
        df_gaps = self.find_gsheet_gaps(weeks_to_check=weeks_to_check)

        if df_gaps is not None:
            print(f"\nFound {df_gaps.shape[0]} gaps:")
            print(df_gaps)

            if input("\nAttempt to fill the gaps from Con Edison data? ") in ['Y', 'y', 'yes', 'Yes', 'YES']:
                print("Attempting now...")
                df_opower = self.pull_gaps_from_opower(df_gaps)
                print(f"Found {df_opower.shape[0]:,} gaps:")
                print(df_opower)

                if input("\nSave CSV file to disk? ") in ['Y', 'y', 'yes', 'Yes', 'YES']:
                    csv_filename = f"df_opower_{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.csv"
                    print(f"Saving data as {csv_filename}...")
                    df_opower.to_csv(csv_filename, index=False)
                else:
                    print("Oh, ok.  Nevermind.")

                if input(f"\nSave Con Edison data to Google Sheet? ") in ['Y', 'y', 'yes', 'Yes', 'YES']:
                    print(f"Saving data to Google Sheet...")
                    self.append_new_opower_rows_to_gsheet(df_opower)
                    self.sort_gsheet_by_date()
                else:
                    print("Oh, ok.  Nevermind.")
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
        if not isinstance(df_opower['start_time'].dtype, pd.DatetimeTZDtype):
            df_opower['start_time'] = df_opower['start_time'].apply(lambda x: pd.Timestamp(x).tz_convert(tz=EASTERN_TIME))
            
        if not isinstance(df_opower['end_time'].dtype, pd.DatetimeTZDtype):
            df_opower['end_time'] = df_opower['end_time'].apply(lambda x: pd.Timestamp(x).tz_convert(tz=EASTERN_TIME))
        
        df_opower['Type'] = 'Electric usage'
        df_opower['Date'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%m/%d/%Y') for x in df_opower['start_time']]
        df_opower['Start'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%H:%M') for x in df_opower['start_time']]
        df_opower['End'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%H:%M') for x in df_opower['end_time']]
        df_opower['Month'] = [x.tz_convert(tz=EASTERN_TIME).strftime('%b %Y') for x in df_opower['start_time']]
        df_opower['Price/kWh'] = [self.lookup_monthly_price_per_kWh(ts) for ts in df_opower['start_time']] # [f"=VLOOKUP(B{x + starting_row},'Electric Bills'!$B:$J,8)" for x in df_opower.index]
        df_opower['Cost/15min'] = [self.get_15min_cost_column(x, y) for x, y in zip(df_opower['consumption'].tolist(), df_opower['Price/kWh'].tolist())] # [f"=E{x + starting_row}*H{x + starting_row}" for x in df_opower.index]
        df_opower['Summer?'] = ['Yes' if x.month in [6, 7, 8, 9] else 'No' for x in df_opower['start_time']]
        df_opower['Period'] = [self.get_period_column(x) for x in df_opower['start_time']]

        # Rename "consumption" column & change the column order
        df_opower = df_opower.rename(columns={'consumption': 'Usage'})
        df_opower = df_opower.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Month', 'Price/kWh', 'Cost/15min', 'Summer?', 'Period'])

        return df_opower

    def sort_gsheet_by_date(self) -> None:
        if self.df_usage is None:
            self.ingest_usage_worksheet()
            
        df = self.df_usage

        backup_csv_filename = f"df_opower_BACKUP_{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.csv"
        print(f"Saving backup of Google Sheets electric-usage data as {backup_csv_filename}...")
        df.to_csv(backup_csv_filename, index=False)
        
        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tzinfo=EASTERN_TIME))

        print("Sorting existing electric-usage worksheet on Google Sheets...")
        df = df.sort_values(by=['dt_start']).drop_duplicates(subset=['dt_start'], ignore_index=True)
        df = df.drop(columns=['dt_start', 'dt_start_str'])

        if df.shape[0] > 0:
            print("Replacing existing Google Sheets electric-usage worksheet with new, sorted worksheet...")
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
        
        if self.df_bills is None:
            self.ingest_bills_worksheet()

        filtered_df = self.df_bills[(self.df_bills['START DATE'] <= ts) & 
                                    (self.df_bills['END DATE'] >= ts)].reset_index()
        if filtered_df.shape[0] > 0:
            return filtered_df.loc[0, '$/kWh']
        else:
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
            batch.format_cell_range(worksheet, 'A:J', 
                                    CellFormat(
                                        backgroundColor=Color(1,1,1),   
                                        textFormat={'bold': False}))
            
            batch.format_cell_range(worksheet, 'A1:J1', 
                                    CellFormat(
                                        backgroundColor=Color(0.85, 0.85, 0.85),
                                        horizontalAlignment='CENTER',
                                        textFormat={'bold': True}))
            
            batch.format_cell_range(worksheet, 'I:J', 
                                    CellFormat(
                                        horizontalAlignment='CENTER'))

            batch.format_cell_range(worksheet, 'G', 
                                    CellFormat(
                                        numberFormat={'type': 'CURRENCY',
                                                      'pattern': '$#,##0.00#'}))
            
            batch.format_cell_range(worksheet, 'H',
                                    CellFormat(
                                        numberFormat={'type': 'CURRENCY',
                                                      'pattern': '$#,##0.00'}))
            
            batch.format_cell_range(worksheet, 'E', 
                                    CellFormat(
                                        numberFormat={'type': 'NUMBER',
                                                      'pattern': '#,##0.000'})) # 'pattern': '#,##0.000\" kWh\"'}))

            batch.set_column_width(worksheet, 'B', 75)
            batch.set_column_width(worksheet, 'C:D', 60)
            batch.set_column_width(worksheet, 'E', 90)
            batch.set_column_width(worksheet, 'F:I', 85)
            batch.set_column_width(worksheet, 'J', 100)

    def reset_electric_usage_worksheet(self) -> None:
        if self.df_usage is None:
            self.ingest_usage_worksheet()
        df = self.df_usage
        df['Usage'] = df['Usage'].str.rstrip(' kWh')
        df = df.astype({'Usage': 'float64'})

        df['dt_start_str'] = df['Date'] + ' ' + df['Start']
        df['dt_start'] = df['dt_start_str'].apply(lambda x: pd.Timestamp(x, tzinfo=EASTERN_TIME))
        
        df['Price/kWh'] = [self.lookup_monthly_price_per_kWh(ts) for ts in df['dt_start']]
        df['Cost/15min'] = [self.get_15min_cost_column(x, y) for x, y in zip(df['Usage'].tolist(), df['Price/kWh'].tolist())] 
        df['Summer?'] = ['Yes' if x.month in [6, 7, 8, 9] else 'No' for x in df['dt_start']]
        df['Period'] = [self.get_period_column(x) for x in df['dt_start']]

        df = df.sort_values(by=['dt_start']).drop_duplicates(subset=['dt_start'], ignore_index=True)
        df = df.drop(columns=['dt_start', 'dt_start_str'])
        df = df.reindex(columns=['Type', 'Date', 'Start', 'End', 'Usage', 'Month', 
                                 'Price/kWh', 'Cost/15min', 'Summer?', 'Period']) # 'Unit

        if df.shape[0] > 0:
            self.spread.open_sheet(USAGE_WORKSHEET)

            self.spread.df_to_sheet(df, replace=True, index=False, headers=True, freeze_headers=True, start='A1')

            self.reformat_electric_usage_worksheet()
            time.sleep(5)
            self.fix_sealed_invoices_worksheet()
        else:
            print("\nUnable to fill gap using Con Edison data.  Exiting now.")

    def fix_sealed_invoices_worksheet(self) -> None:
        ''' Re-populates the "Actual Total Energy Used" and "Con Ed $ per kWh" fields of the 
        "Sealed Invoices" worksheet on Google Sheets.  

        Excel formula for "Actual Total Energy Used": `=SUMIFS('Electric Usage'!E:E, 
        'Electric Usage'!B:B,\">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})`

        Excel formula for "Con Ed $ per kWh": `=AVERAGEIFS('Electric Usage'!G:G,
        'Electric Usage'!B:B,\">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})` '''
        
        worksheet = self.spread.find_sheet('Sealed Invoices')

        energy_list = [Cell(row_num, 18, 
            f"=SUMIFS('Electric Usage'!E:E, 'Electric Usage'!B:B, \">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})")
                for row_num in range(2, 57) if row_num not in [21, 22, 29, 30, 43, 44]]
        price_list = [Cell(row_num, 26, 
            f"=AVERAGEIFS('Electric Usage'!G:G, 'Electric Usage'!B:B, \">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})")
                for row_num in range(2, 57) if row_num not in [21, 22, 29, 30, 43, 44]]
        
        combined_list = energy_list + price_list

        worksheet.update_cells(combined_list, ValueInputOption.user_entered)

    def ingest_electric_bills_csv_from_coned(self, csv_filename: str) -> None:
        if self.df_bills is None:
            self.ingest_bills_worksheet()
        last_existing_row = self.df_bills.shape[0] + 1
        starting_row = last_existing_row + 1
        
        df_csv = (pd.read_csv(csv_filename, header=4)
                    .rename(columns={'USAGE (kWh)': 'USAGE'})
                    .drop(columns=['NOTES'])
                    .astype({'TYPE': 'string',
                             'START DATE': 'datetime64[ns, America/New_York]',
                             'END DATE': 'datetime64[ns, America/New_York]',
                             'USAGE': 'string',
                             'COST': 'string'}))

        df_csv = df_csv[df_csv['START DATE'].isin(self.df_bills['START DATE']) == False].reset_index(drop=True)

        df_csv['USAGE'] = df_csv['USAGE'].str.rstrip('0').str.rstrip('.')
        df_csv['MONTH'] = [f"=EOMONTH(B{x+starting_row},0)" for x in df_csv.index]
        df_csv['kWh/DAY'] = [f"=D{x+starting_row}/(C{x+starting_row}-B{x+starting_row})" for x in df_csv.index]
        df_csv['$/kWh'] = [f"=E{x+starting_row}/D{x+starting_row}" for x in df_csv.index]
        df_csv['$/DAY'] = [f"=E{x+starting_row}/(C{x+starting_row}-B{x+starting_row})" for x in df_csv.index]

        df_csv['START DATE'] = df_csv['START DATE'].dt.strftime('%-m/%d/%Y')
        df_csv['END DATE'] = df_csv['END DATE'].dt.strftime('%-m/%d/%Y')
        df_csv = df_csv.reindex(columns=['TYPE', 'START DATE', 'END DATE', 'USAGE', 
                                         'COST', 'MONTH', 'kWh/DAY', '$/kWh', '$/DAY'])

        print(df_csv)
        self.spread.df_to_sheet(df_csv, sheet=BILLS_WORKSHEET, replace=False, 
                                index=False, headers=False, start=(starting_row, 1))
        

def main():  
    # pass

    usage = ConEdUsage()
    usage.reset_electric_usage_worksheet()



    # test_csv = 'cned_electric_billing_billing_data_Service 2_2_2021-01-16_to_2024-01-12.csv'
    # usage.ingest_electric_bills_csv_from_coned(test_csv)   
    
    # usage.ingest_bills_worksheet()
    # print(usage.df_bills.info())
    # print(usage.df_bills)
    # usage.fix_sealed_invoices_worksheet()

    # usage.reset_electric_usage_worksheet()
    # usage.reformat_electric_usage_worksheet()

    
if __name__ == "__main__":
    main()



