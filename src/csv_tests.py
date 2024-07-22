from zmv_opower_gsheets import ConEdUsage
from datetime import datetime
import pandas as pd

def main():
    usage = ConEdUsage()

    df_gaps = usage.find_gsheet_gaps()

    if df_gaps is None:
        print("No gaps!")
        exit()
    else:
        print(f"Found {df_gaps.shape[0]} gaps in dataset:")
        print(df_gaps)
        df_gaps.to_csv(f"df_gaps_{datetime.now().strftime('%Y_%m_%d-%H_%M_%S')}.csv")
    
    new_df = pd.read_csv('df_opower_2024_04_30-11_01_51.csv')
    print(new_df)
    

if __name__ == '__main__':
    main()