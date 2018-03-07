import pandas as pd
from price_table_columns import (PRICES_COLUMNS_TO_DROP, OPEN_COL, CLOSE_COL, TICKER_COL)

filename = "default_stock_codes_data.csv"
stock_codes = ["COF", "GOOGL", "MSFT"]
start_date = "2017-01-01"
end_date = "2017-06-30"

raw_df = pd.read_csv(filepath_or_buffer=filename, parse_dates=["date"])
df_filtered_by_code = raw_df[raw_df.ticker.isin(stock_codes)]
df_filtered_by_date = df_filtered_by_code[
    (df_filtered_by_code.date >= start_date) & (df_filtered_by_code.date <= end_date)
    ]
df_filtered_col_drop = df_filtered_by_date.drop(PRICES_COLUMNS_TO_DROP, axis=1)

df_final_report = df_filtered_col_drop.groupby(
    by=[TICKER_COL, df_filtered_col_drop.date.dt.to_period("M")])[CLOSE_COL, OPEN_COL].mean()
print(df_final_report)
# ~50 secs with full data 3/3/2018
