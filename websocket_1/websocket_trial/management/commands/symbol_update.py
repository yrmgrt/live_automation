import pandas as pd
strike_diff_df = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
symbols = strike_diff_df.symbol.unique()
# symbols = ['BANKNIFTY']
print(symbols)