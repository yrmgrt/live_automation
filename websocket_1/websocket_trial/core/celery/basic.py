from websocket_trial.management.commands.symbol_update import symbols
import pandas as pd
all_symbols = pd.read_csv(r"C:\Users\Administrator\Downloads\all_symbols.csv")
all_symbols = all_symbols.symbol.unique()
corr_df = pd.read_csv(r"C:\Users\Administrator\Downloads\correlation.csv")
pairs = [[corr_df['stock_1'][i], corr_df['stock_2'][i]] for i in range(len(corr_df))]
scripts = list(set(all_symbols) - set(symbols))
print(scripts)