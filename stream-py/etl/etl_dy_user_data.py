import pandas as pd
import re

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)

excel_file = pd.ExcelFile('../data/input/1.xlsx')
sheet_names = excel_file.sheet_names

for sheet_name in sheet_names:
    df = excel_file.parse(sheet_name)
    print(df.to_string(na_rep='nan'))
