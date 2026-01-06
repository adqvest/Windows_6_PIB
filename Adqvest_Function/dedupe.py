import pandas as pd

def duplicates(df):
    if df.duplicated().any() == True:
        df.drop_duplicates(inplace=True)
        print("Duplicates deleted!")
    return df