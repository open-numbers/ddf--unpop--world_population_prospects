# -*- coding: utf8 -*-

#%%
import pandas as pd

from ddf_utils.str import to_concept_id

# %%
source_file = '../source/WPP2022_Fertility_by_Age1.zip'
source_file2 = '../source/WPP2022_Fertility_by_Age5.zip'

# %%
data = pd.read_csv(source_file)
data5 = pd.read_csv(source_file2)
# %%
data.head()
# %%
for c in data.columns:
    print(c)
# %%
data5.head()
# %%
# 1. data -> change AgeGrp to age_group_1year

df1 = data[['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'ASFR', 'PASFR', 'Births']].copy()
# %%
df1
# %%
df1['LocTypeName'].unique()
# %%
# Next: groupby location type and serve each indicator

def serve_func(df):
    loctype = to_concept_id(df.iloc[0, 0])
    indicators = ['ASFR', 'PASFR', 'Births']
    indicators_id = list(map(to_concept_id, indicators))
    dfnew = df.set_index(['LocID', 'Time', 'AgeGrp'])[indicators]
    dfnew.columns = indicators_id
    dfnew.index.names = [loctype, 'time', 'age_group_1year']
    if loctype == 'country_area':
        gs = dfnew.groupby(by='age_group_1year')
        for g, gdf in gs:
            for c in indicators_id:
                gdf[c].to_csv(
                    f'../../fertility-age1/ddf--datapoints--{c}--by--{loctype}--time--age_group_1year-{g}.csv')
    else:
        for c in indicators_id:
            dfnew[c].to_csv(
                f'../../fertility-age1/ddf--datapoints--{c}--by--{loctype}--time--age_group_1year.csv')

df1.groupby(['LocTypeName']).apply(serve_func)
# %%
df1['AgeGrp'].unique().shape
# %%
# now work on age5:

df5 = data5[['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'ASFR', 'PASFR', 'Births']].copy()

# %%
df5 
# %%
df5['AgeGrp'] = df5['AgeGrp'].map(to_concept_id)
# %%
df5 
# %%
def serve_func(df):
    loctype = to_concept_id(df.iloc[0, 0])
    indicators = ['ASFR', 'PASFR', 'Births']
    indicators_id = list(map(to_concept_id, indicators))
    dfnew = df.set_index(['LocID', 'Time', 'AgeGrp'])[indicators]
    dfnew.columns = indicators_id
    dfnew.index.names = [loctype, 'time', 'age_group_5year']
    if loctype == 'country_area':
        gs = dfnew.groupby(by='age_group_5year')
        for g, gdf in gs:
            for c in indicators_id:
                gdf[c].to_csv(
                    f'../../fertility-age5/ddf--datapoints--{c}--by--{loctype}--time--age_group_5year-{g}.csv')
    else:
        for c in indicators_id:
            dfnew[c].to_csv(
                f'../../fertility-age5/ddf--datapoints--{c}--by--{loctype}--time--age_group_5year.csv')
                
df5.groupby(['LocTypeName']).apply(serve_func)
# %%
