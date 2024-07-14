# -*- coding: utf-8 -*-

#%%
import pandas as pd
import os
from ddf_utils.str import to_concept_id

# %%
source_file = '../source/WPP2024_Fertility_by_Age1.csv'
source_file2 = '../source/WPP2024_Fertility_by_Age5.csv'

# output dir
output_dir1 = '../../fertility-age1'
os.makedirs(output_dir1, exist_ok=True)
output_dir2 = '../../fertility-age5'
os.makedirs(output_dir2, exist_ok=True)

# %%
sample = pd.read_csv(source_file, nrows=10000)
sample.head()
# %%
for c in sample.columns:
    print(c)

#
sample[pd.isnull(sample['LocTypeName'])][['LocID', 'Location']]

sample.to_csv('~/Downloads/sample.csv', index=False)

# %%
usecols = ['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'ASFR', 'PASFR', 'Births', 'Variant']
dtypes = ['str', 'str', 'float', 'str', 'float', 'float', 'float', 'str']
dtypes_ = dict(zip(usecols, dtypes))
dtypes_

data = pd.read_csv(source_file, usecols=usecols, dtype=dtypes_)
data5 = pd.read_csv(source_file2, usecols=usecols, dtype=dtypes_)

# %%
data5.head()
# %%
# 1. data -> change AgeGrp to age_group_1year

# only use Medium Estimate
df1 = data[data['Variant'] == 'Medium']
df1 = df1[['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'ASFR', 'PASFR', 'Births']].copy()
# %%
df1
# %%
df1['LocTypeName'].unique()
# %%

# there are nans, let's see what's there
df1[pd.isnull(df1['LocTypeName'])].sample()

# let's remove them all because they are not in the location metadata.
df1 = df1[~pd.isnull(df1['LocTypeName'])].copy()
df1['Time'] = df1['Time'].astype('int')

# groupby location type and serve each indicator

def serve_func(df):
    loctype = to_concept_id(df.iloc[0, 0])
    indicators = ['ASFR', 'PASFR', 'Births']
    indicators_id = list(map(to_concept_id, indicators))
    dfnew = df.set_index(['LocID', 'Time', 'AgeGrp'])[indicators]
    # make sure loctype name match metadata
    if loctype == 'special_other':
        loctype = "development_group"
    dfnew.columns = indicators_id
    dfnew.index.names = [loctype, 'time', 'age_group_1year']

    # serving
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
data5['Variant'].unique()
df5 = data5[data5['Variant'].isin(['Medium'])][['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'ASFR', 'PASFR', 'Births']].copy()

# %%
df5
# %%
df5['AgeGrp'] = df5['AgeGrp'].map(to_concept_id)
# also remove empty locations
df5 = df5[~pd.isnull(df5['LocTypeName'])]
df5['Time'] = df5['Time'].astype('int')

# %%
df5
# %%
def serve_func(df):
    loctype = to_concept_id(df.iloc[0, 0])
    indicators = ['ASFR', 'PASFR', 'Births']
    indicators_id = list(map(to_concept_id, indicators))
    dfnew = df.set_index(['LocID', 'Time', 'AgeGrp'])[indicators]
    # make sure loctype name match metadata
    if loctype == 'special_other':
        loctype = "development_group"
    dfnew.columns = indicators_id
    dfnew.index.names = [loctype, 'time', 'age_group_5year']

    # serving
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
