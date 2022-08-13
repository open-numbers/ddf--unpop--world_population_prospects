# -*- coding: utf8 -*-

#%%
import pandas as pd
import numpy as np
from ddf_utils.str import to_concept_id

# %%
source_file = "../source/WPP2022_Life_Table_Abridged_Medium_1950-2021.zip"
source_file2 = '../source/WPP2022_Life_Table_Abridged_Medium_2022-2100.zip'

# source_file3 = '../source/WPP2022_Life_Table_Complete_Medium_Both_1950-2021.zip'

# File structure is same between the Abridged one and Complete one. 
# The difference is that abridged one use N years age groups while the complete one use 1 year age groups
# decided to use the abridged one...

# %%
data = pd.read_csv(source_file)
data2 = pd.read_csv(source_file2)
# %%
data
# %%
data.columns

# %%
data[['SexID', 'Sex']].drop_duplicates()

# we will create 2 datapoints, one with the gender 
# %%
df1 = data.set_index(['LocID', 'Time', 'AgeGrp', 'SexID'])['ax']
# %%
df1.loc[:, :, :, 3]
# %%
df2 = data2.set_index(['LocID', 'Time', 'AgeGrpStart', 'SexID'])['ax']
# %%
df2.loc[:, :, :, 3]

# %%

# %%
data['Time'].unique()

# %%
data['AgeGrpSpan'].unique()
# %%
data[data['AgeGrpSpan'] == -1]['AgeGrp']  # 100+
# %%
data[data['AgeGrpSpan'] == 1]['AgeGrp']  # 0
# %%
data[data['AgeGrpSpan'] == 4]['AgeGrp']  # 1-4
# %%
# because there 0, 1-4. let's name it to age group broad
# %%
df1 = data.set_index(['LocID', 'Time', 'SexID', 'AgeGrp'])[
    ['LocTypeName', 'mx', 'qx', 'px', 'lx', 'dx', 'Lx', 'Sx', 'Tx', 'ex', 'ax']].copy()

df2 = data2.set_index(['LocID', 'Time', 'SexID', 'AgeGrp'])[
    ['LocTypeName', 'mx', 'qx', 'px', 'lx', 'dx', 'Lx', 'Sx', 'Tx', 'ex', 'ax']].copy()

# %%
df = pd.concat([df1, df2])

# %%
df
# %%
def rename_agegroup(x):
    if '-' in x:
        return x.replace('-', '_')
    if '+' in x:
        return x.replace('+', 'plus')
    return x
# %%
df = df.rename(index=rename_agegroup, level=3)
# %%
df.index.names = ['location', 'time', 'sex', 'age_group_broad']

# %%
df.index.get_level_values(3).unique().shape

# %%
data[['SexID', 'Sex']].drop_duplicates()
# 1: male
# 2: female
# 3: total
# %%
def serve_func_total(df):
    loctype = to_concept_id(df.iloc[0, 0])
    indicators = ['mx', 'qx', 'px', 'lx', 'dx', 'lx', 'sx', 'tx', 'ex', 'ax']
    df.index.names = [loctype, 'time', 'age_group_broad']
    df.columns = df.columns.map(to_concept_id)
    if loctype == 'country_area':
        gs = df.groupby(by='age_group_broad')
        for g, gdf in gs:
            for c in indicators:
                gdf[c].to_csv(
                    f'../../life_table/ddf--datapoints--{c}--by--{loctype}--time--age_group_broad-{g}.csv')
    else:
        for c in indicators:
            df[c].to_csv(
                f'../../life_table/ddf--datapoints--{c}--by--{loctype}--time--age_group_broad.csv')

# %%
def serve_func_sex(df):
    loctype = to_concept_id(df.iloc[0, 0])
    indicators = ['mx', 'qx', 'px', 'lx', 'dx', 'lx', 'sx', 'tx', 'ex', 'ax']
    df.index.names = [loctype, 'time', 'sex', 'age_group_broad']
    df.columns = df.columns.map(to_concept_id)
    if loctype == 'country_area':
        gs = df.groupby(by='age_group_broad')
        for g, gdf in gs:
            for c in indicators:
                gdf[c].to_csv(
                    f'../../life_table/ddf--datapoints--{c}--by--{loctype}--time--sex--age_group_broad-{g}.csv')
    else:
        for c in indicators:
            df[c].to_csv(
                f'../../life_table/ddf--datapoints--{c}--by--{loctype}--time--sex--age_group_broad.csv')
# %%
df['LocTypeName'] = df['LocTypeName'].map(to_concept_id)

# %%
df_sex = df.loc[:, :, [1, 2], :]
df_total = df.loc[:, :, 3, :]
# %%
df_sex
# %%
df_total
# %%
df_sex.groupby(['LocTypeName']).get_group('country_area').iloc[0, 0]
# %%
df_sex.groupby(['LocTypeName']).apply(serve_func_sex)
# %%
df_total.groupby(['LocTypeName']).apply(serve_func_total)
# %%
