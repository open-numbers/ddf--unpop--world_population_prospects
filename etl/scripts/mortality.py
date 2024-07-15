# -*- coding: utf-8 -*-

# %%
import os
import pandas as pd
import numpy as np
from ddf_utils.str import to_concept_id
# %%
source_file1 = '../source/WPP2024_DeathsBySingleAgeSex_Medium_1950-2023.csv.gz'
source_file2 = '../source/WPP2024_DeathsBySingleAgeSex_Medium_2024-2100.csv.gz'

output_dir = '../../mortality'
os.makedirs(output_dir, exist_ok=True)

# %%
data1 = pd.read_csv(source_file1)
data2 = pd.read_csv(source_file2)
# %%
data1
# %%
data1.columns

#
data1['AgeGrpSpan'].unique()  # [1, -1] where -1 means 100+.

# %%
# there are deathMale deathFemale indicators, we should create new dimension
# %%

df1 = data1[['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'DeathMale', 'DeathFemale', 'DeathTotal']].copy()
df2 = data2[['LocTypeName', 'LocID', 'Time', 'AgeGrp', 'DeathMale', 'DeathFemale', 'DeathTotal']].copy()
# %%

df1
# %%
df = pd.concat([df1, df2], ignore_index=True)
# %%
df
# %%
df = df[~pd.isnull(df['LocTypeName'])].copy()
df['LocTypeName'] = df['LocTypeName'].map(to_concept_id)
df['LocTypeName'] = df['LocTypeName'].replace('special_other', 'development_group')

# df.set_index(['LocID', 'Time', 'AgeGrp']).loc[900, 2100, :]['DeathTotal'].plot()
# %%
# TODO: we can see the 100+ group makes a sudden up trend. we might remove that

# %%
df['AgeGrp'] = df['AgeGrp'].replace('100+', '100plus')

# %%
df = df.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])
# %%
df
# %%
# male = 1, female = 2, total = 0
df.columns = [1, 2, 0]
# %%
df = df.stack()
# %%
df = df.reset_index('LocTypeName')
# %%
df.index.names = ['location', 'time', 'age_group_1year', 'sex']
df.columns = ['LocTypeName', 'deaths']
# %%
df
# %%
df_total = df.loc[:, :, :, 0].copy()
df_gender = df.loc[:, :, :, [1,2]].copy()
# %%
df_total
# %%
df_gender
# %%
import os

def serve_func(age_group_col, gender_col, indicators, outdir):
    def func(df):
        df = df.copy()
        loctype = to_concept_id(df.iloc[0, 0])
        df.index = df.index.rename({'location': loctype})
        if loctype == 'country_area':
            gs = df.groupby(by=age_group_col)
            for g, gdf in gs:
                for c in indicators:
                    if gender_col:
                        dps_file = f'ddf--datapoints--{c}--by--{loctype}--time--{age_group_col}-{g}--{gender_col}.csv'
                    else:
                        dps_file = f'ddf--datapoints--{c}--by--{loctype}--time--{age_group_col}-{g}.csv'
                    gdf[c].to_csv(os.path.join(
                        '../../',
                        outdir,
                        dps_file
                    ))
        else:
            for c in indicators:
                if gender_col:
                    dps_file = f'ddf--datapoints--{c}--by--{loctype}--time--{age_group_col}--{gender_col}.csv'
                else:
                    dps_file = f'ddf--datapoints--{c}--by--{loctype}--time--{age_group_col}.csv'

                df[c].to_csv(os.path.join(
                    '../../',
                    outdir,
                    dps_file
                ))
    return func

# %%
# def serve_func(df):
#     loctype = to_concept_id(df.iloc[0, 0])
#     indicators = ['deaths']
#     if loctype == 'country_area':
#         gs = df.groupby(by='age_group_1year')
#         for g, gdf in gs:
#             for c in indicators:
#                 gdf[c].to_csv(
#                     f'../../mortality/ddf--datapoints--{c}--by--{loctype}--time--age_group_1year-{g}--gender.csv')
#     else:
#         for c in indicators:
#             df[c].to_csv(
#                 f'../../mortality/ddf--datapoints--{c}--by--{loctype}--time--age_group_1year--gender.csv')

serve_func_total = serve_func('age_group_1year', None, ['deaths'], 'mortality')
# %%
df_total.groupby(['LocTypeName']).apply(serve_func_total)
# %%
serve_func_gender = serve_func('age_group_1year', 'sex', ['deaths'], 'mortality')

df_gender.groupby(['LocTypeName']).apply(serve_func_gender)
# %%
