# -*- coding: utf-8 -*-

# %%

import os
import pandas as pd
import numpy as np
from ddf_utils.str import to_concept_id
# %%
# There are 2 types of population:
# 1. population on 1 Jan
# 2. population on 1 Jul
# use 1 Jul, that's what we use and in WPP it's called population.
# in WPP population on 1 Jan is called population 1 january
# %%

def fullpath(x):
    return os.path.join('../source/', x)

# create out dir
for d in ['population', 'population_age5', 'population_age1']:
    os.makedirs(f'../../{d}', exist_ok=True)

# %%
# 'WPP2024_TotalPopulationBySex.csv.gz'
data = pd.read_csv(fullpath('WPP2024_TotalPopulationBySex.csv.gz'))
# %%
data
# %%
df = data[data['Variant'] == 'Medium']
df = df[~pd.isnull(df['LocTypeName'])]
df['LocTypeName'] = df['LocTypeName'].map(to_concept_id)
df['LocTypeName'] = df['LocTypeName'].replace('special_other', 'development_group')
print(df['LocTypeName'].unique())

df['Time'].unique()  # double check if it goes to 2100
# %%
df1 = df.set_index(['LocID', 'Time', 'LocTypeName'])[['PopMale', 'PopFemale']]
df2 = df.set_index(['LocID', 'Time', 'LocTypeName'])[['PopTotal']]
df3 = df.set_index(['LocID', 'Time', 'LocTypeName'])[['PopDensity']]

# %%
df1
# %%
df_total = df1.copy()
df_total.columns = [1, 2]
df_total = df_total.stack()
# %%
df_total
# %%
df_total.index.names = ['location', 'time', 'loctype', 'sex']
df_total.name = 'population'
# %%
df_total = df_total.reset_index()
# %%
for g in df_total['loctype'].unique():
    col = to_concept_id(g)
    ser = df_total[df_total['loctype'] == g][['location', 'time', 'sex', 'population']].copy()
    ser.columns = [col, 'time', 'sex', 'population']
    ser.to_csv(f'../../population/ddf--datapoints--population--by--{col}--time--sex.csv', index=False)
# %%
df_total
# %%
df2.loc[:, :, 'country_area']
# %%
df2.columns = ['population']
for g in df2.index.get_level_values(2).unique():
    df_g = df2.loc[:, :, g].copy()
    col = to_concept_id(g)
    df_g.index.names = [col, 'time']
    df_g.to_csv(f'../../population/ddf--datapoints--population--by--{col}--time.csv')
# %%
df3.columns = ['population_density']
for g in df3.index.get_level_values(2).unique():
    df_g = df3.loc[:, :, g].copy()
    col = to_concept_id(g)
    df_g.index.names = [col, 'time']
    df_g.to_csv(f'../../population/ddf--datapoints--population_density--by--{col}--time.csv')
# %%
# WPP2024_PopulationByAge5GroupSex_Medium.csv

data = pd.read_csv(fullpath('WPP2024_PopulationByAge5GroupSex_Medium.csv.gz'))
# %%
data
# %%
data = data[data['Variant'] == 'Medium']
data = data[~pd.isnull(data['LocTypeName'])]
data['LocTypeName'] = data['LocTypeName'].map(to_concept_id)
data['LocTypeName'] = data['LocTypeName'].replace('special_other', 'development_group')

data['Time'].unique()

data
# %%
def age_grp_concept(x: str):
    if '+' in x:
        return x.replace('+', 'plus')
    return x.replace('-', '_')
# %%
data['AgeGrp'] = data['AgeGrp'].map(age_grp_concept)
# %%
df1 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopMale', 'PopFemale']]
df2 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopTotal']]
# %%
df1
# %%
df1.columns = [1, 2]
df1 = df1.stack()
# %%
df1
# %%
df1.name = 'population'
df1.index.names = ['loctype', 'location', 'time', 'age_group_5year', 'sex']
# %%
def serve_func(age_group_col, gender_col, indicator, outdir):
    def func(df):
        for g in df.index.get_level_values(0).unique():
            col = to_concept_id(g)
            ser = df.loc[g].copy()
            if gender_col:
                ser.index.names = [col, 'time', age_group_col, gender_col]
            else:
                ser.index.names = [col, 'time', age_group_col]
            if 'country' in col:
                for agegrp, ser_ in ser.groupby(by=age_group_col):
                    if gender_col:
                        ser_.to_csv(os.path.join(
                            '../../',
                            outdir,
                            f'ddf--datapoints--{indicator}--by--{col}--time--{age_group_col}-{agegrp}--{gender_col}.csv'
                        ))
                    else:
                        ser_.to_csv(os.path.join(
                            '../../',
                            outdir,
                            f'ddf--datapoints--{indicator}--by--{col}--time--{age_group_col}-{agegrp}.csv'
                        ))
            else:
                if gender_col:
                    ser.to_csv(os.path.join('../../', outdir, f'ddf--datapoints--{indicator}--by--{col}--time--{age_group_col}--{gender_col}.csv'))
                else:
                    ser.to_csv(os.path.join('../../', outdir, f'ddf--datapoints--{indicator}--by--{col}--time--{age_group_col}.csv'))
    return func


# for g in df.index.get_level_values(0).unique():
#     ser = df.loc[g].copy()
#     col = to_concept_id(g)
#     ser.index.names = [g, 'time', 'age_group_5year', 'gender']
#     if 'country' in col:
#         for agegrp, ser_ in ser.groupby(by='age_group_5year'):
#             ser_.to_csv(
#                 f'../../population/ddf--datapoints--population--by--{col}--time--age_group_5year-{agegrp}--gender.csv')
#     else:
#         ser.to_csv(f'../../population/ddf--datapoints--population--by--{col}--time--age_group_5year--gender.csv')
# %%
serve_func('age_group_5year', 'sex', 'population', 'population_age5')(df1)

# %%
df2

# %%
df2.columns = ['population']
df2.index.names = ['loctype', 'location', 'time', 'age_group_5year']

serve_func('age_group_5year', None, 'population', 'population_age5')(df2)

# %%
# WPP2024_PopulationByAge5GroupSex_Percentage_Medium.csv.gz
data = pd.read_csv(fullpath('WPP2024_PopulationByAge5GroupSex_Percentage_Medium.csv.gz'))

# %%
data
# %%
data['AgeGrp'] = data['AgeGrp'].map(age_grp_concept)

data = data[~pd.isnull(data['LocTypeName'])]
data['LocTypeName'] = data['LocTypeName'].map(to_concept_id)
data['LocTypeName'] = data['LocTypeName'].replace('special_other', 'development_group')
# %%
data
# %%
df1 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopMale', 'PopFemale']]
df2 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopTotal']]
# %%
df1.columns = [1, 2]
df1 = df1.stack()
# %%
df1.name = 'population_percentage'
df1.index.names = ['loctype', 'location', 'time', 'age_group_5year', 'sex']

df1
# %%
serve_func('age_group_5year', 'sex', 'population_percentage', 'population_age5')(df1)
# %%
df2.columns = ['population_percentage']
df2.index.names = ['loctype', 'location', 'time', 'age_group_5year']

serve_func('age_group_5year', None, 'population_percentage', 'population_age5')(df2)
# %%
#  'WPP2024_PopulationBySingleAgeSex_Medium_1950-2021.csv.gz',
#  'WPP2024_PopulationBySingleAgeSex_Medium_2022-2100.csv.gz',

data1 = pd.read_csv(fullpath('WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv.gz'))
data2 = pd.read_csv(fullpath('WPP2024_PopulationBySingleAgeSex_Medium_2024-2100.csv.gz'))
# %%
data = pd.concat([data1, data2], ignore_index=True)

# %%
print(data1.shape)
print(data2.shape)
print(data.shape)

# %%
data['AgeGrp'] = data['AgeGrp'].map(age_grp_concept)
data = data[~pd.isnull(data['LocTypeName'])]
data['LocTypeName'] = data['LocTypeName'].map(to_concept_id)
data['LocTypeName'] = data['LocTypeName'].replace('special_other', 'development_group')

data.shape
# %%
df1 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopMale', 'PopFemale']]
df2 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopTotal']]
# %%
df1.columns = [1, 2]
df1 = df1.stack()
# %%
df1.name = 'population'
df1.index.names = ['loctype', 'location', 'time', 'age_group_1year', 'sex']
# %%
df1
# %%
serve_func('age_group_1year', 'sex', 'population', 'population_age1')(df1)
# %%
df2.columns = ['population']
df2.index.names = ['loctype', 'location', 'time', 'age_group_1year']

serve_func('age_group_1year', None, 'population', 'population_age1')(df2)

# %%
# 'WPP2024_PopulationBySingleAgeSex_Medium_Percentage_1950-2021.zip'
# 'WPP2024_PopulationBySingleAgeSex_Medium_Percentage_2022-2100.zip'
data1 = pd.read_csv(fullpath('WPP2024_PopulationBySingleAgeSex_Medium_Percentage_1950-2023.csv.gz'))
data2 = pd.read_csv(fullpath('WPP2024_PopulationBySingleAgeSex_Medium_Percentage_2024-2100.csv.gz'))

# %%
data = pd.concat([data1, data2], ignore_index=True)
data = data[~pd.isnull(data['LocTypeName'])]
data['AgeGrp'] = data['AgeGrp'].map(age_grp_concept)
data['LocTypeName'] = data['LocTypeName'].map(to_concept_id)
data['LocTypeName'] = data['LocTypeName'].replace('special_other', 'development_group')
# %%
print(data1.shape)
print(data2.shape)
print(data.shape)

# %%
df1 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopMale', 'PopFemale']]
df2 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopTotal']]
# %%
df1.columns = [1, 2]
df1 = df1.stack()
# %%
df1.name = 'population_percentage'
df1.index.names = ['loctype', 'location', 'time', 'age_group_1year', 'sex']
# %%
serve_func('age_group_1year', 'sex', 'population_percentage', 'population_age1')(df1)

# %%
df2.columns = ['population_percentage']
df2.index.names = ['loctype', 'location', 'time', 'age_group_1year']

serve_func('age_group_1year', None, 'population_percentage', 'population_age1')(df2)

# %%
# 'WPP2024_PopulationExposureByAge5GroupSex_Medium.zip'
data = pd.read_csv(fullpath('WPP2024_PopulationExposureByAge5GroupSex_Medium.csv.gz'))
# %%
data
# %%
data['AgeGrp'] = data['AgeGrp'].map(age_grp_concept)
data = data[~pd.isnull(data['LocTypeName'])]
data['LocTypeName'] = data['LocTypeName'].map(to_concept_id)
data['LocTypeName'] = data['LocTypeName'].replace('special_other', 'development_group')
# %%
df1 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopMale', 'PopFemale']]
df2 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopTotal']]
# %%
df1
# %%
df1.columns = [1, 2]
df1 = df1.stack()
# %%
df1.name = 'population_exposure'
df1.index.names = ['loctype', 'location', 'time', 'age_group_5year', 'sex']
# %%
serve_func('age_group_5year', 'sex', 'population_exposure', 'population_age5')(df1)
# %%
df2.columns = ['population_exposure']
df2.index.names = ['loctype', 'location', 'time', 'age_group_5year']

serve_func('age_group_5year', None, 'population_exposure', 'population_age5')(df2)

# %%
#  'WPP2024_PopulationExposureBySingleAgeSex_Medium_1950-2021.zip',
#  'WPP2024_PopulationExposureBySingleAgeSex_Medium_2022-2100.zip',
data1 = pd.read_csv(fullpath('WPP2024_PopulationExposureBySingleAgeSex_Medium_1950-2023.csv.gz'))
data2 = pd.read_csv(fullpath('WPP2024_PopulationExposureBySingleAgeSex_Medium_2024-2100.csv.gz'))
# %%
data = pd.concat([data1, data2], ignore_index=True)

# %%
data['AgeGrp'] = data['AgeGrp'].map(age_grp_concept)
data = data[~pd.isnull(data['LocTypeName'])]
data['LocTypeName'] = data['LocTypeName'].map(to_concept_id)
data['LocTypeName'] = data['LocTypeName'].replace('special_other', 'development_group')

# %%
print(data1.shape)
print(data2.shape)
print(data.shape)

# %%
df1 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopMale', 'PopFemale']]
df2 = data.set_index(['LocTypeName', 'LocID', 'Time', 'AgeGrp'])[['PopTotal']]
# %%
df1.columns = [1, 2]
df1 = df1.stack()
# %%
df1.name = 'population_exposure'
df1.index.names = ['loctype', 'location', 'time', 'age_group_1year', 'sex']
# %%
serve_func('age_group_1year', 'sex', 'population_exposure', 'population_age1')(df1)
# %%
df2.columns = ['population_exposure']
df2.index.names = ['loctype', 'location', 'time', 'age_group_1year']

serve_func('age_group_1year', None, 'population_exposure', 'population_age1')(df2)
# %%
# Done!
# %%
df1
# %%
