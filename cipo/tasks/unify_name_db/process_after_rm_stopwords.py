import pandas as pd
from glob import glob
import os

FOLDER = os.path.abspath(os.path.join(__file__, os.pardir))


def merge():
    filelist = glob('/mnt/d/work/cipo/_applicant_remove_stop_word/*.csv')
    filelist.sort()
    df_full = pd.DataFrame()
    for file in filelist:
        print(file)
        df = pd.read_csv(file, index_col=0)
        df_full = pd.concat([df_full, df])
    df_full.to_csv('/mnt/d/work/cipo/applicant_remove_stopwords_2220928.csv')
    df_full = df_full.reset_index()
    return df_full

def remove_dup_clean_name_clean_address():
    df = pd.read_csv(f"{FOLDER}/output/2022-10-09/applicant_distinct_st13_max_proc_date_clean.csv")
    df['legalentityname_fix'] = df.legalentityname_clean
    df['addresslinetext1_fix'] = df.addresslinetext1
    df.st13applicationnumber = df.st13applicationnumber.astype(str)
    df.st13applicationnumber_relative = df.st13applicationnumber_relative.astype(str)

    df['st13applicationnumber_relative'] = df[['st13applicationnumber', 'st13applicationnumber_relative']].agg(', '.join, axis=1)
    df.st13applicationnumber_relative = df.st13applicationnumber_relative.str.replace(', nan', '')

    agg = {'st13applicationnumber_relative': ', '.join}
    for col in df.columns:
        if col not in ['st13applicationnumber_relative', 'legalentityname_fix', 'addresslinetext1_fix']:
            agg[col] = 'first'
    df_final = df.groupby(['legalentityname_fix', 'addresslinetext1_fix'], as_index = False).agg(agg)
    df_final = df_final.drop_duplicates()                    
    df_final.st13applicationnumber_relative = df_final.apply( lambda x: x.st13applicationnumber_relative.replace( x.st13applicationnumber, "" )  , axis=1)
    df_final.st13applicationnumber_relative = df_final.st13applicationnumber_relative.str[2:]
    cols = list(df_final.columns)
    cols.remove('st13applicationnumber_relative')
    cols.remove('proc_date')
    # cols.remove('legalentityname_fix')
    cols.remove('addresslinetext1_fix')
    df_final = df_final[['st13applicationnumber_relative', 'proc_date'] + cols]
    df_final = df_final.set_index('st13applicationnumber')
    df_final = df_final.sort_values(['legalentityname'])
    try:
        df_final = df_final.drop(columns=['Unnamed: 0'])
    except:
        pass
    df_final.to_csv(f'220928_applicant_strim_down_by_cleanname_cleanaddress_after_rm_stopwords.csv')
    return df_final

    
def remove_dup_clean_name_postalcode():
    df = remove_dup_clean_name_clean_address()
    df = df.reset_index()
    df.st13applicationnumber = df.st13applicationnumber.astype(str)
    df.st13applicationnumber_relative = df.st13applicationnumber_relative.astype(str)
    
    df.addresslinetext1 = df.addresslinetext1.astype(str)
    df.addresslinetext1_relative = df.addresslinetext1_relative.astype(str)

    df['st13applicationnumber_relative'] = df[['st13applicationnumber', 'st13applicationnumber_relative']].agg(', '.join, axis=1)
    df.st13applicationnumber_relative = df.st13applicationnumber_relative.str.replace(', nan', '')

    df['addresslinetext1_relative'] = df[['addresslinetext1', 'addresslinetext1_relative']].agg(', '.join, axis=1)
    df.addresslinetext1_relative = df.addresslinetext1_relative.str.replace(', nan', '')

    agg = {'st13applicationnumber_relative': ', '.join,
    'addresslinetext1_relative': ', '.join,
    }
    for col in df.columns:
        if col not in ['st13applicationnumber_relative', 'legalentityname_fix', 'postalcode', 'addresslinetext1_relative']:
            agg[col] = 'first'
    df_final = df.groupby(['legalentityname_fix', 'postalcode'], as_index = False).agg(agg)
    df_final = df_final.drop_duplicates()                    
    df_final.st13applicationnumber_relative = df_final.apply(lambda x: x.st13applicationnumber_relative.replace(x.st13applicationnumber, "" )  , axis=1)
    df_final.st13applicationnumber_relative = df_final.st13applicationnumber_relative.str[2:]

    df_final.addresslinetext1_relative = df_final.apply(lambda x: x.addresslinetext1_relative.replace(x.addresslinetext1, "" )  , axis=1)
    df_final.addresslinetext1_relative = df_final.addresslinetext1_relative.str[2:]

    cols = list(df_final.columns)
    cols.remove('st13applicationnumber_relative')
    cols.remove('proc_date')
    cols.remove('legalentityname')
    cols.remove('addresslinetext1')
    cols.remove('addresslinetext1_relative')
    cols.remove('legalentityname_fix')
    df_final = df_final[['st13applicationnumber_relative', 'proc_date', 'legalentityname', 'addresslinetext1', 'addresslinetext1_relative'] + cols]
    df_final = df_final.set_index('st13applicationnumber')
    df_final = df_final.sort_values(['legalentityname'])
    df_final.to_csv(f'220928_applicant_strim_down_by_cleanname_address1_postalcode_after_rm_stopwords.csv')
    return df_final

def split():
    df = pd.read_csv(f'/mnt/d/work/cipo/20220930/application_distinct_st13_max_proc_date_clean.csv')
    # # df = df.drop(columns=['Unnamed: 0'])
    # df = df.set_index('st13applicationnumber')
    # df = remove_dup_clean_name_postalcode()
    i = 1
    while len(df) > 0:
        print(i)
        df[:950000].to_csv(f'/mnt/d/work/cipo/20220930/application_distinct_st13_max_proc_date_clean_{i}.csv')
        df = df[950000:]
        i += 1

# merge()      
# remove_dup_clean_name_clean_address()
# remove_dup_clean_name_postalcode()
split()