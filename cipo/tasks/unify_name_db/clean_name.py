import pandas as pd
import os
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from datetime import datetime

OUTPUT = 'cipo/tasks/unify_name_db/output'
STOP_WORDS = stopwords.words('english')
NON_WORDS = """-,.;!?@#$%^&*()/"«»'"""

def process_address(df):
    df['legalentityname_clean'] = df.legalentityname.astype(str)
    df['addresslinetext1_clean'] = df.addresslinetext1.str.lower()
    address_cols = [f'addresslinetext{i}' for i in range(1, 7)]
    for col in address_cols:
        df[col] = df[col].astype(str)
    df = df[:1000]
    df['address'] = df[address_cols].agg(' - '.join, axis=1)
    for c in NON_WORDS:
        df.legalentityname_clean = df.legalentityname_clean.str.replace(c, ' ')
        df.addresslinetext1_clean = df.addresslinetext1_clean.str.replace(c, ' ')
    # df.legalentityname_clean = df.legalentityname_clean.str.replace('  ', ' ')
    # df.addresslinetext1_clean = df.addresslinetext1_clean.str.replace('  ', ' ')
    return df

def remove_1_word(df):
    df_1word = pd.read_csv('/mnt/d/work/python_dev/cipo/tasks/unify_name_db/data/count_phrases/count_1_word_dwan_pick.csv')
    df_1word = df_1word.dropna()
    stop_words = list(set(STOP_WORDS + df_1word.word.to_list()))
    stop_words += ['llp', 'kg', 'srl', 'mbh', 'brand']
    print(stop_words)

    df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join([word.lower() for word in word_tokenize(words) if word.lower() not in stop_words]))
    return df

def remove_sublist_in_list(sub_list, _list):
    for sub in sub_list:
        n = len(sub)
        i = 0
        while i <= len(_list):
            # print(_list)
            if _list[i:n+i] == sub:
                _list = _list[:i] + _list[n+i:]
            else:
                i += 1
    return _list

def remove_1_word_v2(df):
    df_1word = pd.read_csv('/mnt/d/work/python_dev/cipo/tasks/unify_name_db/data/count_phrases/count_1_word_dwan_pick.csv')
    df_1word = df_1word.dropna()
    stop_words = list(set(STOP_WORDS + df_1word.word.to_list()))
    stop_words += ['llp', 'kg', 'srl', 'mbh', 'brand']
    stop_words = [word.split(' ') for word in stop_words]
    print(stop_words)

    # df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join([word.lower() for word in word_tokenize(words) if word.lower() not in stop_words]))
    df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join(remove_sublist_in_list(stop_words, [word.lower() for word in word_tokenize(words)])))
    return df

def arrange_df_cols(df):
    cols = list(df.columns)
    cols.remove('entityname')
    for col in ['st13applicationnumber', 'address', 'proc_date', 'legalentityname', 'legalentityname_clean', 'addresslinetext1', 'addresslinetext1_clean']:
        cols.remove(col)
    df = df[['st13applicationnumber', 'proc_date', 'legalentityname', 'legalentityname_clean', 'addresslinetext1', 'addresslinetext1_clean', 'address'] + cols]
    return df

def run():
    df = pd.read_csv('/mnt/d/work/python_dev/cipo/tasks/unify_name_db/data/application_distinct_st13_max_proc_date.csv',
                        index_col=0)
    df = process_address(df)
    # df = remove_1_word(df)
    df = remove_1_word_v2(df)
    df = arrange_df_cols(df)
    output_folder = f"{OUTPUT}/{datetime.now().date()}"
    if not os.path.exists(output_folder):
            os.makedirs(output_folder)
    df.legalentityname_clean = df.legalentityname_clean.str.replace(' ', '')
    df.legalentityname_clean = df.legalentityname_clean.apply(lambda r: None if len(r)==0 else r)
    df.legalentityname_clean = df.legalentityname_clean.fillna(df.legalentityname)
    df.to_csv(f'{output_folder}/applicant_distinct_st13_max_proc_date_clean.csv')

def convert_csv_to_xlsx_v2(csvfile):
    df = pd.read_csv(csvfile)
    df = df.drop(columns=['num_row', 'Unnamed: 0'])
    df = df.sort_values('legalentityname')
    df = df.set_index('st13applicationnumber')
    df = df[:100]
    df.to_excel(csvfile[:-4] + '.xlsx')

if __name__ == '__main__':
    run()
    # print(remove_sublist_in_list([[3], [4, 5]], [1,2,3,4,5,3,4, 3,4,5]))
    output_folder = f"{OUTPUT}/{datetime.now().date()}"
    df = pd.read_csv(f'{output_folder}/applicant_distinct_st13_max_proc_date_clean.csv')
    df_ = pd.read_csv(f'{output_folder}/applicant_distinct_st13_max_proc_date_clean_.csv')
    print(df.legalentityname_clean.to_list() == df_.legalentityname_clean.to_list())