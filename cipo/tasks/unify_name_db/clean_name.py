import pandas as pd
import os
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from datetime import datetime

FOLDER = os.path.abspath(os.path.join(__file__, os.pardir))
STOP_WORDS = stopwords.words('english')
NON_WORDS = """-,.;:!?@#$%^&*()/"«»'[]~`}{+=]\\"""

def process_address(df):
    df['legalentityname_clean'] = df.legalentityname.astype(str)
    df['addresslinetext1_clean'] = df.addresslinetext1.str.lower()
    address_cols = [f'addresslinetext{i}' for i in range(1, 7)]
    for col in address_cols:
        df[col] = df[col].astype(str)
    # df = df[:1000]
    df['address'] = df[address_cols].agg(' - '.join, axis=1)
    for c in NON_WORDS:
        df.legalentityname_clean = df.legalentityname_clean.str.replace(c, ' ')
        df.addresslinetext1_clean = df.addresslinetext1_clean.str.replace(c, ' ')
    df.legalentityname_clean = df.legalentityname_clean.str.replace('  ', ' ')
    df.addresslinetext1_clean = df.addresslinetext1_clean.str.replace('  ', ' ')
    return df

def remove_1_word(df):
    df_1word = pd.read_csv(f'{FOLDER}/data/count_phrases/count_1_word_dwan_pick.csv')
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
    df_1word = pd.read_csv(f'{FOLDER}/data/count_phrases/count_1_word_dwan_pick.csv')
    df_1word = df_1word.dropna()
    stop_words = list(set(STOP_WORDS + df_1word.word.to_list()))
    stop_words += ['llp', 'kg', 'srl', 'mbh', 'brand']
    stop_words = [word.strip().split(' ') for word in stop_words]
    print(stop_words)

    # df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join([word.lower() for word in word_tokenize(words) if word.lower() not in stop_words]))
    df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join(remove_sublist_in_list(stop_words, [word.lower() for word in word_tokenize(words)])))
    return df

def remove_2_word(df):
    df_2word = pd.read_csv(f'{FOLDER}/data/count_phrases/count_2_word_dwan_pick.csv')
    df_2word = df_2word.dropna()
    stop_words = list(set(df_2word['2_word_phrase'].to_list()))
    stop_words = [word.strip().replace('  ', ' ').split(' ') for word in stop_words]
    print(stop_words)

    # df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join([word.lower() for word in word_tokenize(words) if word.lower() not in stop_words]))
    df.legalentityname_clean = df.legalentityname_clean.apply(lambda words: " ".join(remove_sublist_in_list(stop_words, [word.lower() for word in word_tokenize(words)])))
    return df

def remove_3_word(df):
    df_3word = pd.read_csv(f'{FOLDER}/data/count_phrases/count_3_word_dwan_pick.csv')
    df_3word = df_3word.dropna()
    stop_words = list(set(df_3word['3_word_phrase'].to_list()))
    stop_words = [word.strip().replace('  ', ' ').split(' ') for word in stop_words]
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
    df = pd.read_csv(f'{FOLDER}/data/application_distinct_st13_max_proc_date.csv', index_col=0)
    df = process_address(df)
    # df = remove_1_word(df)
    print('remove 3-word-phrases')
    df = remove_3_word(df)
    print('remove 2-word-phrases')
    df = remove_2_word(df)
    print('remove 1 word')
    df = remove_1_word_v2(df)
    df = arrange_df_cols(df)
    output_folder = f"{FOLDER}/output/{datetime.now().date()}"
    if not os.path.exists(output_folder):
            os.makedirs(output_folder)
    df.legalentityname_clean = df.legalentityname_clean.str.replace(' ', '')
    df.legalentityname_clean = df.legalentityname_clean.apply(lambda r: None if len(r)==0 else r)
    df.legalentityname_clean = df.legalentityname_clean.fillna(df.legalentityname)
    df.to_csv(f'{output_folder}/applicant_distinct_st13_max_proc_date_clean.csv')

def convert_csv_to_xlsx_v2(csvfile):
    df = pd.read_csv(csvfile)
    df = df.drop(columns=['num_row'])
    df = df.sort_values('legalentityname')
    df = df.set_index('st13applicationnumber')
    # df = df[:100]
    df[:950000].to_excel(csvfile[:-4] + '1_.xlsx')
    df[950000:].to_excel(csvfile[:-4] + '2_.xlsx')

if __name__ == '__main__':
    from datetime import datetime, timedelta
    print(datetime.now())
    run()
    convert_csv_to_xlsx_v2("/mnt/e/python_dev/cipo/tasks/unify_name_db/output/2022-10-09/applicant_distinct_st13_max_proc_date_clean.csv")
    # df_3word = pd.read_csv(f'{FOLDER}/data/count_phrases/count_3_word_dwan_pick.csv')
    # df_3word = df_3word.dropna()
    # stop_words = list(set(df_3word['3_word_phrase'].to_list()))
    # stop_words = [word.strip().replace('  ', ' ').split(' ') for word in stop_words]
    # for word in stop_words:
    #     if '' in word:
    #         print(word)
    # print(stop_words)
