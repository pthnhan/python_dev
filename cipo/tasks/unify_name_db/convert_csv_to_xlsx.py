# import os
# import glob
import pandas as pd
import csv
from xlsxwriter.workbook import Workbook

def convert_csv_to_xlsx(csvfile):
    # csvfile = '/mnt/d/work/cipo/applicant_strim_down_by_cleanname_postalcode.csv'
    workbook = Workbook(csvfile[:-4] + '.xlsx')
    worksheet = workbook.add_worksheet()
    with open(csvfile, 'rt', encoding='utf8') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                for c, col in enumerate(row):
                    print(r, row, c, col)
                    worksheet.write(r, c, col)

def convert_csv_to_xlsx_v2(csvfile):
    df = pd.read_csv(csvfile)
    df = df.drop(columns=['num_row', 'Unnamed: 0'])
    df = df.sort_values('legalentityname')
    df = df.set_index('st13applicationnumber')
    df = df[:100]
    df.to_excel(csvfile[:-4] + '.xlsx')

convert_csv_to_xlsx_v2('/mnt/d/work/cipo/20220930/application_distinct_st13_max_proc_date_clean_2.csv')