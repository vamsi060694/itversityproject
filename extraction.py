import os
from dotenv import load_dotenv
import camelot
import pandas as pd
import numpy as np
import json
from pp.utils.logging_init import init_logger
load_dotenv()

logger = init_logger()
folder_path = os.getenv('BASE_DATA_PATH')


def pre_cleaning(new_tables):
    if len(new_tables.columns) == 7:
        try:
            new_tables.set_index('Total').filter(like='Yearly', axis=0)
            new_tables = new_tables.drop('Total', axis=1)
        except Exception as e:
            logger.error(e)
    else:
        try:
            new_tables = new_tables[~new_tables['Month'].str.contains("Yearly")]
        except Exception as e:
            logger.error(e)
    return new_tables


def final_cleaning(sample_df):
    sample_df = sample_df.replace('', np.NaN)
    try:
        for columns in ['Well Name', 'Year', 'Month']:
            sample_df[columns] = sample_df[columns].fillna(method='ffill')
            dropped_null = sample_df.dropna()
        return dropped_null
    except Exception as e:
        logger.error(e)


def extraction(folder_path):
    final_data = pd.DataFrame()
    for path, dirs, files in os.walk(folder_path):
        for file in files:
            logger.info(f"extracting {file}")
            filename = os.path.join(path, file)
            tables = camelot.read_pdf(filename, pages='all', flavor='stream', edge_tol=1000)
            table_number = tables.n
            all_tables = pd.DataFrame()
            for table in range(table_number):
                temp_df = tables[table].df
                column_len = len(temp_df.columns)
                if column_len == 6:
                    try:
                        temp_df.columns = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        temp_df['Month'] = temp_df['Month'].replace('', np.NaN, regex=True)
                        temp_df_null = temp_df[~temp_df['Month'].str.contains("Yearly", na=False)]
                        temp_df_up = temp_df_null.drop(['Month'], axis=1)
                        temp_df_up.columns = ['Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        if len(temp_df_up.columns) == 5:
                            temp_df_up['Well Name'] = np.NaN
                            column_names = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                            temp_df_null = temp_df_up.reindex(columns=column_names)
                        all_tables = pd.concat([all_tables, temp_df_null])
                        all_tables = all_tables[~all_tables['Well Name'].astype(str).str.contains('Well Name')]
                    except Exception as e:
                        logger.error(e)
                elif column_len == 5:
                    try:
                        temp_df['Well Name'] = np.NaN
                        temp_df.columns = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        temp_df = temp_df[['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']]
                        all_tables = pd.concat([all_tables, temp_df])
                        all_tables = all_tables[~all_tables['Well Name'].astype(str).str.contains('Well Name')]
                    except Exception as e:
                        logger.error(e)
                else:
                    try:
                        temp_df.columns = ['Well Name', 'Year', 'Month', 'Total', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        all_tables = pd.concat([all_tables, temp_df])
                        all_tables = all_tables[~all_tables['Well Name'].str.contains('Well Name')]
                    except Exception as e:
                        logger.error(e)
            pre_cleaning_data = pre_cleaning(all_tables)
            final_cleaning_data = final_cleaning(pre_cleaning_data)
            final_cleaning_data = final_cleaning_data[~final_cleaning_data['Month'].str.contains("Yearly", na=False)]
            final_cleaning_data['Month'] = pd.to_datetime(final_cleaning_data[['Month', 'Year']].assign(DAY=1))
            final_cleaning_data = final_cleaning_data.drop(columns='Year', axis=1)
            cleaning_data = final_cleaning_data
            final_data = pd.concat([final_data, cleaning_data])
            print(final_data)
            final_data.to_csv(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\new_file.csv')
    return final_data


extraction(folder_path)