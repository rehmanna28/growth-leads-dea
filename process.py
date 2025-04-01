import pandas as pd
import logging
import db_connect as db
import compute

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_file(file):
    logging.info('Loading data...')
    df = pd.DataFrame()
    try:
        df = pd.read_csv(file, encoding='utf-8')
    except Exception as e:
        logging.error(f"Error loading files: {e}")
    return df

def load_data(voluum_file, voluum_mapper, routy_file, manual_file, deals_file):
    
    for df in [voluum_file, voluum_mapper, routy_file, manual_file, deals_file]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.lower()

    if not voluum_file.empty and not voluum_mapper.empty:
        voluum_file['voluum_brand'] = voluum_file['voluum_brand'].str.strip().str.casefold()
        voluum_mapper['voluum_brand'] = voluum_mapper['voluum_brand'].str.strip().str.casefold()

    return voluum_file, voluum_mapper, routy_file, manual_file, deals_file

def clean_data(df):
    if df is None:
        return pd.DataFrame()
    logging.info('Cleaning data...')
    df = df.drop_duplicates()
    df = df.fillna({'countrycode': 'Unknown', 'raw_earnings': 0, 'visits': 0, 'signups': 0, 'clicks': 0})

    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    numeric_cols = df.select_dtypes(include=['object']).columns
    for col in numeric_cols:
        try:
            df[col] = pd.to_numeric(df[col], errors='ignore')
        except Exception as e:
            logging.warning(f"Error converting {col} to numeric: {e}")

    return df

def apply_mapping(data_df, mapper_df):
    if data_df.empty or mapper_df.empty: 
        return data_df
    
    logging.info('Applying mappings...')
    mapping_dict = pd.Series(mapper_df['marketing_source'].values, index=mapper_df['voluum_brand']).to_dict()
    data_df['marketing_source'] = data_df['voluum_brand'].map(mapping_dict)
    data_df['marketing_source'].fillna('Unknown', inplace=True)
    data_df['marketing_source'] = data_df['marketing_source'].astype('category')
    return data_df

def merge_data(data_df, routy_df):
    if data_df.empty:
        return data_df

    logging.info('Merging data...')
    merged_df = pd.merge(data_df, routy_df, on='marketing_source', how='left')
    return merged_df

def append_and_save(merged_df, manual_df, deals_df):
    logging.info('Appending manual data...')
    final_df = pd.concat([merged_df, manual_df], ignore_index=True)
    final_df = compute.compute_commission_from_csv(final_df, deals_df)

    if not final_df.empty:
        connection = db.connect_to_postgres()
        if connection :
            compute.compute_and_save_marketing(connection, final_df)
            compute.compute_and_save_operator(connection, final_df)

            if 'date' in final_df.columns:
                start = final_df['date'].min().to_period('M').start_time
                end = final_df['date'].min().to_period('M').end_time
                month_string = final_df['date'].min().strftime('%Y-%m')
            else:
                start = final_df['date_x'].min().to_period('M').start_time
                end = final_df['date_x'].min().to_period('M').end_time
                month_string = final_df['date_x'].min().strftime('%Y-%m')
                
            result = db.get_month(connection, start, end)
            compute.compute_and_save_monthly_operator(connection, result, month_string)
        connection.close()

def main():
    setup_logging()
    voluum_file = load_file('/Users/annamherrimando/Downloads/data_eng_assignment/data_1a/voluum/2024-03-26.csv')
    voluum_mapper = load_file('/Users/annamherrimando/Downloads/data_eng_assignment/data_1a/voluum_mapper.csv')
    routy_file = load_file('/Users/annamherrimando/Downloads/data_eng_assignment/data_1a/routy/2024-03-26.csv')
    manual_file = load_file('/Users/annamherrimando/Downloads/data_eng_assignment/data_1a/manual/2024-03-26.csv')
    deals_file = load_file('/Users/annamherrimando/Downloads/data_eng_assignment/data_1a/deals.csv')

    data_df, mapper_df, routy_df, manual_df, deals_df = load_data(voluum_file, voluum_mapper, routy_file, manual_file, deals_file)
    data_df = clean_data(data_df)
    manual_df = clean_data(manual_df)
    data_df = apply_mapping(data_df, mapper_df)
    merged_df = merge_data(data_df, routy_df)
    append_and_save(merged_df, manual_df, deals_df)

if __name__ == '__main__':
    main()
