import logging
import math
import db_connect as db


def compute_commission_from_csv(df, deals_df):
    logging.info('Computing commissions dynamically from deals.csv...')
    deals_df = deals_df[['marketing_source', 'comments']].set_index('marketing_source')
    
    for index, row in df.iterrows():
        marketing_source = row['marketing_source']
        formula = deals_df.loc[marketing_source, 'comments'].removeprefix("total commision=") if marketing_source in deals_df.index else 'raw_earnings'

        try:
            raw_earnings = 0 if math.isnan(row['raw_earnings']) else row['raw_earnings'] 
            clicks = 0 if math.isnan(row['clicks']) else row['clicks']
            df.at[index, 'total_commission'] = eval(formula, {}, {'raw_earnings': raw_earnings, 'clicks': clicks})
            if df.at[index, 'total_commission'] < 0 :
                df.at[index, 'total_commision'] = 0
            logging.info(f"Marketing_source: {marketing_source} Formula: {formula} raw_earnings: {raw_earnings} clicks: {clicks} answer: {df.at[index, 'total_commission']}")
        except Exception as e:
            logging.error(f"Error computing commission for {marketing_source}: {e}")
            df.at[index, 'total_commission'] = 0

    return df    

def compute_and_save_marketing(conn, final_df):
    logging.info("Start Computation of Marketing Source Daily Commission")
    if 'date' in final_df.columns:
        final_df['date_x'].fillna(final_df['date'], inplace=True)
    final_df['operator'].fillna('Unknown', inplace=True)
    daily_commission = final_df.groupby(['date_x', 'marketing_source'])['total_commission'].sum().reset_index()
    logging.info("Computation of Marketing Source Daily Commission is done")

    db.save_to_marketing_source(conn, daily_commission)

def compute_and_save_operator(conn, final_df) :
    logging.info("Start Computation of Operator Daily Commission")
    if 'date' in final_df.columns:
        final_df['date_x'].fillna(final_df['date'], inplace=True)
    final_df['operator'].fillna('Unknown', inplace=True)
    daily_commission = final_df.groupby(['date_x', 'operator'])['total_commission'].sum().reset_index()
    logging.info("Computation of Operator Daily Commission is done")

    db.save_to_operator(conn, daily_commission)

def compute_and_save_monthly_operator(conn, result, month):
    consolidated_data = {}
    logging.info("Start Computation of Operator Monthly Commission")
    for operator, commission in result:
        if operator in consolidated_data:
            consolidated_data[operator] += commission
        else:
            consolidated_data[operator] = commission

    logging.info("Computation of Operator Monthly Commission is done")

    db.save_to_operator_monthly(conn, consolidated_data, month)