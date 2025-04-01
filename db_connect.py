
import psycopg2
import logging



def connect_to_postgres():
    try:
        # Update with your actual credentials
        connection = psycopg2.connect(
            dbname="GrowthLeads",
            user="$USER$",
            password="$PASSWORD$",
            host="localhost",  # Change if using a remote server
            port="5432"        # Default PostgreSQL port
        )
        logging.info("Connected to PostgreSQL successfully!")
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        return None
    
def save_to_marketing_source(conn, daily_commission):
    insert_query = """
    INSERT INTO marketing_commissions (date, marketing_source_id, total_commission)
    VALUES (%s, %s, %s)
    ON CONFLICT (date, marketing_source_id)
    DO UPDATE SET total_commission = EXCLUDED.total_commission;
    """
    cursor = conn.cursor()

    for _, row in daily_commission.iterrows():
        cursor.execute(insert_query, (row['date_x'], row['marketing_source'], row['total_commission']))
    
    conn.commit()
    logging.info("Data inserted successfully into marketing_commissions.")
    cursor.close()

def save_to_operator(conn, daily_commission):
    insert_query = """
    INSERT INTO operator_commissions (date, operator, total_commission)
    VALUES (%s, %s, %s)
    ON CONFLICT (date, operator)
    DO UPDATE SET total_commission = EXCLUDED.total_commission;
    """

    cursor = conn.cursor()

    for _, row in daily_commission.iterrows():
        cursor.execute(insert_query, (row['date_x'], row['operator'], row['total_commission']))
    
    conn.commit()
    logging.info("Data inserted successfully into operator_commissions.")
    cursor.close()

def save_to_operator_monthly(conn, consolidated_data, month):
    insert_query = """
                        INSERT INTO operator_monthly_commissions (operator, month, total_commission)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (operator, month)
                        DO UPDATE SET total_commission = EXCLUDED.total_commission;
                    """
    cursor = conn.cursor()
    for operator, total_commission in consolidated_data.items():
        cursor.execute(insert_query, (operator, month, total_commission))
    conn.commit()
    cursor.close()
    logging.info("Data successfully updated.")

def get_month(conn, start, end):
    select_query = """
    SELECT operator, total_commission
    FROM operator_commissions
    WHERE date >= %s
    AND date <= %s;   
    """

    cursor = conn.cursor()
    
    cursor.execute(select_query, (start, end))
    result = cursor.fetchall()
    cursor.close()
    logging.info("Query executed successfully")
    return result