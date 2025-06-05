import requests as r
import pandas as pd
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

api_creds = {
    "username": "my info",
    "password": "my info"
}

sf_account = "my info"
sf_user = "my info"
sf_password = "my info"
sf_warehouse = "my info"
sf_database = "my info"
sf_schema = "my info"
sf_table = "my info"

def get_token(creds):
    url = "https://api.freightwaves.com/Credential/authenticate"
    response = r.post(url=url, json=creds)
    print(response.status_code)
    print(response.text)
    response.raise_for_status()
    token = response.json()['token']
    return token


def market_data(token):
    endpoint = "my info" 
    headers = {"Authorization": "Bearer " + token}
    
    response = r.get(url=endpoint, headers=headers)
    response.raise_for_status()
    return response.text


def parse_json_to_df(json_text):
    json_obj = json.loads(json_text)
    df = pd.json_normalize(json_obj)
    return df


def connect_snowflake():
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    return conn

def create_table_if_not_exists(conn, sf_table):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {sf_table} (
        DATA_TIMESTAMP TIMESTAMP,
        DATA_VALUE FLOAT,
        INDEX VARCHAR,
        QUALIFIER VARCHAR,
        QUALIFIER_CODE VARCHAR
    );
    """
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_sql)
        print(f"Table '{sf_table}' checked/created successfully.")
    finally:
        cursor.close()


def upload_to_snowflake(conn, df, sf_table):
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
    print("Normalized columns:", df.columns.tolist())

    if 'DATA_TIMESTAMP' in df.columns:
        df['DATA_TIMESTAMP'] = pd.to_datetime(df['DATA_TIMESTAMP'])
        df['DATA_TIMESTAMP'] = df['DATA_TIMESTAMP'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
    success, nchunks, nrows, _ = write_pandas(conn, df, sf_table)
    if success:
        print(f"Uploaded {nrows} rows to {sf_table}.")
    else:
        print("Upload failed.")

def main():
    token = get_token(api_creds)
    json_data = market_data(token)
    df = parse_json_to_df(json_data)
    
    conn = connect_snowflake()
    create_table_if_not_exists(conn, sf_table)
    upload_to_snowflake(conn, df, sf_table)
    conn.close()

if __name__ == "__main__":
    main()
