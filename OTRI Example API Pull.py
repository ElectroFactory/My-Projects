import requests as r
import pandas as pd
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta


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


index_code = "OTRI"
days_back = 365  #yr long hist

# kmas
all_kmas = [
     "MSY", "BIL", "MSO", "SHV", "FWA", "HTS", "GEG", "BWI", "ROC", "SYR",
    "CVG", "CLE", "CMH", "TOL", "MFR", "PDX", "ABE", "AUS", "DAL", "FTW",
    "TXK", "SLC", "ORF", "WGO", "SEA", "DEN", "BDL", "JAX", "THL", "RDU",
    "TYS", "CHA", "GSP", "AUG", "DTW", "GRR", "MBS", "RAP", "FSD", "FAT",
    "MCI", "FAR", "BIS", "LBF", "PDT", "DBQ", "JOT", "MEM", "BNA", "CGI",
    "BHM", "DCU", "MOB", "BOS", "CID", "EAU", "MSN", "BMI", "MLI", "RFD",
    "EVV", "STC", "JFK", "BUF", "MGM", "DLH", "MSP", "STL", "ERI", "MDT",
    "GRB", "TUL", "PHL", "GJT", "CAE", "MKE", "CRW", "ALB", "XNA", "LIT",
    "TUS", "AMA", "JEF", "JLN", "SAT", "LAX", "ROA", "LAL", "OKC", "HOU",
    "SCK", "SFO", "DSM", "EWR", "IND", "SBN", "HUF", "BWG", "LEX", "SDF",
    "GVW", "JAN", "CLT", "ILM", "OMA", "BNH", "LAS", "RNO", "ELM", "RIC",
    "MCN", "SAV", "TMA", "DCA", "GYY", "SPR", "PHX", "TWF", "CHI", "HUT",
    "UIN", "ONT", "TAZ", "SAN", "GSO", "ATL", "MIA", "ABQ", "MFE", "PIT",
    "CHS", "LRD", "FLG", "LBB", "ELP"
]


def get_token(creds):
    url = "https://api.freightwaves.com/Credential/authenticate"
    response = r.post(url=url, json=creds)
    print("Auth Status:", response.status_code)
    response.raise_for_status()
    token = response.json()['token']
    return token

def parse_json_to_df(json_text):
    data = json.loads(json_text)
    df = pd.json_normalize(data)
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
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
        print(f"✅ Table '{sf_table}' checked/created.")

def upload_to_snowflake(conn, df, sf_table):
    df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
    print("Raw Columns:", df.columns.tolist())

    df['DATA_TIMESTAMP'] = pd.to_datetime(df['DATA_TIMESTAMP'], errors='coerce')

    if df['DATA_TIMESTAMP'].isna().any():
        print("⚠️ Invalid date(s) found:")
        print(df[df['DATA_TIMESTAMP'].isna()])
    df = df.dropna(subset=['DATA_TIMESTAMP'])

    
    sf_df = pd.DataFrame({
        'DATA_TIMESTAMP': df['DATA_TIMESTAMP'].dt.strftime('%Y-%m-%d %H:%M:%S'),
        'DATA_VALUE': df['DATA_VALUE'],
        'INDEX': df['INDEX_DEFINITION.INDEX_NAME'],
        'QUALIFIER': df['QUALIFIER_ITEM.QUALIFIER'],
        'QUALIFIER_CODE': df['QUALIFIER_LEVEL.QUALIFIER_CODE']
    })

    success, nchunks, nrows, _ = write_pandas(conn, sf_df, sf_table)
    if success:
        print(f"✅ Uploaded {nrows} rows to '{sf_table}'.")
    else:
        print("❌ Upload failed.")


def main():
    token = get_token(api_creds)

    combined_df = pd.DataFrame()
#loop
    for delta in range(days_back):
        day = datetime.today() - timedelta(days=delta)
        day_str = day.strftime('%Y-%m-%d')
        print(f"Fetching data for date: {day_str}")

        for kma in all_kmas:
            try:
                url = f"https://api.freightwaves.com/freight/ticker/{index_code}/{kma}/{day_str}/{day_str}"
                headers = {"Authorization": f"Bearer {token}"}
                response = r.get(url, headers=headers)
                print(f"Data Fetch Status for {kma} on {day_str}: {response.status_code}")
                response.raise_for_status()
                df = parse_json_to_df(response.text)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                print(f"⚠️ Error fetching/parsing data for {kma} on {day_str}: {e}")

    conn = connect_snowflake()
    create_table_if_not_exists(conn, sf_table)
    if not combined_df.empty:
        upload_to_snowflake(conn, combined_df, sf_table)
    else:
        print("No data fetched to upload.")
    conn.close()

if __name__ == "__main__":
    main()
