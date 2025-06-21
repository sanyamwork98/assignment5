import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
from sqlalchemy import create_engine


engine = create_engine("mysql+pymysql://root:root123@localhost:3306/my_test_db")

try:
    
    df = pd.read_sql("SELECT * FROM `customers-100`", engine)

    
    df.to_csv("customers-100.csv", index=False)
    print(" customers-100.csv exported.")

    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "customers-100.parquet")
    print(" customers-100.parquet exported.")

    
    df_reset = df.reset_index(drop=True)

    
    schema = {
        "type": "record",
        "name": "customers_100",  
        "fields": [{"name": col, "type": "string"} for col in df_reset.columns],
    }

    
    df_str = df_reset.astype(str)
    records = df_str.to_dict(orient="records")

    
    with open("customers-100.avro", "wb") as out_file:
        fastavro.writer(out_file, schema, records)
    print(" customers-100.avro exported.")

except Exception as e:
    print(" An error occurred:", e)
