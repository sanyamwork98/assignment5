import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
from sqlalchemy import create_engine


engine = create_engine("mysql+pymysql://root:root123@localhost:3306/my_test_db")

try:
    
    tables_df = pd.read_sql("SHOW TABLES", engine)
    all_tables = tables_df.iloc[:, 0].tolist()
    print(" Tables found:", all_tables)

    for table_name in all_tables:
        print(f"\n Exporting table: {table_name}")

        
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", engine)

        
        df.to_csv(f"{table_name}.csv", index=False)
        print(f" {table_name}.csv exported.")

        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f"{table_name}.parquet")
        print(f" {table_name}.parquet exported.")

        
        df_reset = df.reset_index(drop=True).astype(str)
        schema = {
            "type": "record",
            "name": table_name.replace("-", "_"),
            "fields": [{"name": col, "type": "string"} for col in df_reset.columns],
        }
        with open(f"{table_name}.avro", "wb") as out:
            fastavro.writer(out, schema, df_reset.to_dict(orient="records"))
        print(f" {table_name}.avro exported.")

except Exception as e:
    print(" Error occurred:", e)
