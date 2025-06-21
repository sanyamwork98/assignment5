import pandas as pd
from sqlalchemy import create_engine


source_engine = create_engine("mysql+pymysql://root:root123@localhost:3306/my_test_db")


target_engine = create_engine("mysql+pymysql://root:root123@localhost:3306/target_db")

try:
    
    df = pd.read_sql("SELECT * FROM `customers-100`", source_engine)
    print(" Data read from source database.")

   
    df.to_sql("customers_100", target_engine, index=False, if_exists="replace")
    print(" Data written to target database as `customers_100`.")

except Exception as e:
    print(" Error occurred:", e)
