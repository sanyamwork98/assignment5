from sqlalchemy import create_engine
import pandas as pd


user = "root"
password = "root123"
host = "localhost"
port = "3306"
database = "my_test_db"

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


df = pd.read_sql("SHOW TABLES", engine)
print(df)
