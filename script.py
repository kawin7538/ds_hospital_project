from database import get_conn
import pandas as pd

conn=get_conn()

print(pd.read_sql_query("select * from [dbo].[HIS_PROJECT_OPDDIAG]",conn).info())