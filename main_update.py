import pandas as pd

from db.conn_ibm import ibm_conx
from db.conn_pg import pg_conx

connection_pg, cursor_pg = pg_conx()
connection_ibm, cursor_ibm = ibm_conx()

tb_async = "SELECT * FROM LIBPRDDAT.AS_SYNC as2 WHERE as2.SYTPOPER = 'Update' AND as2.SYTABLA = 'MMETREP' ORDER BY  as2.SYFECHAC DESC FETCH FIRST 10 ROWS ONLY"
cursor_ibm.execute(tb_async)
rows = cursor_ibm.fetchall()

columns_assync = ['SYTABLA', 'SYCADSQL', 'SYTPOPER']
df_assync = pd.DataFrame([(row.SYTABLA.rstrip(), row.SYCADSQL.rstrip(), row.SYTPOPER.rstrip()) for row in rows], columns=columns_assync)

print(df_assync)