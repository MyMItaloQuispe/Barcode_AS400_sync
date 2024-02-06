import pandas as pd
import dotenv

dotenv.load_dotenv()

from db.conn_ibm import cursor_ibm, connection_ibm
from db.conn_pg import cursor_pg, connection_pg
from datetime import datetime

from sqlalchemy import create_engine, Table, Column, VARCHAR, MetaData, DateTime

start = datetime.now()
print(start)

### POSTGRES SCRIPT

tb_repuestos_f = "select sku_repuesto sku from public.repuestos_final"
cursor_pg.execute(tb_repuestos_f)
rows = cursor_pg.fetchall()

df_repuestos_f = pd.DataFrame([(row[0],) for row in rows], columns=['sku'])

### IBM SCRIPT

tb_as_sync = "SELECT DISTINCT as2.SYTABLA, as2.SYCADSQL, as2.SYFECHAC FROM LIBPRDDAT.AS_SYNC as2 WHERE as2.SYTABLA = 'MMETREP' AND as2.SYTPOPER = 'Insert' AND as2.SYFECHAC >= 20240101"
cursor_ibm.execute(tb_as_sync)
rows = cursor_ibm.fetchall()

df_as_sync = pd.DataFrame([(row.SYTABLA, row.SYCADSQL, row.SYFECHAC) for row in rows], columns=['SYTABLA', 'SYCADSQL', 'SYFECHAC'])

df_as_sync['SYCADSQL'] = df_as_sync['SYCADSQL'].str.replace(';', '').str.strip()

df_as_sync['pos_etcodcia'] = df_as_sync['SYCADSQL'].str.find('ETCODCIA')
df_as_sync['pos_etcodlin'] = df_as_sync['SYCADSQL'].str.find('ETCODLIN')

for index, row in df_as_sync.iterrows():
    pos_etcodcia = row['pos_etcodcia']
    pos_etcodlin = row['pos_etcodlin']

    if pos_etcodcia != -1 and pos_etcodlin != -1:
        df_as_sync.at[index, 'SYCADSQL'] = row['SYCADSQL'][:pos_etcodcia] + row['SYCADSQL'][pos_etcodlin:]

df_as_sync['SYCADSQL'] = df_as_sync['SYCADSQL'].str.replace('SELECT * FROM ', 'SELECT ETCODLIN, ETCODORI, ETCODMAR, ETCODART, ETCODFAB, ACDSCLAR FROM ').str.replace('LIBPRDDAT.MMETREP ', 'LIBPRDDAT.MMETREP a INNER JOIN LIBPRDDAT.MMACREL0 b ON a.ETCODLIN = b.ACCODLIN AND a.ETCODART = b.ACCODART ')

df_as_sync = df_as_sync.drop_duplicates()

df_resultados = pd.DataFrame(columns=['ETCODLIN', 'ETCODORI', 'ETCODMAR', 'ETCODART', 'ETCODFAB', 'ACDSCLAR'])

for index, row in df_as_sync.iterrows():
    cursor_ibm.execute(row['SYCADSQL'])
    
    resultados = cursor_ibm.fetchall()
    for resultado in resultados:
        etcodlin = resultado[0]
        etcodori = resultado[1]
        etcodmar = resultado[2]
        etcodart = resultado[3]
        etcodfab = resultado[4]
        acdsclar = resultado[5]
        
        df_temp = pd.DataFrame({
            'ETCODLIN': [etcodlin],
            'ETCODORI': [etcodori],
            'ETCODMAR': [etcodmar],
            'ETCODART': [etcodart],
            'ETCODFAB': [etcodfab],
            'ACDSCLAR': [acdsclar]
        })
        df_resultados = pd.concat([df_resultados, df_temp], ignore_index=True)

connection_ibm.close()

columns_to_strip = ['ETCODART', 'ETCODFAB', 'ACDSCLAR']
df_resultados[columns_to_strip] = df_resultados[columns_to_strip].apply(lambda x: x.str.strip())
df_resultados = df_resultados.drop_duplicates()
df_resultados.loc[df_resultados['ETCODFAB'].str.len() == 0, 'ETCODFAB'] = ' '
df_resultados['sku'] = df_resultados.apply(lambda row: f"{row['ETCODLIN']}:{row['ETCODORI']}:{row['ETCODMAR']}:{row['ETCODART']}", axis=1)

df_merge = pd.merge(df_resultados, df_repuestos_f, on='sku', how='left', indicator=True)
result_df = df_merge[df_merge['_merge'] == 'left_only']
result_df = result_df.drop(['_merge'], axis=1)
col = result_df.pop('sku')
result_df.insert(0, 'sku', col)
new_column_names = {
    'sku': 'sku_repuesto',
    'ETCODLIN': 'cod_linea',
    'ETCODORI': 'cod_origen',
    'ETCODMAR': 'cod_marca',
    'ETCODART': 'cod_articulo',
    'ETCODFAB': 'cod_fabricacion',
    'ACDSCLAR': 'desc_articulo'
}

result_df.rename(columns=new_column_names, inplace=True)
result_df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
result_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
result_df.to_csv('merge.csv', index=False)

### SQLALCHEMY SCRIPTs

tb_target = "tmp_python_repuestos"
# tb_target = "repuestos_final"

engine = create_engine('postgresql+psycopg2://', creator=lambda: connection_pg, pool_size=10, max_overflow=20)
metadata = MetaData()
table = Table(tb_target, metadata,
              Column('sku_repuesto', VARCHAR(150)),
              Column('cod_linea', VARCHAR(50)),
              Column('cod_origen', VARCHAR(50)),
              Column('cod_marca', VARCHAR(50)),
              Column('cod_articulo', VARCHAR(50)),
              Column('cod_fabricacion', VARCHAR(150)),
              Column('desc_articulo', VARCHAR(500)),
              Column('created_at', DateTime),
              Column('updated_at', DateTime),
              )

result_df.to_sql(tb_target, engine, if_exists='append', index=False)

connection_pg.close()

end = datetime.now()
print(end)