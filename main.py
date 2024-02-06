# Importar bibliotecas necesarias
import pandas as pd
import dotenv

from functions import correo
from db.conn_ibm import imb_conx
from db.conn_pg import pg_conx

from datetime import datetime

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import NoSuchTableError


try:
    # Registrar la hora de inicio para medir el rendimiento
    start = datetime.now()
    print(start)

    # Cargar variables de entorno desde un archivo .env
    dotenv.load_dotenv()

    ### SCRIPT PARA POSTGRES
    # Creamos variables de coneccion al PostgreSQL
    connection_pg, cursor_pg = pg_conx()

    # Obtener datos de la tabla 'repuestos_final' en la base de datos PostgreSQL
    tb_repuestos_f = "select sku_repuesto sku from public.repuestos_final"
    cursor_pg.execute(tb_repuestos_f)
    rows = cursor_pg.fetchall()

    # Crear un DataFrame a partir de los datos obtenidos
    df_repuestos_f = pd.DataFrame([(row[0],) for row in rows], columns=['sku'])

    ### SCRIPT PARA IBM
    # Creamos variables de coneccion al IBM
    connection_ibm, cursor_ibm = imb_conx()

    # Obtener datos de las tablas 'MMETREL0' y 'MMACREL0' en la base de datos IBM y realizar el procesamiento necesario
    tb_as_sync = "SELECT DISTINCT ETCODLIN, ETCODORI, ETCODMAR, ETCODART, ETCODFAB, ACDSCLAR FROM LIBPRDDAT.MMETREL0 m LEFT JOIN LIBPRDDAT.MMACREL0 m2 ON m.ETCODLIN = m2.ACCODLIN AND m.ETCODART = m2.ACCODART"
    cursor_ibm.execute(tb_as_sync)
    rows = cursor_ibm.fetchall()

    # Definir los nombres de las columnas para el DataFrame principal
    columns_master = ['ETCODLIN', 'ETCODORI', 'ETCODMAR', 'ETCODART', 'ETCODFAB', 'ACDSCLAR']
    df_master = pd.DataFrame([(row.ETCODLIN, row.ETCODORI, row.ETCODMAR, row.ETCODART, row.ETCODFAB, row.ACDSCLAR) for row in rows], columns=columns_master)

    # Cerrar la conexión a la base de datos IBM
    connection_ibm.close()

    # Crear un DataFrame a partir de los datos obtenidos y realizar limpieza de datos
    df_master[columns_master] = df_master[columns_master].apply(lambda x: x.str.strip())
    df_master.loc[df_master['ETCODFAB'].str.len() == 0, 'ETCODFAB'] = ' '
    df_master.loc[df_master['ACDSCLAR'].str.len() == 0, 'ACDSCLAR'] = ' '
    df_master['sku'] = df_master.apply(lambda row: f"{row['ETCODLIN']}:{row['ETCODORI']}:{row['ETCODMAR']}:{row['ETCODART']}", axis=1)
    df_master = df_master.drop_duplicates()

    # Combinar el DataFrame principal con el DataFrame de 'repuestos_final' según la columna 'sku'
    df_merge = pd.merge(df_master, df_repuestos_f, on='sku', how='left', indicator=True)

    # Crear un DataFrame que contenga solo las filas no coincidentes (izquierda solamente)
    result_df = df_merge[df_merge['_merge'] == 'left_only']
    result_df = result_df.drop(['_merge'], axis=1)

    # Reorganizar columnas y renombrarlas para el DataFrame de resultados final
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

    # Agregar las columnas 'created_at' y 'updated_at' con la marca de tiempo actual
    result_df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    result_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Guardar el DataFrame de resultados combinados en un archivo CSV
    result_df.to_csv('D:\\py_virtual\\1_12_barcode_as400_sync\\merge.csv', index=False)

    ### SCRIPTS PARA SQLALCHEMY

    # Definir el nombre de la tabla objetivo en PostgreSQL
    tb_target = "tmp_python_repuestos"
    # tb_target = "repuestos_final"

    # Crear un motor de SQLAlchemy para la conexión a PostgreSQL
    engine = create_engine('postgresql+psycopg2://', creator=lambda: connection_pg, pool_size=10, max_overflow=20)
    metadata = MetaData()

    # Verificar si la tabla ya existe
    try:
        # Intentar cargar la tabla desde la base de datos
        existing_table = Table(tb_target, metadata, autoload_with=engine)
    except NoSuchTableError:
        # La tabla no existe, puedes manejar esto como prefieras
        raise Exception(f"La tabla {tb_target} no existe en la base de datos.")

    # Insertar datos en la tabla objetivo de PostgreSQL
    result_df.to_sql(tb_target, engine, if_exists='append', index=False)

    # Cerrar la conexión a la base de datos PostgreSQL
    connection_pg.close()

    # Registrar la hora de finalización para medir el rendimiento
    end = datetime.now()
    print(end)

except Exception as e:
    # Manejar el error y enviar notificación por correo
    mensaje_error = f"Error: {str(e)}"
    correo.enviar_correo_error(mensaje_error)
