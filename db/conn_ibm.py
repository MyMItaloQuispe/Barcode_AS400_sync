import os
import pyodbc
import dotenv

# Cargar variables de entorno desde el archivo .env
dotenv.load_dotenv()

def imb_conx():
    try:
        # Configuración de la conexión
        connection_ibm = pyodbc.connect(
            driver=os.getenv("DRIVER_IBM"),
            system=os.getenv("SYSTEM_IBM"),
            uid=os.getenv("USER_IBM"),
            pwd=os.getenv("PASS_IBM")
        )

        cursor_ibm = connection_ibm.cursor()

        # Retornar la conexión y el cursor
        return connection_ibm, cursor_ibm

    except pyodbc.Error as e:
        # Lanzar la excepción nuevamente para que sea capturada en el otro script
        raise e

    except pyodbc.OperationalError as e:
        # Lanzar la excepción nuevamente para que sea capturada en el otro script
        raise e

    except Exception as e:
        # Lanzar la excepción nuevamente para que sea capturada en el otro script
        raise e

