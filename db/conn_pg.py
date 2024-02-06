import os
import psycopg2
import dotenv

dotenv.load_dotenv()

def pg_conx():
    try:
        # Configuración de la conexión PostgreSQL
        connection_pg = psycopg2.connect(
            host=os.getenv("HOST_PG"),
            port=os.getenv("PORT_PG"),
            database=os.getenv("DB_PG"),
            user=os.getenv("USER_PG"),
            password=os.getenv("PWD_PG")
        )

        cursor_pg = connection_pg.cursor()

        # Retornar la conexión y el cursor
        return connection_pg, cursor_pg

    except psycopg2.OperationalError as e:
        # Excepción específica para errores operativos en la conexión a PostgreSQL (p. ej., usuario o contraseña incorrectos)
        raise Exception(f"Error de conexión a PostgreSQL: {str(e)}")

    except psycopg2.Error as e:
        # Otras excepciones específicas de psycopg2
        raise Exception(f"Error de psycopg2: {str(e)}")

    except Exception as e:
        # Cualquier otra excepción
        raise Exception(f"Error desconocido: {str(e)}")