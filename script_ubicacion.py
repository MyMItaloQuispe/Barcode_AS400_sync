import dotenv

from db import imb_conx

from datetime import datetime

start = datetime.now()

dotenv.load_dotenv()

connection_ibm, cursor_ibm = imb_conx()

tb_as_sync = "SELECT TOP 10 ODCODCIA, ODCODSUC, ODCODLIN, ODCODART, ODCODORI, ODITEM01, ODCODALM, ODCODSEC, ODCODEST, ODCODNIV, ODCODPOS, ODCANINI, ODCANING, ODCANSAL, ODSTS, ODUSR, ODJOB, ODJDT, ODJTM FROM LIBPRDDAT.MMODREL0"
cursor_ibm.execute(tb_as_sync)
rows = cursor_ibm.fetchall()

for row in rows:
    print(row)

end = datetime.now ()
