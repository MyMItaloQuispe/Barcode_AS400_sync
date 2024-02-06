import smtplib
import warnings
import dotenv
import os

from email.mime.text import MIMEText

# Ocultar mensaje de advertencias
warnings.filterwarnings("ignore", category=Warning)

# Cargar variables de entorno desde un archivo .env
dotenv.load_dotenv()

def enviar_correo_error(mensaje):

    # Set up the SMTP server
    server = smtplib.SMTP('smtp-mail.outlook.com', 587)
    server.starttls()
    user_mail = os.getenv("USER_MAIL")
    pwd_mail = os.getenv("PWD_MAIL")
    server.login(user_mail, pwd_mail)
    

    # Configura el mensaje
    subject = "Error Job Sincronización AS/400 - Barcode"
    body = mensaje

    # Create the message
    msg = MIMEText('Se produjo un error en el job automatico, verificar que el proceso de la tarea este correcto.'
                   +'\n\nSe ha producido el siguiente error: \n\n'
                   +body                   
                   +'\n\nEste correo es automatico, no responder.')
    msg['Subject'] = subject
    msg['From'] = 'iquispe@mym.com.pe'
    msg['To'] = 'iquispe@mym.com.pe'
    
    # Send the message
    server.sendmail(user_mail, user_mail, msg.as_string())
    print("correo enviado")

    # Close the connection
    server.quit()
