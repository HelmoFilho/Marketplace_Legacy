#=== Importações de módulos externos ===#
from apscheduler.schedulers.background import BackgroundScheduler
from unidecode import unidecode
from flask_restful import Api
from flask_cors import CORS
from flask import Flask
import os

#=== Importações de módulos internos ===#
from app.config.config import DevelopmentConfig, ProductionConfig, TestingConfig
from app.shared.threads_info import ThreadDictGlobal
from log.log_script import LoggerManager

# Iniciando a aplicação flask
app     = Flask(__name__, template_folder = "templates", 
                            static_folder = "templates\\static")

# Configurando o logger principal
logger  = LoggerManager(level = 20)

# Ajustando o flask ao padrão designado
if (enviroment := os.environ.get("PSERVER_PS")):

    choice = unidecode(enviroment.lower())

    # Servidor de Teste
    if choice in ["testing", "debug"]:
        app.config.from_object(TestingConfig)

    # Servidor de desenvolvimento
    elif choice == "development":
        app.config.from_object(DevelopmentConfig)

    # Servidor de produção
    elif choice == "production":
        app.config.from_object(ProductionConfig)

    else:
        logger.critical("Variavel PSERVER recebeu um valor invalido. Servidor nao inicializado.")
        raise KeyError("Variavel PSERVER recebeu um valor invalido")
        
else:
    logger.critical("Variavel PSERVER nao foi declarada no arquivo '.env'. Servidor nao inicializado.")
    raise KeyError("Variavel PSERVER nao foi declarada no arquivo '.env'") 

api  = Api(app)
cors = CORS(app)

scheduler = BackgroundScheduler()
scheduler.start()

global_info = ThreadDictGlobal()

if permission_folder := os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS"):
    if os.path.exists(permission_folder):
        os.chmod(permission_folder, 0o640)