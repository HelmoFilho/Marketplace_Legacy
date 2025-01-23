#=== Importações de módulos externos ===#
import logging, os, shutil, threading, json, traceback
from unidecode import unidecode
from datetime import datetime
from functools import wraps
import numpy as np

#=== Importações de módulos internos ===#
#from threads_info import ThreadDictGlobal
from app.shared.threads_info import ThreadDictGlobal

default_format = "\n%(asctime)s %(levelname)s %(message)s"
log_path = os.environ.get("LOG_FILES_PATH_PS")
production = unidecode(str(os.environ.get("PSERVER_PS")).lower())
separator = '='*150
global_info = ThreadDictGlobal()

class JsonFormatter(logging.Formatter):
    def format(self, record):

        data = global_info.get_info_thread()
        extra = record.__dict__.get("extra")

        json_record = {
            "level": record.levelname,
            "date_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        }

        if (method := data.get("method")):
            json_record["method"] = str(method).upper()

        if (endpoint := data.get("endpoint")):
            json_record["endpoint"] = str(endpoint).lower()

        json_record.update({
            "thread_id": record.thread,
            "thread_name": record.threadName,
        })

        if record.exc_info:
            json_record["message"] = "Uma exceção ocorreu"

        else:
            json_record["message"] = record.msg

        json_record["message"] = unidecode(json_record["message"])

        dict_id = {}

        if (id_usuario := data.get("id_usuario")):
            dict_id["id_usuario"] = int(id_usuario)

            if (id_cliente := data.get("id_cliente")):
                dict_id["id_cliente"] = int(id_cliente)

            if (id_distribuidor := data.get("id_distribuidor")):
                if str(id_distribuidor).isdecimal():
                    dict_id["id_distribuidor"] = int(id_distribuidor)
                
                elif isinstance(id_distribuidor, (list, set, tuple)):
                    dict_id["id_distribuidor"] = list(id_distribuidor)
                
                else:
                    dict_id["id_distribuidor"] = str(id_distribuidor)

            if (id_perfil := data.get("id_perfil")):
                dict_id["id_perfil"] = int(id_perfil)

        if dict_id:
            json_record["identification"] = dict_id

        dict_request = {}

        if (request_headers := data.get("headers")):
            dict_request["headers"] = json.dumps(request_headers)

        if (request_data := data.get("data")):
            if isinstance(request_data, dict):
                dict_request["body"] = json.dumps(request_data)

            else:
                if len(str(request_data)) <= 500:
                    dict_request["body"] = str(request_data)

        if dict_request:
            json_record["request"] = dict_request

        if record.exc_info:

            exception = {
                "type": repr(record.msg),
                "info": traceback.format_exc(),
            }

            json_record["exception"] = exception

        if extra:
            json_record["extra"] = extra
        
        #return json.dumps(json_record)
        return str(json.dumps(json_record, ensure_ascii=False, indent=4))


class Logger(object):
    """
    Classe que cuida dos processos do logger em json
    """

    def __init__(self, name: str = "default", format: str = default_format, level: int = logging.DEBUG):
        """
        Construtor da classe Logger

        Args:
            name (str, optional): Nome do logger. Defaults to "default".
            format (str, optional): String de format do logger. Defaults to default_format.
            level (int, optional): Nível de importância mínima do logger. Defaults to logging.DEBUG.
        """

        self.name            = name
        self.format          = format
        self.level           = level
        self.logger          = logging.getLogger(name)
        self.actual_date     = ''
        self.log_path        = None
        self.lock_path       = threading.Lock()
        self.lock_create     = threading.Lock()

        self.update_path()


    # Métodos de funcionamento
    def update_path(self):
        """
        Atualiza os dados relativos ao local do arquivo do logger
        """
        if production == "production":

            actual_date = datetime.now().strftime('%Y-%m-%d %H')

            if self.actual_date != actual_date:

                with self.lock_path: 

                    if self.actual_date != actual_date:

                        # As informações para criar o caminho até o arquivo de log
                        self.actual_date = actual_date
                        datetime_object = datetime.strptime(self.actual_date, '%Y-%m-%d %H')
                        
                        hour = int(datetime_object.strftime("%H"))
                        day = datetime_object.strftime("%d")
                        month = datetime_object.strftime("%B")
                        year = datetime_object.strftime("%Y")

                        month_digit = f"{datetime_object.month:02d}"
                        year_month = f"{year}_{month_digit}_{month}"
                        
                        file_name = f"{int(np.floor(hour/6))}"

                        self.full_path = f"{log_path}\\{self.name}_logs\\{year_month}\\{day}\\{file_name}"
                          
                        self.create_path(self.full_path)
                        self.create_handlers()


    def create_path(self, path: str):
        """
        Cria o caminho completo para o arquivo de log e deleta as pastas mais antigas
        
        Args:
            path (str): Caminho do arquivo que deve ser criado
        """
        full_path = path
        full_path_split = path.split("\\")

        if not os.path.exists(full_path):

            for index in range(len(full_path_split)):
                
                # Cria as pastas
                if index != len(full_path_split) - 1:
                    path = "\\".join(full_path_split[ : index + 1])
                    if not os.path.exists(path): 
                        os.mkdir(path)

        # Deleta a pasta mais antiga (1 ano atrás)
        folder = "\\".join(full_path_split[:3])
        mtime = lambda f: os.stat(os.path.join(folder, f)).st_mtime
        oldest = list(sorted(os.listdir(folder), key = mtime))

        if len(oldest) >= 12:

            to_delete = len(oldest) - 12
            for i in range(to_delete):
                if os.path.exists(f"{folder}\\{oldest[i]}"):
                    shutil.rmtree(f"{folder}\\{oldest[i]}", ignore_errors=True)


    def create_handlers(self):
        """
        Cria os handlers do logger
        """
        # Remove os handlers anteriores para adicionar os novos
        self.logger.setLevel(self.level)

        try:
            for handlers in self.logger.handlers[:]:
                self.logger.removeHandler(handlers)
        except:
            pass
        
        log_format = JsonFormatter()

        logger_handler_normal = logging.FileHandler(f"{self.full_path}.json")
        logger_handler_normal.setFormatter(log_format)
        logger_handler_normal.setLevel(self.level)
        self.logger.addHandler(logger_handler_normal)

        self.logger.propagate = False


    def __call__(self, generic_function: callable):
        """
        Decorador que têm como função acionar a mensagem de exceção caso algo aconteça

        Args:
            generic_function (function): Função qualquer

        Returns:
            [No defined type]: Retorna seja lá o que o generic_function retornar
        """
        @wraps(generic_function)
        def wrapper(*args, **kwargs):

            try:                
                response = generic_function(*args, **kwargs)
                return response

            except Exception as error_log:
                if production == "production":
                    self.exception(error_log)
                    return {"status": 500,
                            "resposta":{
                                "tipo": "0",
                                "descricao":f"Erro interno no servidor."}}, 500
                else:
                    # Re-chama o ultimo erro
                    raise

        return wrapper


    def log_to_file(self, msg: str, level: str, **kwargs):
        """
        Salva o Log no arquivo

        Args:
            msg (str): Qual a mensagem que será salva
            level (str): String representando qual o nivel do log
        """

        self.update_path()

        level_str = str(level).upper()   
        extra = kwargs

        if level_str == 'EXCEPTION':
            self.logger.exception(msg, extra = extra)

        elif level_str == "INFO":
            self.logger.info(msg, extra = extra)

        elif level_str == "ERROR":
            self.logger.info(msg, extra = extra)

        elif level_str == "WARNING":
            self.logger.info(msg, extra = extra)

        elif level_str == "CRITICAL":
            self.logger.info(msg, extra = extra)

        else:
            self.logger.debug(msg, extra = extra)


    # Métodos dos niveis do logger
    def debug(self, msg: str = "Debug default Message", **kwargs):
        """
        Mostra a mensagem do nivel DEBUG

        Args:
            msg (str, optional): Mensagem que será mostrada pelo logger. Defaults to "Debug default Message".
        
        Kwargs:            
            start (bool, optional): Se esse mensagem é a primeira de um bloco. Defaults to False.
            end (bool, optional): Se esse mensagem é a ultima de um bloco. Defaults to False.
        """
        if production == "production" and self.level <= 10:

            self.log_to_file(msg, "Debug", **kwargs)


    def info(self, msg: str = "Info default Message", **kwargs):
        """
        Mostra a mensagem do nivel INFO

        Args:
            msg (str, optional): Mensagem que será mostrada pelo logger. Defaults to "Info default Message".
            start (bool, optional): Se esse mensagem é a primeira de um bloco. Defaults to False.
            end (bool, optional): Se esse mensagem é a ultima de um bloco. Defaults to False.
        """
        if production == "production" and self.level <= 20:
            
            self.log_to_file(msg, "Info", **kwargs)


    def warning(self, msg: str = "Warning default Message", **kwargs):
        """
        Mostra a mensagem do nivel WARNING

        Args:
            msg (str, optional): Mensagem que será mostrada pelo logger. Defaults to "Warning default Message".
            start (bool, optional): Se esse mensagem é a primeira de um bloco. Defaults to False.
            end (bool, optional): Se esse mensagem é a ultima de um bloco. Defaults to False.
        """
        if production == "production" and self.level <= 30:
            
            self.log_to_file(msg, "Warning", **kwargs)


    def error(self, msg: str = "Error default Message", **kwargs):  
        """
        Mostra a mensagem do nivel ERROR

        Args:
            msg (str, optional): Mensagem que será mostrada pelo logger. Defaults to "Error default Message".
            start (bool, optional): Se esse mensagem é a primeira de um bloco. Defaults to False.
            end (bool, optional): Se esse mensagem é a ultima de um bloco. Defaults to False.
        """
        if production == "production" and self.level <= 40:
            
            self.log_to_file(msg, "Error", **kwargs)


    def critical(self, msg: str = "Critical default Message", **kwargs):
        """
        Mostra a mensagem do nivel CRITICAL

        Args:
            msg (str, optional): Mensagem que será mostrada pelo logger. Defaults to "Critical default Message".
            start (bool, optional): Se esse mensagem é a primeira de um bloco. Defaults to False.
            end (bool, optional): Se esse mensagem é a ultima de um bloco. Defaults to False.
        """
        if production == "production" and self.level <= 50:
            
            self.log_to_file(msg, "Critical", **kwargs)


    def exception(self, msg = "Exception default Message", **kwargs):
        """
        Mostra a mensagem do nivel ERROR que vêm de exceções

        Args:
            msg (str, optional): Mensagem que será mostrada pelo logger. Defaults to "Exception default Message".
            end (bool, optional): Se esse mensagem é a ultima de um bloco. Defaults to True.
        """
        if production == "production" and self.level <= 40:

            self.log_to_file(msg, "Exception", **kwargs)


class LoggerManager(object):
    """
    Classe que gerencia os loggers da api
    """    

    def __init__(self, loggers: dict = None, level: int = logging.DEBUG):
        """
        Construtor da classe Logger

        Args:
            loggers (dict, optional): Loggers já criados com suas chaves de chamada. Defaults to {}.
        """
        if loggers is None:
            loggers = {}

        self.loggers = loggers | {"main": Logger("main", default_format, 20)}
        self.lock = threading.Lock()
        self.default_level = level


    def logger_choice(self, level: str, msg, **kwargs):
        """
        Construtor da classe Logger

        Args:
            loggers (dict, optional): Loggers já criados com suas chaves de chamada. Defaults to {}.
        """

        chosen_api = global_info.get("api")

        if not chosen_api:
            chosen_api = "main"

        logger = self.loggers.get(chosen_api)
        
        if not logger:
            with self.lock:
                if not logger:
                    logger = Logger(chosen_api, default_format, self.default_level)
                    self.loggers[chosen_api] = logger

        level = str(level).lower()

        if level == "exception":
            logger.exception(msg, **kwargs)

        elif level == "info":
            logger.info(msg, **kwargs)

        elif level == "error":
            logger.error(msg, **kwargs)

        elif level == "warning":
            logger.warning(msg, **kwargs)

        elif level == "critical":
            logger.critical(msg, **kwargs)

        else:
            logger.debug(msg, **kwargs)
            

    def __call__(self, generic_function: callable):
        """
        Decorador que têm como função acionar a mensagem de exceção caso algo aconteça

        Args:
            generic_function (function): Função qualquer

        Returns:
            [No defined type]: Retorna seja lá o que o generic_function retornar
        """
        @wraps(generic_function)
        def wrapper(*args, **kwargs):

            try:                
                response = generic_function(*args, **kwargs)
                return response

            except Exception as error_log:
                if production == "production":
                    self.exception(error_log)
                    return {"status": 500,
                            "resposta":{
                                "tipo": "0",
                                "descricao":f"Erro interno no servidor."}}, 500
                else:
                    # Re-chama o ultimo erro
                    raise

        return wrapper

    
    def debug(self, msg: str = "Debug default Message", **kwargs):
        self.logger_choice("debug", msg, **kwargs)


    def info(self, msg: str = "info default Message", **kwargs):
        self.logger_choice("Info", msg, **kwargs)


    def warning(self, msg: str = "Warning default Message", **kwargs):
        self.logger_choice("warning", msg, **kwargs)


    def error(self, msg: str = "Error default Message", **kwargs):
        self.logger_choice("error", msg, **kwargs)


    def critical(self, msg: str = "Critical default Message", **kwargs):
        self.logger_choice("critical", msg, **kwargs)


    def exception(self, msg: str = "Exception default Message", **kwargs):
        self.logger_choice("exception", msg, **kwargs)