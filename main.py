#=== Importações de módulos externos ===#
from sqlalchemy.orm import close_all_sessions
from unidecode import unidecode
from waitress import serve
import os

#=== Importações de módulos internos ===#
from app.routes.resources_set import app # O app teria que ser importado do server, mas é no resources_set que ele recebe as rotas
from app.server import logger

def main():
    """
    Função principal do programa
    """

    # Pega a variável do sistema criada no import do app
    # Lembrando que fui eu que criei essa variável
    # Duvidas, procurar como criar variavel do sistema
    choice = unidecode(os.environ.get("PSERVER_PS").lower())
    port = int(os.environ.get("MAIN_PORT_PS"))
    close_all_sessions()

    # Server utilizado para produção 
    if choice == "production":
        logger.info(f"Starting server in PRODUCTION enviroment")

        port = int(os.environ.get("MAIN_PORT_PS"))
        threads = int(os.environ.get("PROD_THREADS_PS"))
        connection_limit = int(os.environ.get("PROD_CON_LIMIT_PS"))
        backlog = int(os.environ.get("PROD_BACKLOG_LIMIT_PS"))
        cleanup_interval = float(os.environ.get("PROD_CLEANUP_INTERVAL_PS"))
        channel_timeout = float(os.environ.get("PROD_CHANNEL_TIMEOUT_PS"))

        dict_connection = {
            "app": app,
            "host": "0.0.0.0",
            "port": port,
            "threads": threads,
            "backlog": backlog,
            "connection_limit": connection_limit,
            "cleanup_interval": cleanup_interval,
            "channel_timeout": channel_timeout
        }

        serve(**dict_connection)
    
    # Server utilizado para desenvolvimento 
    elif choice == "development":
        print(f"Starting server in DEVELOPMENT enviroment")
        app.run(host = "0.0.0.0", port = port)

    # Server utilizado para debug
    elif choice == "debug":
        print(f"Starting server in DEBUG enviroment")
        app.run(host = "0.0.0.0", port = port) 
    
    # Server utilizado para teste
    elif choice == "testing":
        print(f"Starting server in TESTING enviroment")
        app.run()

    raise ValueError("PSERVER com valor invalido")


if __name__ == "__main__":
    main()