#=== Importações de módulos externos ===#
import os, dotenv

# Carrega as variáveis do sistema
dotenv.load_dotenv("other\environs\.env")

class Config():
    """
    Classe de configuração padrão do servidor
    """

    JSON_SORT_KEYS                  = False
    ERROR_404_HELP                  = False
    CORS_HEADERS                    = 'Content-Type'
    SECRET_KEY                      = os.environ.get("SECRET_KEY_PS")


class ProductionConfig(Config):
    """
    Classe de configuração do servidor de produção
    """
    
    ENV     = "production"
    TESTING = False
    DEBUG   = False


class DevelopmentConfig(Config):
    """
    Classe de configuração do servidor de desenvolvimento
    """
    
    ENV     = "production"
    TESTING = True
    DEBUG   = False


class TestingConfig(Config):
    """
    Classe de configuração do servidor de teste
    """

    ENV     = "development"
    TESTING = False
    DEBUG   = True