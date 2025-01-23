#=== Importações de módulos externos ===#
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import MetaData
import urllib, os, pyodbc

#=== Importações de módulos internos ===#
import app.config.config

pyodbc.pooling = False
params = urllib.parse.quote_plus(os.environ.get("SQL_SERVER_PARAMS_PS"))

SQLALCHEMY_DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={params}"

kwargs = {
    "url": SQLALCHEMY_DATABASE_URL,
    "future": True,
    "poolclass": NullPool,
    "connect_args": {
        'connect_timeout': 10
    }
}

engine = create_engine(**kwargs)

metadata = MetaData(bind = engine)

session_maker = sessionmaker(engine)
db = scoped_session(session_maker)