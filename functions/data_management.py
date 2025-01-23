#=== Importações de módulos externos ===#
from sqlalchemy import text, bindparam, insert, update, delete, Table
from string import ascii_letters
from random import SystemRandom
from unidecode import unidecode
import datetime, decimal, json
from flask import request

#=== Importações de módulos internos ===#
from app.config.sqlalchemy_config import db, metadata, engine
from app.server import logger, global_info

def get_request(norm_keys: bool = True, values_upper: bool = False, values_lower: bool = False,
                    norm_values: bool = False, trim_values: bool = False, 
                    trim_keys: bool = True, not_change_values: list = None,
                    delete_data: list = None, bool_save_request: bool = True) -> dict:
    """
    Pega o request (seja feito por um json ou pela url) e a devolve formatada
    como um dicionário.

    Args:
        norm_keys (bool, optional): 
            Realiza a normalização (se string, retira caixa baixa e unidecoda a string) nas chaves. 
            Defaults to True.
        values_upper (bool, optional): 
            Muda o valor das chaves (se string) para caixa alta. Defaults to False.
        values_lower (bool, optional): 
            Muda o valor das chaves(se string) para caixa baixa. Defaults to False.
        norm_values (bool, optional): 
            Realiza a normalização (se string, unidecoda a string) nos valores. Defaults to False.
        trim_values (bool, optional): 
            Realiza um split e um join para remoção de espaços em branco desnecessarios nos valores. 
            Defaults to False.
        trim_keys (bool, optional): 
            Realiza um split e um join para remoção de espaços em branco desnecessarios nas chaves. 
            Defaults to True.
        not_change_values (list, optional): 
            keys dos valores que não devem ser modificados. 
            Defaults to False.
        delete_data (list, optional):
            keys que devem ser deletadas
            Defaults to None

    Returns:
        dict: Dados do request como um dicionário
    """
    
    if not_change_values is None:
        not_change_values = []

    if delete_data is None:
        delete_data = []

    request_response = {}

    # Se houverem argumentos
    if request.args:
        hold_args = {
            str(key).lower(): value 
            for key, value in request.args.items()
        }

        request_response.update(hold_args)

    # Se houver um form
    if request.form:
        request_response.update(request.form)

    if request.data:
    
        json_data = request.get_json()
        if json_data and isinstance(json_data, dict):
            request_response.update(json_data)    

    # Realiza a normalização dos dados dependendo dos boleanos
    def modify_values(copy: dict = None, norm_keys = False, norm_values = False, 
                        values_upper = False, values_lower = False, trim_keys = False, 
                        trim_values = False) -> dict:
        """
        Realiza a modificação nos valores de um dicionario

        Args:
            copy (dict, optional): 
                dicionario em que serão realizadas as modificações. 
                Defaults to dict().

        Returns:
            dict: Dados do request como um dicionário
        """

        if copy is None:
            copy = {}

        data = {}
        
        for key, value in copy.items():

            ### Modificação da chave

            # Normalização das chaves do dicionario
            if norm_keys:
                key = unidecode(str(key).lower())

            # Remoção de espaços brancos desnecessários nas chaves
            if trim_keys:
                key = "".join(str(key).split())

            if norm_keys or trim_keys:
                
                if isinstance(value, (list, tuple)):
                    n_value = []
                    for v in value:
                        
                        if isinstance(v, (list, tuple)):
                            v = modify_values({"x":v}, norm_keys = norm_keys, 
                                    trim_keys = trim_keys)["x"]

                        elif isinstance(v, (dict)):
                            v = modify_values(v, norm_keys = norm_keys, trim_keys = trim_keys)

                        n_value.append(v)
                    value = n_value

                elif isinstance(value, (dict)):
                    value = modify_values(value, norm_keys = norm_keys, trim_keys = trim_keys)

            ### Modificação dos valores
            if key not in not_change_values:

                # Transformação da caixa do valor
                if values_upper or values_lower:
                    
                    # Caso o valor seja uma string pura
                    if isinstance(value, (str)):
                        if values_upper:
                            value = str(value).upper()
                        if values_lower:
                            value = str(value).lower()
                    
                    # Caso seja uma lista ou uma tupla
                    elif isinstance(value, (list, tuple)):
                        n_value = []
                        for v in value:
                            if isinstance(v, (str)):
                                if values_upper:
                                    v = str(v).upper()
                                if values_lower:
                                    v = str(v).lower()
                            
                            elif isinstance(v, (list, tuple)):
                                v = modify_values({"x":v}, values_upper = values_upper, 
                                        values_lower = values_lower)["x"]

                            elif isinstance(v, (dict)):
                                v = modify_values(v, values_upper = values_upper, 
                                        values_lower = values_lower)

                            n_value.append(v)
                        value = n_value
                    
                    # Caso seja um dicionario
                    elif isinstance(value, (dict)):
                        value = modify_values(value, values_lower = values_lower, 
                                                values_upper = values_upper)

                # Normalização dos valores do dicionario
                if norm_values:

                    # Caso o valor seja uma string pura
                    if isinstance(value, (str)):
                        value = unidecode(str(value))

                    # Caso seja uma lista ou uma tupla
                    if isinstance(value, (list, tuple)):
                        n_value = []
                        for v in value:
                            if isinstance(v, (str)):
                                v = unidecode(str(v))
                            elif isinstance(v, (list, tuple)): 
                                v = modify_values({"x":v}, norm_values = norm_values)["x"]
                            elif isinstance(v, (dict)):
                                v = modify_values(v, norm_values = norm_values)
                            n_value.append(v)
                        value = n_value
                    
                    # Caso seja um dicionario
                    if isinstance(value, (dict)):
                        value = modify_values(value, norm_values = norm_values)
                
                # Remoção de espaços brancos desnecessários nos valores
                if trim_values:

                    # Caso o valor seja uma string pura
                    if isinstance(value, (str)):
                        if trim_values:
                            value = " ".join(str(value).split())
                    
                    # Caso seja uma lista ou uma tupla
                    elif isinstance(value, (list, tuple)):
                        n_value = []
                        for v in value:
                            if isinstance(v, (str)):
                                v = " ".join(str(v).split())
                            elif isinstance(v, (list, tuple)): 
                                v = modify_values({"x":v}, trim_values = trim_values)["x"]
                            elif isinstance(v, (dict)):
                                v = modify_values(v, trim_values = trim_values)
                            n_value.append(v)
                        value = n_value

                    # Caso seja um dicionario
                    elif isinstance(value, (dict)):
                        value = modify_values(value, trim_values = trim_values)
            
            data[key] = value

        return data

    try:    
        # Só para formatar o request como um dicionário
        if isinstance(request_response, (dict)):
            copy = dict(request_response)
        else:
            copy = list(request_response)

    except:
        # Só para formatar o request como uma lista
        copy = list(request_response)

    # Tratamento dos dados
    if any([norm_keys, norm_values, values_upper, values_lower, trim_keys, trim_values]):

        if isinstance(copy, dict):
            copy = modify_values(copy, norm_keys = norm_keys, norm_values = norm_values, 
                        values_upper = values_upper, values_lower = values_lower, 
                        trim_keys = trim_keys, trim_values = trim_values)
        
        elif isinstance(copy, list):
            hold = []
            for dicti in copy:
                if isinstance(dicti, dict):
                    hold.append(modify_values(dicti, norm_keys, norm_values, values_upper, 
                                        values_lower, trim_keys, trim_values))
                else:
                    hold.append(modify_values({"x":dicti}, norm_values=norm_values, 
                                        values_upper=values_upper, values_lower=values_lower,
                                        trim_values = trim_values)["x"])

            copy = hold

    for to_delete in delete_data:
        try:
            copy.pop(to_delete)
        except:
            pass

    if bool_save_request:
        global_info.save_info_thread(data = copy)

    else:

        global_info.remove_info_thread("data")

        
    return copy


def type_corrector(value):
    """
    Devolve o valor com o tipo corrigido

    Args:
        value (tipo não definido): Valor a ser corrigido

    Returns:
        [tipo não definido]: Valor corrigido
    """
    
    # Se o dado já for um string
    if type(value) is str:
        return " ".join(value.split())        

    # Se o dado já for uma data-hora
    elif type(value) is datetime.datetime:
        return value.strftime('%Y-%m-%d %H:%M:%S.%f')

    # Se o dado já for uma data
    elif type(value) is datetime.date:
        return value.strftime('%Y-%m-%d')

    # Se o dado for decimal
    elif type(value) is decimal.Decimal:
        return float(value)

    # Se o dado for vazio
    elif value == None:
        return None

    # Se o dado for numérico
    else: 
        return value


def string_to_json(json_string, to_substitute = '&%$'):

    if json_string:
    
        json_string = str(json_string)

        if json_string.find(to_substitute) != -1:
            json_string = json_string.replace(to_substitute, '"')

        if json_string.find('\\\\') != -1:
            json_string = json_string.replace('\\\\', '')
        
        if json_string.find('\\"') != -1:
            json_string = json_string.replace('\\"', '"')

        if json_string.find('"{') != -1:
            json_string = json_string.replace('"{', '{')

        if json_string.find('}"') != -1:
            json_string = json_string.replace('}"', '}')

        if json_string.find('"[') != -1:
            json_string = json_string.replace('"[', '[')

        if json_string.find(']"') != -1:
            json_string = json_string.replace(']"', ']')

        if json_string.find('\\/') != -1:
            json_string = json_string.replace('\\/', '/')

        return json.loads(json_string)

    return {}


def treat_answer(query, raw = False, first = False) -> list:
    """
    Gera a resposta da query a partir de um objeto query

    Args:
        query (sqlalchemy): Query feita
        raw (bool, optional): Caso a query tenha de ser uma lista de listas. Defaults to False.
        first (bool, optional): Caso somente o primeiro resultado seja necessário. Defaults to False.

    Returns:
        any: Dados tratados
    """

    hold = []

    if raw:

        for row in query:
            if row:
                hold.append(list(map(type_corrector, list(row))))
            if first:  break
    
    else:

        for data in list(query.mappings().all()):

            hold.append({
                k: type_corrector(v)
                for k, v in data.items()
            })

            if first: break

    return hold


def raw_sql_execute(query: str, commit: bool = True, **kwargs):
    """
    Pega uma query pura e a executa e comita

    Args:
        query: Query pura

    kwargs:
        params: Parâmetros que serão utilizados na query
        bindparams: Parâmetros que precisam de tratamento especial (tipo in)
    """

    params = kwargs.get("params") if type(kwargs.get("params")) is dict else {}
    connector = kwargs.get("connector") if kwargs.get("connector") else None

    holder = text(query).bindparams(
        *[
            bindparam(key, expanding = True)
            for key, value in params.items()
            if isinstance(value, list)
        ]
    )

    # com conector manual
    if connector:
        
        connector.execute(holder, params)

        if commit:
            connector.commit()

    # com conector automatico
    session = db()
    session.execute(holder, params)

    if commit:
        session.commit()


def raw_sql_return(query: str, **kwargs) -> list[str] or list[list[str]] or list[dict] or None:
    """
    Pega os valores de uma query pura e a retorna como dicionário ou como listas

    Args:
        query: Resultado de uma query pura

    kwargs:
        first: Retorna o primeiro resultado apenas
        raw: Retorna os resultados como lista de listas
        params: Parâmetros que serão utilizados na query
        bindparams: Parâmetros que precisam de tratamento especial (tipo in)

    Returns:
        [list[str] or list[list[str]] or list[dict] or None]: dato tratado como um dicionário
    """
    
    first = kwargs.get("first")
    raw = kwargs.get("raw")
    params = kwargs.get("params") if type(kwargs.get("params")) is dict else {}
    connector = kwargs.get("connector") if kwargs.get("connector") else None

    holder = text(query).bindparams(
        *[
            bindparam(key, expanding = True)
            for key, value in params.items()
            if isinstance(value, list)
        ]
    )

    hold = []

    if connector:

        data = {
            "query": connector.execute(holder, params),
            "raw": raw,
            "first": first
        }

        hold = treat_answer(**data)

    else:
        session = db()

        data = {
            "query": session.execute(holder, params),
            "raw": raw,
            "first": first
        }

        hold = treat_answer(**data)

    if first:
        if len(hold) <= 0:
            return None
        
        return hold[0]
    
    if len(hold) <= 0:
        return []
    
    return hold 


def raw_sql_insert(table_name: str, data: list[dict], **kwargs):
    """
    Realiza um batch insert no MSSQL

    Args:
        table_name (str): Nome da tabela
        data (list[dict]): Dicionários que serão utilizados para os inserts
    """
    assert data, "Missing data"
    assert ((type(data) is list and type(data[0]) is dict and data[0]) or (type(data) is dict)), "Data with invalid types"

    max_batch = 500

    # Tratamentos iniciais
    table_name = str(table_name).upper()
    
    if type(data) is dict:
        data = [data]

    total = len(data)
    add_columns = [key   for key in data[0].keys()]
    columns_total = len(add_columns)

    # Escolha do melhor valor de batch
    batch = max_batch

    if (batch * columns_total) > 2000:
        batch = (2000 // columns_total)
        if batch > max_batch:
            batch = max_batch

    # Criando as expressões do algoritmo

    ## Expressão das colunas utilizadas para o insert
    insert_columns = ", ".join(add_columns)

    ## Query para o insert
    raw_insert = """
        SET NOCOUNT ON;

        INSERT INTO 
            {table_name}
                (
                    {insert_columns}
                )
            
            VALUES
                {insert_values};
    """

    # Paginando os dados do insert pelo batch
    quantity = (total // batch) + (total % batch > 0)

    for page in range(quantity):

        actual_data = data[page * batch: (page + 1) * batch]

        # Manipulação dos dados que serão utilizados na sessão atual
        insert_values = ""
        insert_data = {}

        for i in range(len(actual_data)):

            # Criando as chaves dos valores para o insert
            values = [f":{column}_{i}" for column in add_columns]
            values = f"({', '.join(values)})"

            if i != len(actual_data) - 1:
                values += ",\n"
            
            insert_values += values

            # Configura os dados para serem utilizados na query
            insert_data.update({
                f"{key}_{i}": actual_data[i].get(key)
                for key in add_columns
            })

        # Realizando o insert
        insert_expressions = {
            "table_name": table_name,
            "insert_columns": insert_columns,
            "insert_values": insert_values
        }

        if not all(list(insert_expressions.values())):
            continue

        insert_query = raw_insert.format(**insert_expressions)  

        raw_sql_execute(insert_query, params = insert_data)


def core_sql_insert(table_name: str, data: list[dict], **kwargs):
    """
    Realiza um batch insert no MSSQL

    Args:
        table_name (str): Nome da tabela
        data (list[dict]): Dicionários que serão utilizados para os inserts
    """
    assert data, "Missing data"
    assert ((type(data) is list and type(data[0]) is dict and data[0]) or (type(data) is dict)), "Data with invalid types"

    # Tratamentos iniciais
    table_name = str(table_name).upper()

    if isinstance(data, dict):
        data = [data]

    if data:
        key_columns = list(set([key   for key in data[0].keys()]))
    
    else:
        key_columns = []
    
    # Criando o statement
    connector = kwargs.get("connector") if kwargs.get("connector") else None
    my_table = Table(table_name, metadata, autoload_with = engine)

    table_columns = {
        unidecode(f"{col.name}").lower(): col.name
        for col in my_table.c
    }
    
    stmt = insert(my_table)

    values_dict = {
        table_columns.get(key): bindparam(unidecode(f"insert_value_{key}").lower())
        for key in key_columns
        if table_columns.get(unidecode(f"{key}").lower()) is not None
    }

    if values_dict:
        stmt = stmt.values(**values_dict)

    # Tratando os dados
    new_data = []

    for old_data in data:

        new_data_dict = {
            unidecode(f"insert_value_{key}").lower(): value
            for key, value in old_data.items()  
            if unidecode(str(key)).lower() in key_columns
        }

        if new_data_dict:
            new_data.append(new_data_dict)

    if not connector:
        session = db()

    else:
        session = connector

    try:
        batch = 100
        paginas = (len(new_data) // batch) + (len(new_data) % batch > 0)

        for page in range(paginas):

            paginated_data = new_data[page * batch : (page + 1) * batch]

            session.execute(stmt, paginated_data)
            session.commit()

    except:
        raise


def raw_sql_update(table_name: str, data: list[dict], key_columns: list[str] = None, 
                    like_columns: dict[dict] = None, exceptions_columns: dict = None, **kwargs):
    """
    Realiza um batch update no MSSQL

    Args:
        table_name (str): Nome da tabela
        data (list[dict]|dict): Dicionários que serão utilizados para os update
        key_column (list[str]): Colunas que serão referência quando for realizado o update. defaults to []
    """

    assert data, "Missing data"
    assert ((type(data) is list and type(data[0]) is dict and data[0]) or (type(data) is dict)), "Data with invalid types"
    if like_columns:
        assert (type(like_columns) is dict), "Like_Columns with invalid types"

    max_batch = 100

    if key_columns is None:
        key_columns = []

    if like_columns is None:
        like_columns = {}

    if exceptions_columns is None:
        exceptions_columns = {}

    # Tratamentos iniciais
    table_name = str(table_name).upper()

    if type(data) is dict:
        data = [data]

    try:
        like_column_string = list(like_columns.keys())
    except:
        like_column_string = []
    
    key_columns =    [key   for key in key_columns   if key not in like_column_string]
    change_columns = [key   for key in data[0].keys()   if key not in (key_columns + like_column_string)]
    
    key_columns = list(set(key_columns))
    change_columns = list(set(change_columns))
    like_column_string = list(set(like_column_string))

    total = len(data)
    all_columns = list(set(change_columns + key_columns + like_column_string))
    columns_total = len(all_columns)

    excpt_data = {
        key: value
        for key, value in exceptions_columns.items()
        if key in all_columns
    }

    # Escolha do melhor valor de batch
    batch = max_batch

    if (batch * columns_total) > 2000:
        batch = (2000 // columns_total)
        if batch > max_batch:
            batch = max_batch

    # Criando as expressões do algoritmo

    ## Expressão da tabela temporária
    update_create_exp = ""

    for key in all_columns:

        update_create_exp += f"{key} VARCHAR(MAX)"
        if key != all_columns[-1]:
            update_create_exp += f",\n"

    ## Expressão do insert da tabela temporária
    update_insert_keys_exp = ", ".join(all_columns)

    ## Expressão para criação dos valores para o SET do update
    update_set_exp = []

    for column in change_columns:
        update_set_exp.append(f"{column} = hp.{column}")
    
    update_set_exp = ", ".join(update_set_exp)

    ## Expressão para criação dos valores para o WHERE do update
    update_where_exp = []
    excpt_to_add = {}

    for column in key_columns:
        update_where_exp.append(f"CONVERT(VARCHAR(MAX), tn.{column}) COLLATE DATABASE_DEFAULT = CONVERT(VARCHAR(MAX) ,hp.{column}) COLLATE DATABASE_DEFAULT")

    for column in like_column_string:
        
        expression_holder = like_columns.get(column)

        try:
            ex_start = "%" if expression_holder.get("start") else ""
        except:
            ex_start = ""

        try:
            ex_end = "%" if expression_holder.get("end") else ""
        except:
            ex_end = ""    

        update_where_exp.append(f"tn.{column} COLLATE DATABASE_DEFAULT LIKE '{ex_start}' + CONVERT(VARCHAR(MAX), hp.{column}) + '{ex_end}'") 
    
    for column in excpt_data.keys():

        update_where_exp.append(f"CONVERT(VARCHAR(MAX), tn.{column}) COLLATE DATABASE_DEFAULT = CONVERT(VARCHAR(MAX), :{f'x_{column}'}) COLLATE DATABASE_DEFAULT")
        excpt_to_add[f'x_{column}'] = excpt_data.get(column)

    update_where_exp = " AND ".join(update_where_exp)

    ## Query para o update
    raw_update = """
        SET NOCOUNT ON;

        -- Verificando se a tabela temporária existe
        IF Object_ID('tempDB..#HOLD_UPDATE','U') IS NOT NULL 
        BEGIN
            DROP TABLE #HOLD_UPDATE
        END;

        -- Criando uma tabela temporaria
        CREATE TABLE #HOLD_UPDATE (
            id_update_key INT IDENTITY(1,1),
            {update_create_exp}
        );

        -- Criando o insert na tabela temporaria
        INSERT INTO
            #HOLD_UPDATE
                (
                    {update_insert_keys_exp}
                )

            VALUES
                {update_insert_values_exp};

        -- Realizando o update
        UPDATE
            {table_name}
        
        SET
            {update_set_exp}
        
        FROM
            {table_name} as tn,
            #HOLD_UPDATE as hp

        {update_if_where}
            {update_where_exp};

        -- Deleta a tabela temporaria
        DROP TABLE #HOLD_UPDATE
    """
    
    # Paginando os dados do update pelo batch
    quantity = (total // batch) + (total % batch > 0)

    for page in range(quantity):

        actual_data = data[page * batch: (page + 1) * batch]

        # Manipulação dos dados que serão utilizados na sessão atual
        update_insert_values_exp = ""
        update_data = {}

        for i in range(len(actual_data)):

            if not actual_data[i]:
                continue

            # Criando as chaves dos valores para o insert na tabela temporária
            values = [f":{column}_{i}" for column in all_columns]
            values = f"({', '.join(values)})"

            if i != len(actual_data) - 1:
                values += ",\n"
            
            update_insert_values_exp += values

            # Configura os dados para serem utilizados na query
            update_data.update({
                f"{key}_{i}": actual_data[i].get(key)
                for key in all_columns
            })
            
        # Realizando o update
        update_expressions = {
            "table_name": table_name,
            "update_create_exp": update_create_exp,
            "update_insert_keys_exp": update_insert_keys_exp,
            "update_insert_values_exp": update_insert_values_exp,
            "update_set_exp": update_set_exp,
            'update_if_where': 'WHERE' if update_where_exp else '',
            "update_where_exp": update_where_exp if update_where_exp else ''
        }

        if not all([value   for key, value in update_expressions.items()   if 'where' not in key]):
            continue

        update_query = raw_update.format(**update_expressions)

        if excpt_to_add:
            update_data.update(excpt_to_add)

        raw_sql_execute(update_query, params = update_data)


def core_sql_update(table_name: str, data: list[dict], key_columns: list[str] = None, 
                    like_columns: dict[dict] = None, exceptions_columns: dict = None, **kwargs):
    """
    Realiza um batch update no MSSQL

    Args:
        table_name (str): Nome da tabela
        data (list[dict]|dict): Dicionários que serão utilizados para os update
        key_column (list[str]): Colunas que serão referência quando for realizado o update. defaults to []
    """

    assert data, "Missing data"
    assert ((type(data) is list and type(data[0]) is dict and data[0]) or (type(data) is dict)), "Data with invalid types"
    if like_columns:
        assert (type(like_columns) is dict), "Like_Columns with invalid types"

    # Tratamentos iniciais
    table_name = str(table_name).upper()

    if isinstance(data, dict):
        data = [data]

    if key_columns is None:
        key_columns = []

    if like_columns is None:
        like_columns = {}

    if exceptions_columns is None:
        exceptions_columns = {}

    try:
        like_column_string = list(like_columns.keys())
        like_column_string = [key  for key in like_column_string]
    except:
        like_column_string = []

    key_columns =    [key   for key in key_columns   if key not in like_column_string]
    change_columns = [key   for key in data[0].keys()   if key not in (key_columns + like_column_string)]

    key_columns = list(set(key_columns))
    change_columns = list(set(change_columns))
    like_column_string = list(set(like_column_string))

    # Criando o statement
    my_table = Table(table_name, metadata, autoload_with = engine)

    table_columns = {
        unidecode(f"{col.name}").lower(): col.name
        for col in my_table.c
    }

    stmt = update(my_table)

    if key_columns:

        for key in key_columns:

            key = unidecode(f"{key}").lower()
            column = table_columns.get(key)
            
            if column is not None:
                stmt = stmt.where(getattr(my_table.c, column) == bindparam(f"update_key_{key}"))

    if like_column_string:

        for key in like_column_string:

            key = unidecode(f"{key}").lower()
            column = table_columns.get(key)

            if column is not None:
                if hasattr(getattr(my_table.c, column), 'like'):
                    stmt = stmt.where(getattr(my_table.c, column).like(bindparam(f"update_key_{key}")))

        for old_data in data:
            for key in like_column_string:

                expression_holder = like_columns.get(key)

                try:
                    ex_start = "%" if expression_holder.get("start") else ""
                except:
                    ex_start = ""

                try:
                    ex_end = "%" if expression_holder.get("end") else ""
                except:
                    ex_end = ""

                if old_data.get(key):
                    old_data[key] = ex_start + f"{old_data.get(key)}" + ex_end

    if change_columns:

        values_dict = {}

        for key in change_columns:

            key = unidecode(f"{key}").lower()
            column = table_columns.get(key)

            if column is not None:
                values_dict.update({
                    column: bindparam(f"update_value_{key}".lower())
                })

        if values_dict:
            stmt = stmt.values(**values_dict)

    # Tratando os dados
    new_data = []

    for old_data in data:

        new_data_dict = {
            unidecode(f"update_value_{key}").lower()  if key in change_columns  else unidecode(f"update_key_{key}").lower(): value
            for key, value in old_data.items()  
        }

        if new_data_dict:
            new_data.append(new_data_dict)

    if new_data:

        session = db()

        try:
            batch = 100
            paginas = (len(new_data) // batch) + (len(new_data) % batch > 0)

            for page in range(paginas):

                paginated_data = new_data[page * batch : (page + 1) * batch]

                session.execute(stmt, paginated_data)
                session.commit()

        except:
            raise


def raw_sql_delete(table_name: str, data: list[dict] = None, **kwargs):
    """
    Realiza um batch delete no MSSQL

    Args:
        table_name (str): Nome da tabela
        data (list[dict]|dict): Dicionários que serão utilizados para os delete
    """

    if data:
        assert ((type(data) is list and type(data[0]) is dict and data[0]) or (type(data) is dict)), "Data with invalid types"

    else:
        data = []

    max_batch = 100

    # Tratamentos iniciais
    table_name = str(table_name).upper()

    if type(data) is dict:
        data = [data]

    if data:
        key_columns = list(set([key   for key in data[0].keys()]))
    
    else:
        key_columns = []

    total = len(data)
    columns_total = len(key_columns)

    # Escolha do melhor valor de batch
    batch = max_batch

    if (batch * columns_total) > 2000:
        batch = (2000 // columns_total)
        if batch > max_batch:
            batch = max_batch

    # Criando as expressões do algoritmo

    ## Expressão da tabela temporária
    delete_create_exp = ""

    if data:
        for key in key_columns:

            delete_create_exp += f"{key} VARCHAR(MAX)"
            if key != key_columns[-1]:
                delete_create_exp += f",\n"
    
    else:
        random_name = ''.join(SystemRandom().choice(ascii_letters) for _ in range(100))
        delete_create_exp = f"dummy___{random_name} VARCHAR(MAX)"

    ## Expressão do insert da tabela temporária
    if data:
        delete_insert_keys_exp = ", ".join(key_columns)
    
    else:
        delete_insert_keys_exp = f"dummy___{random_name}"

    ## Expressão para criação dos valores para o WHERE do delete
    delete_where_exp = []

    for column in key_columns:
        delete_where_exp.append(f"CONVERT(VARCHAR(MAX), tn.{column}) COLLATE DATABASE_DEFAULT = CONVERT(VARCHAR(MAX), hp.{column}) COLLATE DATABASE_DEFAULT")

    delete_where_exp = " AND ".join(delete_where_exp)

    ## Query para o delete
    raw_delete = """
        SET NOCOUNT ON;

        -- Verificando se a tabela temporária existe
        IF Object_ID('tempDB..#HOLD_DELETE','U') IS NOT NULL 
        BEGIN
            DROP TABLE #HOLD_DELETE
        END;

        -- Criando uma tabela temporaria
        CREATE TABLE #HOLD_DELETE (
            {delete_create_exp}
        );

        -- Criando o insert na tabela temporaria
        INSERT INTO
            #HOLD_DELETE
                (
                    {delete_insert_keys_exp}
                )

            VALUES
                {delete_insert_values_exp};

        -- Realizando o delete
        DELETE
            {table_name}
        
        FROM
            {table_name} as tn,
            #HOLD_DELETE as hp

        {delete_if_where}
            {delete_where_exp};

        -- Deleta a tabela temporaria
        DROP TABLE #HOLD_DELETE
    """
    
    # Paginando os dados do delete pelo batch
    quantity = (total // batch) + (total % batch > 0)
    quantity = quantity   if quantity > 0   else 1
    
    for page in range(quantity):

        actual_data = data[page * batch: (page + 1) * batch]

        # Manipulação dos dados que serão utilizados na sessão atual
        delete_insert_values_exp = ""
        delete_data = {}

        if actual_data:

            for i in range(len(actual_data)):

                if not actual_data[i]:
                    continue

                # Criando as chaves dos valores para o insert na tabela temporária
                values = [f":{column}_{i}" for column in key_columns]
                values = f"({', '.join(values)})"

                if i != len(actual_data) - 1:
                    values += ",\n"
                
                delete_insert_values_exp += values

                # Configura os dados para serem utilizados na query
                delete_data.update({
                    f"{key}_{i}": actual_data[i].get(key)
                    for key in key_columns
                })

        else:
            delete_insert_values_exp = "(0)"
            
        # Realizando o delete
        delete_expressions = {
            "table_name": table_name,
            "delete_create_exp": delete_create_exp,
            "delete_insert_keys_exp": delete_insert_keys_exp,
            "delete_insert_values_exp": delete_insert_values_exp,
            "delete_if_where": 'WHERE' if delete_where_exp else '',
            "delete_where_exp": delete_where_exp if delete_where_exp else ''
        }

        if not all([value   for key, value in delete_expressions.items()   if 'where' not in key]):
            continue

        delete_query = raw_delete.format(**delete_expressions)

        raw_sql_execute(delete_query, params = delete_data)


def core_sql_delete(table_name: str, data: list[dict] = None, **kwargs):
    """
    Realiza um batch delete no MSSQL

    Args:
        table_name (str): Nome da tabela
        data (list[dict]|dict): Dicionários que serão utilizados para os delete
    """

    if data:
        assert ((type(data) is list and type(data[0]) is dict and data[0]) or (type(data) is dict)), "Data with invalid types"

    else:
        data = []

    # Tratamentos iniciais
    table_name = str(table_name).upper()

    if isinstance(data, dict):
        data = [data]

    if data:
        key_columns = list(set([key  for key in data[0].keys()]))
    
    else:
        key_columns = []
    
    # Criando o statement
    my_table = Table(table_name, metadata, autoload_with = engine)

    table_columns = {
        unidecode(f"{col.name}").lower(): col.name
        for col in my_table.c
    }
    
    stmt = delete(my_table)

    if key_columns:

        for key in key_columns:

            key = unidecode(f"{key}").lower()
            column = table_columns.get(key)

            if column is not None:
                stmt = stmt.where(getattr(my_table.c, column) == bindparam(f"delete_key_{key}"))

    # Tratando os dados
    new_data = []

    for old_data in data:

        new_data_dict = {
            unidecode(f"delete_key_{key}").lower(): value
            for key, value in old_data.items()  
            if key in key_columns
        }

        if new_data_dict:
            new_data.append(new_data_dict)

    try:
        session = db()
        session.execute(stmt, new_data)
        session.commit()

    except:
        raise


def cliente_check(id_cliente: int = None) -> tuple:
    """
    Checa se o cliente enviado está associado ao usuario

    Args:
        id_cliente (int): cliente utilizado pelo usuario atualmente

    Returns:
        tuple: Resposta da transação na primeira parte e mensagem de erro na segunda
    """

    token_info = global_info.get("token_info")

    if not token_info:
        logger.error("Usuario nao autenticado para verificacao de cliente.")
        return False, ({"status":401,
                        "resposta":{
                            "tipo": "13", 
                            "descricao": "Ação recusada: Token inválido."}}, 401)

    try:
        id_cliente = int(id_cliente)

    except:
        logger.error(f"Usuario logado mas o mesmo nao enviou o cliente utilizado.")
        return False, ({"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario não enviou cliente atrelado ao mesmo."}}, 403)

    id_cliente_token = token_info.get("id_cliente")

    if id_cliente not in id_cliente_token:
        logger.error(f"Tentativa de realizacao de acao por C:{id_cliente} nao atrelado ao mesmo.")
        return False, ({"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario tentando realizar ação por cliente não atrelado ao mesmo."}}, 403)

    global_info.save_info_thread(id_cliente = id_cliente)

    return True, None    


def distr_usuario_check(id_distribuidor: int = None) -> tuple:
    """
    Checa se o distribuidor enviado está associado ao usuario

    Args:
        id_distribuidor (int): distribuidor utilizado pelo usuario atualmente

    Returns:
        tuple: Resposta da transação na primeira parte e mensagem de erro na segunda
    """
    
    try:
        id_distribuidor = int(id_distribuidor)

    except:
        logger.error(f"Usuario nao enviou o distribuidor utilizado.")
        return False, ({"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario não enviou distribuidor atrelado ao mesmo."}}, 403)

    token_info = global_info.get("token_info")

    if not token_info:
        logger.error("Usuario nao autenticado para verificacao de distribuidor.")
        return False, ({"status":401,
                        "resposta":{
                            "tipo": "13", 
                            "descricao": "Ação recusada: Token inválido."}}, 401)

    id_distribuidor_token = token_info.get("id_distribuidor")
    id_perfil = token_info.get("id_perfil")

    if not (0 in id_distribuidor_token and id_perfil == 1):
        if id_distribuidor not in id_distribuidor_token:
            logger.error(f"Tentativa de realizacao de acao por D:{id_distribuidor} nao atrelado ao mesmo.")
            return False, ({"status":403,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: Usuario tentando realizar ação por distribuidor não atrelado ao mesmo."}}, 403)

    global_info.save_info_thread(id_distribuidor = id_distribuidor)

    return True, None    


def distribuidor_check(id_distribuidor: list[int], connector = None) -> tuple[bool, dict]:
    """
    Verifica a existência do id_distribuidor informado

    Args:
        parameters (list[int]): id_distribuidor para ser verificado.

    Returns:
        tuple(bool, dict):
            bool: Resultado da transação
            dict: informação que deve ser retornada em caso de erro
    """

    if type(id_distribuidor) is int:
        id_distribuidor = [id_distribuidor]
    
    elif type(id_distribuidor) in [list, tuple, set]:
        pass

    else:
        raise AssertionError("Invalid format for id_distribuidor")

    if connector:
        session = connector

    else:
        session = db()

    for distribuidor in id_distribuidor:

        distribuidor_query = """
            SELECT
                TOP 1 status

            FROM
                DISTRIBUIDOR

            WHERE
                id_distribuidor = :id_distribuidor
        """

        params = {
            "id_distribuidor": distribuidor
        }

        dict_query = {
            "query": distribuidor_query,
            "params": params,
            "first": True, 
            "raw": True,
            "connector": session
        }

        distribuidor_query = raw_sql_return(**dict_query)

        if not distribuidor_query:
            logger.error(f"Falha pois D:{distribuidor} informado nao existe.")
            return False, ({"status": 200,
                           "resposta":{
                               "tipo": "13", 
                               "descricao": "Ação recusada: Distribuidor informado não existe."}
                           }, 200)

        status = str(distribuidor_query[0]).upper()

        if status != 'A':
            logger.error(f"Falha pois D:{distribuidor} informado esta inativo.")
            return False, ({"status": 200,
                           "resposta":{
                               "tipo": "13", 
                               "descricao": "Ação recusada: Distribuidor informado está inativo."}
                           }, 200)

    return True, {}


def page_limit_handler(parameters: dict = None) -> tuple:
    """
    Cuida dos parametros da pagina

    Args:
        parameters (dict, optional): Parâmetros que serão verificados. Defaults to dict().

    Returns:
        tuple: pagina e limite de arquivos
    """

    if parameters is None:
        parameters = {}

    # Valores padrões
    defaults = {1: parameters.get("pagina"), 20: parameters.get("limite")}
    to_send = []

    for default, value in defaults.items():

        if not value:
            value = default
    
        else:
            if not str(value).isdecimal():
                value = default

            else:
                if int(value) <= 0:
                    value = default

                else:
                    value = int(value)
        
        to_send.append(value)

    return tuple(to_send)


def id_handler_raw(table: str, key: str, connector = None) -> int:
    """
    Pega o os valores do atributo da tabela e o retorna o id correto para aquele novo registro

    Args:
        table: tabela completa do sqlalchemy
        column ([type]): nome do atributo

    Returns:
        [int]: valor do id
    """

    if connector:
        session = connector

    else:
        session = db()
    
    query = f"""
        SELECT
            MAX({str(key).lower()})

        FROM
            {str(table).upper()}
    """

    max_id = raw_sql_return(query, raw = True, first = True, connector = session)[0]

    if max_id:
        return max_id + 1

    else:
        return 1


def check_validity(request_response: dict, comparison_columns: list = None,
                    no_use_columns: list = None, not_null: list = None,
                    correct_types: dict = None, bool_missing: bool = True,
                    bool_invalid_keys: bool = True, bool_incorrect: bool = True,
                    bool_check_password = True, bool_not_null: bool = True) -> dict:

    """
    Checa a validade dos dados inseridos pelo usuário.

    Parameters:
        request_response (dict):
                    Dados inseridos pelo usuário
        comparison_columns (list, optional):
                    Colunas que serão utilizadas nas comparações. Padrão para [].
        no_use_columns (list, optional):
                    Colunas que podem ser ignoradas na verificação de chaves inválidas. Padrão para [].
        not_null (list, optional):
                    Colunas que não podem ter valores nulos. Padrão para [].
        correct_type (dict, optional):
                    Tipos que certas colunas devem ter. Padrão para {}.
        bool_missing (bool, optional):
                    Define se a verificação de valores faltantes deve ser feito. Padrão para True.
        bool_invalid_keys (bool, optional):
                    Define se a verificação de chaves invalidas deve ser feito. Padrão para True.
        bool_incorrect (bool, optional):
                    Define se a verificação de valores do tipo incorreto deve ser feito. Padrão para True.
        bool_check_password (bool, optional):
                    Define se a verificação de tamanho de senha deve ser feito. Padrão para True.
        bool_not_null (bool, optional):
                    Define se a verificação de valores não-nuloss deve ser feito. Padrão para True.
        
    Returns:
        dict|None: Retorna um dicionário contendo qual erro foi visto nos dados e retorna vazio se tudo estiver ok.
    """

    if comparison_columns is None:
        comparison_columns = []

    if no_use_columns is None:
        no_use_columns = []

    if not_null is None:
        not_null = []

    if correct_types is None:
        correct_types = {}

    # Verifica se os dados possuem a variavel senha e se ela atende os pre-requisitos
    if bool_check_password:
        if request_response.get("senha"):

            length = len(str(request_response["senha"]))

            if length != 32:
                logger.error(f"Usuario digitou senha com tamanho incompativel.")
                return {"status":400,
                        "resposta":{
                            "tipo":"8",
                            "descricao":"Senha fora do padrão."}}, 400

    # Verifica se existe alguma chave que não existe na tabela
    if bool_invalid_keys:
        
        valid_keys = set(comparison_columns + no_use_columns + ["pagina", "limite"])

        invalid_keys = [
            key
            for key in request_response.keys()
            if key not in valid_keys
        ]

        if invalid_keys:
            invalid_keys = list(set(invalid_keys))
            logger.error(f"As chaves fornecidas {invalid_keys} sao invalidas.")
            return {"status":400,
                    "resposta":{
                        "tipo":"2",
                        "descricao":f"Campos {invalid_keys} inválidos."}}, 400


    # Verifica se alguma das chaves obrigatórias está faltando
    if bool_missing:
        
        found_keys = set(request_response.keys())

        missing_keys = [
            key
            for key in comparison_columns
            if key not in found_keys
        ]

        if missing_keys:
            missing_keys = list(set(missing_keys))
            logger.error(f"As chaves {missing_keys} nao foram fornecidas.")
            return {"status":400,
                    "resposta":{
                        "tipo":"3",
                        "descricao":f"Campos {missing_keys} faltando."}}, 400

    # Verifica se as chaves escolhidas receberam nulo
    if bool_not_null and not_null:

        nulls = [
            key
            for key, value in request_response.items()
            if key in not_null and str(value) not in {"0", "False"} and not bool(value)
        ]

        # Caso algum not_null tenha recebido nulo...
        if nulls:
            nulls = list(set(nulls))
            logger.error(f"As chaves {nulls} estao com valores nulos.")
            return {"status":400,
                    "resposta":{
                        "tipo":"14",
                        "descricao":f"Campos {nulls} com valores nulos."}}, 400

    # Verifica se os valores estão no tipo correto
    if bool_incorrect:  
        incorrect_values = []
        
        def incorrect_test(class_type, value, key):
            
            if value is None and key not in not_null:
                return True

            elif class_type is int:
                if value != "":
                    try:
                        int(str(value))
                        return True
                    except:
                        return False   

            elif class_type is float:
                if value != "":
                    try:
                        float(str(value))
                        return True
                    except:
                        return False   

            elif class_type is str:
                if not isinstance(value, str): 
                    return False
                return True
            
            elif class_type in [list, set, tuple]:
                if not isinstance(value, (list, set, tuple)):
                    return False
                return True
            
            elif class_type is dict:
                if not isinstance(value, dict): 
                    return False
                return True
            
            return True

        no_check_columns = set(no_use_columns)
        pass_columns = ["pagina", "limite"]
        for key, value in request_response.items():

            if key not in pass_columns:
            
                try:
                    if (key not in no_check_columns) or (key in no_check_columns and value is not None):
                        
                            data_type = correct_types.get(key) if correct_types.get(key) else str
                            
                            if not isinstance(data_type, (list, tuple, set)):
                                data_type = [data_type]

                            data_type = list(data_type)
                            
                            holder = [incorrect_test(d_t, value, key)   for d_t in data_type]
                            if not any(holder):
                                raise

                except:
                    incorrect_values.append(key) 

        # Se alguma chave recebeu um valor de tipo incorreto (str é coringa)
        if incorrect_values:
            incorrect_values = list(set(incorrect_values))
            logger.error(f"As chaves {incorrect_values} estao no tipo incorreto.")
            return {"status":400,
                    "resposta":{
                        "tipo":"4",
                        "descricao":f"Campos {incorrect_values} com valores de tipos incorretos."}}, 400