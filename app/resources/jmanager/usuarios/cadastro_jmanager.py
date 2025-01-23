#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime
import re

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class CadastroJManager(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def post(self) -> dict:
        """
        Método POST do Cadastro do JManager

        Returns:
            [dict]: Status da transação
        """
        
        # Pega os dados do front-end
        response_data = dm.get_request(values_upper=True)

        unnecessary_keys = ["id_usuario", "data_registro", "status", "senha_temporaria"]

        necessary_keys = ["id_distribuidor", "id_perfil", "email", "senha", "nome_completo", 
                "cpf", "id_cargo", "telefone", "cep", "endereco", "endereco_num", 
                "complemento", "bairro", "cidade", "estado", "alterar_senha"]

        correct_types = {
            "id_distribuidor": int, "id_perfil": int, "id_cargo": int, "cep": int,
            "endereco_num": int, "complemento": str, "alterar_senha": [bool, str, None, int]
        }

        not_null = [key  for key in necessary_keys  if key != "complemento"]

        # Remove chaves desnecessárias da informação trazida do front-end
        for key in unnecessary_keys:
            try: del response_data[key]
            except: pass

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys, 
                                          no_use_columns = unnecessary_keys,
                                          bool_check_password = False,
                                          not_null = not_null,
                                          correct_types = correct_types)):
            return validity

        nome = str(response_data["nome_completo"]).upper()
        if len(nome.split()) < 2:
            logger.error("Administrador tentou cadastrar usuario sem fornecer nome e sobrenome")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Forneca nome e sobrenome."}}, 200 

        response_data["nome_completo"] = nome

        try:
            cpf = re.sub("[^0-9]", "", response_data["cpf"])
            if not secure.cpf_validator(cpf):
                logger.error("Administrador tentou cadastrar usuario com cpf invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cpf invalido."}}, 200 
            
            response_data["cpf"] = cpf

        except:
            logger.error("Administrador tentou cadastrar usuario com cpf invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cpf invalido."}}, 200 

        # Verifica a existência do usuario
        usuario_query = """
            SELECT
                TOP 1 id_usuario
            
            FROM
                JMANAGER_USUARIO

            WHERE
                cpf = :cpf
                OR email = :email
        """

        params = {
            "cpf": response_data.get("cpf"),
            "email": response_data.get("email")
        }

        dict_query = {
            "query": usuario_query,
            "params": params,
            "raw": True,
            "first": True,
        }

        usuario_query = dm.raw_sql_return(**dict_query)

        if usuario_query:
            logger.error("Usuario ja esta cadastrado.")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Registro já existe."}}, 409

        # Verificando existência do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))
        
        answer = dm.distr_usuario_check(id_distribuidor)
        if not answer[0]:
            return answer[1]
        
        # Verifica a existência do perfil
        perfil_query = """
            SELECT
                TOP 1 id

            FROM
                JMANAGER_PERFIL

            WHERE
                id = :id_perfil
                AND id != 1
        """

        params = {
            'id_perfil': response_data.get("id_perfil")
        }

        dict_query = {
            "query": perfil_query,
            "params": params,
            "raw": True,
            "first": True,
        }

        perfil_query = dm.raw_sql_return(**dict_query)
        
        if not perfil_query:
            logger.error("Variavel perfil fornecida com valor inexistente no database.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Perfil fornecido é inválido."}}, 400

        # Verifica a existência do cargo
        cargo_query = """
            SELECT
                TOP 1 id

            FROM
                JMANAGER_CARGO

            WHERE
                id = :id_cargo
        """

        params = {
            'id_cargo': response_data.get("id_cargo")
        }

        dict_query = {
            "query": cargo_query,
            "params": params,
            "raw": True,
            "first": True,
        }

        cargo_query = dm.raw_sql_return(**dict_query)
        
        if not cargo_query:
            logger.error("Variavel cargo fornecida com valor inexistente no database.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Cargo fornecido é inválido."}}, 400

        # Verificao alterar_senha
        response_data["alterar_senha"] = "S" \
                    if response_data.get("alterar_senha") not in ["n", "N", False, None] else "N"

        if response_data.get("alterar_senha") == "S":
            
            response_data["senha_temporaria"] = response_data.get("senha")
            
            try: del response_data["senha"]
            except: pass
        
        # Criação dos objetos que serão utilizados para atualizar a tabela
        new_user = response_data
        new_user["data_registro"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        new_user["status"] = "A"

        # Comita os dados nas tabelas JMANAGER_USUARIO
        dm.raw_sql_insert("JMANAGER_USUARIO", new_user)

        # Pegando dados para inserir na JMANAGER_USUARIO_DISTRIBUIDOR
        usuario_query = """
            SELECT
                TOP 1 id_usuario
            
            FROM
                JMANAGER_USUARIO

            WHERE
                cpf = :cpf
                AND email = :email
        """

        params = {
            "cpf": response_data.get("cpf"),
            "email": response_data.get("email")
        }

        usuario_query = dm.raw_sql_return(usuario_query, params = params, raw = True, first = True)

        id_usuario = usuario_query[0]

        new_user_distribuidor = {
            "id_usuario": id_usuario,
            "id_distribuidor": response_data.get("id_distribuidor"),
            "d_e_l_e_t_": 0,
            "status": "A"
        }

        dm.raw_sql_insert("JMANAGER_USUARIO_DISTRIBUIDOR", new_user_distribuidor)

        id_usuario_admin = int(global_info.get("id_usuario"))

        logger.info(f"Administrador {id_usuario_admin} cadastrou o usuario {id_usuario} com sucesso.")
        return {"status":201,
                "resposta":{"tipo":"1","descricao":f"Cadastro do jmanager concluido."}}, 201

        
    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def put(self) -> dict:
        """
        Método PUT do Cadastro do JManager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Dados recebidos
        response_data = dm.get_request(norm_keys = True, values_upper = True, trim_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["id_usuario"]
        
        no_use_columns = ["id_perfil", "id_cargo", "telefone", "cep", "endereco",
                            "endereco_num", "complemento", "bairro", "cidade", "estado",
                            "status", "alterar_senha", "nome_completo"]

        correct_types = {
            "id_usuario": int, "id_perfil": int, "id_cargo": int, "cep": int,
            "endereco_num": int, "alterar_senha": [str, bool], "cpf": int
        }

        # Checa chaves inválidas, valores incorretos, nulos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys, 
                                          correct_types = correct_types,
                                          bool_check_password = False, 
                                          no_use_columns = no_use_columns,
                                          not_null = necessary_keys)):
            
            return validity

        # Pegando dados
        nome = response_data.get("nome_completo")
        if nome:
            nome = str(nome).upper()
            if len(nome.split()) < 2:
                logger.error("Usuario tentou se cadastrar sem fornecer nome e sobrenome")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Forneca nome e sobrenome."}}, 200 

            response_data["nome_completo"] = nome

        perfil_token = int(info.get("id_perfil"))
        distribuidor_token = info.get("id_distribuidor")
        usuario_token = int(info.get("id_usuario"))

        id_usuario = int(response_data.get("id_usuario"))

        # Verificando existência do usuario enviado
        query = """
            SELECT
                TOP 1 1

            FROM
                JMANAGER_USUARIO ju

            WHERE
                ju.id_usuario = :id_usuario                
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
            "first": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Administrador informou usuario:{id_usuario} nao-existente para alteracao de informacoes.")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Registro não existe."}}, 409

        # Se o cara é root...
        if perfil_token != 1:

            query = """
                SELECT
                    *

                FROM
                    JMANAGER_USUARIO ju

                    INNER JOIN JMANAGER_USUARIO_DISTRIBUIDOR jud ON
                        ju.id_usuario = jud.id_usuario

                    INNER JOIN DISTRIBUIDOR d ON
                        jud.id_distribuidor = d.id_distribuidor

                WHERE
                    ju.id_usuario = :id_usuario
                    AND jud.status = 'A'
                    AND ju.d_e_l_e_t_ = 0
                    AND d.status = 'A'
            """

            params = {
                "id_usuario": id_usuario
            }

            dict_query = {
                "query": query,
                "params": params,
                "raw": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:
                logger.error(f"Administrador informou usuario:{id_usuario} sem distribuidores ativos para realizar alteração.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario sem distribuidores ativos associados."}}, 400
        
            # Se o cara não é administrador da industria e se o cara não é da mesma industria...
            a = perfil_token == 2
            b = any([id_distribuidor[0] in distribuidor_token  for id_distribuidor in response])

            if not (a and b):
            
                logger.error(f"Admin:{usuario_token} sem permissao de alterar informacoes do U:{id_usuario}.")
                return {"status":403,
                        "resposta":{
                            "tipo":"12",
                            "descricao":f"Usuario sem permissão para realizar ação."}}, 403

        # Objeto de atualização do usuario
        update_user = {}
        logout_key = False

        # Verifica a existência do perfil
        if response_data.get("id_perfil"):

            query = """
                SELECT
                    TOP 1 id

                FROM
                    JMANAGER_PERFIL

                WHERE
                    id = :id_perfil
                    AND id != 1
            """

            params = {
                'id_perfil': response_data.get("id_perfil")
            }

            dict_query = {
                "query": query,
                "params": params,
                "raw": True,
                "first": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:
                logger.error(f"Administrador informou perfil invalido para alteracao de perfil do U:{id_usuario}.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Perfil fornecido é inválido."}}, 400

            update_user["id_perfil"] = response_data.get("id_perfil")

            logout_key = True

        # Verifica a existência do cargo
        if response_data.get("id_cargo"):

            query = """
                SELECT
                    TOP 1 id

                FROM
                    JMANAGER_CARGO

                WHERE
                    id = :id_cargo
            """

            params = {
                'id_cargo': response_data.get("id_cargo")
            }

            dict_query = {
                "query": query,
                "params": params,
                "raw": True,
                "first": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:
                logger.error(f"Administrador informou cargo invalido para alteracao de cargo do U:{id_usuario}.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Cargo fornecido é inválido."}}, 400

            update_user["id_cargo"] = response_data.get("id_cargo")

            logout_key = True

        # Verifica mudança de status
        if response_data.get("status") is not None:

            status = response_data.get("status")

            if status not in ['i', 'I', False]:
                update_user["status"] = "A"
            
            else:
                update_user["status"] = "I"

            logout_key = True

        # Verificar troca de senha
        if response_data.get("alterar_senha") != None:

            alterar_senha = response_data.get("alterar_senha")

            if alterar_senha not in ['n', 'N', False]:
                update_user["alterar_senha"] = "S"
            
            else:
                update_user["alterar_senha"] = "N"
            
            logout_key = True

        # Verificar email
        if response_data.get("email"):

            email = str(response_data.get("email")).upper()

            email_query = """
                SELECT
                    email

                FROM
                    JMANAGER_USUARIO

                WHERE
                    email COLLATE Latin1_General_CI_AI LIKE :email
                    AND id_usuario = :id_usuario
            """

            params = {
                "email": f"%{email}%",
                "id_usuario": id_usuario
            }

            email_query = dm.raw_sql_return(email_query, params = params, raw = True, first = True)

            if not email_query:

                email_query = """
                    SELECT
                        TOP 1 email

                    FROM
                        JMANAGER_USUARIO

                    WHERE
                        email COLLATE Latin1_General_CI_AI LIKE :email
                        AND id_usuario != :id_usuario
                """

                params = {
                    "email": f"%{email}%",
                    "id_usuario": id_usuario
                }

                email_query = dm.raw_sql_return(email_query, params = params, raw = True, first = True)

                if email_query:

                    logger.error(f"Admin:{usuario_token} informou email ja existente para alteracao de email do U:{id_usuario}.")
                    return {"status":400,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: Email fornecido já existente."}}, 400

                update_user["email"] = email
                logout_key = True

        # Verificar cpf
        if response_data.get("cpf"):

            cpf = str(response_data.get("cpf")).upper()

            cpf_query = """
                SELECT
                    cpf

                FROM
                    JMANAGER_USUARIO

                WHERE
                    cpf = :cpf
                    AND id_usuario = :id_usuario
            """

            params = {
                "cpf": cpf,
                "id_usuario": id_usuario
            }

            cpf_query = dm.raw_sql_return(cpf_query, params = params, raw = True, first = True)

            if not cpf_query:

                cpf_query = """
                    SELECT
                        cpf

                    FROM
                        JMANAGER_USUARIO

                    WHERE
                        cpf = :cpf
                        AND id_usuario != :id_usuario
                """

                params = {
                    "cpf": cpf,
                    "id_usuario": id_usuario
                }

                cpf_query = dm.raw_sql_return(cpf_query, params = params, raw = True, first = True)

                if cpf_query:

                    logger.error(f"Admin:{usuario_token} informou cpf ja existente para alteracao de cpf do U:{id_usuario}.")
                    return {"status":400,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: CPF fornecido já existente."}}, 400

                update_user["cpf"] = cpf
                logout_key = True

        # Verificando o resto dos dados
        dont_look = ["id_usuario", "id_distribuidor", "id_perfil", "id_cargo", "status", "alterar_senha", "cpf", "email"]

        for column in dont_look:
            response_data.pop(column, None)

        for key, value in response_data.items():

            if value:

                if key in {"endereco_num", "cep"}:
                    if int(value) >= 0:
                        update_user[key] = value

                if key == "estado":
                    if len(str(value)) == 2:
                        update_user[key] = value

                else:
                    if key == "nome_completo":
                        logout_key = True
                        
                    update_user[key] = value

        # Comitando o resultado
        if update_user:
            
            update_user["id_usuario"] = id_usuario
            key_columns = ["id_usuario"]

            dm.raw_sql_update("JMANAGER_USUARIO", update_user, key_columns)

            if logout_key:

                delete_login =  {
                    "id_usuario": id_usuario
                }

                dm.raw_sql_delete("JMANAGER_LOGIN", delete_login)

            logger.info(f"Admin:{usuario_token} mudanca do registro jmanager U:{id_usuario} concluido.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Mudança do registro jmanager concluido."}}, 200

        logger.info(f"Admin:{usuario_token} nao realizou mudancas no registro jmanager U:{id_usuario}.")
        return {"status":200,
                "resposta":{"tipo":"16","descricao":f"Não houveram modificações na transação."}}, 200