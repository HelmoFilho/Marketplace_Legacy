#=== Importações de módulos externos ===#
from flask_restful import Resource
import re

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroUsuarioJmanager(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self):
        """
        Método GET do Registro Usuario Jmanager Específico

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Query do usuario
        id_usuario = info.get("id_usuario")
        distribuidores = info.get("id_distribuidor")

        query = """
            SELECT
                TOP 1 ju.id_usuario,
                ju.id_perfil,
                UPPER(jp.descricao) as descricao_perfil,
                ju.id_cargo,
                UPPER(jc.descricao) as descricao_cargo,
                ju.email,
                UPPER(ju.nome_completo) as nome_completo,
                ju.cpf,
                ju.telefone,
                ju.cep,
                ju.endereco,
                ju.endereco_num,
                ju.complemento,
                ju.bairro,
                ju.cidade,
                ju.estado,
                ju.data_registro,
                ju.status

            FROM
                JMANAGER_USUARIO ju

                INNER JOIN JMANAGER_PERFIL jp ON
                    ju.id_perfil = jp.id

                INNER JOIN JMANAGER_CARGO jc ON
                    ju.id_cargo = jc.id

            WHERE
                ju.id_usuario = :id_usuario
                AND ju.status = 'A'
        """
        
        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
        }

        user_data = dm.raw_sql_return(**dict_query)

        # Trazendo os distribuidores atrelados ao usuario
        data = {}

        if query:
            
            query = """
                SELECT
                    d.id_distribuidor,
                    UPPER(d.nome_fantasia) AS nome_fantasia

                FROM
                    DISTRIBUIDOR d

                WHERE
                    (
                        (
                            0 IN :distribuidores
                        )
                            OR
                        (
                            0 NOT IN :distribuidores
                            AND d.id_distribuidor IN :distribuidores
                        )
                    )

                ORDER BY
                    d.id_distribuidor
            """

            params = {
                "distribuidores": distribuidores,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            response = dm.raw_sql_return(**dict_query)

            if response:

                user_data["distribuidores"] = response
                data = user_data

        if data:

            logger.info("Dados do usuario enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados do usuario enviados."},
                    "data": data}, 200   
        
        logger.info(f"Usuario {id_usuario} nao cadastrado ou validado.")
        return {"status":409,
                "resposta":{
                    "tipo":"5",
                    "descricao":f"Registro de usuário não existente."}}, 200


    @logger
    @secure.auth_wrapper()
    def put(self) -> dict:
        """
        Método PUT do Registro de Usuario JManager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Dados recebidos
        response_data = dm.get_request(norm_keys = True, values_upper = True, trim_values = True)

        # Chaves que precisam ser mandadas        
        no_use_columns = ["nome_completo", "telefone", "cep", "endereco","endereco_num", "complemento", 
                            "bairro", "cidade", "estado", "email", "cpf"]

        correct_types = {
            "cep": int, "endereco_num": int
        }

        # Checa chaves inválidas, valores incorretos, nulos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data,
                                          correct_types = correct_types,
                                          bool_check_password = False, 
                                          no_use_columns = no_use_columns)):
            
            return validity

        if response_data.get("email"):
            email = str(response_data.get("email")).upper()
            if not re.match(r"^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", email):
                logger.error("Usuario alterar o email com valor invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: email invalido."}}, 200 

        if response_data.get("nome_completo"):
            nome = str(response_data["nome_completo"]).upper()
            if len(nome.split()) < 2:
                logger.error("Usuario tentou alterar nome sem fornecer nome e sobrenome")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Forneca nome e sobrenome."}}, 200 

            response_data["nome_completo"] = nome

        if response_data.get("cpf"):
            try:
                cpf = re.sub("[^0-9]", "", response_data["cpf"])
                if not secure.cpf_validator(cpf):
                    logger.error("Usuario tentou alterar cpf por valor invalido.")
                    return {"status":400,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: cpf invalido."}}, 200 
                
                response_data["cpf"] = cpf

            except:
                logger.error("Usuario tentou alterar cpf por valor invalido.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cpf invalido."}}, 200 

        # Pegando dados do token
        id_usuario = int(info.get("id_usuario"))

        update_user = {}
        logout_key = False

        # Verificar email
        if response_data.get("email"):

            query = """
                SELECT
                    email

                FROM
                    JMANAGER_USUARIO

                WHERE
                    id_usuario = :id_usuario
                    AND TRIM(email) = :email
            """

            params = {
                "email": email,
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

                query = """
                    SELECT
                        TOP 1 email

                    FROM
                        JMANAGER_USUARIO

                    WHERE
                        id_usuario != :id_usuario
                        AND TRIM(email) = :email
                """

                params = {
                    "id_usuario": id_usuario,
                    "email": email,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "raw": True,
                    "first": True,
                }

                response = dm.raw_sql_return(**dict_query)

                if response:

                    logger.error(f"Usuario informou email ja existente para outro usuario para alteracao de email proprio.")
                    return {"status":400,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: Email fornecido já existente."}}, 400

                update_user["email"] = email
                logout_key = True

            else:
                logger.error("Usuario tentou alterar email pelo mesmo email utilizado por si.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: email já utilizado."}}, 200 

        # Verificar cpf
        if response_data.get("cpf"):

            query = """
                SELECT
                    cpf

                FROM
                    JMANAGER_USUARIO

                WHERE
                    id_usuario = :id_usuario
                    AND cpf = :cpf
            """

            params = {
                "id_usuario": id_usuario,
                "cpf": cpf,
            }

            dict_query = {
                "query": query,
                "params": params,
                "raw": True,
                "first": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:

                query = """
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

                dict_query = {
                    "query": query,
                    "params": params,
                    "raw": True,
                    "first": True,
                }

                response = dm.raw_sql_return(**dict_query)

                if response:

                    logger.error(f"Usuario informou cpf ja existente para outro usuario para alteracao de cpf proprio.")
                    return {"status":400,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: CPF fornecido já existente."}}, 400

                update_user["cpf"] = cpf
                logout_key = True

            else:
                logger.error("Usuario tentou alterar cpf pelo mesmo cpf utilizado por si.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cpf já utilizado."}}, 200 

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

            dm.core_sql_update("JMANAGER_USUARIO", update_user, key_columns)

            if logout_key:

                query = """
                    UPDATE
                        JMANAGER_LOGIN

                    SET
                        manter_logado = NULL,
                        data_login = NULL,
                        token_sessao = NULL

                    WHERE
                        id_usuario = :id_usuario
                """

                params =  {
                    "id_usuario": id_usuario
                }

                dict_query = {
                    "query": query,
                    "params": params,
                }

                dm.raw_sql_execute(**dict_query)

            logger.info(f"Usuario realizou mudancas no proprio registro")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Mudança do registro jmanager concluido."}}, 200

        logger.info(f"Nao foram realizadas mudancas no registro do proprio usuario.")
        return {"status":200,
                "resposta":{
                    "tipo":"16",
                    "descricao":f"Não houveram modificações de dados cadastrais."}}, 200