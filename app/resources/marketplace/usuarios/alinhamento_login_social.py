#=== Importações de módulos externos ===#
from flask_restful import Resource
from flask import request
import re

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class AlinhamentoLoginSocial(Resource):

    @logger
    def post(self):
        """
        Método POST do Alinhamento do Login Social

        Returns:
            [dict]: Status da transação
        """

        token_social = request.headers.get("auth-token-social")
        plataforma = request.headers.get("social-platform")

        if not token_social:
            logger.error("Token social nao foi fornecido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"14",
                        "descricao":f"Campos 'token_social' com valores nulos."}}, 400

        if not plataforma:
            logger.error("Plataforma social nao foi fornecido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"14",
                        "descricao":f"Campos 'plataforma' com valores nulos."}}, 400

        plataforma = str(plataforma).lower()

        if plataforma not in ["google", "facebook", "apple"]:
            logger.error(f"Plataforma {plataforma} nao esta entre as disponiveis")
            return {"status":400,
                    "resposta":{
                        "tipo":"14",
                        "descricao":f"Campos 'plataforma' com valores invalidos."}}, 400

        # Dados recebidos
        response_data = dm.get_request(values_upper=True)

        necessary_keys = ["email", "senha", "cnpj"]

        if response_data.get("cnpj"):
            try:
                cnpj = re.sub("[^0-9]", "", response_data.get("cnpj"))
                response_data["cnpj"] = cnpj

            except:
                response_data["cnpj"] = 1

        # Checagem de chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys, 
                                          not_null = necessary_keys)):
            
            return validity

        answer = secure.social_validate(plataforma, token_social)

        if not answer[0]:
            return answer[1]

        social_id = str(answer[1].get(f"social_id"))

        # Verificando dados enviados
        email = response_data.get("email")
        senha = response_data.get("senha")
        cnpj = response_data.get("cnpj")

        user_query = """
            SELECT
                TOP 1 id_usuario,
                status,
                senha,
                id_google,
                id_facebook,
                id_apple

            FROM
                USUARIO

            WHERE
                d_e_l_e_t_ = 0
                AND email COLLATE Latin1_General_CI_AI LIKE :email
        """

        params = {
            "email": email
        }

        user_query = dm.raw_sql_return(user_query, params = params, first = True)

        if not user_query:
            logger.error("Usuario com email informado nao existe")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        # Pegando os dados da query
        id_usuario = user_query.get("id_usuario")
        global_info.save_info_thread(id_usuario = id_usuario)

        status = str(user_query.get("status")).upper()
        user_senha = user_query.get("senha")
        user_social_id = user_query[f"id_{plataforma}"]

        if status == 'P':
            logger.info(f"Usuario com aprovacao pendente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário com aprovação pendente."}}, 409

        if not secure.password_compare(user_senha, senha):
            logger.info(f"Usuario com senha errada.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        # Verificando o cnpj enviado
        cnpj_query = """
            SELECT
                TOP 1 u.id_usuario

            FROM
                USUARIO u

                INNER JOIN USUARIO_CLIENTE uc ON
                    u.id_usuario = uc.id_usuario

                INNER JOIN CLIENTE c ON
                    uc.id_cliente = c.id_cliente

            WHERE
                c.cnpj = :cnpj
                AND u.id_usuario = :id_usuario
                AND c.status = 'A'
                AND uc.status = 'A'
                AND uc.d_e_l_e_t_ = 0
        """

        params = {
            "id_usuario": id_usuario,
            "cnpj": cnpj
        }

        cnpj_query = dm.raw_sql_return(cnpj_query, params = params, first = True)

        if not cnpj_query:
            logger.error(f"Usuario nao atrelado ao cnpj:{cnpj}")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Usuario não-atrelado ao cnpj informado."}}, 200     

        # Verificação do social_id
        social_id_query = f"""
            SELECT
                id_usuario

            FROM
                USUARIO

            WHERE
                id_{plataforma}  = :social_id
        """

        params = {
            "social_id": social_id
        }

        social_id_query = dm.raw_sql_return(social_id_query, params = params, raw = True)

        if social_id_query:

            social_id_query = [i[0]   for i in social_id_query]

            if id_usuario in social_id_query:
                logger.error(f"Usuario ja atrelado ao login social do {plataforma} informado")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario já atrelado a conta informada."}}, 200 

            logger.error(f"Login social requerido pelo usuario ja atrelado a outro usuario")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Conta já-atrelada a outro usuario."}}, 200 

        if user_social_id:
            logger.error(f"Usuario ja atrelado a um login social do {plataforma} diferente")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Usuario já atrelado a outra conta."}}, 200 

        # Atrelação da conta
        update_data = {
            "id_usuario": id_usuario,
            f"id_{plataforma}": social_id
        }

        key_columns = ["id_usuario"]

        dm.raw_sql_update("USUARIO", update_data, key_columns)

        logger.info(f"Usuario atrelado a conta do {plataforma}:{social_id} com sucesso")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Usuario atrelado a conta informada."}}, 200 