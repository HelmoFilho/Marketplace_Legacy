#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class AlterarSenhaJmanager(Resource):

    @logger
    @secure.auth_wrapper(bool_alterar_senha=False)
    def post(self) -> dict:
        """
        Método POST do Alterar Senha do JManager

        Returns:
            [dict]: Status da transação
        """
        id_usuario = int(global_info.get("id_usuario"))

        response_data = dm.get_request(norm_keys = True)

        necessary_keys = ["senha_temporaria", "nova_senha"]

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys, 
                                          bool_check_password = False, 
                                          not_null = necessary_keys)):
            
            return validity

        senha_temporaria = response_data.get("senha_temporaria")
        nova_senha = response_data.get("nova_senha")

        # Query do usuario
        query = """
            SELECT
                TOP 1 senha_temporaria,
                status,
                alterar_senha

            FROM
                JMANAGER_USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True,
        }

        user_query = dm.raw_sql_return(**dict_query)

        if not user_query:
            logger.error("Usuario não encontrado na base.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        senha_temporaria_real = user_query[0]
        status = user_query[1]
        trocar = user_query[2]

        if status == 'P':
            logger.error(f"Usuario com status pendente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário com status pendente."}}, 409

        if status != 'A':
            logger.error(f"Usuario com status inválido '{status}'.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário com status inválido."}}, 409

        if trocar != 'S' or not senha_temporaria_real:
            logger.error("Usuario não solicitou alteração de senha.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        if str(senha_temporaria_real).upper() == str(senha_temporaria).upper():

            # Troca de senha
            update_jmanager_usuario = {
                "id_usuario": id_usuario,
                "senha_temporaria": None,
                "senha": str(nova_senha).upper(),
                "alterar_senha": "N"
            }
            
            key_columns = ["id_usuario"]
            dm.core_sql_update("JMANAGER_USUARIO", update_jmanager_usuario, key_columns)

            update_jmanager_login = {
                "id_usuario": id_usuario,
                'token_sessao': None,
                "manter_logado": None,
                "data_login": None
            }

            dm.core_sql_update("JMANAGER_LOGIN", update_jmanager_login, key_columns)

            logger.info("Senha do usuario alterada com sucesso.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Troca de senha realizada."}}, 200

        logger.error("Usuario errou senha temporaria.")
        return {"status":409,
                "resposta":{
                    "tipo":"9",
                    "descricao":f"Usuário ou senha inválido."}}, 409