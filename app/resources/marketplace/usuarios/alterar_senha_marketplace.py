#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class AlterarSenhaMarketplace(Resource):

    @logger
    @secure.auth_wrapper(bool_alterar_senha = False)
    def post(self) -> dict:
        """
        Método POST do Alterar Senha do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Pegando e validando os dados da request
        response_data = dm.get_request(values_upper = True)

        necessary_keys = ["senha_temporaria", "nova_senha"]

        # Checa chaves inválidas e faltantes, valores nulos e se o usuario não existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys,
                                          not_null = necessary_keys)):
            
            return validity

        id_usuario = global_info.get("id_usuario")

        # Verificando a situação do usuario
        user_query = """
            SELECT
                TOP 1 id_usuario,
                status,
                senha,
                senha_temporaria,
                alterar_senha

            FROM
                USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        user_query = dm.raw_sql_return(user_query, params = params, first = True)

        if not user_query:
            logger.warning("Usuario do token nao encontrado na tabela.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        # Verificando as credenciais enviadas
        senha_temporaria_enviada = response_data.get("senha_temporaria")
        nova_senha_enviada = response_data.get("nova_senha")

        senha_temporaria = user_query.get("senha_temporaria")
        status = user_query.get("status")
        trocar = user_query.get("alterar_senha")

        token = global_info.get("token")

        if trocar != "S" or not senha_temporaria:
            logger.error(f"Usuario nao solicitou alterar senha.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Usuário não solicitou alteração senha."}}, 400

        # Verificando se o usuario pode alterar a senha
        autorizacao = secure.password_compare(senha_temporaria_enviada, senha_temporaria) and \
                            status == "A"

        if autorizacao:

            # Troca de senha
            to_update = {
                "id_usuario": id_usuario,
                "d_e_l_e_t_": 0,
                "senha_temporaria": None,
                "alterar_senha": "N",
                "senha": nova_senha_enviada
            }

            key_columns = ["id_usuario", "d_e_l_e_t_"]
            
            exception = {
                "senha_temporaria": senha_temporaria,
                "alterar_senha": "S"
            }

            dm.raw_sql_update("USUARIO", to_update, key_columns, exceptions_columns = exception)

            # Invalidando o token
            to_update = {
                "id_usuario": id_usuario,
                "token": token,
                "data_logout": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "d_e_l_e_t_": 1
            }

            key_columns = ["id_usuario", "token"]

            exception = {
                "data_logout": None,
                "d_e_l_e_t_": 0
            }

            dm.raw_sql_update("Login", to_update, key_columns, exceptions_columns = exception)

            logger.info("Senha alterada com sucesso.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Troca de senha realizada."}}, 200

        logger.error("Senha temporaria informada e incorreta.")
        return {"status":409,
                "resposta":{"tipo":"9","descricao":f"Usuário ou senha inválido."}}, 409