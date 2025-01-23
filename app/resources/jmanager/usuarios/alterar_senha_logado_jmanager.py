#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class AlterarSenhaLogadoJmanager(Resource):

    @logger
    @secure.auth_wrapper()
    def put(self) -> dict:
        """
        Método PUT do Alterar Senha de Usuario Logado do JManager

        Returns:
            [dict]: Status da transação
        """
        id_usuario = int(global_info.get("id_usuario"))

        # Verificando dados de entrada
        response_data = dm.get_request()

        necessary_keys = ["senha_atual", "nova_senha"]

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys, 
                                          bool_check_password = False, 
                                          not_null = necessary_keys)):
            
            return validity

        # Query do usuario
        user_query = """
            SELECT
                TOP 1 id_usuario,
                senha

            FROM
                JMANAGER_USUARIO

            WHERE
                id_usuario = :id_usuario
                AND status = 'A'
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": user_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        user_query = dm.raw_sql_return(**dict_query)

        if user_query:

            senha_atual_real = str(user_query[1]).upper()
            senha_atual_digitada = str(response_data.get("senha_atual")).upper()

            if senha_atual_real != senha_atual_digitada:

                # Troca de senha
                nova_senha = str(response_data.get("nova_senha")).upper()

                update = {
                    "id_usuario": id_usuario,
                    "senha": nova_senha,
                }

                key_columns = ["id_usuario"]

                dm.raw_sql_update("Jmanager_Usuario", update, key_columns)

                logger.info(f"Senha do usuario alterada com sucesso.")
                return {"status":200,
                        "resposta":{
                            "tipo":"1",
                            "descricao":f"Troca de senha realizada."}}, 200
            
            logger.error(f"Usuario colocou senha atual incorreta.")
            return {"status":409,
                    "resposta":{"tipo":"9","descricao":f"Usuário ou senha inválido."}}, 200

        logger.error(f"Usuario {id_usuario} não encontrado.")
        return {"status":409,
                "resposta":{
                    "tipo":"9",
                    "descricao":f"Usuário ou senha inválido."}}, 200