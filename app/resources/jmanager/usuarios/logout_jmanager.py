#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class LogoutJmanager(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Logout do JManager

        Returns:
            [dict]: Status da transação
        """
        id_usuario = global_info.get("id_usuario")
        token = global_info.get("token")

        update_query = """
            UPDATE
                JMANAGER_LOGIN

            SET
                token_sessao = NULL,
                data_login = NULL,
                manter_logado = NULL

            WHERE
                id_usuario = :id_usuario
                AND token_sessao = :token_sessao
        """

        params = {
            "id_usuario": id_usuario,
            "token_sessao": token
        }

        dm.raw_sql_execute(update_query, params = params)

        logger.info(f"Usuario deslogado e token deletado.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Usuário deslogado com sucesso."}}, 200