#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm 
import functions.security as secure

class ListarPerfilJManager(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self):
        """
        Método GET do Listar Perfil JManager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        perfil_token = info.get("id_perfil")

        # Realizando a query
        perfil_query = """
            SELECT
                id,
                UPPER(descricao) descricao

            FROM
                JMANAGER_PERFIL

            WHERE
                (
                    (
                        :id_perfil != 1 
                        AND id != 1
                    )
                        OR
                    (
                        :id_perfil = 1
                    )
                )
        """

        params = {
            "id_perfil": perfil_token
        }

        dict_query = {
            "query": perfil_query,
            "params": params
        }

        # Criando o JSON
        data = dm.raw_sql_return(**dict_query)
        
        if data:
            logger.info("Dados de perfil enviados para o front-end")
            return {"status":200,
                    "resposta":{
                        "tipo":"1","descricao":f"Dados enviados."},
                    "dados": data}, 200
        
        logger.info("Nao existem dados para serem retornados")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200