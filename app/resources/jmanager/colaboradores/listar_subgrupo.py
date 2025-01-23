#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm 
import functions.security as secure

class ListarSubgrupo(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self):
        """
        Método GET do Listar Subgrupo

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Realizando a query
        query = f"""
            SELECT
                *

            FROM
                SUBGRUPO
            
            WHERE
                (
                    (
                        0 NOT IN :distribuidores
                        AND id_distribuidor IN :distribuidores 
                    )
                        OR
                    (
                        0 IN :distribuidores
                    )
                )

            ORDER BY
                descricao ASC
        """

        params = {
            "distribuidores": info.get("id_distribuidor")
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        # Criando o JSON
        data = dm.raw_sql_return(**dict_query)
        
        if data:
            logger.info("Dados de subgrupo enviados para o front-end")
            return {"status":200,
                    "resposta":{
                        "tipo":"1","descricao":f"Dados enviados."}, "dados": data}, 200
        
        logger.logger("Nao existem dados para serem retornados")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200