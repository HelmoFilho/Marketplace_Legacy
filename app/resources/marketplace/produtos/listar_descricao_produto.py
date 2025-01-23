#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm 
from app.server import logger

class ListarDescricaoProduto(Resource):

    @logger
    def get(self) -> dict:
        """
        Método GET do Listar Marcas JManager

        Returns:
            [dict]: Status da transação
        """

        # Cria o JSON
        query = f"""
            SELECT
                DISTINCT id_produto,
                descricao_completa,
                sku,
                dun14

            FROM
                PRODUTO
        """

        data = dm.raw_sql_return(query)

        if data:
            logger.info("Dados de marcas enviados para o front-end.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."}, "dados": data}, 200
        
        logger.info("Nao existem dados para serem retornados.")
        return {"status":404,"resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200