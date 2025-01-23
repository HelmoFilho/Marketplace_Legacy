#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm 
import functions.security as secure

class ListarFornecedor(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self) -> dict:
        """
        Método GET do Listar Marcas JManager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Cria o JSON
        id_distribuidor = info.get("id_distribuidor")

        query = f"""
            SELECT
                f.id_fornecedor,
                f.desc_fornecedor,
                f.status,
                f.data_cadastro

            FROM
                FORNECEDOR f

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    f.id_fornecedor = pd.id_fornecedor

            WHERE
                (
                    (
                        0 IN :id_distribuidor
                    )
                        OR
                    (
                        0 NOT IN :id_distribuidor 
                        AND pd.id_distribuidor IN :id_distribuidor
                    )
                )       
        """

        params = {
            "id_distribuidor": id_distribuidor
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        data = dm.raw_sql_return(**dict_query)

        if data:
            logger.info("Dados de fornecedores enviados para o front-end.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados."}, 
                    "dados": data}, 200
        
        logger.info("Nao existem dados para serem retornados.")
        return {"status":404,
                "resposta":{
                    "tipo":"7",
                    "descricao":f"Sem dados para retornar."}}, 200