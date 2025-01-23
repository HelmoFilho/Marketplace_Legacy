#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RemocaoFavoritosMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Remoção dos Favoritos do Marketplace

        Returns:
            [dict]: Status da transação
        """
        id_usuario = int(global_info.get('id_usuario'))

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["id_produto"]

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys, 
                                not_null = necessary_keys,)):
            
            return validity

        # Verificando se o produto exite nos favoritos do usuario
        id_produto = response_data.get("id_produto")   

        query = """
            SELECT
                1

            FROM
                USUARIO_FAVORITO

            WHERE
                id_usuario = :id_usuario
                AND id_produto = :id_produto
        """

        params = {
            "id_produto": id_produto,
            "id_usuario": id_usuario,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error(f"Produto {id_produto} não faz parte dos favoritos do usuario.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Produto não faz parte dos favoritos do usuario."}}, 200
        
        # Realizando a exclusão
        delete_data = {
            "id_usuario": id_usuario,
            "id_produto": id_produto,
        }

        dm.raw_sql_delete("USUARIO_FAVORITO", delete_data)

        logger.info(f"Produto {id_produto} deletado dos favoritos do usuario")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Produto deletado dos favoritos do usuario."
                }}, 200