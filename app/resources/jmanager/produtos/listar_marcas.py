#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm 
import functions.security as secure

class ListarMarcasJManager(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1,2,3])
    def get(self, id_distribuidor = None) -> dict:
        """
        Método GET do Listar Marcas JManager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        if id_distribuidor:
            answer, response = dm.distr_usuario_check(id_distribuidor)
            if not answer:
                return response

            distribuidores = [id_distribuidor]

        else:
            global_info.save_info_thread(id_distribuidor = 0)
            distribuidores = info.get('id_distribuidor')

        # Cria o JSON
        marca_query = """
            SELECT
                id_marca,
                id_distribuidor,
                desc_marca,
                status,
                data_cadastro

            FROM
                MARCA

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
        """

        params = {
            "distribuidores": distribuidores
        }

        dict_query = {
            "query": marca_query,
            "params": params,
        }

        data = dm.raw_sql_return(**dict_query)

        if data:
            logger.info("Dados de marcas enviados para o front-end.")
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