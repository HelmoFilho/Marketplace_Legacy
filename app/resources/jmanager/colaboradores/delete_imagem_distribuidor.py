#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class DeleteImagemDistribuidor(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1])
    def post(self) -> dict:
        """
        Método POST do Delete de Imagens do Distribuidor

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request()

        necessary_keys = ["id_distribuidor"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys, 
                                correct_types = correct_types)):
            return validity

        # Checando se o distribuidor existe
        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distribuidor_check(id_distribuidor)

        if not answer:
            return response

        distribuidor_query = """
            SELECT
                imagem

            FROM
                DISTRIBUIDOR

            WHERE
                id_distribuidor = :id_distribuidor
        """

        params = {
            "id_distribuidor": id_distribuidor
        }

        distribuidor_query = dm.raw_sql_return(distribuidor_query, params = params, 
                                                raw = True, first = True)

        imagem = distribuidor_query[0]

        if not imagem:
            logger.error(f"Nao existem imagens atreladas ao D:{id_distribuidor}")
            return {"status":200,
                    "resposta":{
                        "tipo": "15", 
                        "descricao": "Sem imagens para serem deletadas."}
                    },200

        imagem = str(distribuidor_query[0])

        # Deletando imagem do banco de dados
        update_data = {
            "imagem": None,
            "id_distribuidor": id_distribuidor
        }

        key_columns = ["id_distribuidor"]

        dm.raw_sql_update("DISTRIBUIDOR", update_data, key_columns)

        # Deletando imagem do servidor
        image_folder = os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS")
        used_folder = os.environ.get("IMAGE_USED_PATH_PS")

        path = f"{image_folder}\\imagens\\{used_folder}\\distribuidores\\{id_distribuidor}\\logos\\{imagem.lower()}"

        if os.path.exists(path):
            os.remove(path)

        logger.info(f"Imagem do D:{id_distribuidor} deletada.")
        return {"status":200,
                "resposta":{
                    "tipo": "1", 
                    "descricao": "Imagem deletada com sucesso."}
                },200