#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure
from app.server import logger

class Limites(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Limites do Marketplace

        Returns:
            [dict]: Status da transação
        """
        # Pega os dados do front-end
        response_data = dm.get_request()

        # Checa dados enviados
        necessary_keys = ["id_cliente"]

        no_use_columns = ["id_distribuidor"]
        
        correct_types = {
            "id_distribuidor": [int, list],
            "id_cliente": int
        }

        if (validity := dm.check_validity(request_response = response_data,
                                            comparison_columns = necessary_keys,
                                            no_use_columns = no_use_columns, 
                                            correct_types = correct_types)):
            
            return validity

        # Pegando os dados enviados
        id_cliente = response_data.get("id_cliente")
        id_distribuidor = response_data.get("id_distribuidor")

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response
        
        # Pegando o limite de credito
        dict_limites = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

        dados = dj.json_limites(**dict_limites)

        if not dados:
            logger.info(f"Nao existem informacoes de limite cadastradas para o usuario.")
            return {"status":404,
                    "resposta":{
                        "tipo":"7",
                        "descricao":f"Sem dados para retornar."}}, 200

        logger.info(f"Informacoes de limites enviados para o usuario.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Dados enviados."},
                "dados": {
                    "id_cliente": id_cliente,
                    "limites": dados,
                }}, 200 