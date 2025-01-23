#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure
from app.server import logger

class LimiteCreditoCliente(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Clientes do Usuario Marketplace

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

        dict_limite_credito = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

        distribuidor_query = dj.json_limite_credito(**dict_limite_credito)

        data = {}

        if distribuidor_query:
            data = {
                "id_cliente": id_cliente,
                "limites": distribuidor_query
            }

        if data:

            logger.info(f"Informacoes de limite de credito enviadas para o usuario.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados."},
                    "dados": data}, 200   
        
        logger.info(f"Nao existem informacoes de limite de credito para o usuario.")
        return {"status":404,
                "resposta":{
                    "tipo":"7",
                    "descricao":f"Sem dados para retornar."}}, 200   