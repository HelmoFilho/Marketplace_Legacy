#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure
from app.server import logger

class ListarFretesDistribuidores(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self, id_cliente = None):
        """
        Método GET do Listar Fretes do Distribuidor
        Returns:
            [dict]: Status da transação
        """
        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        # Criando o JSON
        data = dj.json_fretes_distribuidor(id_cliente)

        if data:

            logger.info("Dados de distribuidor enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados."}, 
                    "dados": {
                        "id_cliente": id_cliente,
                        "fretes": data
                    }}, 200
        
        logger.info("Nao existem dados de distribuidor para enviar.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200