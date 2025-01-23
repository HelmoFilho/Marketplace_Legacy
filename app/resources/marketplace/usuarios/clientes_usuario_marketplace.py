#=== Importações de módulos externos ===#
from flask_restful import Resource
from flask import request

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ClientesUsuarioMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self):
        """
        Método GET do Clientes do Usuario Marketplace

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get_info_thread().get("token_info")

        id_usuario = info.get("id_usuario")
        id_cliente = None

        data = request.args

        if data:
            if str(dict(data).get("id_cliente")).isdecimal():
                temp_id_cliente = int(data.get("id_cliente"))

                answer, response = dm.cliente_check(temp_id_cliente)
                if not answer:
                    return response
                
                id_cliente = [temp_id_cliente]

        dict_cliente = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        data = dj.json_cliente(**dict_cliente)        

        if data:

            logger.info("Dados do usuario enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados do usuario enviados."},
                    "dados": data}, 200   
        
        logger.info(f"Sem clientes cadastrados ou validados.")
        return {"status":409,
                "resposta":{
                    "tipo":"5",
                    "descricao":f"Registro de usuário não existente."}}, 200   