#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime, timedelta

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class LoginIntegracao(Resource):

    @logger
    def post(self):
        """
        Método POST do Login da integração

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(bool_save_request = False)

        necessary_keys = ["usuario", "senha"]

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                bool_check_password = False)):
            
            return validity[0], 200  

        usuario = response_data.get("usuario")
        senha = response_data.get("senha")

        if usuario != "usuario-teste" or senha != "senha-teste":
            logger.error("Usuario ou senha inválida")
            return {"status":409,
                    "resposta":{
                        "tipo": "9", 
                        "descricao": "Usuario ou senha inválida."}}, 409

        payload = {
            "exp": datetime.utcnow() + timedelta(hours = 8),
            "data_login": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        }

        private_key = open(file = f"C:\\Users\\t_jsantosfilho\\keys\\api-registro", mode = "r").read()

        dict_encode = {
            "payload": payload,
            "private_key": private_key,
        }

        token = secure.encode_token(**dict_encode)

        logger.info("Token para requisição da api de registro enviada.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Token enviado."}, 
                "token": token}, 200