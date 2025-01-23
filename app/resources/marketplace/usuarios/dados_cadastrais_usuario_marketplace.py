#=== Importações de módulos externos ===#
from flask_restful import Resource
import re

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class RegistroUsuarioMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self):
        """
        Método GET do Registro do Usuario Marketplace Específico

        Returns:
            [dict]: Status da transação
        """

        info = global_info.get_info_thread().get("token_info")
        id_usuario = info.get("id_usuario")

        data = dj.json_usuario(id_usuario = id_usuario)

        if data:
            logger.info("Dados do usuario enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados do usuario enviados."},
                    "dados": data}, 200   
        
        logger.warning(f"Usuario nao cadastrado, validado ou sem cliente associado.")
        return {"status":409,
                "resposta":{
                    "tipo":"5",
                    "descricao":f"Registro de usuário não existente."}}, 200   


    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Registro do Usuario Marketplace Específico

        Returns:
            [dict]: Status da transação
        """

        id_usuario = global_info.get("id_usuario")

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["nome", "telefone", "senha", "data_nascimento"]

        # Checagem dos dados de entrada
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys,
                                          bool_missing = False)):
            
            return validity

        # Verificando quais dados serão atualizados
        if response_data.get("nome"):
            nome = str(response_data["nome"]).upper()
            if len(nome.split()) < 2:
                logger.error("Usuario tentou se cadastrar sem fornecer nome e sobrenome")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Forneca nome e sobrenome."}}, 200

        update_data = {}

        for key, value in response_data.items():

            if value:

                if key == "nome":
                    update_data[key] = str(value).upper()

                elif key == "telefone":
                    update_data[key] = str(value)

                elif key == "senha": 
                    if len(str(value)) == 32:
                        update_data[key] = str(value).upper()

                elif key == "data_nascimento":
                    if re.match("\\b\d{4}-\d{2}-\d{2}\\b", str(value)):
                        update_data[key] = str(value).upper()

        if update_data:

            update_data["id_usuario"] = id_usuario

            key_columns = ["id_usuario"]

            dm.raw_sql_update("USUARIO", update_data, key_columns)

            logger.info("Modificações dos registros realizadas com sucesso.")
            return {"status":200,
                    "resposta":{
                        "tipo": "1", 
                        "descricao": "Dados do usuario modificados."}}, 200

        logger.info("Não houveram modificações no registro do usuario.")
        return {"status":400,
                "resposta":{
                    "tipo":"16",
                    "descricao":f"Não houveram modificações na transação."
                }}, 200