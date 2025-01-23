#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.payment_management as pm
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class RemocaoCartaoUsuarioMarketplace(Resource):    

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Remocao de cartao do Usuario Marketplace Específico

        Returns:
            [dict]: Status da transação
        """

        id_usuario = global_info.get("id_usuario")

        # Dados recebidos
        response_data = dm.get_request()

        # Chaves que precisam ser mandadas
        necessary_keys = ["id_cartao"]

        correct_types = {
            "id_cartao": int
        }

        # Checagem dos dados de entrada
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys,
                                          not_null = necessary_keys,
                                          correct_types = correct_types)):
            
            return validity

        # Verificando o id_cliente enviado
        id_cartao = int(response_data.get("id_cartao"))

        # Deletando o token na maxipago
        dict_remocao = {
            "id_usuario": id_usuario,
            "id_cartao": id_cartao,
        }

        answer, response = pm.maxipago_remover_cartao(**dict_remocao)
        if not answer:
            return response
            
        # Deletando do banco
        delete_data = {
            "id_usuario": id_usuario,
            "id_cartao": id_cartao,
        }

        dm.raw_sql_delete("USUARIO_CARTAO_DE_CREDITO", delete_data)

        delete_data = {
            "id_cartao": id_cartao,
        }

        dm.raw_sql_delete("CARTAO_MAXIPAGO", delete_data)

        # Pegando cartões restantes
        response = dj.json_cartoes(id_usuario)    

        logger.info(f"Cartao com id {id_cartao} deletado.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Cartão deletado com sucesso."},
                "dados": response}, 200 