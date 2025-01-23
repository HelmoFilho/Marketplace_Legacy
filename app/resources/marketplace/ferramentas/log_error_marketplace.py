#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
from app.server import logger

class LogErrorMarketplace(Resource):

    @logger
    def post(self):
        """
        Método POST do Log de Error do marketplace

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(bool_save_request=False)

        # Chaves que precisam ser mandadas
        necessary_keys = ["phone_token", "message", "request"]
        not_null = ["phone_token", "message"]

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = necessary_keys,
                                not_null = not_null)):
            
            return validity

        phone_token = response_data.get("phone_token")
        message = response_data.get("message")
        request = response_data.get("request")

        if isinstance(request, str):
            request = request.strip()

        request = request if request else None

        query = """
            INSERT INTO
                LOG_ERROS
                    (
                        session,
                        phone_token,
                        message,
                        request
                    )

                VALUES
                    (
                        (SELECT	TOP 1 id_sessao FROM SESSAO WHERE token_aparelho = :token_aparelho AND d_e_l_e_t_ = 0 ORDER BY inicio_sessao DESC),
                        :token_aparelho,
                        :mensagem,
                        :request
                    )
        """

        params = {
            "token_aparelho": phone_token,
            "mensagem": message,
            "request": request,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        return {"status": 200,
                "resposta": {
                    "tipo": "1", 
                    "descricao": "Log salvo."}}, 200