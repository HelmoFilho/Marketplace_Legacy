#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
from app.server import logger

class SessaoOutMarketplace(Resource):

    @logger
    def post(self):
        """
        Método POST da Finalização de Sessão do Marketplace

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(values_upper=True, not_change_values = ["token_aparelho"])

        necessary_keys = ["token_aparelho"]
        no_use_columns = ["navegador"]

        # Checa chaves inválidas e faltantes e valores incorretos e nulos
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = necessary_keys,
                                            no_use_columns = no_use_columns)):
            
            return validity

        # Variaveis de controle
        token_aparelho = response_data.get("token_aparelho")
        navegador = response_data.get("navegador") if response_data.get("navegador") else "APP"
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Verificando a sessão
        query = """
            SELECT
                id_sessao
            
            FROM
                SESSAO

            WHERE
                token_aparelho = :token_aparelho
                AND navegador = :navegador
                AND status = 'A'
                AND d_e_l_e_t_ = 0
        """

        params = {
            "token_aparelho": token_aparelho,
            "navegador": navegador
        }

        query = dm.raw_sql_return(query, params = params, raw = True, first = True)
        
        if not query:
            logger.info("Sessao nao existente.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Sessão não existente."}}, 400
            
        # Finalizando a sessão
        id_sessao = query[0]

        update_data = {
            "id_sessao": id_sessao,
            "status": "I",
            "d_e_l_e_t_": 0,
            "navegador": navegador,
            "fim_sessao": data_hora
        }

        exception = {
            "status": 'A'
        }

        key_columns = ["id_sessao", "navegador", "d_e_l_e_t_"]

        dm.raw_sql_update("SESSAO" , update_data, key_columns, exceptions_columns = exception)

        logger.info(f"Sessao {id_sessao} finalizada.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Sessão finalizada."}}, 200 