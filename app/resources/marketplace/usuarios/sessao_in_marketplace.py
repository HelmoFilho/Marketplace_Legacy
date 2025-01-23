#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
from app.server import logger

class SessaoInMarketplace(Resource):

    @logger
    def post(self):
        """
        Método POST do Inicializacao de Sessão do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Dados recebidos
        response_data = dm.get_request(values_upper= True, not_change_values = ["token_aparelho", "token_firebase"])

        # Chaves que precisam ser mandadas
        necessary_keys = ["token_aparelho", "token_firebase", "latitude", "longitude", "os", "modelo_aparelho", "versao_app"]

        not_null = ["token_aparelho", "token_firebase", "os", "modelo_aparelho"]

        no_use_columns = ["latitude", "longitude", "navegador"]

        # Checa chaves inválidas, faltantes e valores incorretos e nulos
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys, 
                                          no_use_columns = no_use_columns,
                                          not_null = not_null)):
            
            return validity

        # Variáveis de controle
        token_aparelho = response_data.get("token_aparelho")
        token_firebase = response_data.get("token_firebase")
        latitude = response_data.get("latitude") if response_data.get("latitude") else None
        longitude = response_data.get("longitude") if response_data.get("longitude") else None
        os = response_data.get("os")
        modelo_aparelho = response_data.get("modelo_aparelho")
        navegador = response_data.get("navegador") if response_data.get("navegador") else "APP"
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        versao_app = response_data.get("versao_app")

        # Verificando situação da sessão
        query = """
            SELECT
                TOP 1 id_sessao

            FROM
                SESSAO

            WHERE
                token_aparelho = :token_aparelho
                AND d_e_l_e_t_ = 0
                AND navegador = :navegador
        """

        params = {
            "token_aparelho": token_aparelho,
            "navegador": navegador
        }

        query = dm.raw_sql_return(query, params = params, raw = True, first = True)
        
        if query:

            # Reabrindo uma sessão
            id_sessao = query[0]

            key_columns = ["token_aparelho", "d_e_l_e_t_", "navegador", "id_sessao"]

            session = {
                "id_sessao": id_sessao,
                "latitude": latitude,
                "longitude": longitude,
                "token_firebase": token_firebase,
                "modelo_aparelho": modelo_aparelho,
                "os": os,
                "inicio_sessao": data_hora,
                "fim_sessao": None,
                "status": "A",
                "versao_app": versao_app,
                "d_e_l_e_t_": 0,
                "navegador": navegador,
                "token_aparelho": token_aparelho
            }

            session_log = {
                "id_sessao": id_sessao,
                "token_aparelho": token_aparelho,
                "token_firebase": token_firebase,
                "latitude": latitude,
                "longitude": longitude,
                "modelo_aparelho": modelo_aparelho,
                "navegador": navegador,
                "os": os,
                "inicio_sessao": data_hora,
                "versao_app": versao_app
            }

            dm.raw_sql_update("SESSAO", session, key_columns)
            dm.raw_sql_insert("SESSAO_LOG", session_log)

            logger.info(f"Sessao {id_sessao} atualizada.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Sessão atualizada."}}, 200 
            
        # Criando uma nova sessão   
        new_session = {
            "token_aparelho": token_aparelho,
            "token_firebase": token_firebase,
            "latitude": latitude,
            "longitude": longitude,
            "modelo_aparelho": modelo_aparelho,
            "navegador": navegador,
            "os": os,
            "d_e_l_e_t_": 0,
            "status": 'A',
            "inicio_sessao": data_hora,
            "versao_app": versao_app
        }

        dm.raw_sql_insert("SESSAO", new_session)

        query = """
            SELECT
                TOP 1 id_sessao
            
            FROM
                SESSAO

            WHERE
                token_aparelho = :token_aparelho
                AND token_firebase = :token_firebase
                AND modelo_aparelho = :modelo_aparelho
                AND navegador = :navegador
                AND os = :os
                AND d_e_l_e_t_ = 0
                AND status = 'A'
                AND inicio_sessao = :inicio_sessao
                AND versao_app = :versao_app
        """

        params = {
            "token_aparelho": token_aparelho,
            "token_firebase": token_firebase,
            "modelo_aparelho": modelo_aparelho,
            "navegador": navegador,
            "os": os,
            "inicio_sessao": data_hora,
            "versao_app": versao_app
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True
        }

        sessao = dm.raw_sql_return(**dict_query)
        id_sessao = sessao[0]

        session_log = {
            "id_sessao": id_sessao,
            "token_aparelho": token_aparelho,
            "token_firebase": token_firebase,
            "latitude": latitude,
            "longitude": longitude,
            "modelo_aparelho": modelo_aparelho,
            "navegador": navegador,
            "os": os,
            "inicio_sessao": data_hora,
            "versao_app": versao_app
        }

        dm.raw_sql_insert("SESSAO_LOG", session_log)

        logger.info(f"Nova sessao {id_sessao} iniciada.")
        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Nova sessão iniciada."}}, 201 