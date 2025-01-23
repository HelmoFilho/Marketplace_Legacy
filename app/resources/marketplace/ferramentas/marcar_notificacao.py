#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
from app.server import logger


class MarcarNotificacoe(Resource):

    @logger
    def post(self):
        """
        Método POST do Marcar Notificações como Lidas do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        not_null = ["token_firebase", "id_notificacao"]

        correct_type = {
            "id_notificacao": [list, int]
        }

        # Checa chaves inválidas, faltantes e valores incorretos
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = not_null,
                                correct_types = correct_type,
                                not_null = not_null)):
            
            return validity

        # Pegando os dados
        token_firebase = response_data.get("token_firebase")
        id_notificacao = response_data.get("id_notificacao")

        if isinstance(id_notificacao, (list, set, tuple)):
            id_notificacao = list(id_notificacao)

        else:
            id_notificacao = [id_notificacao]

        # Checando a notificacao
        query = """
            SELECT
                DISTINCT
                    id_notificacao,
                    UPPER(status) as status

            FROM
                NOTIFICACAO

            WHERE
                id_notificacao IN :id_notificacao
        """

        params = {
            "id_notificacao": id_notificacao
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.info(f"Notificações não existem.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Notificações não existentes."}}, 200

        # Registro de falhas
        falhas = 0
        falhas_hold = {}
        total = len(response)

        notificacoes = response

        hold_notificacacoes = []

        for notificacao in notificacoes:

            id_notificacao = notificacao.get("id_notificacao")
            status = notificacao.get("status")

            if status != 'E':

                falhas += 1

                if falhas_hold.get("notificacao-nao-enviada"):
                    falhas_hold["notificacao-nao-enviada"]["combinacao"].append({
                        "id_notificacao": id_notificacao,
                        "status": status,
                    })
                
                else:
                    falhas_hold["notificacao-nao-enviada"] = {
                        "motivo": f"Notificação não foi enviada.",
                        "combinacao": [{
                            "id_notificacao": id_notificacao,
                            "status": status,
                        }],
                    }
                continue

            query = """
                SELECT
                    TOP 1 UPPER(status) as status,
                    ISNULL(lida, 0) as lida                

                FROM
                    NOTIFICACAO_USUARIO

                WHERE
                    id_notificacao = :id_notificacao
                    AND token = :token_firebase
            """

            params = {
                "id_notificacao": id_notificacao,
                "token_firebase": token_firebase
            }

            dict_query = {
                "query": query,
                "params": params,
                "raw": True,
                "first": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:

                falhas += 1

                if falhas_hold.get("notificacao-invalida-usuario"):
                    falhas_hold["notificacao-invalida-usuario"]["combinacao"].append({
                        "id_notificacao": id_notificacao,
                    })
                
                else:
                    falhas_hold["notificacao-invalida-usuario"] = {
                        "motivo": f"Notificação não pertence ao token enviado.",
                        "combinacao": [{
                            "id_notificacao": id_notificacao,
                        }],
                    }
                continue

            status_token = response[0]
            lido = response[1]

            if status_token != 'E':

                falhas += 1

                if falhas_hold.get("notificacao-nao-enviada-token"):
                    falhas_hold["notificacao-nao-enviada-token"]["combinacao"].append({
                        "id_notificacao": id_notificacao,
                    })
                
                else:
                    falhas_hold["notificacao-nao-enviada-token"] = {
                        "motivo": f"Notificação não foi enviada para o token em especifico.",
                        "combinacao": [{
                            "id_notificacao": id_notificacao,
                        }],
                    }
                continue

            if lido:

                falhas += 1

                if falhas_hold.get("notificacao-ja-lida"):
                    falhas_hold["notificacao-ja-lida"]["combinacao"].append({
                        "id_notificacao": id_notificacao,
                    })
                
                else:
                    falhas_hold["notificacao-ja-lida"] = {
                        "motivo": f"Notificação já lida para token especifico.",
                        "combinacao": [{
                            "id_notificacao": id_notificacao,
                        }],
                    }
                continue

            hold_notificacacoes.append({
                "id_notificacao": id_notificacao,
                "token": token_firebase,
                "lida": 1
            })

        # Atualizando o status de lida
        if hold_notificacacoes:

            key_columns = ["id_notificacao", "token"]

            dict_update = {
                "table_name": "NOTIFICACAO_USUARIO",
                "data": hold_notificacacoes,
                "key_columns": key_columns
            }
            
            dm.raw_sql_update(**dict_update)

        sucessos = len(hold_notificacacoes)

        if falhas > 0:
            logger.info(f"Hoveram {falhas} erros e {sucessos} sucessos ao marcar as notificacoes como lidas.", extra = falhas_hold)
            return {"status": 200,
                    "resposta": {
                        "tipo": "15", 
                        "descricao": "Houveram notificações que não puderam ser lidas."}}, 200

        logger.info(f"Foram lidas um total de {sucessos} notificações.")
        return {"status": 200,
                "resposta": {
                    "tipo": "1", 
                    "descricao": "Notificações lidas com sucesso."}}, 200