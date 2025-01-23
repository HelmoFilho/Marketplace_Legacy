#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.message_send as ms
import functions.security as secure

class EnvioNotificacao(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1,2,3,4,5,6])
    def post(self) -> dict:
        """
        Método POST de Envio de Notificação

        Returns:
            [dict]: Status da transação
        """

        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(norm_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_notificacao"]

        correct_types = {"id_notificacao": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                not_null = produto_columns, 
                                correct_types = correct_types,
                                comparison_columns = produto_columns)):
            return validity

        # Pegando informacoes do token
        id_notificacao = int(response_data["id_notificacao"])

        query = """
            SELECT
                ntfc.id_notificacao,
                ntfc.id_distribuidor,
                ntfc.status

            FROM
                NOTIFICACAO ntfc

                INNER JOIN ROTAS_APP rapp ON
                    ntfc.id_rota = rapp.id_rota

            WHERE
                ntfc.id_notificacao = :id_notificacao

            ORDER BY
                ntfc.id_notificacao
        """

        params = {
            "id_notificacao": id_notificacao
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Notificacao {id_notificacao} não existe")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: notificação não existente."}}, 200

        # Verificando a notificação
        notificacao = response.copy()

        id_distribuidor = notificacao.get("id_distribuidor")

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        status = notificacao.get("status")

        if status == 'C':
            logger.info(f"Notificação {id_notificacao} com envio cancelado.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: Notificação com envio cancelado."},
                   }, 200

            return msg

        elif status == 'F':
            logger.info(f"Notificação {id_notificacao} falhou no envio anterior.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: Notificação falhou no envio anterior."},
                   }, 200

            return msg

        elif status == 'E':
            logger.info(f"Notificação {id_notificacao} já enviado anteriormente.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: Notificação já enviado anteriormente."},
                   }, 200

            return msg

        elif status != 'P':
            logger.info(f"Notificação {id_notificacao} com status - {status} inválido.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: Notificação com status inválido."},
                   }, 200

            return msg

        # Enviando a notificacao
        nome = info.get("nome")

        dict_notificacao = {
            "id_notificacao": id_notificacao,
            "enviado_por": nome
        }

        answer, response = ms.send_notification(**dict_notificacao)
        if not answer:
            return response

        # Verificando o resultado do envio
        resultado = response.get("resultado")
        sucessos = response.get("sucessos")
        erros = response.get("erros")

        if not resultado:
            return {"status":400,
                    "resposta":{
                        "tipo":"15",
                        "descricao":f"Falha com o envio da notificação."},
                    }, 200

        elif erros > 0:
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Notificação enviada. Houveram {sucessos} sucessos e {erros} falhas."},
                    }, 200

        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Notificação enviada com sucesso."},
                }, 200