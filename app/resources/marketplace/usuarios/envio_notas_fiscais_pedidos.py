#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.message_send as sm
import functions.security as secure

class EnvioNotasFiscaisPedidos(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Envio de Notas fiscais do pedido

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request()

        necessary_keys = ["id_cliente", "id_pedido"]

        correct_types = {
            "id_cliente": int,
            "id_pedido": int
        }

        # Checa body do request
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys, 
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity

        id_cliente = int(response_data.get("id_cliente"))
        id_pedido = int(response_data.get("id_pedido"))

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        id_usuario = int(global_info.get("id_usuario"))
        email = global_info.get("email")

        # Pegando os dados do pedido
        query = """
            SELECT
                TOP 1 pddetp.id_etapa,
                pddetp.id_subetapa

            FROM
                PEDIDO pdd

                LEFT JOIN
                (
                
                    SELECT
                        id_pedido,
                        id_etapa,
                        MAX(id_subetapa) as id_subetapa

                    FROM
                        PEDIDO_ETAPA

                    WHERE
                        id_pedido = :id_pedido
                        AND id_etapa = (
                                        
                                            SELECT
                                                MAX(id_etapa)

                                            FROM
                                                PEDIDO_ETAPA

                                            WHERE
                                                id_pedido = :id_pedido

                                       )

                    GROUP BY
                        id_pedido, id_etapa
                
                ) pddetp ON
                    pdd.id_pedido = pddetp.id_pedido

            WHERE
                pdd.id_pedido = :id_pedido
                AND pdd.id_usuario = :id_usuario
                AND pdd.id_cliente = :id_cliente
        """

        params = {
            "id_pedido": id_pedido,
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Pedido {id_pedido} nao atrelado ao cliente ou não existente.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Pedido não atrelado ao cliente ou inexistente."}}, 200

        id_subetapa = response[-1]

        if id_subetapa == 9:
            logger.info(f"Pedido {id_pedido} cancelado.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Pedido cancelado."}}, 200

        query = """
            SELECT
                nf,
                serie

            FROM
                (
                    SELECT
                        DISTINCT
                            tipo,
                            nf,
                            serie

                    FROM
                        PEDIDO_JSL

                    WHERE
                        id_pedido = :id_pedido
                        AND LEN(nf) > 0
                        AND LEN(serie) > 0
                ) nf

            ORDER BY
                CASE WHEN tipo = 'V' THEN 0 ELSE 1 END
        """

        params = {
            "id_pedido": id_pedido,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.info(f"Pedido {id_pedido} sem informacoes de nota fiscal salvas.")
            return {"status":200,
                    "resposta":{
                        "tipo": "18", 
                        "descricao": "Sem notas fiscais salvas."}}, 200

        erros = 0
        url = os.environ.get("JSLEIMAN_DANFE_URL_PS")

        for cotacao in response:

            nf = cotacao.get("nf")
            serie = cotacao.get("serie")

            params = {
                "NotaFisca": nf,
                "Serie": serie,
                "Tipo": "D",
                "EmailAdd": email
            }

            dict_request = {
                "url": url,
                "method": "get",
                "params": params,
                "timeout": None
            }

            answer, content = sm.request_api(**dict_request)

            if not answer:
                logger.error(f"Erro com o envio da nota fiscal do pedido {id_pedido} de nf - {nf} e serie - {serie}. Erro => {content.content}")
                erros += 1

        if erros == 0:
            logger.info(f"Notas fiscais do Pedido {id_pedido} enviadas para o email {email}.")
            return {"status":200,
                    "resposta":{
                        "tipo": "1", 
                        "descricao": "Notas fiscais enviadas para o email."}}, 200

        logger.info(f"Houveram {erros} erros com o envio de nota fiscal do pedido {id_pedido}.")
        return {"status":200,
                "resposta":{
                    "tipo": "18", 
                    "descricao": "Houveram erros com o envio de nota fiscal. Por favor tente novamente mais tarde."}}, 200