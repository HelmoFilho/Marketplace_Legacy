#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.payment_management as pm
import functions.data_management as dm
import functions.security as secure

class ChecarStatusPagamentoPix(Resource):
      
    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Checar status do Pagamento Pix

        Returns:
            [dict]: Status da transação
        """
        id_usuario = global_info.get("id_usuario")

        response_data = dm.get_request()

        necessary_columns = ["id_pedido", "id_cliente"]

        correct_types = {
            "id_pedido": int,
            "id_cliente": int
        }

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          not_null = necessary_columns,
                                          correct_types = correct_types)):
            return validity

        # Verificando o id_cliente utilizado
        id_cliente = int(response_data.get("id_cliente"))
        id_pedido = int(response_data.get("id_pedido"))
        
        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        # Checando a existencia do pedido
        query = """
            SELECT
                TOP 1 pdd.forma_pagamento,
                pdd.valor,
                pdd.id_distribuidor,               
                pxc.txid,
                pxc.copia_e_cola,
                pxc.data_expiracao,
                pxc.valor,
                pxc.status

            FROM
                PEDIDO pdd

                LEFT JOIN
                (
                
                    SELECT
                        TOP 1 pddpx.id_pedido,
                        pxc.txid,
                        pxc.copia_e_cola,
                        pxc.data_expiracao,
                        pxc.valor,
                        pxc.status

                    FROM
                        PEDIDO_PIX pddpx

                        INNER JOIN PIX_COBRANCAS pxc ON
                            pddpx.id_cobranca = pxc.id_cobranca

                    WHERE
                        pddpx.id_pedido = :id_pedido
                        AND pxc.id_usuario = :id_usuario
                        AND pxc.d_e_l_e_t_ = 0

                    ORDER BY
                        pxc.data_expiracao DESC

                ) pxc ON
                    pdd.id_pedido = pxc.id_pedido

            WHERE
                pdd.id_pedido = :id_pedido
                AND pdd.id_usuario = :id_usuario
                AND pdd.id_cliente = :id_cliente            
        """

        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "id_pedido": id_pedido,
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error(f"Pedido {id_pedido} não existente para o usuario informado.")
            return {"status":400, 
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Pedido não existênte."}}, 200
        
        pedido = response
        id_formapgto = pedido.get("forma_pagamento")
        valor_pedido = pedido.get("valor")
        id_distribuidor = pedido.get("id_distribuidor")

        if id_formapgto != 3:
            logger.error(f"Pedido {id_pedido} com forma de pagamento diferente de pix.")
            return {"status":400, 
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Pedido com forma de pagamento diferente de pix."}}, 200
        
        # Checando a finalização do pedido
        query = """
            SELECT
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
        """

        params = {
            "id_pedido": id_pedido,
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error(f"Pedido {id_pedido} sem etapas salvas. Favor verificar.")
            return {"status":400, 
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Pedido não existênte."}}, 200
        
        id_etapa = response[0]
        id_subetapa = response[1]

        if id_subetapa == 9:
            logger.error(f"Pedido {id_pedido} já cancelado.")
            return {"status":400, 
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Pedido já cancelado."}}, 200
        
        if id_etapa >= 2:
            return {"status":200, 
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Informações do pix enviado"},
                    "dados": {
                        "txid": pedido.get("txid"),
                        "status": "CONCLUIDA",
                    }}, 200
        
        # Pegando a ultima cobranca
        cobranca = {}

        if pedido.get("txid"):
            
            cobranca = {
                "txid": pedido.get("txid"),
                "copia_e_cola": pedido.get("copia_e_cola"),
                "data_expiracao": pedido.get("data_expiracao"),
                "valor": pedido.get("valor"),
                "status": pedido.get("status"),
            }

        if not cobranca:
            
            dict_cobranca = {
                "id_usuario": id_usuario,
                "valor": valor_pedido,
            }

            answer, response = pm.pix_gerar_cobranca(**dict_cobranca)
            if not answer:
                return {"status":400, 
                        "resposta":{
                            "tipo":"18",
                            "descricao":f"Erro ao gerar um novo pix. Por favor aguarde alguns minutos e tente novamente."}}, 200
            
            cobranca = response
            id_cobranca = cobranca.get("id_cobranca")

            dict_pedido_cobranca = {
                "id_pedido": id_pedido,
                "id_distribuidor": id_distribuidor,
                "id_cobranca": id_cobranca,
            }

            pm.pix_alinhar_pedido_cobranca(**dict_pedido_cobranca)

        else:

            txid = cobranca.get("txid")
            old_status = cobranca.get("status")
            
            answer, response = pm.pix_checar_status(txid)
            if not answer:
                return response
            
            new_status = response.get("status")

            if old_status != new_status:

                query = """
                    UPDATE
                        PIX_COBRANCAS

                    SET
                        status = :status

                    WHERE
                        txid = :txid
                """

                params = {
                    "txid": txid,
                    "status": new_status,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                }

                dm.raw_sql_execute(**dict_query)

                cobranca["status"] = new_status
                
            if new_status == "CONCLUIDA":

                dict_atualizar_etapa = {
                    "id_pedido": id_pedido,
                    "id_formapgto": 3,
                    "id_etapa": 2,
                    "id_subetapa": 1
                }

                pm.atualizar_etapa_pedido(**dict_atualizar_etapa)

            elif new_status != "ATIVA":

                dict_cobranca = {
                    "id_usuario": id_usuario,
                    "valor": valor_pedido,
                }

                answer, response = pm.pix_gerar_cobranca(**dict_cobranca)
                if not answer:
                    return {"status":400, 
                            "resposta":{
                                "tipo":"18",
                                "descricao":f"Erro ao gerar um novo pix. Por favor aguarde alguns minutos e tente novamente."}}, 200
                
                cobranca = response
                id_cobranca = cobranca.get("id_cobranca")

                dict_pedido_cobranca = {
                    "id_pedido": id_pedido,
                    "id_distribuidor": id_distribuidor,
                    "id_cobranca": id_cobranca,
                }

                pm.pix_alinhar_pedido_cobranca(**dict_pedido_cobranca)

        dados = {
            "txid": cobranca.get("txid"),
            "copia_e_cola": cobranca.get("copia_e_cola"),
            "data_expiracao": cobranca.get("data_expiracao"),
            "valor": cobranca.get("valor"),
            "status": cobranca.get("status"),
        }

        logger.info(f"Informações do pix do pedido {id_pedido} foram enviadas")
        return {"status":200, 
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Informações do pix enviado"},
                "dados": dados}, 200