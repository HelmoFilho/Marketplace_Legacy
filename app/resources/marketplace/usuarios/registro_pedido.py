#=== Importações de módulos externos ===#
from flask_restful import Resource
import numpy as np

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.payment_management as pm
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class RegistroPedidos(Resource):
      
    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Registro do pedido

        Returns:
            [dict]: Status da transação
        """

        id_usuario = global_info.get("id_usuario")

        to_delete = ["id_usuario", "itens", "id_pedido", "status_pedido", "status_pagamento",
                       "codigo_status", "percent"]

        response_data = dm.get_request(delete_data = to_delete)

        necessary_columns = ["cliente", "metodo_pagamento", "qtde_itens", "id_distribuidor",
                                "bonus", "desconto", "subtotal", "liquido", "id_orcamento",
                                "total", "frete_liquido", "total_parcelado", "frete"]

        no_use_columns = ["cupom", "desconto_cupom", "data_pedido"]

        correct_types = {
            "cliente": dict,
            "id_orcamento": int,
            "id_distribuidor": int,
            "metodo_pagamento": dict,
            "qtde_itens": int,
            "bonus": float,
            "desconto": float,
            "subtotal": float,
            "frete": float,
            "frete_liquido": float,
            "liquido": float,
            "total": float,
            "cupom": dict,
            "desconto_cupom": float,
            "total_parcelado": float
        }

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          not_null = necessary_columns,
                                          no_use_columns = no_use_columns,
                                          correct_types = correct_types)):
            return validity

        # Pegando os dados enviados
        cliente = dict(response_data.get("cliente"))

        if not cliente:
            logger.error(f"Cliente do usuario não enviado.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: id_cliente não foi enviado."}}, 400

        try:
            id_cliente = int(cliente.get("id_cliente"))

        except:
            logger.error(f"id_cliente enviado invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: id_cliente enviado é invalido."}}, 400

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        cliente_query = """
            SELECT
                TOP 1 UPPER(status_receita) as status_receita

            FROM
                CLIENTE

            WHERE
                id_cliente = :id_cliente
        """

        params = {
            "id_cliente": id_cliente
        }

        dict_query = {
            "query": cliente_query,
            "params": params,
            "raw": True,
            "first": True,
        }

        status_receita = dm.raw_sql_return(**dict_query)

        if not status_receita:
            logger.error(f"Cliente C:{id_cliente} enviado, mas o mesmo nao existe.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: id_cliente fornecido é inválido."}}, 200

        status_receita = status_receita[0]

        id_orcamento = int(response_data.get("id_orcamento"))
        id_distribuidor = int(response_data.get("id_distribuidor"))
        qtde_itens = int(response_data.get("qtde_itens"))
        bonus = float(response_data.get("bonus"))
        desconto = float(response_data.get("desconto"))
        subtotal = float(response_data.get("subtotal"))
        liquido = float(response_data.get("liquido"))
        frete_liquido = float(response_data.get("frete_liquido"))
        total = float(response_data.get("total"))
        metodo_pagamento = response_data.get("metodo_pagamento")
        cupom = response_data.get("cupom")
        desconto_cupom = response_data.get("desconto_cupom")
        total_parcelado = float(response_data.get("total_parcelado"))

        # Verificação do orcamento
        orcamento_query = """
            SELECT
                TOP 1 o.status

            FROM
                ORCAMENTO o

                INNER JOIN ORCAMENTO_ITEM oi ON
                    o.id_orcamento = oi.id_orcamento

            WHERE
                o.id_orcamento = :id_orcamento
                AND o.id_distribuidor = :id_distribuidor
                AND o.id_usuario = :id_usuario
                AND o.id_cliente = :id_cliente
                AND oi.d_e_l_e_t_ = 0
                AND o.d_e_l_e_t_ = 0
        """

        params = {
            'id_orcamento': id_orcamento,
            "id_distribuidor": id_distribuidor,
            "id_usuario": id_usuario,
            "id_cliente": id_cliente
        }

        dict_query = {
            "query": orcamento_query,
            "params": params,
            "first": True,
            "raw": True
        }

        orcamento_query = dm.raw_sql_return(**dict_query)

        if not orcamento_query:
            logger.error(f"Id_orcamento:{id_orcamento} nao existe ou nao esta associado ao usuario.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Orçamento enviado não associado ao usuario."}}, 200

        status_orcamento = orcamento_query[0]

        if str(status_orcamento).upper() != 'A':
            logger.error(f"Id_orcamento:{id_orcamento} inativo ou ja finalizado.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Orçamento escolhido inativo ou já finalizado."}}, 200

        pedido_query = """
            SELECT
                TOP 1 id_orcamento

            FROM
                PEDIDO p

                INNER JOIN PEDIDO_ITEM pitm ON
                    p.id_pedido = pitm.id_pedido

            WHERE
                p.id_orcamento = :id_orcamento
                AND p.d_e_l_e_t_ = 0
        """

        params = {
            'id_orcamento': id_orcamento
        }

        pedido_query = dm.raw_sql_return(pedido_query, params = params, first = True, raw = True)

        if pedido_query:
            logger.error(f"Id_orcamento:{id_orcamento} ja esta atrelado a um pedido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Orcamento fornecido já esta atrelado a um pedido."}}, 200

        # Verificando o metodo de pagamento
        id_formapgto = metodo_pagamento.get("id")
        id_condpgto = metodo_pagamento.get("id_condpgto")
        id_cartao = metodo_pagamento.get("id_cartao")
        cvv = metodo_pagamento.get("cvv")

        dict_pgto = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor,
            "id_formapgto": id_formapgto,
            "id_condpgto": id_condpgto,
        }

        answer, response = pm.checar_id_pagamento(**dict_pgto)

        if not answer:
            return response

        # Pegando os limites do cliente
        dict_limite = {
            "id_cliente": id_cliente,
            "id_distribuidor": None
        }

        limite = dj.json_limites(**dict_limite)[0]
        
        valor_pedido_minimo_frete = limite.get("valor_minimo_frete")
        frete = limite.get("valor_frete")

        percentual = response.get("percentual")
        parcelas = response.get("parcelas")
        juros = response.get("juros")

        # Verificando o cupom
        id_cupom = None
        if cupom:
            id_cupom = cupom.get("id_cupom")

            dict_cupom_valid = {
                "id_cliente": id_cliente,
                "id_cupom": id_cupom
            }

            answer, response = dj.json_cupom_validado(**dict_cupom_valid)
            if not answer:

                message = response[0].get("motivo")

                logger.error(f"Cupom invalido por: {message}")
                return {"status":200,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Pedido não-registrado."},
                        "dados": {
                            "status": False,
                            "id_pedido": None,
                            "id_motivo": 15,
                            "motivo": f"Cupom invalido por: {message}"
                        }}, 200

            dict_cupom = {
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor,
                "id_cupom": id_cupom
            }

            cupom = dj.json_cupons(**dict_cupom).get("cupons")[0]

        # Pegando o orcamento
        dict_orcamento = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "id_orcamento": id_orcamento,
            "id_distribuidor": id_distribuidor
        }

        answer, response = dj.json_orcamento(**dict_orcamento)

        if not answer:
            logger.error(f"Id_orcamento:{id_orcamento} nao retornou produtos.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Orçamento escolhido inativo ou já finalizado."}}, 200

        orcamento = response.get("orcamentos")[0]
        ofertas = response.get("ofertas")
        cupons = response.get("cupons")

        if cupom:

            if not cupons:
                logger.error(f"Id_cupom:{id_cupom} nao salvo no carrinho Id_orcamento:{id_orcamento}.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Não existem cupons salvos no carrinho."}}, 200
            
            cupom_check = cupons[0]
            if str(cupom_check.get("id_cupom")) != str(id_cupom):
                logger.error(f"Id_cupom:{id_cupom} nao salvo no carrinho Id_orcamento:{id_orcamento}.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Cupom escolhido nao foi salvo no carrinho."}}, 200

            id_distribuidor_cupom = cupom.get("id_distribuidor")

            if id_distribuidor_cupom not in {0, id_distribuidor}:
                logger.error(f"Id_cupom:{id_cupom} com id_distribuidor diferente do carrinho de Id_orcamento:{id_orcamento}.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Cupom escolhido não-aplicavel ao carrinho."}}, 200                

        itens = orcamento.get("itens")

        dict_verify_products = {
            "itens": itens,
            "qtde_itens": qtde_itens,
            "percentual": percentual,
            "desconto": desconto,
            "bonus": bonus,
            "subtotal": subtotal,
            "frete": frete,
            "frete_liquido": frete_liquido,
            "valor_minimo_frete": valor_pedido_minimo_frete,
            "liquido": liquido,
            "juros": juros,
            "cupom": cupom,
            "desconto_cupom": desconto_cupom,
            "total": total,
            "total_parcelado": total_parcelado
        }

        answer, response = pm.verificar_produto_pedido(**dict_verify_products)

        if not answer:
            return response

        if status_receita[0] != 'A':
            logger.error(f"Cliente com status da receita inativado.")
            return {"status":200,
                    "resposta":{
                        "tipo":"15",
                        "descricao":f"Pedido não-registrado."},
                    "dados": {
                        "status": False,
                        "id_pedido": None,
                        "id_motivo": 15,
                        "motivo": "CNPJ inativo na Receita Federal."
                    }}, 200

        cupom_aplicado = response.get("cupom_aplicado")
        desconto_cupom_aplicado = response.get("desconto_cupom_aplicado")

        # Criando o pedido
        hold_pedido = {
            "id_distribuidor": id_distribuidor,
            "id_cliente": id_cliente,
            "id_usuario": id_usuario,
            "id_orcamento": id_orcamento,
            "percentual": float(np.round(percentual, 2)),
            "product_list": itens,
            "value": float(np.round(total_parcelado, 2)),
            "frete": float(np.round(frete_liquido, 2)),
            "juros": float(np.round(juros, 2)),
            "id_formapgto": id_formapgto,
            "id_condpgto": id_condpgto,
            "parcelas": parcelas,
            "id_cartao": id_cartao,
            "cvv": cvv,
            "id_cupom": id_cupom,
            "desconto_cupom": desconto_cupom_aplicado,
            "cupom_aplicado": cupom_aplicado
        }

        answer, response = pm.registro_pedido(**hold_pedido)

        if not answer:
            dados = response[0].pop("dados", None)
            if dados:
                return {"status":200,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Pedido não-registrado."},
                        "dados": dados}, 200

            return response

        id_pedido = response.pop("id_pedido", None)

        dados = {
            "status": True,
            "id_pedido": id_pedido,
        }

        if id_formapgto == 3:
            dados["pix"] = response

        logger.info(f"Pedido {id_pedido} criado com sucesso.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Pedido recebido com sucesso."},
                "dados": dados}, 200