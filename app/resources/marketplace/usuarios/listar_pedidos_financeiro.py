#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ListarFinanceiro(Resource):
      
    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Listar pedidos

        Returns:
            [dict]: Status da transação
        """
        id_usuario = int(global_info.get('id_usuario'))

        response_data = dm.get_request()

        necessary_columns = ["id_cliente"]
        no_use_columns = ["id_distribuidor", "data_pedido_de", "data_pedido_ate", "busca", "status"]

        correct_types = {
            "id_cliente": int,
            "id_distribuidor": [list, int],
        }

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          no_use_columns = no_use_columns,
                                          not_null = necessary_columns,
                                          correct_types = correct_types)):
            return validity

        pagina, limite = dm.page_limit_handler(response_data)

        id_cliente = int(response_data.get("id_cliente"))
        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        # Pegando os distribuidores dos pedidos do cliente
        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        string_id_distribuidor = ""
        string_data_pedido_de = ""
        string_data_pedido_ate = ""
        string_busca = ""
        string_status = ""

        ## Filtro do distribuidor
        id_distribuidor = response_data.get("id_distribuidor")

        if id_distribuidor:

            if type(id_distribuidor) not in [list, tuple, set]:
                id_distribuidor = [id_distribuidor]

            id_distribuidor = list(id_distribuidor)
            string_id_distribuidor = "AND pdd.id_distribuidor IN :distribuidores"
            
            params["distribuidores"] = id_distribuidor

        ## Filtro por data de criacao
        data_pedido_de = response_data.get("data_pedido_de")

        if data_pedido_de:
            try:
                pattern = "%Y-%m-%d"
                datetime.datetime.strptime(data_pedido_de, pattern)
            
            except:
                logger.error("Variavel data_pedido_de com valor invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"4",
                            "descricao":"Campos ['data_pedido_de'] com valores de tipos incorretos."}}, 400

            string_data_pedido_de = "AND pdd.data_criacao >= :data_pedido_de"
            params["data_pedido_de"] = data_pedido_de

        data_pedido_ate = response_data.get("data_pedido_ate")

        if data_pedido_ate:
            try:
                pattern = "%Y-%m-%d"
                datetime.datetime.strptime(data_pedido_ate, pattern)
            
            except:
                logger.error("Variavel data_pedido_ate com valor invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"4",
                            "descricao":"Campos ['data_pedido_ate'] com valores de tipos incorretos."}}, 400

            string_data_pedido_ate = "AND pdd.data_criacao <= :data_pedido_ate"
            params["data_pedido_ate"] = data_pedido_ate

        ## Filtro por campo de busca
        busca = response_data.get("busca")

        if busca:

            busca = str(busca)

            params.update({
                f"word": busca
            })

            string_busca += f"""AND (
                pdd.id_pedido = :word
            )"""

        # Filtro de status
        status = response_data.get("status")

        if status:

            status = str(status).upper()
            if status in ["PAGO", "EM ATRASO", "EM ABERTO", "CANCELADO"]:

                string_status = "AND status_pedido = :status"

                params.update({
                    f"status": status
                })

        # Realizando a query de pedido
        pedido_query = f"""
            SELECT
                pdd.id_pedido,
                pdd.id_distribuidor,
                fp.id_formapgto,
                UPPER(fp.descricao) as forma_pagamento,
                cp.id_condpgto,
                UPPER(cp.descricao) as condicao_pagamento,
                SUM(pddf.valor_boleto) OVER(PARTITION BY pdd.id_pedido) as valor_total,
                SUM(pddf.valor_restante) OVER(PARTITION BY pdd.id_pedido) as saldo_total,
                pddj.nf as nota_fiscal,
                pddj.serie,
                pddf.parcela,
                pddf.titulo,
                pddf.codigo_barra,
                pddf.valor_boleto,
                pddf.valor_restante,
                pddf.dias_atraso,
                pddf.data_vencimento,
                pddf.data_pagamento,
                pddf.data_baixa,
                UPPER(pddf.status_pagamento) as status,  
                status_pedido,             
                p.count__

            FROM
                (

                    SELECT
                        id_pedido,
                        status_pedido,
                        COUNT(*) OVER() as count__

                    FROM
                        (
                                    
                            SELECT
                                DISTINCT pdd.id_pedido,
                                pdd.data_criacao,
                                CASE
                                    WHEN EXISTS (SELECT 1 FROM PEDIDO_FINANCEIRO WHERE id_pedido = pdd.id_pedido AND status_pagamento = 'CANCELADO')
                                        THEN 'CANCELADO'
                                    WHEN EXISTS (SELECT 1 FROM PEDIDO_FINANCEIRO WHERE id_pedido = pdd.id_pedido AND status_pagamento = 'EM ATRASO')
                                        THEN 'EM ATRASO'
                                    WHEN EXISTS (SELECT 1 FROM PEDIDO_FINANCEIRO WHERE id_pedido = pdd.id_pedido AND status_pagamento = 'EM ABERTO')
                                        THEN 'EM ABERTO'
                                    ELSE
                                        'PAGO'
                                END status_pedido

                            FROM
                                PEDIDO pdd

                                INNER JOIN PEDIDO_ETAPA pddetp ON
                                    pdd.id_pedido = pddetp.id_pedido
                                    AND pdd.id_distribuidor = pddetp.id_distribuidor
                                    AND pdd.id_usuario = pddetp.id_usuario
                                    AND pdd.id_cliente = pddetp.id_cliente

                                INNER JOIN PEDIDO_FINANCEIRO pddf ON
                                    pdd.id_pedido = pddf.id_pedido
                                    AND pdd.id_distribuidor = pddf.id_distribuidor

                            WHERE
                                pdd.id_usuario = :id_usuario
                                AND pdd.id_cliente = :id_cliente
                                AND pdd.forma_pagamento = 1
                                AND CONVERT(DATE, pdd.data_criacao) >= DATEADD(MONTH, -12, GETDATE())
                                {string_id_distribuidor}
                                {string_data_pedido_de}
                                {string_data_pedido_ate}
                                {string_busca}
                                AND d_e_l_e_t_ = 0
                                    
                        ) pdd

                    WHERE
                        1 = 1
                        {string_status}

                    ORDER BY
                        pdd.data_criacao DESC

                    OFFSET
                        :offset ROWS

                    FETCH
                        NEXT :limite ROWS ONLY

                ) p

                INNER JOIN PEDIDO pdd ON
                    p.id_pedido = pdd.id_pedido

                INNER JOIN FORMA_PAGAMENTO fp ON
                    pdd.forma_pagamento = fp.id_formapgto

                INNER JOIN CONDICAO_PAGAMENTO cp ON
                    pdd.condicao_pagamento = cp.id_condpgto
                    AND pdd.forma_pagamento = cp.id_formapgto

                INNER JOIN PEDIDO_JSL pddj ON
                    pdd.id_pedido = pddj.id_pedido

                INNER JOIN PEDIDO_FINANCEIRO pddf ON
                    pdd.id_pedido = pddf.id_pedido
                    AND pdd.id_distribuidor = pddf.id_distribuidor

            WHERE
                pddj.tipo = 'V'

            ORDER BY
                pdd.data_criacao DESC
        """

        dict_query = {
            "query": pedido_query,
            "params": params,
        }

        pedido_query = dm.raw_sql_return(**dict_query)

        if not pedido_query:
            logger.info(f"Para os filtros informados, nao existem boletos salvos.")
            return {"status":404,
                    "resposta":{
                        "tipo": "7", 
                        "descricao": "Sem dados para retornar."}}, 200

        counter = pedido_query[0].get("count__")
        pedidos = []

        for pedido in pedido_query:

            id_pedido = pedido.get("id_pedido")
            parcela = pedido.get("parcela")

            for saved_pedido in pedidos:
                
                if saved_pedido.get("id_pedido") == id_pedido:
                    
                    saved_boletos = saved_pedido.get("boletos")

                    for saved_boleto in saved_boletos:

                        if saved_boleto.get("parcela") == parcela:
                            break

                    else:

                        saved_pedido["boletos"].append({
                            "parcela": parcela,
                            "titulo": pedido.get("titulo"),
                            "codigo_barra": pedido.get("codigo_barra"),
                            "valor_boleto": pedido.get("valor_boleto"),
                            "valor_restante": pedido.get("valor_restante"),
                            "dias_atraso": pedido.get("dias_atraso"),
                            "data_vencimento": pedido.get("data_vencimento"),
                            "data_pagamento": pedido.get("data_pagamento"),
                            "data_baixa": pedido.get("data_baixa"),
                            "status": pedido.get("status"),
                        })

                    break
                        

            else:
                
                bool_nf = bool(pedido.get("nota_fiscal") and pedido.get("serie"))

                new_pedido = {
                    "id_pedido": id_pedido,
                    "id_distribuidor": pedido.get("id_distribuidor"),
                    "id_cliente": id_cliente,
                    "id_formapgto": pedido.get("id_formapgto"),
                    "forma_pagamento": pedido.get("forma_pagamento"),
                    "id_condpgto": pedido.get("id_condpgto"),
                    "condicao_pagamento": pedido.get("condicao_pagamento"),
                    "valor_total": pedido.get("valor_total"),
                    "saldo_total": pedido.get("saldo_total"),
                    "nota_fiscal": pedido.get("nota_fiscal"),
                    "serie": pedido.get("serie"),
                    "bool_nf": bool_nf,
                    "status": pedido.get("status_pedido"),
                    "boletos": [
                        {
                            "parcela": parcela,
                            "titulo": pedido.get("titulo"),
                            "codigo_barra": pedido.get("codigo_barra"),
                            "valor_boleto": pedido.get("valor_boleto"),
                            "valor_restante": pedido.get("valor_restante"),
                            "dias_atraso": pedido.get("dias_atraso"),
                            "data_vencimento": pedido.get("data_vencimento"),
                            "data_pagamento": pedido.get("data_pagamento"),
                            "data_baixa": pedido.get("data_baixa"),
                            "status": pedido.get("status"),
                        }
                    ]
                }

                pedidos.append(new_pedido)

        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Dados enviados."},
                "informacoes": {
                    "itens": counter,
                    "paginas": counter//limite + (counter % limite > 0),
                    "pagina_atual": pagina},
                "dados":pedidos}, 200