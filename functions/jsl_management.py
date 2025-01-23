#=== Importações de módulos externos ===#
import requests, os
import numpy as np

#=== Importações de módulos internos ===#
import functions.data_management as dm
from app.server import logger

user = os.environ.get('REGISTER_API_USER')
password = os.environ.get('REGISTER_API_PASSWORD')
domain = os.environ.get('REGISTER_API_URL')

def request_api(url: str, headers: dict = None, body: dict = None, timeout: float = None, **kwargs) -> tuple:
    """
    Realiza uma request para a url requerida

    Args:
        url (str): _description_
        headers (dict, optional): _description_. Defaults to None.
        data (dict, optional): _description_. Defaults to None.
        timeout (float, optional): _description_. Defaults to None.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if not isinstance(headers, dict):
        headers = {}

    if not isinstance(body, dict):
        headers = {}

    if not (isinstance(timeout, (float, int))):

        if timeout is not None:
            timeout = 5

    method = kwargs.get("method")
    if not method or str(method).lower() == "post":
        method = "post"

    elif str(method).lower() == "get":
        method = "get"

    elif str(method).lower() == "put":
        method = "put"

    elif str(method).lower() == "delete":
        method = "delete"

    else:
        method = "post"

    ignore_answer = bool(kwargs.get("ignore_answer"))

    try:
        headers["Content-Type"] = "application/json"

        dict_request = {
            "url": url, 
            "json": body, 
            "headers": headers, 
            "timeout": timeout
        }

        if method == "post":
            request_response = requests.post(**dict_request)

        elif method == "get":
            request_response = requests.get(**dict_request)

        elif method == "put":
            request_response = requests.put(**dict_request)

        elif method == "delete":
            request_response = requests.delete(**dict_request)

        status = request_response.json().get("status")
        message = request_response.json().get("message")
        
        if (request_response.status_code != 200 or status != "success") and not ignore_answer:

            logger.error(message)
            msg = {"status":400,
                   "resposta":{
                       "tipo":"15",
                       "descricao":f"Houve erro com a transação: {message}."}}, 200

            return False, msg

        return True, request_response

    except requests.exceptions.Timeout as e:
        msg = "Api de registro nao respondeu com o timeout."

        logger.error(msg)
        logger.exception(e)
        
        msg = {"status":400,
               "resposta":{
                   "tipo":"15",
                   "descricao":f"Houve erro com a transação: Tempo de comunicação estourado"}}, 200

        return False, msg

    except Exception as e:
        msg = "Houve um erro na comunicacao com a api de registro."

        logger.error(msg)
        logger.exception(e)

        msg = {"status":400,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Houve erro com a transação: {msg}."}}, 200

        return False, None


def get_register_api_token() -> tuple:

    url = f"{domain}/auth".lower()

    json_data = {
        "auth": {
            "user": user,
            "password": password
        }
    }

    dict_request = {
        "url": url,
        "body": json_data
    }

    answer, response = request_api(**dict_request)

    if not answer:
        return False, response

    token = response.json().get("message")

    return True, token
    

def send_client_jsl(id_usuario: int, id_cliente: int) -> bool:

    # Pegando o token de acesso
    answer, response = get_register_api_token()
    if not answer:
        return False

    token = response

    # Pegando os dados de cliente
    query = """
        SELECT
            u.id_usuario as USUARIOID,
            CONVERT(VARCHAR(10), id_cliente) as CLIENTEID,
            CONVERT(VARCHAR(10), ud.id_distribuidor) as FILIAL,
            NULL as LOJAID,
            cnpj as CNPJ,
            cpf as CPF,
            razao_social as RAZAO_SOCIAL,
            nome as NOME,
            email as EMAIL,
            senha as SENHA,
            telefone as TELEFONE,
            NULL as CHAVE,
            alterar_senha as TROCASENHA,
            status_cliente as STATUS,
            status_usuario as APROVADO,
            observacao as MENSAGEM,
            token_firebase as TOKEN,
            modelo_aparelho as APARELHO,
            token_aparelho as IDAPARELHO,
            os as SO,
            data_cadastro as DATA_INSERT,
            data_nascimento as DATA_NASCIMENTO,
            NULL as LOG_APROVACAO,
            NULL as LOG_HISTORICO,
            imagem as IMAGEM,
            udm.id_maxipago as IDMAXIPAGO,
            permite_notificacao as PERMITE_NOTIFICACAO,
            permite_email as PERMITE_EMAIL,
            NULL as PERMITE_SMS,
            NULL as TERMO_DE_USO,
            aceite_termos as DATA_ACEITE_TERMO,
            NULL as ACEITASERCLIENTE,
            NULL as VENDEDOR,
            alterar_senha as ESQUECEUSENHA,
            NULL as DATAESQUECEUSENHA,
            NULL as PERMITE_WHATSAPP,
            permite_notificacao as NOTIFICA_PERMISSAO

        FROM
            (

                SELECT
                    DISTINCT 
                        u.id_usuario,
                        c.id_cliente,
                        c.cnpj,
                        u.cpf,
                        c.razao_social,
                        u.nome,
                        u.email,
                        u.senha,
                        u.telefone,
                        u.alterar_senha,
                        c.status as status_cliente,
                        u.status as status_usuario,
                        u.observacao,
                        s.token_firebase,
                        s.modelo_aparelho,
                        s.token_aparelho,
                        s.os,
                        u.data_cadastro,
                        u.data_nascimento,
                        u.imagem,
                        u.permite_notificacao,
                        u.permite_email,
                        u.aceite_termos,
                        uc.data_aprovacao as data_usuario_cliente

                FROM
                    USUARIO u

                    INNER JOIN USUARIO_CLIENTE uc ON
                        u.id_usuario = uc.id_usuario

                    INNER JOIN CLIENTE c ON
                        uc.id_cliente = c.id_cliente           

                    INNER JOIN
                    (

                        SELECT
                            DISTINCT
                                us.id_usuario,
                                s.id_sessao,
                                us.data_insert

                        FROM
                            SESSAO s

                            INNER JOIN USUARIO_SESSAO us ON
                                s.id_sessao = us.id_sessao

                            INNER JOIN 
                            (

                                SELECT
                                    DISTINCT
                                        id_usuario,
                                        MAX(data_insert) as data_usuario_sessao

                                FROM
                                    USUARIO_SESSAO

                                WHERE
                                    id_usuario = :id_usuario

                                GROUP BY
                                    id_usuario
                    
                            ) usb ON
                                us.id_usuario = usb.id_usuario
                                AND us.data_insert = usb.data_usuario_sessao

                        WHERE
                            s.d_e_l_e_t_ = 0
                                    
                    ) us ON
                        u.id_usuario = us.id_usuario

                    INNER JOIN SESSAO s ON
                        us.id_sessao = s.id_sessao

                WHERE
                    1 = 1
                    AND u.id_usuario = :id_usuario
                    AND c.id_cliente = :id_cliente
                    AND u.status != 'I'
                    AND uc.d_e_l_e_t_ = 0
                    AND uc.status != 'I'
                    AND s.d_e_l_e_t_ = 0

            ) u

            INNER JOIN
            (
                
                SELECT
                    :id_usuario as id_usuario,
                    d.id_distribuidor

                FROM
                    DISTRIBUIDOR d

                WHERE
                    status = 'A'
                    AND d.id_distribuidor != 0

            ) ud ON
                u.id_usuario = ud.id_usuario

        LEFT JOIN USUARIO_DISTRIBUIDOR_MAXIPAGO udm ON
		    ud.id_distribuidor = udm.id_distribuidor
		    AND ud.id_usuario = udm.id_usuario

        ORDER BY
            u.id_usuario,
            u.data_usuario_cliente,
            ud.id_distribuidor
    """

    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    users = dm.raw_sql_return(**dict_query)

    if not users:
        logger.error("Dados do usuario nao encontrados.")
        return False

    url = f"{domain}/send-clients"

    header = {
        'Authorization': 'Bearer ' + token
    }

    dict_request = {
        "url": url,
        "headers": header, 
        "body": {
            "clients": users
        },
        "ignore_answer": True
    }

    answer, response = request_api(**dict_request)

    status = response.json().get("status")
    message = response.json().get("message")
    
    if (response.status_code != 200 or status != "success"):

        logger.error(f"Erro no envio dos dados do usuario: {message}")
        return False

    logger.info(f"Usuario enviado para o protheus.")
    return True


def send_order_jsl(id_usuario: int, id_cliente: int, id_pedido: int) -> bool:
    
    # Pegando o token de acesso
    answer, response = get_register_api_token()
    if not answer:
        return False

    token = response

    # Pegando os dados de pedido
    query = """
        SELECT
            CONVERT(VARCHAR(10), pdd.id_distribuidor) as filial,
            CONVERT(VARCHAR(10), pdd.id_pedido) as pedido,
            pdd.data_criacao as data_pedido,
            '' as vendedor,
            CONVERT(VARCHAR(10), c.chave_integracao) as cliente,
            CONVERT(VARCHAR(10), pdd.condicao_pagamento) as condpagto,
            cp.descricao as desccondpagto,
            CONVERT(VARCHAR(10), pdd.forma_pagamento) as formapagto,
            fp.descricao as descformapagto,
            cp.percentual as percentual_cond_pagamento,
            'N' as finalizado,
            CASE WHEN EXISTS(SELECT 1 FROM PEDIDO_ITEM WHERE id_pedido = pdd.id_pedido AND tipo_venda = 'V') THEN 'S' ELSE 'N' END as venda,
            CASE WHEN EXISTS(SELECT 1 FROM PEDIDO_ITEM WHERE id_pedido = pdd.id_pedido AND tipo_venda = 'B') THEN 'S' ELSE 'N' END as bonificacao,
            pdd.frete as frete,
            '' as observacao_pedido,
            s.os as so,
            s.modelo_aparelho as aparelho,
            s.versao_app as versao_app,
            '' as obsfat,
            'APP UNO MARKET' as tipo_venda,
            pdditm.id_produto as produto_mkt,
            pd.cod_prod_distr as produto,
            CASE WHEN pdditm.tipo_venda != 'B' THEN 'VENDA' ELSE 'BONIFICADO' END as tipo,
            pdditm.quantidade_venda as quantidade,
            pdditm.preco_produto as preco_base,
            pdditm.preco_aplicado as preco_venda,
            CASE WHEN pdditm.id_oferta > 0 THEN CONVERT(VARCHAR(10), pdditm.id_oferta) ELSE '.' END as oferta,
            o.descricao_oferta as descoferta,
            '' as observacao_produto,
            pdditm.desconto_aplicado as desconto_especial,
            CONVERT(VARCHAR(10), pdd.id_cupom) id_cupom,
            CONVERT(VARCHAR(10), pdd.desconto_cupom) desconto_cupom,
            UPPER(cpm.tipo_cupom) as tipo_cupom,
            pdditm.participa_cupom

        FROM
            PEDIDO pdd

            INNER JOIN PEDIDO_ITEM pdditm ON
                pdd.id_pedido = pdditm.id_pedido
                AND pdd.id_distribuidor = pdditm.id_distribuidor

            INNER JOIN CLIENTE c ON
                pdd.id_cliente = c.id_cliente
            
            INNER JOIN FORMA_PAGAMENTO fp ON
                pdd.forma_pagamento = fp.id_formapgto

            INNER JOIN CONDICAO_PAGAMENTO cp ON
                pdd.condicao_pagamento = cp.id_condpgto

            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                pdditm.id_produto = pd.id_produto
                AND pdd.id_distribuidor = pd.id_distribuidor

            INNER JOIN PRODUTO p ON
                pd.id_produto = p.id_produto

            INNER JOIN
            (

                SELECT
                    DISTINCT
                        us.id_usuario,
                        s.id_sessao,
                        us.data_insert

                FROM
                    SESSAO s

                    INNER JOIN USUARIO_SESSAO us ON
                        s.id_sessao = us.id_sessao

                    INNER JOIN 
                    (

                        SELECT
                            DISTINCT
                                id_usuario,
                                MAX(data_insert) as data_usuario_sessao

                        FROM
                            USUARIO_SESSAO

                        WHERE
                            id_usuario = :id_usuario

                        GROUP BY
                            id_usuario
                            
                    ) usb ON
                        us.id_usuario = usb.id_usuario
                        AND us.data_insert = usb.data_usuario_sessao

                WHERE
                    s.d_e_l_e_t_ = 0
                                            
            ) us ON
                pdd.id_usuario = us.id_usuario

            INNER JOIN SESSAO s ON
                us.id_sessao = s.id_sessao

            LEFT JOIN OFERTA o ON
                pdditm.id_oferta = o.id_oferta

            LEFT JOIN 
            (
            
                SELECT
                    cp.id_cupom,
                    tpcp.descricao as tipo_cupom

                FROM
                    CUPOM cp

                    INNER JOIN TIPO_CUPOM tpcp ON
                        cp.tipo_cupom = tpcp.id_tipo_cupom

                    INNER JOIN PEDIDO pdd ON
                        cp.id_cupom = pdd.id_cupom

                WHERE
                    pdd.id_pedido = :id_pedido
            
            ) cpm ON
                pdd.id_cupom = cpm.id_cupom

        WHERE
            pdd.id_cliente = :id_cliente
            AND pdd.id_pedido = :id_pedido
            AND pdd.d_e_l_e_t_ = 0
            AND pdditm.d_e_l_e_t_ = 0

        ORDER BY
            pdd.id_pedido,
            pdditm.ordem_produto
    """

    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "id_pedido": id_pedido
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    query = dm.raw_sql_return(**dict_query)

    if not query:
        logger.error(f"Dados do pedido {id_pedido} nao encontrados para envio a integracao.")
        return False
    
    orders = []

    for order in query:

        tipo = order.get("tipo")
        preco_base = order.get("preco_base")
        preco_venda = order.get("preco_venda")
        percentual = float(order.get("percentual_cond_pagamento")) if order.get("percentual_cond_pagamento") else 0
        participa_cupom = True if order.get("participa_cupom") else False

        if str(tipo).lower() == "bonificado":
            preco_venda = np.round(preco_base * (1 + (percentual/100)), 2)

        for saved_order in orders:
            id_pedido = saved_order.get("PEDIDO")

            if order.get("pedido") == id_pedido:

                for saved_produto in saved_order["ITENS"]:

                    saved_id_produto = saved_produto.get("PRODUTO")
                    saved_id_oferta = saved_produto.get("OFERTA")
                    saved_tipo = saved_produto.get("TIPO")

                    if (order.get("produto") == saved_id_produto) and (order.get("oferta") == saved_id_oferta)\
                        and (order.get("tipo") == saved_tipo):

                        break

                else:
                    new_product = {
                        "PRODUTO_MKT": order.get("produto_mkt"),
                        "PRODUTO": order.get("produto"),
                        "TIPO": tipo,
                        "QUANTIDADE": order.get("quantidade"),
                        "PRECO_VENDA":  preco_venda,
                        "PRECO_BASE": preco_base,
                        "OFERTA": order.get("oferta"),
                        "DESCOFERTA": order.get("descoferta"),
                        "OBSERVACAO": order.get("observacao_produto"),
                        "DESCONTO_ESPECIAL": order.get("desconto_especial"),
                        "PERCENTUAL_COND_PAGAMENTO": percentual,
                        "ID_CUPOM": "0",
                        "TIPO_CUPOM": " ",
                        "DESCONTO_CUPOM": "0",
                    }

                    if participa_cupom:
                        new_product.update({
                            "ID_CUPOM": order.get("id_cupom"),
                            "TIPO_CUPOM": order.get("tipo_cupom"),
                            "DESCONTO_CUPOM": order.get("desconto_cupom"),
                        })

                    saved_order["ITENS"].append(new_product)

                break

        else:
        
            new_order = {
                "FILIAL": order.get("filial"),
                "PEDIDO": order.get("pedido"),
                "DATA_PEDIDO": order.get("data_pedido"),
                "VENDEDOR": order.get("vendedor"),
                "CLIENTE": order.get("cliente"),
                "CONDPAGTO": order.get("condpagto"),
                "DESCCONDPAGTO": order.get("desccondpagto"),
                "FORMAPAGTO": order.get("formapagto"),
                "DESCFORMAPAGTO": order.get("descformapagto"),
                "FINALIZADO": order.get("finalizado"),
                "VENDA": order.get("venda"),
                "BONIFICACAO": order.get("bonificacao"),
                "FRETE": order.get("frete"),
                "OBSERVACAO": order.get("observacao_pedido"),
                "SO": order.get("so"),
                "APARELHO": order.get("aparelho"),
                "VERSAOAPP": order.get("versao_app"),
                "OBSFAT": order.get("obsfat"),
                "TIPO_VENDA": order.get("tipo_venda"),
                "ID_CUPOM": order.get("id_cupom") if order.get("id_cupom") else '0',
                "TIPO_CUPOM": order.get("tipo_cupom") if order.get("tipo_cupom") else ' ',
                "DESCONTO_CUPOM": order.get("desconto_cupom") if order.get("desconto_cupom") else '0',
                "ITENS": []
            }

            new_product = {
                "PRODUTO_MKT": order.get("produto_mkt"),
                "PRODUTO": order.get("produto"),
                "TIPO": tipo,
                "QUANTIDADE": order.get("quantidade"),
                "PRECO_VENDA":  preco_venda,
                "PRECO_BASE": preco_base,
                "OFERTA": order.get("oferta"),
                "DESCOFERTA": order.get("descoferta"),
                "OBSERVACAO": order.get("observacao_produto"),
                "DESCONTO_ESPECIAL": order.get("desconto_especial"),
                "PERCENTUAL_COND_PAGAMENTO": percentual,
                "ID_CUPOM": "0",
                "TIPO_CUPOM": " ",
                "DESCONTO_CUPOM": "0",
            }

            if participa_cupom:
                new_product.update({
                    "ID_CUPOM": order.get("id_cupom"),
                    "TIPO_CUPOM": order.get("tipo_cupom"),
                    "DESCONTO_CUPOM": order.get("desconto_cupom"),
                })

            new_order["ITENS"].append(new_product)
            orders.append(new_order)

    url = f"{domain}/send-orders"

    header = {
        'Authorization': 'Bearer ' + token
    }

    dict_request = {
        "url": url,
        "headers": header, 
        "body": {
            "orders": orders
        },
        "ignore_answer": True,
        "timeout": 10
    }

    answer, response = request_api(**dict_request)

    if not answer:
        return False
        
    status = response.json().get("status")
    message = response.json().get("message")
    
    if (response.status_code != 200 or status != "success"):

        logger.error(f"Erro no envio dos dados do pedido: {message}")
        return False

    logger.info(f"Pedido {id_pedido} enviado para o protheus.")
    return True