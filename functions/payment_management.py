#=== Importações de módulos externos ===#
import requests, os, datetime, json, base64, xmltodict
import concurrent.futures
import numpy as np
import re

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.jsl_management as jm
import functions.default_json as dj
from app.server import logger

#=== Inicialização das variaveis globais ===#
url_main_maxipago = os.environ.get("MAXIPAGO_MAIN_URL_PS").lower()
maxipago_provider = os.environ.get("MAXIPAGO_PROVIDER_PS").upper()
url_main_pix = os.environ.get("PIX_MAIN_URL_PS").lower()

server = os.environ.get("PSERVER_PS").lower()

if server == "production":
    route = str(os.environ.get("SERVER_GUARANY_PS")).lower()
    image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
    
elif server == "development":
    route = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

else:
    route = f"http://localhost:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"


#### ============ Modulos de apoio ============ ####


######### Validadores padrões

def atualizar_etapa_pedido(id_pedido: int, id_formapgto: int, id_subetapa: int, id_etapa: int = None,
                            adicional_etapa: str = None, adicional_subetapa: str = None):
    """
    Atualiza a etapa e subetapa atual do pedido até a determinada

    Args:
        id_pedido (int): Id do pedido que deve ser atualizado
        id_formapgto (int): Qual a forma de pagamento do pedido
        id_etapa (int): Id da etapa atual do pedido
        id_subetapa (int): Id da subetapa atual do pedido
        adicional_etapa (str, optional): Descricao adicional para a descricao da etapa. Defaults to None.
        adicional_subetapa (str, optional): Descricao adicional para a descricao da subetapa. Defaults to None.
    """

    if id_subetapa != 9 and id_etapa is None:
        raise TypeError("Quando id_subetapa != 9, id_etapa deve ser fornecido.")

    pedido_query = """
        SELECT
            TOP 1 pdd.id_orcamento,
            pdd.id_distribuidor,
            pdd.id_cliente,
            pdd.id_usuario,
            pddetp.id_etapa,
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
    """

    params = {
        "id_pedido": id_pedido
    }

    pedido_query = dm.raw_sql_return(pedido_query, params = params, first = True)

    if not pedido_query: return

    old_id_etapa = pedido_query.get("id_etapa") if pedido_query.get("id_etapa") else 0
    old_id_subetapa = pedido_query.get("id_subetapa") if pedido_query.get("id_subetapa") else 0

    if old_id_subetapa == 9: return

    with open("other\json\etapas_pedido.json", "r", encoding = "utf-8") as file:
        data = json.loads(file.read())

    formapgto = data.get(f"{id_formapgto}")

    if formapgto:

        etapas = formapgto.get("etapas")

        if etapas:

            if id_subetapa == 9:
                
                id_etapa = old_id_etapa
                etapa = etapas.get(f"{id_etapa}")

                if etapa:
            
                    descricao_etapa = etapa.get("descricao")
                    subetapas = etapa.get("subetapas")

                    if subetapas and descricao_etapa:

                        descricao_subetapa = subetapas.get(f"{id_subetapa}")
                        if descricao_subetapa:

                            if isinstance(adicional_etapa, str):
                                descricao_etapa += f" {adicional_etapa}"
                                descricao_etapa = " ".join(descricao_etapa.split())

                            if isinstance(adicional_subetapa, str):
                                descricao_subetapa += f" {adicional_subetapa}"
                                descricao_subetapa = " ".join(descricao_subetapa.split())

                            insert_etapa_pedido = {
                                "id_pedido": id_pedido,
                                "id_orcamento": pedido_query.get("id_orcamento"),
                                "id_distribuidor": pedido_query.get("id_distribuidor"),
                                "id_cliente": pedido_query.get("id_cliente"),
                                "id_usuario": pedido_query.get("id_usuario"),
                                "id_etapa": id_etapa,
                                "descricao_etapa": str(descricao_etapa).upper(),
                                "id_subetapa": id_subetapa,
                                "descricao_subetapa": str(descricao_subetapa).upper(),
                            }

                            dm.raw_sql_insert("PEDIDO_ETAPA", insert_etapa_pedido)

            else:

                first_time = True

                for actual_id_etapa in range(old_id_etapa, id_etapa + 1):

                    actual_etapa = etapas.get(f"{actual_id_etapa}")

                    if actual_etapa:

                        if first_time:
                            first_time = False
                            min_id_subetapa = old_id_subetapa + 1

                        else:
                            min_id_subetapa = 1

                        if actual_id_etapa == id_etapa:
                            max_id_subetapa = id_subetapa + 1

                        else:
                            max_id_subetapa = 4

                        descricao_actual_etapa = actual_etapa.get("descricao")
                        actual_subetapas = actual_etapa.get("subetapas")

                        if descricao_actual_etapa and actual_subetapas:

                            for actual_id_subetapa in range(min_id_subetapa, max_id_subetapa):

                                descricao_actual_subetapa = actual_subetapas.get(f"{actual_id_subetapa}")

                                if descricao_actual_subetapa:

                                    if isinstance(adicional_etapa, str):
                                        descricao_actual_etapa += f" {adicional_etapa}"
                                        descricao_actual_etapa = " ".join(descricao_actual_etapa.split())

                                    if isinstance(adicional_subetapa, str):
                                        descricao_actual_subetapa += f" {adicional_subetapa}"
                                        descricao_actual_subetapa = " ".join(descricao_actual_subetapa.split())

                                    insert_etapa_pedido = {
                                        "id_pedido": id_pedido,
                                        "id_orcamento": pedido_query.get("id_orcamento"),
                                        "id_distribuidor": pedido_query.get("id_distribuidor"),
                                        "id_cliente": pedido_query.get("id_cliente"),
                                        "id_usuario": pedido_query.get("id_usuario"),
                                        "id_etapa": actual_id_etapa,
                                        "descricao_etapa": str(descricao_actual_etapa).upper(),
                                        "id_subetapa": actual_id_subetapa,
                                        "descricao_subetapa": str(descricao_actual_subetapa).upper(),
                                    }

                                    dm.raw_sql_insert("PEDIDO_ETAPA", insert_etapa_pedido)


def retorno_valores_produto_orcamento(list_products: list, percentual: float,
                                cupom: dict = None) -> dict:
    """
    Retorna os valores de preços calculados para o orcamento a ser finalizado

    Args:
        list_products (list): Lista de produtos do orcamento
        percentual (float): Percentual da condição de pagamento
        cupom (dict, optional): Cupom aplicado. Defaults to None.

    Returns:
        dict: Valores calculados para a finalização do orcamento
    """
    
    if not isinstance(cupom, dict) or cupom is None:
        cupom = {}

    subtotal = 0
    desconto = 0
    bonus = 0
    desconto_aplicado_cupom = 0
    valor_atingido_validar_cupom = 0

    tipo_cupom = None
    desconto_cupom_porcentagem = None

    if cupom:
        tipo_cupom = cupom.get("tipo_cupom")
        desconto_cupom_porcentagem = cupom.get("desconto_percentual")

    for dict_itens in list_products:

        # Verificação nos itens enviados

        ## Quantidade
        quantidade = dict_itens.get("quantidade") if dict_itens.get("quantidade") else 0

        if quantidade < 0:
            quantidade = 0

        ## Ofertas
        id_campanha = dict_itens.get("id_campanha")
        bonificado = dict_itens.get("bonificado") if id_campanha else None

        id_escalonado = dict_itens.get("id_escalonado")
        desconto_escalonado = dict_itens.get("desconto_escalonado") if id_escalonado else 0

        if desconto_escalonado < 0:
            desconto_escalonado = 0

        ## Desconto
        desconto_aplicado = 0

        desconto_produto = dict_itens.get("desconto") if dict_itens.get("desconto") else 0

        if desconto_produto < 0:
            desconto_produto = 0

        if desconto_produto < desconto_escalonado:
            desconto_aplicado = desconto_escalonado

        elif desconto_produto >= desconto_escalonado:
            desconto_aplicado = desconto_produto

        else:
            desconto_aplicado = 0

        # Calculos
        participa_cupom = True if dict_itens.get("participa_cupom") else False

        preco_tabela = dict_itens.get("preco_tabela") if dict_itens.get("preco_tabela") else 0

        porc_financeiro = np.round(1 + (percentual/100), 4)
        preco_financeiro = np.round(preco_tabela * porc_financeiro, 2)

        calculo = np.round(preco_financeiro * quantidade, 2)
        subtotal += calculo

        porc_desconto = np.round(1 - (desconto_aplicado/100),4)
        preco_aplicado = np.round(preco_financeiro * porc_desconto, 2)

        dict_itens["preco_final"] = np.round(preco_aplicado, 2)
        dict_itens["desconto_aplicado"] = np.round(desconto_aplicado, 2)

        if participa_cupom:
            valor_cupom_produto = preco_aplicado * quantidade
            valor_atingido_validar_cupom += np.round(valor_cupom_produto, 2)

            if tipo_cupom == 2 and desconto_cupom_porcentagem:

                valor_cupom_desconto = valor_cupom_produto * (desconto_cupom_porcentagem/100)
                desconto_aplicado_cupom += np.round(valor_cupom_desconto, 2)
            
        if id_campanha and bonificado:
            dict_itens["preco_final"] = 0
            bonus += calculo

        else:
            desconto += np.round((preco_financeiro - preco_aplicado) * quantidade, 2)

    subtotal = float(np.round(subtotal, 2))
    desconto = float(np.round(desconto, 2))
    bonus = float(np.round(bonus, 2))
    desconto_aplicado_cupom = float(np.round(desconto_aplicado_cupom, 2))
    valor_atingido_validar_cupom = float(np.round(valor_atingido_validar_cupom, 2))

    subtotal_liquido = float(np.round(subtotal - desconto - bonus, 2))

    if subtotal_liquido < 0:
        subtotal_liquido = 0

    return {
        "subtotal_liquido": subtotal_liquido,
        "subtotal": subtotal,
        "desconto": desconto,
        "bonus": bonus,
        "desconto_aplicado_cupom": desconto_aplicado_cupom,
        "valor_atingido_validar_cupom": valor_atingido_validar_cupom
    }


def retorno_valores_produto_pedido(list_products: list, **kwargs) -> dict:
    """
    Retorna os valores de preços calculados para o visualizador de pedido

    Args:
        list_products (list): Lista de produtos do orcamento
    
    Kwargs:
        juros: Juros aplicado no pedido.
        percentual: Percentual aplicado nos produtos do pedido.
        desconto_cupom: Desconto do cupom aplicado nos produtos do pedido.
        frete: Frete aplicado no pedido.

    Returns:
        dict: Valores calculados para o visualizador de pedido
    """

    subtotal = 0
    desconto = 0
    bonus = 0

    juros = kwargs.get('juros') if kwargs.get('juros') else 0
    percentual = kwargs.get('percentual') if kwargs.get('percentual') else 0
    desconto_cupom = kwargs.get('desconto_cupom') if kwargs.get('desconto_cupom') else 0
    frete = kwargs.get('frete') if kwargs.get('frete') else 0

    for dict_itens in list_products:

        # Verificação nos itens enviados
        quantidade = dict_itens.get("quantidade") if dict_itens.get("quantidade") else 0

        if quantidade < 0:
            quantidade = 0

        preco_aplicado = dict_itens.get("preco_aplicado") if dict_itens.get("preco_aplicado") else 0

        if preco_aplicado < 0:
            preco_aplicado = 0

        preco_tabela = dict_itens.get("preco_tabela") if dict_itens.get("preco_tabela") else 0

        if preco_tabela < 0:
            preco_tabela = 0

        bonificado = dict_itens.get("bonificado")

        percentual = np.round(percentual, 2)
        preco_aplicado = np.round(preco_aplicado, 2)
        preco_tabela = np.round(preco_tabela, 2)

        # Calculos
        preco_tabela = dict_itens.get("preco_tabela") if dict_itens.get("preco_tabela") else 0

        porc_financeiro = np.round(1 + (percentual/100), 4)
        preco_financeiro = np.round(preco_tabela * porc_financeiro, 2)

        calculo = np.round(preco_financeiro * quantidade, 2)
        subtotal += calculo

        if bonificado:
            bonus += calculo

        else:
            desconto += np.round((preco_financeiro - preco_aplicado) * quantidade, 2)

    subtotal = np.round(subtotal, 2)
    desconto = np.round(desconto, 2)
    bonus = np.round(bonus, 2)

    desconto_cupom = np.round(desconto_cupom, 2)
    juros = np.round(juros, 2)
    frete = np.round(frete, 2)

    subtotal_liquido = np.round(subtotal - desconto - bonus, 2)

    if subtotal_liquido < 0:
        subtotal_liquido = 0

    total_orcamento = 0
    total = 0

    if subtotal_liquido > 0:
        total_orcamento = np.round(subtotal_liquido + frete - desconto_cupom,2)
        total =  np.round(total_orcamento * (1 + (juros/100)), 2)

    return {
        "bonus": bonus,
        "desconto": desconto,
        "subtotal": subtotal,
        "subtotal_liquido": subtotal_liquido,
        "total_orcamento": total_orcamento,
        "total": total,
    }


def verificar_saldo_cliente(id_cliente: int, id_distribuidor: int, 
                                valor: float, check_minimum: bool = True, 
                                check_balance: bool = True) -> tuple:
    """
    Verifica se o cliente possui saldo suficiente para realizar a compra por boleto

    Args:
        id_cliente (int): ID do cliente
        id_distribuidor (int): ID do distribuidor
        valor (float): Valor que será verificado com o saldo

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """
    dict_limite_credito = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor,
    }

    response_data = dj.json_limite_credito(**dict_limite_credito)

    if not response_data:
        logger.error(f"Usuario sem limite cadastrado para D:{id_distribuidor}")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Usuário sem limite para o distribuidor informado."}}, 200

        return False, msg

    limite = response_data[0]

    saldo = limite["credito_atual"]
    pedido_minimo = limite["pedido_minimo"]

    if (valor < pedido_minimo) and check_minimum:
        logger.error(f"Usuario nao atingiu limite minimo de {pedido_minimo}")

        pedido_minimo = f"{pedido_minimo:.2f}".replace(".",",")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Valor mínimo de pedido não foi atingido."},
               "dados": {
                   "status": False,
                   "id_pedido": None,
                   "id_motivo": 3,
                   "motivo": f"Cliente não atingiu limite mínimo de R$ {pedido_minimo}"
               }}, 200

        return False, msg

    if (valor > saldo) and check_balance:
        logger.error(f"Usuario sem saldo para realizar compra de {saldo}")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Saldo insuficiente."},
               "dados": {
                   "status": False,
                   "id_pedido": None,
                   "id_motivo": 4,
                   "motivo": f"Cliente sem saldo suficiente para realizar a compra."
               }}, 200

        return False, msg

    return True, None


def atualizar_saldo_cliente(id_cliente: int, id_distribuidor: int, valor: float):
    """
    Atualiza o saldo do cliente

    Args:
        id_cliente (int): ID do cliente
        id_distribuidor (int): ID do distribuidor
        valor (float): Novo valor final do saldo disponivel

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    update_client_balance = """
        UPDATE
            PARAMETRO_CLIENTE

        SET
            saldo_limite = saldo_limite - :valor

        WHERE
            id_cliente = :id_cliente
            AND id_distribuidor = :id_distribuidor
    """

    params = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor,
        "valor": valor
    }

    dm.raw_sql_execute(update_client_balance, params = params)

    return True, None


def atualizar_orcamento(id_orcamento: int, status: str = 'F') -> tuple:
    """
    Atualiza o status do orcamento

    Args:
        id_usuario (int): ID do usuario pai do orcamento
        id_cliente (int): ID do cliente pai do orcamento
        id_orcamento (int): ID do orcamento
        status (str, optional): Novo status do orcamento. Defaults to 'F'.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    orcamento_update = {
        "id_orcamento": id_orcamento,
        "status": str(status).upper()[0]
    }

    key_columns = ["id_orcamento"]

    dm.raw_sql_update("ORCAMENTO", orcamento_update, key_columns)

    return True, None


def atualizar_transacao_inicial(id_pedido: int, id_distribuidor: int, id_orcamento: int) -> tuple:
    """
    Atualiza a transação de first check para terem o id_pedido e id_distribuidor

    Args:
        id_usuario (int): ID do usuario pai do pedido
        id_cliente (int): ID do cliente pai do pedido
        id_pedido (int): ID do pedido
        id_distribuidor (int): ID do distribuidor
        id_orcamento (int): ID do orcamento pai do pedido

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    transacao_update = {
        "reference_number": id_orcamento,
        "transaction_type": "FIRST CHECK",
        "id_pedido": id_pedido,
        "id_distribuidor": id_distribuidor
    }

    key_columns = ["reference_number", "transaction_type", "id_distribuidor"]

    dm.raw_sql_update("PEDIDO_TRANSACAO", transacao_update, key_columns)

    return True, None


def criar_id_pedido(id_distribuidor: int, id_orcamento: int, id_cliente: int, id_usuario: int, 
                    id_formapgto: int, id_condpgto: int, data: datetime, valor: float, 
                    percentual: float = 0, juros: float = 0, frete: float = 0, 
                    id_cupom = None, desconto_cupom: float = 0) -> tuple:
    """
    Cria um objeto de pedido e retorna o id

    Args:
        id_distribuidor (int): ID do distribuidor dos produtos do pedido
        id_orcamento (int): ID do orcamento utilizado no pedido
        id_cliente (int): ID do cnpj responsavel pela compra
        id_usuario (int): ID do usuario que está realizando a compra
        id_formapgto (int): Forma de pagamento escolhida
        id_condpgto (int): Condição de pagamento escolhida

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    pedido_delete = {
        "id_orcamento": id_orcamento,
        "d_e_l_e_t_": 0
    }

    dm.raw_sql_delete("PEDIDO", pedido_delete)

    pedido_insert = {
        "id_distribuidor": id_distribuidor,
        "id_cliente": id_cliente,
        "id_usuario": id_usuario,
        "forma_pagamento": id_formapgto,
        "condicao_pagamento": id_condpgto,
        "percentual": percentual,
        "juros": juros,
        "frete": frete,
        "valor": valor,
        "data_criacao": data,
        "id_orcamento": id_orcamento,
        "id_cupom": id_cupom,
        "desconto_cupom": desconto_cupom,
        "d_e_l_e_t_": 0
    }

    dm.raw_sql_insert("PEDIDO", pedido_insert)

    pedido_retrieve = """
        SELECT
            TOP 1 id_pedido

        FROM
            PEDIDO

        WHERE
            id_orcamento = :id_orcamento
            AND data_criacao = :data_criacao
            AND d_e_l_e_t_ = 0
    """

    params = {
        "id_distribuidor": id_distribuidor,
        "id_cliente": id_cliente,
        "id_usuario": id_usuario,
        "id_formapgto": id_formapgto,
        "id_condpgto": id_condpgto,
        "data_criacao": data,
        "id_orcamento": id_orcamento
    }

    id_pedido = dm.raw_sql_return(pedido_retrieve, params = params, first = True, raw = True)

    if not id_pedido:
        logger.error(f"Erro no retorno do id_pedido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Erro no retorno do id_pedido."}}, 200

        return False, msg    

    id_pedido = id_pedido[0] 

    return True, {"id_pedido": id_pedido}


def adicionar_produto_pedido(id_distribuidor: int, id_pedido: int, id_cliente: int, 
                                product_list: list[dict], id_usuario: int) -> tuple:
    """
    Adiciona os produtos ao pedido já cadastrado

    Args:
        id_distribuidor (int): ID do distribuidor do pedido
        id_pedido (int): ID do pedido
        id_cliente (int): ID do cliente dono do pedido
        product_list (list[dict]): Lista de produtos a serem adicionados no pedido
        id_usuario (int): ID do usuario dono do pedido

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    pedido_list = []
    oferta_list = []

    # Salvando os itens do pedido
    for index, produto in enumerate(product_list, start = 1):

        id_produto = produto.get('id_produto')
        cod_prod_distr = produto.get('cod_prod_distr')
        quantidade = produto.get('quantidade')
        preco_aplicado = produto.get('preco_final')
        preco_tabela = produto.get('preco_tabela')
        unidade_venda = produto.get('unidade_venda')
        desconto_aplicado = produto.get('desconto_aplicado') if produto.get('desconto_aplicado') else 0
        desconto_tipo = produto.get('desconto_tipo') if desconto_aplicado else None
        id_oferta = None
        tipo_oferta = None
        bonificado = 'V'
        participa_cupom = True if produto.get("participa_cupom") else False

        if desconto_tipo == 1:
            desconto_tipo_string = 'DESCONTO'

        elif desconto_tipo == 2:
            desconto_tipo_string = 'OFERTA ESCALONADA'

        else:
            desconto_tipo_string = None
            desconto_tipo = 0

        # Ajustando o id_oferta e o preco
        if produto.get("id_escalonado"):
            id_oferta = produto.get("id_escalonado")
            tipo_oferta = 3

        elif produto.get("id_campanha"):
            id_oferta = produto.get("id_campanha")
            tipo_oferta = 2
            
            if produto.get("bonificado"):
                bonificado = 'B'

        pedido_list.append({
            "ordem_produto": index,
            "id_pedido": id_pedido,
            "id_distribuidor": id_distribuidor,
            "id_produto": id_produto,
            "quantidade_venda": quantidade,
            "preco_produto": preco_tabela,
            "id_oferta":  id_oferta,
            "tipo_oferta": tipo_oferta,
            "desconto_aplicado": desconto_aplicado,
            "desconto_tipo": desconto_tipo,
            "desconto_tipo_string": desconto_tipo_string,
            "preco_aplicado": preco_aplicado,
            "und_venda": unidade_venda,
            "tipo_venda": bonificado,
            "d_e_l_e_t_": 0,
            "participa_cupom": participa_cupom,
            "cod_prod_distr": cod_prod_distr,
        })

        if id_oferta and id_oferta not in oferta_list:
            oferta_list.append(id_oferta)
    
    # Salvando os produtos, estoques e verifica o preço final do pedido
    if not pedido_list:
        logger.error(f"Sem produtos para serem salvos.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Não foram enviados produtos para serem salvos."}}, 200

        return False, msg

    dm.raw_sql_insert("PEDIDO_ITEM", pedido_list)

    return True, {
        "ofertas": oferta_list
    }


def adicionar_cupom_produto_pedido(list_products: list, cupom: dict = None):
    """
    Verifica se aquele produto participa da promoção do cupom

    Args:
        list_products (list): lista com os produtos
        cupom (dict, optional): Objeto de cupom. Defaults to None.
    """

    if cupom and isinstance(cupom, dict):
        if isinstance(list_products, list):

            id_tipo_itens = cupom.get("tipo_itens")
            lista_objeto = cupom.get("itens")

            if id_tipo_itens in {1,2,3,4,5}:

                for produto in list_products:

                    if produto and isinstance(produto, dict):

                        if id_tipo_itens == 1:
                            produto["participa_cupom"] = True

                        elif id_tipo_itens == 2:
                            if str(produto.get("id_produto")) in lista_objeto:
                                produto["participa_cupom"] = True

                        elif id_tipo_itens == 3:
                            if (str(produto.get("id_marca")) in lista_objeto):
                                produto["participa_cupom"] = True

                        elif id_tipo_itens == 4:
                            if any([str(id_grupo) in lista_objeto for id_grupo in produto.get("id_grupo")]):
                                produto["participa_cupom"] = True

                        elif id_tipo_itens == 5:
                            if any([str(id_subgrupo) in lista_objeto for id_subgrupo in produto.get("id_subgrupo")]):
                                produto["participa_cupom"] = True       

                                                 
def checar_cartao_salvo(id_usuario: int, id_cartao: int, cvv: str = None) -> tuple:
    """
    Checa se os dados enviados batem com um cartão já registrado do cliente

    Args:
        id_usuario (int): ID do usuario que realizou o pedido
        id_cliente (int): ID do cliente de quem será cobrado o pedido
        id_cartao (int): ID do cartão do usuario
        cvv (str, optional): cvv do cartão. Defaults to None.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if not id_cartao:
        logger.error(f"Cartao não informado no pedido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cartao não informado."}}, 200

        return False, msg

    cvv_query = """
        SELECT
            TOP 1 REPLICATE('X', 12) + numero_cartao as numero_cartao,
            bandeira,
            cvv

        FROM
            USUARIO_CARTAO_DE_CREDITO

        WHERE
            id_usuario = :id_usuario
            AND id_cartao = :id_cartao
    """

    params = {
        "id_usuario": id_usuario,
        "id_cartao": id_cartao,
    }

    cvv_query = dm.raw_sql_return(cvv_query, params = params, first = True)

    if not cvv_query:
        logger.error(f"Cartao informado nao cadastrado.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cartao informado nao cadastrado."}}, 200

        return False, msg

    cvv_internal = cvv_query.get("cvv")

    if not cvv_internal:

        if not cvv:
            logger.error(f"CVV nao informado.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: CVV não informado."},
                   "dados": {
                       "status": False,
                       "id_pedido": None,
                       "id_motivo": 6,
                       "motivo": f"CVV não informado."
                    }}, 200

            return False, msg

    else:
        binary_string = bytes(cvv_internal, encoding='ascii')
        cvv = base64.b64decode(binary_string).decode('utf-8')
    
    cvv_query["cvv"] = cvv

    return True, cvv_query


def checar_id_pagamento(id_cliente: int, id_distribuidor: int, 
                            id_formapgto: int, id_condpgto: int) -> tuple:
    """
    Verifica a condição e forma de pagamento para o cliente selecionado

    Args:
        id_cliente (int): Id cliente pai do pedido.
        id_distribuidor (int): Id distribuidor pai do pedido.
        id_formapgto (int): Forma de pagamento escolhida.
        id_condpgto (int): Condição de pagamento escolhida.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado ou com as informações
        da forma e condição de pagamento caso dê certo.
    """

    # Verificar forma e condição de pagamento
    pgto_query = """

        SET NOCOUNT ON;

        --## Variaveis de apoio
        DECLARE @parcelas VARCHAR(1000) = NULL;
        DECLARE @percentual DECIMAL(18,2) = NULL;
        DECLARE @juros DECIMAL(18,2) = NULL;

        --## Variaveis de erro
        DECLARE @id_erro INT = 0;

        -- Verificando forma de pagamento
        IF NOT EXISTS (
                            SELECT
                                TOP 1 1

                            FROM
                                FORMA_PAGAMENTO

                            WHERE
                                id_formapgto = :id_formapgto
                      )
        BEGIN
            SET @id_erro = 1;
        END

        IF @id_erro = 0
        BEGIN

            IF NOT EXISTS (
                                SELECT
                                    TOP 1 1

                                FROM
                                    CONDICAO_PAGAMENTO

                                WHERE
                                    id_condpgto = :id_condpgto
                          )
            BEGIN
                SET @id_erro = 2;
            END

        END

        IF @id_erro = 0
        BEGIN

            SELECT
                TOP 1 @parcelas = ISNULL(cp.descricao, 0),
                @percentual = ISNULL(cp.percentual, 0),
                @juros = ISNULL(cp.juros_parcela, 0)

            FROM
                (

                    SELECT
                        :id_cliente as id_cliente,
                        gp.id_grupo

                    FROM
                        GRUPO_PAGTO gp

                    WHERE
                        (
                            (
                                EXISTS(SELECT cgp.id_grupo FROM CLIENTE_GRUPO_PAGTO cgp WHERE id_cliente = :id_cliente AND status = 'A')
                                AND gp.id_grupo = (SELECT TOP 1 cgp.id_grupo FROM CLIENTE_GRUPO_PAGTO cgp WHERE id_cliente = :id_cliente AND status = 'A')
                            )
                                OR
                            (
                                NOT EXISTS(SELECT gp.id_grupo FROM CLIENTE_GRUPO_PAGTO cgp WHERE id_cliente = :id_cliente AND status = 'A')
                                AND gp.padrao = 'S'
                            )
                        )
                        AND gp.status = 'A'
                
                ) cgp

                INNER JOIN GRUPO_PAGTO_ITEM gpi ON
                    cgp.id_grupo = gpi.id_grupo

                INNER JOIN CONDICAO_PAGAMENTO cp ON
                    gpi.id_condpgto = cp.id_condpgto

                INNER JOIN FORMA_PAGAMENTO fp ON
                    cp.id_formapgto = fp.id_formapgto

            WHERE
                cgp.id_cliente = :id_cliente
                AND gpi.id_distribuidor = :id_distribuidor
                AND cp.id_condpgto = :id_condpgto
                AND fp.id_formapgto = :id_formapgto
                AND gpi.status = 'A'
                AND cp.status = 'A'
                AND fp.status = 'A'

            IF @percentual IS NULL OR @parcelas IS NULL
            BEGIN
                SET @id_erro = 3;
            END

        END

        SELECT
            @id_erro as id_erro,
            @percentual as percentual,
            @parcelas as parcelas,
            @juros as juros
    """

    params = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor,
        "id_formapgto": id_formapgto,
        "id_condpgto": id_condpgto
    }

    pgto_query = dm.raw_sql_return(pgto_query, params = params, first = True)

    id_erro = pgto_query.pop('id_erro')
    msg = ""

    if id_erro == 1:
        logger.error(f"Forma de pagamento {id_formapgto} informada e invalida.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Forma de pagamento inválida."}}, 200

    elif id_erro == 2:
        logger.error(f"Condicao de pagamento {id_condpgto} informada e invalida.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Condição de pagamento inválida."}}, 200

    elif id_erro == 3:
        logger.error(f"Forma de pagamento {id_formapgto} informada e invalida.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Forma de pagamento inválida."}}, 200

    if msg:
        return False, msg

    return True, pgto_query


######### Maxipago

def request_maxipago_api(url: str, headers: dict = None, body: dict = None, timeout: float = None, 
                            ignore_error: bool = False, **kwargs) -> tuple:
    """
    Realiza um POST para os serviços da maxipago e retorna o resultado

    Args:
        url (str): Url da maxipago que deverá ser feita a requisição
        headers (dict, optional): Headers adicionais da requisição. Defaults to None.
        body (dict, optional): Body da requisição. Defaults to None.
        timeout (float, optional): Timeout da requisição. Defaults to None.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if not isinstance(headers, dict):
        headers = {}

    if not isinstance(body, dict):
        body = {}

    if not (isinstance(timeout, (float, int))):
        timeout = 10

    try:
        headers["Content-Type"] = "application/json"

        if body:
            body["aplicacao"] = "UNOMARKET"

        dict_request = {
            "url": url,
            "json": body,
            "headers": headers,
            "timeout": timeout,
        }

        request_response = requests.post(**dict_request)

        if not ignore_error:

            status_code = request_response.status_code

            if status_code != 200:

                json_data = request_response.json()

                if status_code == 422:

                    extra = json_data.get("details")

                    logger.error("Erro com o envio dos dados para a api da maxipago", extra = extra)

                    msg = {"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Houve erro com a request para a maxipago."}}, 200

                    return False, msg
                
                elif status_code == 400:

                    tipo = str(json_data["resposta"]["tipo"])

                    if tipo == "5":
                        msg = json_data["erro"]["descricao"]
                        logger.error(f"Erro especifico da maxipago: {msg}")
                    
                    elif tipo == "6":
                        extra = json_data["detalhes"]
                        logger.error(f"Erro de validação de dados na maxipago", extra = extra)

                    else:
                        msg = json_data["resposta"]["descricao"]
                        logger.error(f"Erro de comunicação com a api da maxipago: {msg}")
                    
                    msg = {"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Houve erro com a request para a maxipago."}}, 200

                    return False, msg
                
                else:
                    logger.error("Erro no servidor de integração do maxipago", extra = json_data)
                
                    msg = {"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Houve erro com a request para a maxipago."}}, 200

                    return False, msg

        return True, request_response.json()

    except requests.exceptions.Timeout as e:
        logger.exception(e)
        
        msg = {"status":400,
               "resposta":{
                   "tipo":"15",
                   "descricao":f"Api da maxipago não respondeu no tempo determindado."}}, 200

        return False, msg

    except Exception as e:
        logger.exception(e)

        msg = {"status":400,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Houve erro com a request para a maxipago."}}, 200

        return False, msg


def maxipago_distribuidor_keys(id_distribuidor: int) -> tuple:
    """
    Pega as keys para realizar uma transacao na maxipago para um distribuidor especifico

    Args:
        id_distribuidor (int): id_distribuidor

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado ou as chaves caso dê certo.
    """

    query = """
        SELECT
            TOP 1 dm.merchant_id,
            dm.merchant_key

        FROM
            DISTRIBUIDOR d

            INNER JOIN DISTRIBUIDOR_MAXIPAGO dm ON
                d.id_distribuidor = dm.id_distribuidor

        WHERE
            d.id_distribuidor != 0
            AND d.id_distribuidor = :id_distribuidor

        ORDER BY
            d.id_distribuidor
    """

    params = {
        "id_distribuidor": id_distribuidor
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.error(f"Falha na recuperação das keys da maxipago para o distribuidor {id_distribuidor}. Por favor, cadastra-las.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Falha na transação. Entre em contato com o suporte."}}, 200

        return False, msg

    return True, response


def maxipago_pedido_item_list_gen(produtos: list[dict]) -> list[dict]:
    """
    Normaliza os produtos para serem enviados para a maxipago

    Args:
        produtos (list[dict]): Lista de produtos do orcamento/pedido

    Returns:
        list[dict]: lista de produtos normalizada
    """

    assert isinstance(produtos, (list, dict)), "Products with invalid type"

    if isinstance(produtos, dict):
        produtos = [produtos]

    item_list = []

    for item in produtos:
        preco_final = item.get('preco_final') if item.get('preco_final') else item.get("preco_aplicado")
        id_produto = item.get("id_produto")
        quantidade = item.get("quantidade")
        descricao = str(item.get("descricao_produto"))

        item_list.append({
            "codigo_produto": id_produto,
            "descricao_produto": descricao,
            "quantidade": quantidade,
            "preco_unitario": preco_final,
        })

    return item_list


def maxipago_autorizacao_cartao(id_usuario: int, id_cliente: int, id_orcamento: int, id_distribuidor: int,
                                numero_cartao: str, bandeira: str, cvv: str, parcelas: int, valor: float, 
                                frete: float = 0, produtos: list[dict] = None) -> tuple:
    """
    Realiza a autorização inicial para verificar se o cliente pode comprar com o cartão

    Args:
        id_usuario (int): ID do usuario dono do cartão
        id_cliente (int): ID cliente de quem será feito a compra
        id_distribuidor (int): ID do distribuidor dono dos produtos
        id_orcamento (int): ID do orcamento pai do pedido
        numero_cartao (str): Numero do cartão do cliente (somente os 4 ultimos digitos)
        bandeira (str): Bandeira do cartão de crédito
        cvv (str): cvv do cartão
        parcelas (int): Parcelas escolhidas do cartão
        valor (float): Valor preliminar da compra
        frete (float): Valor pago no frete, defaults to 0.
        produtos (list[dict]): Produtos do pedido. defaults to [].

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if produtos is None or not isinstance(produtos, (list, dict)):
        produtos = []

    # Pegando dados de cartão
    payer_data = """
        SELECT
            TOP 1 ucdc.portador as nome,
            ucdc.endereco,
            ucdc.numero as numero_endereco,
            ucdc.complemento,
            ucdc.bairro,
            ucdc.cidade,
            ucdc.uf,
            ucdc.cep,
            (ucdc.ddd + ucdc.telefone) as telefone,
            ucdc.email,
            d.nome_fantasia as distribuidor,
            udm.id_maxipago,
            cm.token_maxipago

        FROM
            USUARIO_CARTAO_DE_CREDITO ucdc

            INNER JOIN ORCAMENTO o ON
                ucdc.id_usuario = o.id_usuario
                    
            INNER JOIN DISTRIBUIDOR d ON
                o.id_distribuidor = d.id_distribuidor

            INNER JOIN DISTRIBUIDOR_MAXIPAGO dm ON
                d.id_distribuidor = dm.id_distribuidor

            INNER JOIN USUARIO_DISTRIBUIDOR_MAXIPAGO udm ON
                ucdc.id_usuario = udm.id_usuario
                AND dm.id_distribuidor = udm.id_distribuidor

            INNER JOIN CARTAO_MAXIPAGO cm ON
                udm.id_maxipago = cm.id_maxipago
                AND ucdc.id_cartao = cm.id_cartao

        WHERE
            o.id_cliente = :id_cliente
            AND ucdc.id_usuario = :id_usuario
            AND ucdc.numero_cartao = :numero_cartao
            AND ucdc.bandeira COLLATE Latin1_General_CI_AI LIKE :bandeira
            AND o.id_orcamento = :id_orcamento
            AND d.id_distribuidor = :id_distribuidor
    """

    params = {
        "id_cliente": id_cliente,
        "id_usuario": id_usuario,
        "numero_cartao": numero_cartao[-4:],
        "bandeira": f'%{bandeira}%',
        "id_orcamento": id_orcamento,
        "id_distribuidor": id_distribuidor
    }

    payer_data = dm.raw_sql_return(payer_data, params = params, first = True)

    if not payer_data:
        logger.error(f"Erro na recuperação dos dados do cartão.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Erro na recuperação dos dados do cartão."}}, 200

        return False, msg

    id_maxipago = payer_data.pop("id_maxipago")
    token_maxipago = payer_data.pop("token_maxipago")

    # Pegando dados de envio
    shipping_data = """
        SELECT
            TOP 1 UPPER(c.nome_fantasia) as nome,
            c.endereco,
            c.endereco_num as numero_endereco,
            c.endereco_complemento as complemento,
            c.bairro,
            c.cidade,
            c.estado as uf,
            c.cep,
            c.telefone,
            u.email

        FROM
            CLIENTE c

            INNER JOIN USUARIO_CLIENTE uc ON
                c.id_cliente = uc.id_cliente

            INNER JOIN USUARIO u ON
                uc.id_usuario = u.id_usuario

        WHERE
            c.id_cliente = :id_cliente
            AND uc.d_e_l_e_t_ = 0
            AND uc.status = 'A'
            AND u.id_usuario = :id_usuario
    """

    params = {
        "id_cliente": id_cliente,
        "id_usuario": id_usuario
    }

    shipping_data = dm.raw_sql_return(shipping_data, params = params, first = True)

    if not shipping_data:
        logger.error(f"informacoes do cliente nao encontradas.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cliente não encontrado."}}, 200

    parcelas = (str(parcelas).split()[-1]).strip()[0]
    if "A" == parcelas:
        parcelas = 1

    payment_obj = {
        "valor_total": valor,
        "parcelas": parcelas,
    }

    answer, response = maxipago_distribuidor_keys(id_distribuidor)
    if not answer:
        return False, response

    authorization_obj = response

    # Criando o xml de verificação
    create_authorization = {
        "processante": maxipago_provider,
        "id_pedido": id_orcamento,
        "id_maxipago": id_maxipago,
        "pagante": payer_data,
        "recebedor": shipping_data,
        "forma_pagamento": {
            "token_cartao_maxipago": token_maxipago,
            "cvv": cvv,
        },
        "pagamento": payment_obj,
    } 

    if produtos:
        create_authorization["produtos"] = maxipago_pedido_item_list_gen(produtos)

    # Salvando a transação
    dict_request = {
        "url": url_main_maxipago + '/api/v1/maxipago/cliente/cartao-credito/autorizacao',
        "body": create_authorization | authorization_obj,
    }

    answer, request_response = request_maxipago_api(**dict_request)

    tipo_erro = request_response["resposta"]["tipo"]

    if not answer and (tipo_erro != '5'):
        return answer, request_response

    motive = "Transação negada."
    id_motive = 14

    if tipo_erro == '5':

        error_message = str(request_response["erro"]["descricao"]).lower()
        
        with open("other\json\erros_pedidos.json", "r", encoding="utf-8") as f:
            errors_json = json.load(f)

        check_1 = ["customer", "id", "validation", "error"]
        if all([word in error_message for word in check_1]):
            id_motive = 17

        for dict_error in errors_json["cartao"]:
            if dict_error["id_motivo"] == id_motive:
                motive = dict_error["motivo"]
                break

        else:
            id_motive = 14

        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cartão de crédito recusado."},
               "dados": {
                   "status": False,
                   "id_pedido": None,
                   "id_motivo": id_motive,
                   "motivo": motive
               }}, 200

        return False, msg

    transaction_response = request_response.get("dados")

    if transaction_response.get("responseCode") in {"0", '5'}:
        
        data_hora = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        timestamp = transaction_response.get("transactionTimestamp")
        timestamp = datetime.datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]\
                        if str(timestamp).isdecimal else data_hora
        
        match maxipago_provider.upper():

            case "REDE":
                provider_id = "2"
            case "GETNET":
                provider_id = "3"
            case "CIELO":
                provider_id = "4"
            case "STONE":
                provider_id = "9"
            case _:
                provider_id = '1'

        transaction_insert = {
            "id_pedido": None,
            "id_distribuidor": id_distribuidor,
            "transaction_id": transaction_response.get("transactionID"),
            "auth_code": transaction_response.get("authCode"),
            "order_id": transaction_response.get("orderID"),
            "reference_number": transaction_response.get("referenceNum"),
            "transaction_type": 'FIRST CHECK',
            "customer_id": id_maxipago,
            "transaction_amount": f"{valor:.2f}",
            "shipping_amount": frete,
            "transaction_date": timestamp,
            "approval_code": None,
            "payment_type": None,
            "response_code": transaction_response.get("responseCode"),
            "billing_name": payer_data.get("name"),
            "billing_address": payer_data.get("address"),
            "billing_address_2": payer_data.get("address2") if payer_data.get("address2") else None,
            "billing_city": payer_data.get("city"),
            "billing_state": payer_data.get("state"),
            "billing_country": 'BR',
            "billing_phone": payer_data.get("phone"),
            "billing_email": payer_data.get("email"),
            "comments": None,
            "transaction_status": None,
            "transaction_state": 1,
            "credit_card_type": str(bandeira).upper(),
            "boleto_url": None,
            "boleto_number": None,
            "expiration_date": None,
            "processor_id": provider_id,
            "number_of_installments": parcelas if str(parcelas).isdecimal() else None,
            "charge_interest": 'N',
            "processor_transaction_id": transaction_response.get("processorTransactionID"),
            "processor_reference_number": transaction_response.get("processorReferenceNumber"),
            "data_insert": data_hora
        }

        dm.raw_sql_insert("PEDIDO_TRANSACAO", transaction_insert)

        return True, transaction_response | {"id_maxipago": id_maxipago}

    elif transaction_response.get("responseCode") in {"1", '2'}:

        error_message = transaction_response.get("errorMessage")

        if error_message:
            logger.error(f"Cartao de credito recusado: {error_message}")
        
        elif str(transaction_response.get("responseMessage")).lower() == "declined":
            logger.error(f"Valor de {valor} recusado pelo cartão de crédito.")
        
        else:
            logger.error(f"Cartao de credito recusado porem a maxipago não retornou mensagem de erro")

        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cartão de crédito recusado."},
               "dados": {
                   "status": False,
                   "id_pedido": None,
                   "id_motivo": 7,
                   "motivo": f"Compra pelo cartão de crédito foi recusada."
               }}, 200

        return False, msg

    elif transaction_response.get("responseCode") in {"1024", "1025"}:

        error_message = str(transaction_response.get("errorMessage"))

        logger.error(f"Erro na transacao de cartao de credito. {error_message}")

        check_1 = ["insufficient", "funds", "contact", "issuer"]
        check_2 = ["contact", "issuer"]
        check_3 = ["card","number","not","valid","invalid"]
        check_4 = ["card","expired"]
        check_5 = ["card","month","not","valid","range"]
        check_6 = ["card","verification","value","digits","length"]
        check_7 = ["merchantid", "type", "unknown", "run", "credit", "cards", "authorized"]

        error_message = error_message.lower()

        with open("other\json\erros_pedidos.json", "r", encoding="utf-8") as f:
            errors_json = json.load(f)

        if all([word in error_message for word in check_1]):
            id_motive = 8

        elif all([word in error_message for word in check_2]):
            id_motive = 9

        elif all([word in error_message for word in check_3]):
            id_motive = 10

        elif all([word in error_message for word in check_4]):
            id_motive = 11

        elif all([word in error_message for word in check_5]):
            id_motive = 12

        elif all([word in error_message for word in check_6]):
            id_motive = 13

        elif all([word in error_message for word in check_7]):
            id_motive = 16

        for dict_error in errors_json["cartao"]:
            if dict_error["id_motivo"] == id_motive:
                motive = dict_error["motivo"]
                break

        else:
            id_motive = 14

        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cartão de crédito recusado."},
               "dados": {
                   "status": False,
                   "id_pedido": None,
                   "id_motivo": id_motive,
                   "motivo": motive
               }}, 200

        return False, msg

    elif transaction_response.get("responseCode") in {"2048"}:

        error_message = transaction_response.get("errorMessage")

        logger.error(f"Erro na transacao de cartao de credito. {error_message}")
        msg = {"status":400,
                "resposta":{
                    "tipo":"13",
                    "descricao":f"Ação recusada: Erro com o servidor de cartão de credito."},
                "dados": {
                    "status": False,
                    "id_pedido": None,
                    "id_motivo": 7,
                    "motivo": f"Erro de comunicação com o servidor de cartão de crédito."
                }}, 200

        return False, msg

    logger.error(f"Erro Codigo: {transaction_response.get('responseCode')} -> {transaction_response.get('errorMessage')}")
    msg = {"status":400,
           "resposta":{
               "tipo":"13",
               "descricao":f"Ação recusada: Erro com a aprovação de crédito do cartão."},
           "dados": {
                "status": False,
                "id_pedido": None,
                "id_motivo": 14,
                "motivo": f"Erro com a aprovação de crédito do cartão."
           }}, 200

    return False, msg


def maxipago_cancelamento_transacao(transaction_id: str, id_maxipago: str, id_distribuidor: int) -> tuple:
    """
    Cancela o pedido que já foi autorizado pela maxipago

    Args:
        transaction_id (str): Transaction Id do pedido entregue pela autorização da maxipago.
        id_distribuidor (int): Id distribuidor pai do pedido.
        id_maxipago (str): Id da maxipago para o usuario

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """
    answer, response = maxipago_distribuidor_keys(id_distribuidor)
    if not answer:
        return False, response

    void_transaction = {
        "id_maxipago": id_maxipago,
        "id_transacao": transaction_id,
    }

    dict_request = {
        "url": url_main_maxipago + "/api/v1/maxipago/cliente/cartao-credito/captura/cancelamento",
        "body": void_transaction | response,
    }

    return request_maxipago_api(**dict_request)

    
def maxipago_cancelamento_pedido_compra(id_pedido: int, transaction_id: str, id_maxipago: str) -> tuple:
    """
    Realiza o cancelamento da compra da maxipago

    Args:
        id_pedido (int): ID do pedido da maxipago
        transaction_id (str): ID da transação da maxipago a ser cancelada
        id_maxipago (str): Id da maxipago para o usuario

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    pedido_query = """
        SELECT
            TOP 1 p.id_orcamento,
            p.id_distribuidor,
            pt.order_id,
            pt.reference_number,
            p.valor

        FROM
            PEDIDO p

            INNER JOIN PEDIDO_TRANSACAO pt ON
                p.id_pedido = pt.id_pedido

        WHERE
            p.id_pedido = :id_pedido
    """

    params = {
        "id_pedido": id_pedido
    }

    pedido_query = dm.raw_sql_return(pedido_query, params = params, first = True)

    void_transaction = {
        "transaction_id": transaction_id,
        "id_maxipago": id_maxipago,
        "id_distribuidor": pedido_query.get("id_distribuidor"),
    }

    answer, response = maxipago_cancelamento_transacao(**void_transaction)
    if not answer:
        return False, response

    transaction_response = response

    data_hora = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    insert_transaction = {
        "id_distribuidor": pedido_query.get("id_distribuidor"),
        "id_pedido": id_pedido,
        "order_id": pedido_query.get("order_id"),
        "reference_number": pedido_query.get("reference_number"),
        "transaction_type": str(transaction_response.get('responseMessage')).upper(),
        "customer_id": None,
        "transaction_amount": pedido_query.get("valor"),
        "shipping_amount": '0.00',
        "transaction_date": data_hora,
        "approval_code": None,
        "payment_type": None,
        "response_code": transaction_response.get("responseCode"),
        "billing_name": None,
        "billing_address": None,
        "billing_address_2": None,
        "billing_city": None,
        "billing_state": None,
        "billing_country": None,
        "billing_phone": None,
        "billing_email": None,
        "comments": None,
        "transaction_status": None,
        "transaction_state": None,
        "credit_card_type": transaction_response.get("creditCardScheme"),
        "boleto_url": None,
        "boleto_number": None,
        "expiration_date": None,
        "processor_id": None,
        "number_of_installments": None,
        "charge_interest": None,
        "processor_transaction_id": None,
        "processor_reference_number": None,
        "data_insert": data_hora,
        "auth_code": None,
        "transaction_id": transaction_response.get("transactionID"),
    }

    dm.raw_sql_insert("PEDIDO_TRANSACAO", insert_transaction)

    update_orcamento = {
        "id_orcamento": pedido_query.get("id_orcamento"),
        "status": 'I'
    }

    key_columns = ["id_orcamento"]

    dm.raw_sql_update("ORCAMENTO", update_orcamento, key_columns)

    return True, None


######### Pix

def request_pix_api(url: str, headers: dict = None, body: dict = None, timeout: float = None,
                    ignore_error: bool = False) -> tuple:
    """
    Realiza um POST para os serviços da pix e retorna uma mensage deu certo ou não

    Args:
        url (str): Url da pix que deverá ser feita a requisição
        headers (dict, optional): Headers adicionais da requisição. Defaults to None.
        body (dict, optional): Body da requisição. Defaults to None.
        timeout (float, optional): Timeout da requisição. Defaults to None.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if not isinstance(headers, dict):
        headers = {}

    if not isinstance(headers, dict):
        headers = {}

    if not (isinstance(timeout, (float, int))) and timeout is not None:
        timeout = 5

    try:
        headers["Content-Type"] = "application/json"

        if body:
            body["aplicacao"] = "UNOMARKET"

        dict_request = {
            "url": url,
            "json": body,
            "headers": headers,
            "timeout": timeout,
        }

        request_response = requests.post(**dict_request)

        status_code = request_response.status_code
        
        if not ignore_error:

            if status_code != 200:

                json_data = request_response.json()

                if status_code == 422:

                    extra = json_data.get("details")

                    logger.error("Erro com o envio dos dados para a api do pix", extra = extra)

                    msg = {"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Houve erro com a request para o pix."}}, 200

                    return False, msg
                
                elif status_code == 400:

                    tipo = str(json_data["resposta"]["tipo"])

                    if tipo == "5":

                        extra = json_data["erro"]
                        logger.error(f"Erro especifico do pix", extra = extra)

                    elif tipo == "6":
                        extra = json_data["detalhes"]
                        logger.error(f"Erro de validação de dados no pix", extra = extra)

                    else:
                        msg = json_data["resposta"]["descricao"]
                        logger.error(f"Erro de comunicação com a api do pix: {msg}")

                    msg = {"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Houve erro com a request para o pix."}}, 200

                    return False, msg
                
                else:
                    
                    logger.error("Erro no servidor de integração do pix", extra = json_data)
                
                    msg = {"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Houve erro com a request para o pix."}}, 200

                    return False, msg

        return True, request_response

    except requests.exceptions.Timeout as e:

        logger.exception(e)
        
        msg = {"status":400,
               "resposta":{
                   "tipo":"15",
                   "descricao":f"Api do pix não respondeu no tempo determindado."}}, 200

        return False, msg

    except Exception as e:

        logger.exception(e)

        msg = {"status":400,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Houve erro com a request para o pix."}}, 200

        return False, msg


def pix_gerar_cobranca(id_usuario: int, valor: float) -> tuple:
    """
    Gera uma cobranca pix para o pedido

    Args:
        id_usuario (int): Id do usuario
        valor (float): Valor que será cobrado na cobrança

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    query = """
        SELECT
            TOP 1 UPPER(Nome) nome,
            cpf,
            email

        FROM
            USUARIO

        WHERE
            id_usuario = :id_usuario
    """

    params = {
        "id_usuario": id_usuario,
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
        "first": True,
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:

        logger.error(f"Id do usuario inválido na criação da cobrança pix.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Usuario não existente."}}, 200

        return False, msg
    
    nome, cpf, email = response

    # Realizando a requisição

    dict_request = {
        "url": f"{url_main_pix}/api/v1/pix/cobranca",
        "body": {
            "valor_cobrado": valor,
            "cpf": cpf,
            "nome": nome,
            "email": email,
        },
        "timeout": 60,
    }

    answer, response = request_pix_api(**dict_request)
    if not answer:
        return False, response
    
    dict_pix = response.json()["dados"]

    insert_data = dict_pix | {"id_usuario": id_usuario}

    dm.raw_sql_insert("PIX_COBRANCAS", insert_data)

    # Pegando o id_cobranca
    query = """
        SELECT
            id_cobranca

        FROM
            PIX_COBRANCAS

        WHERE
            txid = :txid
    """

    params = {
        "txid": dict_pix["txid"],
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
        "first": True,
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:

        logger.error(f"Erro no salvamento da cobranca.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"15",
                   "descricao":f"Erro no salvamento da cobrança."}}, 200

        return False, msg
    
    id_cobranca = response[0]

    return True, dict_pix | {"id_cobranca": id_cobranca}


def pix_alinhar_pedido_cobranca(id_cobranca: int, id_pedido: int, id_distribuidor: int):
    """
    Alinha o pedido a cobranca pix

    Args:
        id_cobranca (int): ID da cobranca pix
        id_pedido (int): ID do pedido
        id_distribuidor (int): ID do distribuidor
    """

    insert_pedido_cobranca = {
        "id_cobranca": id_cobranca,
        "id_pedido": id_pedido,
        "id_distribuidor": id_distribuidor,
    }

    dm.raw_sql_insert("PEDIDO_PIX", insert_pedido_cobranca)


def pix_checar_status(txid: str = None) -> tuple:
    """
    Checa o status de uma cobranca pix

    Args:
        txid (str): Codigo da cobranca. Defaults to None

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    query = """
        SELECT
            TOP 1 txid

        FROM
            PIX_COBRANCAS

        WHERE
            txid = :txid
    """

    params = {
        "txid": txid,
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
        "first": True,
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:

        logger.error(f"Codigo de cobrança txid:{txid} não existente para checagem de status.")

        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Codigo de cobrança inexitente para checagem de status."}}, 200

        return False, msg
    
    # Realizando a requisição
    dict_request = {
        "url": f"{url_main_pix}/api/v1/pix/cobranca/status",
        "body": {
            "txid": txid,
        },
        "timeout": 60,
    }

    answer, response = request_pix_api(**dict_request)
    if not answer:
        return False, response
    
    dict_pix = response.json()["dados"]

    return True, dict_pix    


def pix_job(id_cobranca: int) -> tuple:
    """
    Checa e atualiza o status de uma cobranca pix

    Args:
        id_cobranca (int): Id da cobranca a ser verificada. Defaults to None

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    query = """
        SELECT
            TOP 1 txid,
            CASE WHEN data_expiracao < GETDATE() THEN 1 ELSE 0 END check_expiracao, 
            status

        FROM
            PIX_COBRANCAS

        WHERE
            id_cobranca = :id_cobranca
            and d_e_l_e_t_ = 0
    """

    params = {
        "id_cobranca": id_cobranca,
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:

        logger.error(f"Codigo de cobrança id_cobranca:{id_cobranca} não existente para checagem de status.")    
        return False
    
    old_status = str(response.get("status")).upper()
    old_expiration_date_check = response.get("check_expiracao")
    txid = response.get("txid")

    if old_status != "ATIVA":

        logger.info(f"Codigo de cobrança id_cobranca:{id_cobranca} com status diferente de 'ATIVA'.")    
        return True

    # Realizando a requisição
    dict_request = {
        "url": f"{url_main_pix}/api/v1/pix/cobranca/status",
        "body": {
            "txid": txid,
        },
        "timeout": 60,
    }

    answer, response = request_pix_api(**dict_request)

    if not answer:

        if old_expiration_date_check:

            query = """
                UPDATE
                    PIX_COBRANCAS

                SET
                    status = 'EXPIRADO'

                WHERE
                    id_cobranca = :id_cobranca
            """

            params = {
                "id_cobranca": id_cobranca,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            dm.raw_sql_execute(**dict_query)

            return True

        return False

    dict_pix = response.json()["dados"]

    query = """
        UPDATE
            PIX_COBRANCAS

        SET
            status = :status

        WHERE
            id_cobranca = :id_cobranca
    """

    params = {
        "id_cobranca": id_cobranca,
        "status": dict_pix.get("status"),
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    dm.raw_sql_execute(**dict_query)

    return True


######### Finalizações

def deletar_pedido(id_pedido: int):
    """
    Atualiza a etapa do pedido e o deleta

    Args:
        id_pedido (int): Id do pedido que deve ser deletado
    """
    query = """
        SELECT
            TOP 1 id_orcamento,
            forma_pagamento

        FROM
            PEDIDO

        WHERE
            id_pedido = :id_pedido
    """

    params = {
        "id_pedido": id_pedido
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)
    if not response:
        return

    update_orcamento = {
        "id_orcamento": response[0],
        "status": 'A'
    }

    key_columns = ["id_orcamento"]
    dm.raw_sql_update("ORCAMENTO", update_orcamento, key_columns)

    update_pedido = {
        "id_pedido": id_pedido,
        "d_e_l_e_t_": 1
    }

    key_columns = ["id_pedido"]

    dm.raw_sql_update("PEDIDO", update_pedido, key_columns)
    dm.raw_sql_update("PEDIDO_ITEM", update_pedido, key_columns)

    dict_atualizar_etapa = {
        "id_pedido": id_pedido,
        "id_formapgto": response[1],
        "id_subetapa": 9
    }

    atualizar_etapa_pedido(**dict_atualizar_etapa)


def decrementar_uso_cupom(id_cupom: int):
    """
    Decrementa a quantidade de usos do cupom

    Args:
        id_cupom (int): id do cupom a ser afetado
    """

    if isinstance(id_cupom, int):
        query = """
            UPDATE
                CUPOM

            SET
                qt_reutiliza = CASE WHEN qt_reutiliza - 1 <= 0 THEN 0 ELSE qt_reutiliza - 1 END

            WHERE
                id_cupom = :id_cupom
        """

        params = {
            "id_cupom": id_cupom
        }

        dm.raw_sql_execute(query, params = params)


def decrementar_uso_oferta(ofertas: list):
    """
    Decrementa quantidade de ofertas utilizaveis para as ofertas informadas

    Args:
        id_oferta (list): Lista com os id de ofertas
    """

    if ofertas and isinstance(ofertas, list):

        for id_oferta in ofertas:
            
            if id_oferta and isinstance(id_oferta, int):

                query = """
                    UPDATE
                        OFERTA

                    SET
                        limite_ativacao_oferta = CASE
                                                     WHEN (limite_ativacao_oferta - 1) <= 0
                                                         THEN 0
                                                     ELSE
                                                         limite_ativacao_oferta - 1
                                                 END
                    
                    WHERE
                        id_oferta = :id_oferta
                """

                params = {
                    "id_oferta": id_oferta
                }

                dict_query = {
                    "query": query,
                    "params": params
                }

                dm.raw_sql_execute(**dict_query)


def comunicar_api_distribuidor(*args, **kwargs):

    dict_order = {
        "id_pedido": kwargs.get("id_pedido"),
        "id_usuario": kwargs.get("id_usuario"),
        "id_cliente": kwargs.get("id_cliente"),
    }

    try:
        answer = jm.send_order_jsl(**dict_order)
    
    except:
        deletar_pedido(kwargs.get("id_pedido"))
        raise        
    
    if not answer:
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Distribuidor negou registro do pedido."},
               "dados": {
                   "status": False,
                   "id_pedido": None,
                   "id_motivo": 5,
                   "motivo": f"Distribuidor negou registro do pedido."
               }}, 200

        return False, msg
               
    return True, None


#### ============ Modulos Principais ============ ####


def registro_pedido(id_distribuidor: int, id_orcamento: int, id_cliente: int, id_usuario: int, 
                        product_list: list[dict], value: float, id_formapgto: int, 
                        id_condpgto: int, **kwargs) -> tuple:
    """
    Registra o pedido do cliente

    Args:
        id_distribuidor (int): ID do distribuidor do pedido
        id_orcamento (int): ID do orcamento pai do pedido
        id_cliente (int): ID do cliente no qual será registrado o pedido
        id_usuario (int): ID do usuário que solicitou o pedido
        product_list (list[dict]): Lista de produtos que pertecem ao pedido
        value (float): Valor final do pedido
        id_formapgto (int): ID da forma de pagamento escolhida
        id_condpgto (int): ID da condição de pagamento escolhida

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    msg = ""

    percentual = kwargs.get('percentual')
    parcelas = kwargs.get('parcelas')
    id_cartao = kwargs.get("id_cartao")
    cvv = kwargs.get('cvv')
    frete = kwargs.get('frete')
    juros = kwargs.get('juros')
    id_cupom = kwargs.get('id_cupom')
    desconto_cupom = kwargs.get('desconto_cupom')
    cupom_aplicado = kwargs.get("cupom_aplicado")

    data_criacao = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    id_pedido = 0

    try:

        # Pagamento com boleto
        if id_formapgto == 1:

            dict_saldo = {
                "id_distribuidor": id_distribuidor,
                "id_cliente": id_cliente,
                "valor": value,
            }

            answer, response = verificar_saldo_cliente(**dict_saldo)

            if not answer:
                return False, response

            dict_pedido = {
                "id_distribuidor": id_distribuidor,
                "id_orcamento": id_orcamento,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "id_formapgto": id_formapgto,
                "id_condpgto": id_condpgto,
                "percentual": percentual,
                "valor": value,
                "juros": juros,
                "frete": frete,
                "id_cupom": id_cupom,
                "desconto_cupom": desconto_cupom,
                "data": data_criacao
            }

            answer, response = criar_id_pedido(**dict_pedido)

            if not answer:
                return False, response

            id_pedido = response.get('id_pedido')

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 0,
                "id_subetapa": 1
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            dict_produto = {
                "id_distribuidor": id_distribuidor,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "id_pedido": id_pedido,
                "product_list": product_list,
            }

            answer, response = adicionar_produto_pedido(**dict_produto)

            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            ofertas = response.get("ofertas")

            dict_atualizar_saldo = {
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor,
                "valor": value
            }

            answer, response = atualizar_saldo_cliente(**dict_atualizar_saldo)

            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            dict_orcamento_off = {
                "id_orcamento": id_orcamento,
                "status": 'F'
            }

            answer, response = atualizar_orcamento(**dict_orcamento_off)

            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            dict_api_distribuidor = {
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
                "id_pedido": id_pedido,
            }

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 0,
                "id_subetapa": 2
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            answer, response = comunicar_api_distribuidor(**dict_api_distribuidor)
            
            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 1,
                "id_subetapa": 1
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            if cupom_aplicado:
                decrementar_uso_cupom(id_cupom)

            decrementar_uso_oferta(ofertas)

            return True, {"id_pedido": id_pedido}

        # Pagamento com cartão de credito
        elif id_formapgto == 2:

            dict_check_card = {
                "id_usuario": id_usuario,
                "id_cartao": id_cartao,
                "cvv": cvv
            }

            answer, response = checar_cartao_salvo(**dict_check_card)

            if not answer:
                return False, response

            cvv = response.get("cvv")
            numero_cartao = response.get("numero_cartao")
            bandeira = response.get("bandeira")

            dict_authorization_card = {
                "id_usuario": id_usuario, 
                "id_cliente": id_cliente, 
                "id_distribuidor": id_distribuidor,
                "id_orcamento": id_orcamento,
                "numero_cartao": numero_cartao, 
                "bandeira": bandeira, 
                "cvv": cvv, 
                "parcelas": parcelas,
                "valor": value,
                "frete": frete,
                "produtos": product_list
            }

            answer, response = maxipago_autorizacao_cartao(**dict_authorization_card)

            if not answer:
                return False, response

            transaction_id = response.get('transactionID')
            id_maxipago = response.get("id_maxipago")

            void_transaction = {
                "transaction_id": transaction_id,
                "id_maxipago": id_maxipago,
                "id_distribuidor": id_distribuidor,
            }

            dict_pedido = {
                "id_distribuidor": id_distribuidor,
                "id_orcamento": id_orcamento,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "id_formapgto": id_formapgto,
                "id_condpgto": id_condpgto,
                "percentual": percentual,
                "valor": value,
                "juros": juros,
                "frete": frete,
                "id_cupom": id_cupom,
                "desconto_cupom": desconto_cupom,
                "data": data_criacao
            }

            answer, response = criar_id_pedido(**dict_pedido)

            if not answer:
                maxipago_cancelamento_transacao(**void_transaction)
                return False, response

            id_pedido = response.get('id_pedido')

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 0,
                "id_subetapa": 1
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            dict_produto = {
                "id_distribuidor": id_distribuidor,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "id_pedido": id_pedido,
                "product_list": product_list,
            }

            answer, response = adicionar_produto_pedido(**dict_produto)

            if not answer:
                maxipago_cancelamento_transacao(**void_transaction)
                deletar_pedido(id_pedido)
                return False, response

            ofertas = response.get("ofertas")

            dict_orcamento_off = {
                "id_orcamento": id_orcamento,
                "status": 'F'
            }

            answer, response = atualizar_orcamento(**dict_orcamento_off)

            if not answer:
                maxipago_cancelamento_transacao(**void_transaction)
                deletar_pedido(id_pedido)
                return False, response

            dict_update_pedido = {
                "id_orcamento": id_orcamento,
                "id_pedido": id_pedido,
                "id_distribuidor": id_distribuidor
            }

            answer, response = atualizar_transacao_inicial(**dict_update_pedido)

            if not answer:
                maxipago_cancelamento_transacao(**void_transaction)
                deletar_pedido(id_pedido)
                return False, response

            dict_api_distribuidor = {
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
                "id_pedido": id_pedido,
            }

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 0,
                "id_subetapa": 2
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            answer, response = comunicar_api_distribuidor(**dict_api_distribuidor)
            
            if not answer:
                deletar_pedido(id_pedido)
                maxipago_cancelamento_transacao(**void_transaction)
                return False, response

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 1,
                "id_subetapa": 1
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            if cupom_aplicado:
                decrementar_uso_cupom(id_cupom)
                
            decrementar_uso_oferta(ofertas)

            return True, {"id_pedido": id_pedido}

        # Pagamento com pix
        elif id_formapgto == 3:

            # Gerar cobranca pix
            dict_pix = {
                "id_usuario": id_usuario,
                "valor": value,
            }

            answer, response = pix_gerar_cobranca(**dict_pix)

            if not answer:
                return False, response
            
            id_cobranca = response.get("id_cobranca")
            pix_info = response.copy()
            
            # Criar o pedido
            dict_pedido = {
                "id_distribuidor": id_distribuidor,
                "id_orcamento": id_orcamento,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "id_formapgto": id_formapgto,
                "id_condpgto": id_condpgto,
                "percentual": percentual,
                "valor": value,
                "juros": juros,
                "frete": frete,
                "id_cupom": id_cupom,
                "desconto_cupom": desconto_cupom,
                "data": data_criacao
            }

            answer, response = criar_id_pedido(**dict_pedido)

            if not answer:
                return False, response

            id_pedido = response.get('id_pedido')

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 0,
                "id_subetapa": 1
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            dict_produto = {
                "id_distribuidor": id_distribuidor,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "id_pedido": id_pedido,
                "product_list": product_list,
            }

            answer, response = adicionar_produto_pedido(**dict_produto)

            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            ofertas = response.get("ofertas")

            dict_orcamento_off = {
                "id_orcamento": id_orcamento,
                "status": 'F'
            }

            answer, response = atualizar_orcamento(**dict_orcamento_off)

            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            dict_api_distribuidor = {
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
                "id_pedido": id_pedido,
            }

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 0,
                "id_subetapa": 2
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            answer, response = comunicar_api_distribuidor(**dict_api_distribuidor)
            
            if not answer:
                deletar_pedido(id_pedido)
                return False, response

            dict_atualizar_etapa = {
                "id_pedido": id_pedido,
                "id_formapgto": id_formapgto,
                "id_etapa": 1,
                "id_subetapa": 1
            }

            atualizar_etapa_pedido(**dict_atualizar_etapa)

            dict_pedido_cobranca = {
                "id_pedido": id_pedido,
                "id_distribuidor": id_distribuidor,
                "id_cobranca": id_cobranca,
            }

            pix_alinhar_pedido_cobranca(**dict_pedido_cobranca)

            if cupom_aplicado:
                decrementar_uso_cupom(id_cupom)
                
            decrementar_uso_oferta(ofertas)

            dict_return = {"id_pedido": id_pedido} | pix_info

            return True, dict_return

        else:
            logger.error(f"Forma de pagamento {id_formapgto} informada nao implementada.")
            msg = {"status":400,
                "resposta":{
                    "tipo":"13",
                    "descricao":f"Ação recusada: Forma de pagamento escolhida ainda não suportada."}}, 200

        return False, msg
    
    except:

        if id_pedido:
            deletar_pedido(id_pedido)

        raise
   

def pegar_pedido(id_pedido: int, id_usuario: int, id_cliente:int):
    """
    Pega as informações do pedido escolhido

    Args:
        id_pedido (int): Id do pedido
        id_usuario (int): Id do usuario pai do pedido
        id_cliente (int): Id do cliente pai do pedido
    """

    # Pegar informações do pedido
    pedido_query = """
        SELECT
            TOP 1 p.id_distribuidor,
            p.data_finalizacao,
            p.valor,
            p.forma_pagamento,
            p.condicao_pagamento,
            p.data_criacao,
            p.id_orcamento,
            p.percentual,
            p.juros,
            p.frete,
            p.id_cupom,
            p.desconto_cupom

        FROM
            PEDIDO p

        WHERE
            p.id_pedido = :id_pedido
            AND p.id_usuario = :id_usuario
            AND p.id_cliente = :id_cliente
            AND d_e_l_e_t_ = 0
    """

    params = {
        "id_pedido": id_pedido,
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
    }

    dict_query = {
        "query": pedido_query,
        "params": params,
        "first": True,
    }

    pedido_query = dm.raw_sql_return(**dict_query)

    if not pedido_query:
        return {}

    # Pegando os metodos de pagamento
    id_formapgto = pedido_query.pop("forma_pagamento")
    id_condpgto = pedido_query.pop("condicao_pagamento")
    percentual = pedido_query.pop("percentual")
    juros = pedido_query.pop("juros")

    metodo_pagamento_query = """
        SELECT
            TOP 1 fp.id_formapgto as 'id',
            fp.descricao as 'nome',
            cp.id_condpgto,
            cp.descricao,
            cp.percentual,
            ISNULL(cp.juros_parcela, 0) as juros

        FROM
            FORMA_PAGAMENTO fp

            INNER JOIN CONDICAO_PAGAMENTO cp ON
                fp.id_formapgto = cp.id_formapgto

        WHERE
            fp.id_formapgto = :id_formapgto
            AND cp.id_condpgto = :id_condpgto
    """

    params = {
        "id_formapgto": id_formapgto,
        "id_condpgto": id_condpgto
    }

    dict_query = {
        "query": metodo_pagamento_query,
        "params": params,
        "first": True,
    }

    metodos_pagamentos = dm.raw_sql_return(**dict_query)

    if not metodos_pagamentos:
        metodos_pagamentos = {
            "id": id_formapgto,
            "nome": '',
            "id_condpgto": id_condpgto,
            "descricao": "",
            "percentual": percentual,
            "juros": juros
        }

    # Pegando os itens do pedido
    id_orcamento = pedido_query.get("id_orcamento")

    dict_itens = {
        "id_pedido": id_pedido,
        "percentual": percentual
    }

    itens_pedido = dj.json_pedido_produto(**dict_itens)

    # Pegando os valores atrelados aos itens
    frete = pedido_query.pop("frete", 0)
    id_cupom = pedido_query.pop("id_cupom")
    desconto_cupom = pedido_query.pop("desconto_cupom", 0)
    cupom = {}

    dict_valores = {
        "list_products": itens_pedido,
        "frete": frete,
        "percentual": percentual,
        "juros": juros,
        "desconto_cupom": desconto_cupom
    }

    valores = retorno_valores_produto_pedido(**dict_valores)

    bonus = valores.get("bonus", 0)
    desconto = valores.get("desconto", 0)
    subtotal = valores.get("subtotal", 0)
    liquido = valores.get("subtotal_liquido", 0)

    total_calculado = float(valores.get("total"))

    total = pedido_query.get("valor")

    if total_calculado != total:
        key_columns = ["id_pedido"]
        update_data = {"id_pedido": id_pedido, "valor": total_calculado}
        dm.raw_sql_update("PEDIDO", update_data, key_columns)

    # Pegando as etapas do pedido
    etapas_query = """
        SELECT
            pe.id_etapa,
            pe.descricao_etapa,
            pe.id_subetapa,
            pe.descricao_subetapa,
            pe.data_registro

        FROM
            (

                SELECT
                    id_pedido,
                    id_etapa,
                    MAX(id_subetapa) as id_subetapa
                
                FROM
                    PEDIDO_ETAPA

                WHERE
                    id_pedido = :id_pedido
                    AND id_cliente = :id_cliente
                    AND id_orcamento = :id_orcamento
                    AND id_usuario = :id_usuario

                GROUP BY
                    id_pedido,
                    id_etapa

            ) etapas

            INNER JOIN PEDIDO_ETAPA pe ON
                etapas.id_pedido = pe.id_pedido
                AND etapas.id_etapa = pe.id_etapa
                AND etapas.id_subetapa = pe.id_subetapa

        ORDER BY
            pe.id_pedido,
            pe.id_etapa
    """

    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "id_orcamento": id_orcamento,
        "id_pedido": id_pedido,
    }

    dict_query = {
        "query": etapas_query,
        "params": params,
    }

    etapas_query = dm.raw_sql_return(**dict_query)

    etapas = {
        str(etapa.get("id_etapa")): {
            "etapa": etapa.get("descricao_etapa"),
            "id": etapa.get("id_subetapa"),
            "mensagem": etapa.get("descricao_subetapa"),
            "status": True if etapa.get("id_subetapa") != 9 else False
        }

        for etapa in etapas_query
    }  

    # Pegando o cupom
    if id_cupom:
        cupom = dj.json_pedido_cupom(id_cupom = id_cupom)
        if cupom:
            cupom = cupom[0]

    # Verificando se o pedido possui nota fiscal
    query = """
        SELECT
            TOP 1 1

        FROM
            PEDIDO_JSL

        WHERE
            id_pedido = :id_pedido
            AND LEN(nf) > 0
            AND LEN(serie) > 0
    """

    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "id_orcamento": id_orcamento,
        "id_pedido": id_pedido,
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True,
    }

    bool_nf = bool(dm.raw_sql_return(**dict_query))

    # Pegando informações do pix
    pix = {}
    
    if id_formapgto == 3:
        
        query = """
            SELECT
                TOP 1 pxc.id_cobranca,
                pxc.txid,
                pxc.valor,
                pxc.copia_e_cola,
                pxc.expiracao,
                pxc.data_expiracao,
                pxc.status,
                CASE WHEN (pxc.data_expiracao < GETDATE()) AND (pxc.status != 'CONCLUIDA') THEN 1 ELSE 0 END as expirado 

            FROM
                PEDIDO pdd

                INNER JOIN PEDIDO_PIX pddpx ON
                    pdd.id_pedido = pddpx.id_pedido
                    AND pdd.id_distribuidor = pddpx.id_distribuidor

                INNER JOIN PIX_COBRANCAS pxc ON
                    pddpx.id_cobranca = pxc.id_cobranca
                    AND pdd.id_usuario = pxc.id_usuario

            WHERE
                pdd.id_pedido = :id_pedido
                AND pdd.id_usuario = :id_usuario
                AND pdd.id_cliente = :id_cliente
                AND pxc.d_e_l_e_t_ = 0

            ORDER BY
                pxc.data_expiracao DESC
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
        cobranca = response if response else {}
        
        if not cobranca or cobranca.get("expirado"):

            count = 0

            while (count < 3):
            
                dict_cobranca = {
                    "id_usuario": id_usuario,
                    "valor": float(total_calculado),
                }

                answer, response = pix_gerar_cobranca(**dict_cobranca)
                
                if answer:
                    cobranca = response
                    break

                count += 1

            if count >= 3:
                return {}
            
            cobranca = response
            id_cobranca = cobranca.get("id_cobranca")

            dict_pedido_cobranca = {
                "id_pedido": id_pedido,
                "id_distribuidor": pedido_query.get("id_distribuidor"),
                "id_cobranca": id_cobranca,
            }

            pix_alinhar_pedido_cobranca(**dict_pedido_cobranca)

        pix = {
            "id_cobranca": cobranca.get("id_cobranca"),
            "txid": cobranca.get("txid"),
            "valor": cobranca.get("valor"),
            "copia_e_cola": cobranca.get("copia_e_cola"),
            "expiracao": cobranca.get("expiracao"),
            "data_expiracao": cobranca.get("data_expiracao"),
            "status": cobranca.get("status"),
        }

    # Criando o JSON
    dict_pedido = {
        "id_pedido": id_pedido,
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "id_orcamento": id_orcamento,
        "id_distribuidor": pedido_query.get("id_distribuidor"),
        "itens": itens_pedido,
        "data_pedido": pedido_query.get("data_criacao"),
        "metodo_pagamento": metodos_pagamentos,
        "qtde_itens": len(itens_pedido),
        "subtotal": float(subtotal),
        "desconto": float(desconto),
        "desconto_cupom": desconto_cupom,
        "bonus": float(bonus),
        "liquido": float(liquido),
        "frete": float(frete),
        "total": float(total_calculado),
        "status_pedido": etapas,
        "cupom": cupom,
        "bool_nf": bool_nf,
    }

    if pix:
        dict_pedido["pix"] = pix

    return dict_pedido


def verificar_produto_pedido(itens: list[dict], qtde_itens: int, total: float, 
                        subtotal: float, desconto: float, bonus: float, 
                        valor_minimo_frete: float, frete: float, frete_liquido: float, 
                        liquido: float, juros: float, percentual: float = 0,
                        cupom: dict = None, desconto_cupom: float = None,
                        total_parcelado: float = 0) -> tuple:
    """
    Checa se os produtos no orcamento batem com o do pedido

    Args:
        itens (list[dict]): Lista dos itens do orcamento
        qtde_itens (int): Quantidade de itens no pedido
        percentual (float): Percentual do financeiro do metodo de pagamento
        desconto (float): Valor total do desconto no pedido
        bonus (float): Valor total do produtos bonificados
        subtotal (float): Valor total calculado dos produtos sem descontos e bonus
        liquido (float): Subtotal com os descontos e bonus
        cupom (dict): Cupom utilizado no pedido
        desconto_cupom (float): Valor que será reduzido do pedido devido ao cupom
        frete (float): Valor do frete
        frete_liquido (float): Valor do frete considerando descontos do cupom
        valor_minimo_frete (float): Valor minimo para frete gratuito
        total (float): Valor liquido aplicado o frete e o desconto do cupom
        juros (float): Juros do metodo de pagamento
        total_parcelado (float): Valor total aplicado o juros

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if not isinstance(cupom, dict) or not cupom:
        cupom = {}

    # Verificar qntd de itens do pedido
    if len(itens) != qtde_itens:
        logger.error(f"Informado qtde_itens diferente da quantidade do orcamento.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Quantidade de itens divergentes."}}, 200
        
        return False, msg

    # Checando o preço
    desconto_cupom_orcamento = 0
    desconto_cupom_aplicado = 0

    if cupom:
        adicionar_cupom_produto_pedido(itens, cupom)

    dict_check_valores = {
        "list_products": itens,
        "percentual": percentual,
        "cupom": cupom
    }

    hold_value_orcamento = retorno_valores_produto_orcamento(**dict_check_valores)

    liquido_orcamento = hold_value_orcamento.get("subtotal_liquido")
    subtotal_orcamento = hold_value_orcamento.get("subtotal")
    bonus_orcamento = hold_value_orcamento.get("bonus")
    desconto_orcamento = hold_value_orcamento.get("desconto")

    desconto_aplicado_cupom_orcamento = hold_value_orcamento.get("desconto_aplicado_cupom")
    valor_atingido_validar_cupom_orcamento = hold_value_orcamento.get("valor_atingido_validar_cupom")

    frete_orcamento = frete

    if liquido_orcamento >= valor_minimo_frete:
        frete_orcamento = 0

    if cupom:
        tipo_cupom = cupom.get("tipo_cupom")
        tipo_desconto_cupom = cupom.get("tipo_desconto")
        desconto_valor_cupom = cupom.get("desconto_valor")
        valor_limite_ativacao_cupom = cupom.get("valor_ativar")
        valor_limite_desconto_cupom = cupom.get("valor_limite")

        if valor_atingido_validar_cupom_orcamento >= valor_limite_ativacao_cupom:

            if tipo_cupom == 1 and frete_orcamento:

                if tipo_desconto_cupom == 1:
                    
                    if desconto_valor_cupom > frete:
                        desconto_valor_cupom = frete

                    desconto_cupom_aplicado = desconto_valor_cupom

                else:
                    desconto_percentual_cupom = cupom.get("desconto_percentual")
                    desconto_cupom_aplicado = frete_orcamento * (desconto_percentual_cupom/100)

                if desconto_cupom_aplicado > valor_limite_desconto_cupom:
                    desconto_cupom_aplicado = valor_limite_desconto_cupom
                    
                frete_orcamento -= desconto_cupom_aplicado

            elif tipo_cupom == 2:

                if tipo_desconto_cupom == 1:
                    desconto_cupom_aplicado = desconto_valor_cupom

                else:
                    desconto_cupom_aplicado = desconto_aplicado_cupom_orcamento

                if desconto_cupom_aplicado > valor_limite_desconto_cupom:
                    desconto_cupom_aplicado = valor_limite_desconto_cupom

                desconto_cupom_orcamento = desconto_cupom_aplicado

    desconto_cupom_aplicado = np.round(desconto_cupom_aplicado, 2)
    if desconto_cupom_aplicado <= 0: desconto_cupom_aplicado = 0

    desconto_cupom_orcamento = np.round(desconto_cupom_orcamento, 2)
    if desconto_cupom_orcamento <= 0: desconto_cupom_orcamento = 0

    frete_orcamento = np.round(frete_orcamento, 2)
    if frete_orcamento <= 0: frete_orcamento = 0

    if desconto_cupom_aplicado <= 0:
        desconto_cupom_aplicado = 0
        cupom_aplicado = False

    total_orcamento = np.round(liquido_orcamento + frete_orcamento - desconto_cupom_orcamento,2)
    total_final =  np.round(total_orcamento * (1 + (juros/100)), 2)
        
    # Verificando os dados conseguidos
    dict_verification = {
        "bonus": {
            "orcamento": bonus_orcamento,
            "informado": bonus
        }, 
        "desconto": {
            "orcamento": desconto_orcamento,
            "informado": desconto
        }, 
        "subtotal": {
            "orcamento": subtotal_orcamento,
            "informado": subtotal
        }, 
        "liquido": {
            "orcamento": liquido_orcamento,
            "informado": liquido
        },
        "frete_liquido": {
            "orcamento": frete_orcamento,
            "informado": frete_liquido
        },
        "desconto_cupom": {
            "orcamento": desconto_cupom_orcamento,
            "informado": desconto_cupom
        },
        "total valor": {
            "orcamento": total_orcamento,
            "informado": total
        }, 
        "total final": {
            "orcamento": total_final,
            "informado": total_parcelado
        }
    }
    
    for key, value in dict_verification.items():

        orc_data = float(np.round(value.get("orcamento"), 2))
        real_data = float(np.round(value.get("informado"), 2))
        
        if real_data != orc_data:
            logger.error(f"{key.capitalize()} real - {orc_data} difere do {key.capitalize()} fornecido - {real_data} informado.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: {key.capitalize()} fornecido é diferente do real."}}, 200
            
            return False, msg

    return True, {
        "cupom_aplicado": cupom_aplicado,
        "desconto_cupom_aplicado": desconto_cupom_aplicado
    }


def maxipago_cadastrar_cliente(id_usuario: int, id_distribuidor: int, nome: str, endereco: str, 
                        numero: str, complemento: str, cidade: str, estado: str, cep: str, 
                        telefone: str, email: str, data_nascimento: datetime.datetime, sexo: str):
    """
    Cadastra um usuario na maxipago

    Args:
        id_usuario (int): ID do usuario que será cadastrado
        id_distribuidor (int): ID do distribuidor que será utilizado na transação da maxipago
        first_name (str): Primeiro nome
        last_name (str): Resto do nome
        endereco (str): Endereço do usuario
        complemento (str): Complemento ao endereço
        cidade (str): Cidade do endereço
        estado (str): Estado da cidade
        cep (str): Cep do endereço
        telefone (str): Telefone do usuario
        email (str): Email do usuario
        data_nascimento (datetime): Objeto de data de nascimento do usuario.
        sexo (str): sexo do usuario
    """

    # Cadastro do usuario no maxipago
    if not isinstance(data_nascimento, datetime.datetime):
        logger.error("Data de nascimento inválida para cadastro de cliente.")
        return False, ({"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Data de nascimento em formato inválido."}}, 200)

    answer, response = maxipago_distribuidor_keys(id_distribuidor)
    if not answer:
        return False, response

    create_client = {
        "id_usuario": id_usuario,
        "email": email,
        "nome": nome,
        "endereco": endereco,
        "numero": numero,
        "complemento": complemento,
        "cidade": cidade,
        "uf": estado,
        "cep": cep,
        "telefone": telefone,
        "sexo": sexo,
        "data_nascimento": data_nascimento.strftime("%Y-%m-%d"),
    }

    dict_request = {
        "url": url_main_maxipago + "/api/v1/maxipago/cliente",
        "body": create_client | response
    }

    answer, response = request_maxipago_api(**dict_request)
    if not answer:
        return False, response

    id_maxipago = response['dados']["id_maxipago"]

    query = """
        IF EXISTS( 
                    SELECT 
                        id_usuario 
                        
                    FROM 
                        USUARIO_DISTRIBUIDOR_MAXIPAGO 
                        
                    WHERE 
                        id_distribuidor = :id_distribuidor
                        AND id_usuario = :id_usuario
                 )

        BEGIN

            UPDATE
                USUARIO_DISTRIBUIDOR_MAXIPAGO

            SET
                id_maxipago = :id_maxipago

            WHERE 
                id_distribuidor = :id_distribuidor
                AND id_usuario = :id_usuario

        END

        ELSE
        BEGIN

            INSERT INTO
                USUARIO_DISTRIBUIDOR_MAXIPAGO
                    (
                        id_usuario,
                        id_distribuidor,
                        id_maxipago
                    )

                VALUES
                    (
                        :id_usuario,
                        :id_distribuidor,
                        :id_maxipago
                    )

        END
    """

    params = {
        "id_usuario": id_usuario,
        "id_distribuidor": id_distribuidor,
        "id_maxipago": id_maxipago
    }

    dm.raw_sql_execute(query, params = params)

    return True, {"id_maxipago": id_maxipago}


def maxipago_cadastrar_cartao(id_usuario: int, numero_cartao: str, ano_vencimento: str, 
                        mes_vencimento: str, bandeira: str, nome: str, endereco: str, 
                        numero_endereco: str, complemento: str, bairro: str, cidade: str, 
                        estado: str, cep: str, cpf: str, ddd: str, telefone: str, email: str, 
                        data_nascimento: datetime.datetime, sexo: str, cvv: str = None, data_criacao: str = None):
    """
    Cadastra um cartão na maxipago para o usuario

    Args:
        id_usuario (int): ID da maxipago do usuario
        numero_cartao (str): Numero do cartão do cliente
        ano_vencimento (str): Ano de vencimento do cartão
        mes_vencimento (str): Mês de vencimento do cartão
        bandeira (str): Bandeira do cartão
        nome (str): Nome do usuario
        endereco (str): Endereço do usuario
        numero_endereco (str): Numero da residência do usuario
        complemento (str): Complemento ao endereço
        bairro (str): Bairro do endereço
        cidade (str): Cidade do endereço
        estado (str): Estado da cidade
        cep (str): Cep do endereço
        cpf (str): Cpf do usuario
        ddd (str): ddd do telefone
        telefone (str): Telefone do usuario
        email (str): Email do usuario
        data_nascimento (datetime): Objeto de data de nascimento do usuario.
        sexo (str): Sexo do usuario
        cvv (str): cvv do cartao. defaults to None.
        data_criacao (str): Data de criacao do registro. defaults to None.
    """

    if not isinstance(data_nascimento, datetime.datetime):
        logger.error("Data de nascimento inválida para cadastro de cartão.")
        return False, ({"status":400,
                        "resposta":{
                            "tipo":"15",
                            "descricao":f"Data de nascimento em formato inválido."}}, 200)

    user_maxipago_query = """
        SELECT
            d.id_distribuidor,
            udm.id_maxipago

        FROM
            DISTRIBUIDOR d

            LEFT JOIN USUARIO_DISTRIBUIDOR_MAXIPAGO udm ON
                d.id_distribuidor = udm.id_distribuidor

        WHERE
            d.status = 'A'
            AND udm.id_usuario = :id_usuario
            AND d.id_distribuidor != 0
            AND LEN(udm.id_maxipago) > 0

        ORDER BY
            d.id_distribuidor
    """

    params = {
        'id_usuario': id_usuario
    }

    dict_query = {
        "query": user_maxipago_query,
        "params": params,
    }

    user_maxipago = dm.raw_sql_return(**dict_query)

    if not user_maxipago:

        logger.error(f"Usuario não possui ids maxipago salvos na base.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Falha na transação. Entre em contato com o suporte."}}, 200

        return False, msg

    card_maxipago = []

    create_card = {
        "numero_cartao": numero_cartao,
        "ano_vencimento": f"20{ano_vencimento}",
        "mes_vencimento": mes_vencimento,
        "email": email,
        "nome": nome,
        "endereco": endereco,
        "complemento": complemento,
        "numero": numero_endereco,
        "bairro": bairro,
        "cep": cep,
        "cidade": cidade,
        "uf": estado,
        "telefone": ddd + telefone,
    }

    for data in user_maxipago:

        id_distribuidor = data.get("id_distribuidor")
        id_maxipago = data.get("id_maxipago")

        answer, response = maxipago_distribuidor_keys(id_distribuidor)
        if not answer:
            return False, response

        dict_request = {
            "url": url_main_maxipago + "/api/v1/maxipago/cliente/cartao-credito",
            "body": create_card | response | {"id_maxipago": id_maxipago}
        }

        answer, response = request_maxipago_api(**dict_request)

        if not answer:
            return False, response

        card_maxipago.append({
            "id_maxipago": id_maxipago,
            "token_maxipago": response["dados"]["token_cartao_maxipago"]
        })

    encoded_cvv = str(base64.b64encode(cvv.encode("ascii")))[2:-1] if cvv else None

    # Checando existência do cartão
    query = """
        SELECT
            id_cartao

        FROM
            USUARIO_CARTAO_DE_CREDITO

        WHERE
            id_usuario = :id_usuario
            AND email = :email
            AND numero_cartao = :numero_cartao
            AND bandeira = :bandeira
            AND ano_vencimento = :ano_vencimento
            AND mes_vencimento = :mes_vencimento
    """

    params = {
        "id_usuario": id_usuario,
        "email": email,
        "numero_cartao": numero_cartao[-4:],
        "bandeira": bandeira,
        "ano_vencimento": f"20{ano_vencimento}",
        "mes_vencimento": mes_vencimento,
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:

        new_user_card = {
            "id_usuario": id_usuario,
            "email": email,
            "numero_cartao": numero_cartao[-4:],
            "bandeira": bandeira,
            "ano_vencimento": f"20{ano_vencimento}",
            "mes_vencimento": mes_vencimento,
            "cvv": encoded_cvv,
            "data_criacao": data_criacao,
            "portador": nome,
            "cpf": cpf,
            "cep": cep,
            "endereco": endereco,
            "numero": numero_endereco,
            "complemento": complemento if complemento else None,
            "bairro": bairro,
            "cidade": cidade,
            "UF": estado,
            "ddd": ddd,
            "telefone": telefone,
            "sexo": str(sexo[0]).upper(),
            "data_nascimento": data_nascimento
        }

        dm.raw_sql_insert("USUARIO_CARTAO_DE_CREDITO", new_user_card)

        query = """
            SELECT
                id_cartao

            FROM
                USUARIO_CARTAO_DE_CREDITO

            WHERE
                id_usuario = :id_usuario
                AND email = :email
                AND numero_cartao = :numero_cartao
                AND bandeira = :bandeira
                AND ano_vencimento = :ano_vencimento
                AND mes_vencimento = :mes_vencimento
                AND data_criacao = :data_criacao
        """

        params = {
            "id_usuario": id_usuario,
            "email": email,
            "numero_cartao": numero_cartao[-4:],
            "bandeira": bandeira,
            "ano_vencimento": f"20{ano_vencimento}",
            "mes_vencimento": mes_vencimento,
            "data_criacao": data_criacao,
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

    id_cartao = response[0]

    new_card = []

    for data in card_maxipago:

        # Checando se o cartão já está salvo para o distribuidor
        query = """
            SELECT
                TOP 1 1

            FROM
                CARTAO_MAXIPAGO

            WHERE
                id_cartao = :id_cartao
                AND id_maxipago = :id_maxipago
                AND token_maxipago = :token_maxipago
        """

        params = {"id_cartao": id_cartao} | data

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:

            new_card.append(params)

    if not new_card:

        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Cartão já salvo."}}, 200

        return False, msg
    
    dm.raw_sql_insert("CARTAO_MAXIPAGO", new_card)

    return True, None


def maxipago_remover_cartao(id_usuario: int, id_cartao: int) -> tuple:
    """
    Remove um cartão da base da maxipago

    Args:
        id_usuario (int): ID do usuario
        id_cartao (int): ID do cartao a ser deletado

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e um
                    objeto com informações do cartão no segundo
    """    

    # Pegando id maxipago
    query = """
        SELECT
            udm.id_maxipago,
            cm.token_maxipago,
            udm.id_distribuidor	

        FROM
            USUARIO_CARTAO_DE_CREDITO ucdc

            INNER JOIN CARTAO_MAXIPAGO cm ON
                ucdc.id_cartao = cm.id_cartao

            INNER JOIN USUARIO_DISTRIBUIDOR_MAXIPAGO udm ON
                cm.id_maxipago = udm.id_maxipago
                AND ucdc.id_usuario = udm.id_usuario

        WHERE
            cm.id_cartao = :id_cartao
            AND ucdc.id_usuario = :id_usuario
            AND LEN(udm.id_maxipago) > 0
            AND LEN(cm.token_maxipago) > 0
    """

    params = {
        "id_usuario": id_usuario,
        "id_cartao": id_cartao
    }

    response = dm.raw_sql_return(query, params = params)

    if not response:

        logger.error(f"Usuario tentando deletar cartao {id_cartao} nao atrelado a si.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Cartão não existente."}}, 200

        return False, msg

    for user_card in response:
        
        id_distribuidor = user_card.get("id_distribuidor")
        id_maxipago = user_card.get("id_maxipago")
        token_maxipago = user_card.get("token_maxipago")

        answer, response = maxipago_distribuidor_keys(id_distribuidor)
        if not answer:
            return False, response

        remove_card = {
            "token_maxipago_cartao": token_maxipago,
            "id_maxipago": id_maxipago,
        }

        # Se comunicando com a maxipago
        url = url_main_maxipago + '/api/v1/maxipago/cliente/cartao-credito/remocao'

        dict_request = {
            "url": url,
            "body": remove_card | response,
        }
            
        answer, response = request_maxipago_api(**dict_request)

        if not answer:
            return False, response

    return True, None