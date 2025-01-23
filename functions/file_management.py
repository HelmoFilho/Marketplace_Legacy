#=== Importações de módulos externos ===#
from unidecode import unidecode
from flask import request
import os, re

#=== Importações de módulos internos ===#
import functions.data_management as dm
from app.server import global_info


def get_files(norm_keys: bool = True, trim_keys: bool = True, not_change_values: list = None) -> dict:
    """
    Pega os arquivos enviados na request e a devolve formatada como um dicionário.

    Args:
        norm_keys (bool, optional): 
            Realiza a normalização (se string, retira caixa baixa e unidecoda a string) nas chaves. 
            Defaults to True.
        trim_keys (bool, optional): 
            Realiza um split e um join para remoção de espaços em branco desnecessarios nas chaves. 
            Defaults to True.
        not_change_values (list, optional): 
            keys dos valores que não devem ser modificados. 
            Defaults to False.

    Returns:
        dict: Dados do request como um dicionário
    """
    
    if not_change_values is None:
        not_change_values = []

    hold = dict(request.files)
    request_files = dict()

    # Realiza a normalização dos dados dependendo dos boleanos
   
    for key, value in hold.items():
        
        if norm_keys or trim_keys:

            if key not in not_change_values:

                # Remoção de caracteres especiais e transformação em caixa-baixa
                if norm_keys:
                    key = unidecode(str(key).lower())

                # Remoção de espaços brancos desnecessários nas chaves
                if trim_keys:
                    key = "".join(str(key).split())

        request_files[key] = value

    global_info.save_info_thread(files = request_files)
    return request_files


def criar_token_imagem_produto(sku_list: list[str] or str):
    """
    Cria os tokens de imagem de produto

    Returns:
        [flask.wrappers.Response]: Situação da transação
    """
    if isinstance(sku_list, str):
        sku_list = [sku_list]

    query = """
        SELECT
            DISTINCT pimg.sku,
            LOWER(p.descricao_completa) as descricao
                
        FROM
            PRODUTO p

            INNER JOIN PRODUTO_IMAGEM pimg ON
                p.sku = pimg.sku
                        
        WHERE
            p.sku IN :skus
            AND LEN(p.descricao_completa) > 0
            AND LEN(pimg.imagem) > 0
            AND LEN(p.sku) > 0
    """

    params = {
        "skus": sku_list
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    produtos = dm.raw_sql_return(**dict_query)

    if not produtos:
        return

    update_list = []

    for produto in produtos:

        sku = str(produto.get("sku"))
        descricao = produto.get("descricao")

        if descricao:

            descricao = unidecode(str(descricao))
            descricao = " ".join(re.sub("[^ 0-9a-z]", " ", descricao).split())
            descricao = descricao.replace(" ", "-")

            update_list.append({
                "sku": sku,
                "descricao_produto": descricao
            })

    if update_list:

        key_columns = ["sku"]
        dm.raw_sql_update("PRODUTO_IMAGEM", update_list, key_columns)

    # Salvando os tokens na tabela de imagem
    query = """
        UPDATE
            PRODUTO_IMAGEM

        SET
            token = 'produto/auto/' + sku + '/' + (CASE WHEN descricao_produto IS NOT NULL THEN (descricao_produto + '/') ELSE '' END) + imagem

        WHERE
            sku IN :skus
            AND LEN(sku) > 0
            AND LEN(imagem) > 0
    """

    params = {
        "skus": sku_list
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    dm.raw_sql_execute(**dict_query)

    # Salvando os tokens na tabela de produto
    query = """
        SET NOCOUNT ON;

        IF OBJECT_ID('tempDB..#HOLD_TOKEN', 'U') IS NOT NULL
        BEGIN
            DROP TABLE #HOLD_TOKEN
        END;

        -- Salvando os tokens
        SELECT
            p.sku,
            STRING_AGG(pimg.token, ',') tokens

        INTO
            #HOLD_TOKEN

        FROM
            PRODUTO_IMAGEM pimg

            INNER JOIN PRODUTO p ON
                pimg.sku = p.sku

        WHERE
            p.sku IN :skus

        GROUP BY
            p.sku;

        -- Update em produtos
        UPDATE
            p

        SET
            p.tokens_imagem = thi.tokens

        FROM
            #HOLD_TOKEN thi

            INNER JOIN PRODUTO p ON
                thi.sku = p.sku;

        UPDATE
            PRODUTO

        SET
            tokens_imagem = NULL

        WHERE
            sku NOT IN (SELECT sku FROM #HOLD_TOKEN);

        DROP TABLE #HOLD_TOKEN;
    """

    params = {
        "skus": sku_list
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    dm.raw_sql_execute(**dict_query)


def product_image_url(sku: str, auto_replace: str = "auto", connector = None):

    server = str(os.environ.get("PSERVER_PS")).lower()

    if server == "production":
        route = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
        
    elif server == "development":
        route = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

    else:
        route = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

    image_query = f"""

        SELECT
            TOP 3 token

        FROM
            PRODUTO_IMAGEM

        WHERE
            sku = :sku     

        ORDER BY
            imagem 
    """

    params = {
        "route": route,
        "sku": sku
    }

    if connector:
        imagens = dm.raw_sql_return(image_query, params = params, raw = True, connector = connector)
        
    else:
        imagens = dm.raw_sql_return(image_query, params = params, raw = True)

    return [
        f"{route}/imagens/{imagem[0]}".replace("/auto/", f"/{auto_replace}/")
        for imagem in imagens
    ]