#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.file_management as fm
import functions.data_management as dm
import functions.security as secure

class ProdutosDistribuidor(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Produtos do Distribuidor

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys=True, trim_values=True)

        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_distribuidor"]
        
        no_use = ["id_subgrupo", "id_grupo", "id_tipo", "status", 'objeto', "data_cadastro", "id_produto"]

        for i in no_use:
            if not response_data.get(i):
                response_data.pop(i, None)

        correct_types = {
            "id_distribuidor": int,
            "id_subgrupo": int,
            "id_grupo": int,
            "id_tipo": int,
            "data_cadastro": list,
            "id_produto": list
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response=response_data,  
                                          comparison_columns = produto_columns,
                                          no_use_columns = no_use, 
                                          not_null = produto_columns,
                                          correct_types = correct_types)):
            return validity

        # Cuida do limite de informações mandadas e da pagina atual
        pagina, limite = dm.page_limit_handler(response_data)

        string_date_1 = ""
        string_date_2 = ""
        string_busca = ""
        string_tipo = ""
        string_grupo = ""
        string_subgrupo = ""
        string_status = ""
        string_id_produto = ""

        params = {
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        # Tratamento do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        params["id_distribuidor"] = id_distribuidor

        # Tratamento da data
        data_cadastro = response_data.get("data_cadastro")

        if data_cadastro:
            
            if len(data_cadastro) <= 0:
                date_1, date_2 = None, None

            elif len(data_cadastro) == 1:
                date_1, date_2 = data_cadastro[0], None
            
            else:
                date_1, date_2 = data_cadastro[0], data_cadastro[1]
                
            if date_1:
                
                try:
                    date_1 = datetime.strptime(date_1, '%Y-%m-%d')
                    date_1 = date_1.strftime('%Y-%m-%d')
                    if date_1 < "1900-01-01":
                        date_1 = "1900-01-01"

                    params["date_1"] = date_1
                    string_date_1 = "AND pd.data_cadastro >= :date_1"

                except:
                    pass

            if date_2:

                try:
                    date_2 = datetime.strptime(date_2, '%Y-%m-%d')
                    date_2 = date_2.strftime('%Y-%m-%d')
                    if date_2 > "3000-01-01":
                        date_2 = "3000-01-01"

                    params["date_2"] = date_2
                    string_date_2 = "AND pd.data_cadastro <= :date_2"

                except:
                    pass

        # Tratamento do objeto
        busca = response_data.get('objeto')

        if busca:

            busca = str(busca).upper().split()

            string_busca = ""

            for index, word in enumerate(busca):

                params.update({
                    f"word_{index}": f"%{word}%"
                })

                string_busca += f"""AND (
                    p.descricao_completa COLLATE Latin1_General_CI_AI LIKE :word_{index}
                    OR p.sku LIKE :word_{index}
                    OR p.id_produto LIKE :word_{index}
                    OR p.dun14 LIKE :word_{index}
                )"""

        # Tratamento de tipo/grupo/subgrupo
        id_tipo = int(response_data.get('id_tipo')) if response_data.get('id_tipo') else None
        id_grupo = int(response_data.get('id_grupo')) if response_data.get('id_grupo') else None
        id_subgrupo = int(response_data.get('id_subgrupo')) if response_data.get('id_subgrupo') else None
        
        if id_tipo or id_grupo or id_subgrupo:

            if id_subgrupo:
                string_subgrupo = f"AND s.id_subgrupo = :id_subgrupo"
                params["id_subgrupo"] = id_subgrupo

            if id_grupo:
                string_grupo = f"AND g.id_grupo = :id_grupo"
                params["id_grupo"] = id_grupo

            if id_tipo:
                string_tipo = f"AND t.id_tipo = :id_tipo"
                params["id_tipo"] = id_tipo

        # Tratamento de status
        status = response_data.get("status")

        if status is not None:

            status = str(status).upper()

            if status in {"I", "False"}:
                status = "I"

            else:
                status = 'A'

            params["status"] = status
            string_status = "AND pd.status = :status AND p.status = :status"

        # Tratamento de id_produto
        id_produto = response_data.get("id_produto")

        if id_produto:
            string_id_produto = "AND p.id_produto IN :produtos"
            params["produtos"] = list(id_produto)

        # Realizando as queries nas tabelas
        produto_query = f"""
            SELECT
                pd.id_distribuidor,
                d.nome_fantasia,
                pd.id_produto,
                pd.agrupamento_variante,
                pd.cod_prod_distr,
                SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
                pd.multiplo_venda,
                pd.ranking,
                pd.unidade_venda,
                pd.quant_unid_venda,
                pd.giro,
                pd.agrup_familia,
                pd.status,
                p.descricao_completa,
                p.sku,    
                CASE WHEN pe.qtd_estoque > 0 THEN pe.qtd_estoque ELSE 0 END qtd_estoque,
                p.volumetria,
                p.variante,
                p.unidade_embalagem,
                p.quantidade_embalagem,
                p.dun14,
                p.tipo_produto,
                t.id_distribuidor as distribuidor_tgs,
                t.id_tipo,
                t.descricao as descricao_tipo,
                t.status as status_tipo,
                g.id_grupo,
                g.descricao as descricao_grupo,
                g.status as status_grupo,
                s.id_subgrupo,
                s.descricao as descricao_subgrupo,
                s.status as status_subgrupo,
                m.id_marca as cod_marca,
                m.desc_marca as descr_marca,
                f.id_fornecedor as cod_fornecedor,
                f.desc_fornecedor as descri_fornecedor,
                pd.data_cadastro,
                pimg.token,
                hp.count__

            FROM
                (
                    SELECT
                        id_produto,
                        id_distribuidor,
                        ordem_ranking,
                        COUNT(*) OVER() as count__

                    FROM
                        (
                            
                            SELECT
                                DISTINCT
                                    p.id_produto,
                                    pd.id_distribuidor,
                                    CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_ranking,
                                    p.descricao_completa

                            FROM
                                PRODUTO p
                                    
                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN DISTRIBUIDOR d ON
                                    pd.id_distribuidor = d.id_distribuidor

                                INNER JOIN
                                (

                                    SELECT
                                        DISTINCT ps.codigo_produto as id_produto,
                                        s.id_subgrupo,
                                        g.id_grupo,
                                        t.id_tipo

                                    FROM
                                        PRODUTO_SUBGRUPO ps

                                        INNER JOIN SUBGRUPO s ON
                                            ps.id_subgrupo = s.id_subgrupo
                                            AND ps.id_distribuidor = s.id_distribuidor

                                        INNER JOIN GRUPO_SUBGRUPO gs ON
                                            s.id_subgrupo = gs.id_subgrupo

                                        INNER JOIN GRUPO g ON
                                            gs.id_grupo = g.id_grupo

                                        INNER JOIN TIPO t ON
                                            g.tipo_pai = t.id_tipo

                                    WHERE
                                        ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END
                                        {string_tipo}
                                        {string_grupo}
                                        {string_subgrupo}
                                        AND ps.status = 'A'
                                        AND s.status = 'A'
                                        AND gs.status = 'A'
                                        AND g.status = 'A'
                                        AND t.status = 'A'

                                ) ps ON
                                    pd.id_produto = ps.id_produto

                            WHERE
                                pd.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN pd.id_distribuidor ELSE :id_distribuidor END
                                {string_busca}
                                {string_date_1}
                                {string_date_2}
                                {string_status}
                                AND LEN(p.sku) > 0

                        ) p

                    ORDER BY
                        id_distribuidor ASC,
                        ordem_ranking,
                        descricao_completa

                    OFFSET
                        :offset ROWS

                    FETCH
                        NEXT :limite ROWS ONLY
                        
                ) hp

                INNER JOIN PRODUTO p ON
                    hp.id_produto = p.id_produto
                                    
                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto
                    AND hp.id_distribuidor = pd.id_distribuidor

                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca

                INNER JOIN FORNECEDOR f ON
                    pd.id_fornecedor = f.id_fornecedor

                LEFT JOIN PRODUTO_ESTOQUE pe ON
                    p.id_produto = pe.id_produto

                INNER JOIN DISTRIBUIDOR d ON
                    pd.id_distribuidor = d.id_distribuidor

                INNER JOIN PRODUTO_SUBGRUPO ps ON
                    pd.id_produto = ps.codigo_produto

                INNER JOIN SUBGRUPO s ON
                    ps.id_subgrupo = s.id_subgrupo
                    AND ps.id_distribuidor = s.id_distribuidor

                INNER JOIN GRUPO_SUBGRUPO gs ON
                    s.id_subgrupo = gs.id_subgrupo

                INNER JOIN GRUPO g ON
                    gs.id_grupo = g.id_grupo

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

                LEFT JOIN PRODUTO_IMAGEM pimg ON
                    p.sku = pimg.sku

            WHERE
                ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END

            ORDER BY
                d.id_distribuidor ASC,
                hp.ordem_ranking,
                p.descricao_completa,
                pimg.token,
                distribuidor_tgs,
                CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END ASC,
                CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END ASC,
                CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END ASC
        """

        produto_query = f"""
            SELECT
                pd.id_distribuidor,
                d.nome_fantasia,
                pd.id_produto,
                pd.agrupamento_variante,
                pd.cod_prod_distr,
                SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
                pd.multiplo_venda,
                pd.ranking,
                pd.unidade_venda,
                pd.quant_unid_venda,
                pd.giro,
                pd.agrup_familia,
                pd.status,
                p.descricao_completa,
                p.sku,    
                CASE WHEN pe.qtd_estoque > 0 THEN pe.qtd_estoque ELSE 0 END qtd_estoque,
                p.volumetria,
                p.variante,
                p.unidade_embalagem,
                p.quantidade_embalagem,
                p.dun14,
                p.tipo_produto,
                t.id_distribuidor as distribuidor_tgs,
                t.id_tipo,
                t.descricao as descricao_tipo,
                t.status as status_tipo,
                g.id_grupo,
                g.descricao as descricao_grupo,
                g.status as status_grupo,
                gs.status as status_grupo_subgrupo,
                s.id_subgrupo,
                s.descricao as descricao_subgrupo,
                s.status as status_subgrupo,
                m.id_marca as cod_marca,
                m.desc_marca as descr_marca,
                f.id_fornecedor as cod_fornecedor,
                f.desc_fornecedor as descri_fornecedor,
                pd.data_cadastro,
                p.tokens_imagem as tokens,
                hp.count__

            FROM
                (
                    SELECT
                        id_produto,
                        id_distribuidor,
                        COUNT(*) OVER() as count__

                    FROM
                        (
                                        
                            SELECT
                                DISTINCT
                                    p.id_produto,
                                    pd.id_distribuidor,
                                    pd.ranking,
                                    p.descricao_completa

                            FROM 
                                PRODUTO p

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                    pd.id_produto = ps.codigo_produto 

                                INNER JOIN SUBGRUPO s ON
                                    ps.id_subgrupo = s.id_subgrupo
                                    AND ps.id_distribuidor = s.id_distribuidor
                                                                        
                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                pd.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN pd.id_distribuidor ELSE :id_distribuidor END
                                AND ps.id_distribuidor = ISNULL(:id_distribuidor, 0)
                                {string_id_produto}
                                {string_status}
                                {string_busca}
                                {string_date_1}
                                {string_date_2}
                                {string_tipo}
                                {string_grupo}
                                {string_subgrupo}
                                AND ps.status = 'A'
                                AND LEN(p.sku) > 0

                        ) p

                    ORDER BY
                        id_distribuidor ASC,
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END,
                        descricao_completa

                    OFFSET
                        :offset ROWS

                    FETCH
                        NEXT :limite ROWS ONLY
                                    
                ) hp

                INNER JOIN PRODUTO p ON
                    hp.id_produto = p.id_produto

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto
                    AND hp.id_distribuidor = pd.id_distribuidor

                INNER JOIN DISTRIBUIDOR d ON
                    pd.id_distribuidor = d.id_distribuidor

                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca
                    AND pd.id_distribuidor = m.id_distribuidor

                INNER JOIN FORNECEDOR f ON
                    pd.id_fornecedor = f.id_fornecedor

                INNER JOIN PRODUTO_SUBGRUPO ps ON
                    pd.id_produto = ps.codigo_produto 

                INNER JOIN SUBGRUPO s ON
                    ps.id_subgrupo = s.id_subgrupo
                    AND ps.id_distribuidor = s.id_distribuidor
                                                                        
                INNER JOIN GRUPO_SUBGRUPO gs ON
                    s.id_subgrupo = gs.id_subgrupo

                INNER JOIN GRUPO g ON
                    gs.id_grupo = g.id_grupo

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

                LEFT JOIN PRODUTO_ESTOQUE pe ON
                    pd.id_produto = pe.id_produto 
                    AND pd.id_distribuidor = pe.id_distribuidor

            WHERE
                ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END
                AND m.status = 'A'
                AND f.status = 'A'
                AND d.status = 'A'

            ORDER BY
                d.id_distribuidor ASC,
                CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END,
                p.descricao_completa,
                distribuidor_tgs,
                CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END ASC,
                CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END ASC,
                CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END ASC
        """

        dict_query = {
            "query": produto_query,
            "params": params,
        }

        data = dm.raw_sql_return(**dict_query)

        if data:

            server = os.environ.get("PSERVER_PS").lower()

            if server == "production":
                image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
                
            elif server == "development":
                image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

            else:
                image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

            maximum_all = int(data[0].get("count__"))
            maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

            produtos = []

            for produto in data:

                id_produto = produto.get("id_produto")
                distribuidor_tgs = produto.get("distribuidor_tgs")

                for saved_produto in produtos:

                    if id_produto == saved_produto.get("id_produto"):

                        for saved_tgs in saved_produto.get("categorizacao"):

                            if saved_tgs["id_distribuidor"] == distribuidor_tgs:
                                
                                for saved_tgsd in saved_tgs.get("categorizacao_distribuidor"):
                                    
                                    if saved_tgsd["id_tipo"] == produto["id_tipo"]:
                                    
                                        grupos = saved_tgsd["grupo"]

                                        for grupo in grupos:

                                            if grupo["id_grupo"] == produto["id_grupo"]:

                                                subgrupos = grupo["subgrupo"]

                                                for subgrupo in subgrupos:
                                                    if subgrupo["id_subgrupo"] == produto["id_subgrupo"]:
                                                        break

                                                else:
                                                    subgrupos.append({
                                                        "id_subgrupo": produto["id_subgrupo"],
                                                        "descricao_subgrupo": produto["descricao_subgrupo"],
                                                        "status_subgrupo": produto.get("status_subgrupo"),
                                                    })

                                                break

                                        else:
                                            grupos.append({
                                                "id_grupo": produto["id_grupo"],
                                                "descricao_grupo": produto["descricao_grupo"],
                                                "status_grupo": produto.get("status_grupo"),
                                                "subgrupo": [{
                                                    "id_subgrupo": produto["id_subgrupo"],
                                                    "descricao_subgrupo": produto["descricao_subgrupo"],
                                                    "status_subgrupo": produto.get("status_subgrupo"),
                                                }]
                                            })
                                            
                                        break

                                else:
                                    saved_tgs.get("categorizacao_distribuidor").append({
                                        "id_tipo": produto["id_tipo"],
                                        "descricao_tipo": produto["descricao_tipo"],
                                        "grupo": [{
                                            "id_grupo": produto["id_grupo"],
                                            "descricao_grupo": produto["descricao_grupo"],
                                            "subgrupo": [{
                                                "id_subgrupo": produto["id_subgrupo"],
                                                "descricao_subgrupo": produto["descricao_subgrupo"],
                                            }]
                                        }]
                                    })

                                break

                        else:
                            saved_produto.get("categorizacao").append({
                                "id_distribuidor": distribuidor_tgs,
                                "categorizacao_distribuidor": [{
                                    "id_tipo": produto.get("id_tipo"),
                                    "descricao_tipo": produto.get("descricao_tipo"),
                                    "status_tipo": produto.get("status_tipo"),
                                    "grupo": [{
                                        "id_grupo": produto.get("id_grupo"),
                                        "descricao_grupo": produto.get("descricao_grupo"),
                                        "status_grupo": produto.get("status_grupo"),
                                        "subgrupo": [{
                                            "id_subgrupo": produto.get("id_subgrupo"),
                                            "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                            "status_subgrupo": produto.get("status_subgrupo"),
                                        }]
                                    }]
                                }]
                            })

                        break

                else:

                    imagens = []

                    imagens_list = []

                    imagens = produto.get("tokens")
                    if imagens:

                        imagens = imagens.split(",")
                        imagens_list = [
                            f"{image}/imagens/{imagem}"
                            for imagem in imagens
                        ]

                    product = {
                        "id_produto": id_produto,
                        "id_distribuidor": produto.get("id_distribuidor"),
                        "nome_distribuidor": produto.get("nome_fantasia"),
                        "agrupamento_variante": produto.get("agrupamento_variante"),
                        "cod_prod_distr": produto.get("cod_prod_distr"),
                        "cod_frag_distr": produto.get("cod_frag_distr"),
                        "imagens": imagens_list,                    
                        "multiplo_venda": produto.get("multiplo_venda"),
                        "categorizacao": [{
                            "id_distribuidor": distribuidor_tgs,
                            "categorizacao_distribuidor": [{
                                "id_tipo": produto.get("id_tipo"),
                                "descricao_tipo": produto.get("descricao_tipo"),
                                "status_tipo": produto.get("status_tipo"),
                                "grupo": [{
                                    "id_grupo": produto.get("id_grupo"),
                                    "descricao_grupo": produto.get("descricao_grupo"),
                                    "status_grupo": produto.get("status_grupo"),
                                    "subgrupo": [{
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                        "status_subgrupo": produto.get("status_subgrupo"),
                                    }]
                                }]
                            }]
                        }],
                        "ranking": produto.get("ranking"),
                        "unidade_venda": produto.get("unidade_venda"),
                        "quant_unid_venda": produto.get("quant_unid_venda"),
                        "giro": produto.get("giro"),
                        "agrup_familia": produto.get("agrup_familia"),
                        "status": produto.get("status"),
                        "descr_completa_distr": produto.get("descricao_completa"),
                        "sku": produto.get("sku"),
                        "quantidade_estoque": produto.get("qtd_estoque"),
                        "volumetria": produto.get("volumetria"),
                        "variante": produto.get("variante"),
                        "unidade_embalagem": produto.get("unidade_embalagem"),
                        "quantidade_embalagem": produto.get("quantidade_embalagem"),
                        "dun14": produto.get("dun14"),
                        "tipo_produto": produto.get("tipo_produto"),
                        "cod_marca": produto.get("cod_marca"),
                        "descr_marca": produto.get("descr_marca"),
                        "cod_fornecedor": produto.get("cod_fornecedor"),
                        "descri_fornecedor": produto.get("descri_fornecedor"),
                        "data_cadastro": produto.get("data_cadastro"),
                    }

                    produtos.append(product)

            logger.info(f"Dados do produto foram enviados.")
            return {"status": 200,
                    "resposta": {
                        "tipo": "1",
                        "descricao": f"Dados encontrados."},
                    "informacoes": {
                        "itens": maximum_all,
                        "paginas": maximum_pages,
                        "pagina_atual": pagina},
                    "dados": produtos}, 200

        logger.info(f"Dados do produto nao foram encontrados.")
        return {"status": 200,
                "resposta": {
                    "tipo": "6",
                    "descricao": f"Dados não encontrados para estes filtros."}}, 200


    @logger
    @secure.auth_wrapper(permission_range=[1])
    def put(self) -> dict:
        """
        Método PUT do Editar Produtos do Distribuidor

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get_info_thread().get("token_info")

        usuario = int(info.get("id_usuario"))

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys=True, trim_values=True)

        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_distribuidor", "id_produto", "cod_prod_distr", 
            "descr_completa_distr", "dun14", "status", "tipo_produto", "cod_marca",
            "variante", "multiplo_venda", "unidade_venda", "quant_unid_venda", 
            "unidade_embalagem", "quantidade_embalagem", "ranking", "giro",
            "agrup_familia", "volumetria", "cod_fornecedor"]
        
        not_null = ["id_distribuidor", "id_produto", "cod_prod_distr"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response=response_data,
                                        not_null = not_null,
                                        correct_types = correct_types,
                                        comparison_columns=produto_columns)):
            return validity
        
        # Verificando informações de entrada
        id_produto = str(response_data.get("id_produto"))
        codigo_produto = str(response_data.get("cod_prod_distr"))
        id_distribuidor = int(response_data.get)

        # Verificando o id do distribuidor
        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response
                
        # Verificando a existência do produto para o distribuidor
        existence_query = f"""
            SELECT
                TOP 1 id_produto
            
            FROM
                PRODUTO_DISTRIBUIDOR
            
            WHERE
                id_distribuidor = :id_distribuidor
                AND id_produto = :id_produto
                AND cod_prod_distr = :codigo_produto
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "id_produto": id_produto,
            "codigo_produto": codigo_produto
        }

        existence_query = dm.raw_sql_return(existence_query, params = params, raw = True, first = True)
        
        if not existence_query:
            logger.error(f"Produto especificado nao existe")
            return {"status": 409,
                    "resposta": {
                        "tipo": "5",
                        "descricao": f"Produto do distribuidor não existe."}}, 200
        
        changes = {
            key:  response_data.get(key)  if bool(response_data.get(key))  else None
            for key in produto_columns
        }

        # Variaveis
        descr_completa_distr = changes.get("descr_completa_distr")
        tipo_produto = changes.get("tipo_produto")
        dun14 = changes.get("dun14")
        status = str(changes.get("status")).upper() \
                    if str(changes.get("status")).upper() in {"I", "A"} else None
        cod_marca = changes.get("cod_marca")
        variante = changes.get("variante")
        multiplo_venda = changes.get("multiplo_venda")
        unidade_venda = changes.get("unidade_venda")
        quant_unid_venda = changes.get("quant_unid_venda")
        unidade_embalagem = changes.get("unidade_embalagem")
        quantidade_embalagem = changes.get("quantidade_embalagem")
        ranking = changes.get("ranking")
        giro = changes.get("giro")
        agrup_familia = changes.get("agrup_familia")
        volumetria = changes.get("volumetria")
        cod_fornecedor = changes.get("cod_fornecedor")

        # Update na tabela produto
        params_p = {
            "id_produto": id_produto,
            "cod_prod_distr": codigo_produto,
            "descr_completa_distr": descr_completa_distr,
            "tipo_produto": tipo_produto,
            "dun14": dun14,
            "variante": variante,
            "unidade_venda": unidade_venda,
            "quant_unid_venda": quant_unid_venda,
            "unidade_embalagem": unidade_embalagem,
            "quantidade_embalagem": quantidade_embalagem,
            "volumetria": volumetria
        }

        for key, value in params_p.items():

            if not value:
                del params_p[key]

        key_columns = ["id_produto"]

        dm.raw_sql_update("PRODUTO", params_p, key_columns)

        # Update na tabela produto_distribuidor
        params_pd = {
            "id_distribuidor": id_distribuidor,
            "id_produto": id_produto,
            "cod_prod_distr": codigo_produto,
            "status": status,
            "id_marca": cod_marca,
            "multiplo_venda": multiplo_venda,
            "unidade_venda": unidade_venda,
            "quant_unid_venda": quant_unid_venda,
            "ranking": ranking,
            "giro": giro,
            "agrup_familia": agrup_familia,
            "cod_fornecedor": cod_fornecedor
        }

        key_columns = ["id_distribuidor", "id_produto", "cod_prod_distr"]

        for key, value in params_pd.items():

            if not value:
                del params_p[key]

        dm.raw_sql_update("PRODUTO_DISTRIBUIDOR", params_pd, key_columns)

        logger.info("Todas as mudancas foram realizadas com sucesso")
        return {"status":200,
                "resposta":{"tipo":"1", "descricao": "Todas as mudanças foram feitas."}}, 200