#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ListarProdutosMarketplace(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self) -> dict:
        """
        Método POST do Listar Produtos do Marketplace

        Returns:
            [dict]: Status da transação
        """
        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_cliente", "id_tipo", "id_grupo", "id_subgrupo", "busca", 
                        "id_produto", "ordenar", "estoque", "multiplo_venda", "desconto", 
                        "tipo_oferta", "marca", "grupos", "subgrupos", "id_oferta", "sku"]

        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "id_produto": [list, str],
            "sku": [list, str],
            "ordenar": int,
            "estoque": bool,
            "multiplo_venda": int,
            "desconto": bool,
            "tipo_oferta": int,
            "marca": [list, int],
            "grupos": [list, int],
            "subgrupos": [list, int],
            "id_oferta": [list, int],
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = ["id_distribuidor"],
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = int(response_data.get("id_cliente")) \
                        if response_data.get("id_cliente") and id_usuario else None

        if id_usuario:
            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

        # Filtros do usuario
        
        ## Busca por tipo-grupo-subgrupo
        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = int(response_data.get("id_grupo")) if response_data.get("id_grupo") else None
        id_subgrupo = int(response_data.get("id_subgrupo")) if response_data.get("id_subgrupo") else None

        ## Busca por campo de busca
        busca = str(response_data.get("busca")) if response_data.get("busca") else None
        
        ## Busca por id_produto
        id_produto = response_data.get("id_produto") 
        id_oferta = response_data.get("id_oferta") 
        sku = response_data.get("sku")

        ## Busca por existência de estoque
        estoque = True if response_data.get("estoque") and id_usuario else False

        ## Busca por desconto
        desconto = True if response_data.get("desconto") and id_usuario else False

        ## Busca por multiplo_venda
        multiplo_venda = int(response_data.get("multiplo_venda")) if response_data.get("multiplo_venda") else 0
        
        ## Busca por tipo de oferta
        tipo_oferta = int(response_data.get("tipo_oferta")) if response_data.get("tipo_oferta") and id_usuario else None

        ## Busca por marca
        marca = response_data.get("marca")

        ## Busca por grupos e/ou subgrupos
        grupos = response_data.get("grupos")
        subgrupos = response_data.get("subgrupos")

        ## Ordenação do produto
        ordenar_por = int(response_data.get("ordenar")) if response_data.get("ordenar") else None

        if not busca and not ordenar_por:
            ordenar_por = 3

        if ordenar_por:
            if (ordenar_por > 6 or ordenar_por < 1):
                ordenar_por = 3

            if not id_usuario:
                if not ordenar_por in {3,5,6}:
                    ordenar_por = 3

        # Realizando a query
        full_data = []

        # Pegando os distribuidores validos do cliente
        if not id_usuario:
      
            query = """
                SELECT
                    DISTINCT id_distribuidor

                FROM
                    DISTRIBUIDOR

                WHERE
                    id_distribuidor = CASE
                                          WHEN :id_distribuidor = 0
                                              THEN id_distribuidor
                                          ELSE
                                              :id_distribuidor
                                      END
                    AND status = 'A'
                    
            """

            params = {
                "id_distribuidor": id_distribuidor
            }

        else:
            
            query = """
                SELECT
                    DISTINCT d.id_distribuidor

                FROM
                    CLIENTE c

                    INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                        c.id_cliente = cd.id_cliente

                    INNER JOIN DISTRIBUIDOR d ON
                        cd.id_distribuidor = d.id_distribuidor

                WHERE
                    c.id_cliente = :id_cliente
                    AND d.id_distribuidor = CASE
                                                WHEN :id_distribuidor = 0
                                                    THEN d.id_distribuidor
                                                ELSE
                                                    :id_distribuidor
                                            END
                    AND c.status = 'A'
                    AND c.status_receita = 'A'
                    AND cd.status = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND d.status = 'A'
            """

            params = {
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor
            }

        distribuidores = dm.raw_sql_return(query, params = params, raw = True)
        if not distribuidores:
            logger.error("Sem distribuidores cadastrados válidos para a operação.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Sem distribuidores cadastrados."}}, 200

        if distribuidores:

            distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

            # Filtros
            string_id_distribuidor_tgs = "AND ps.id_distribuidor = :id_distribuidor"
            string_busca = ""
            string_tgs = ""
            string_id_produto = ""
            string_id_oferta = ""
            string_tipo_oferta = ""
            string_sku = ""
            string_ordenar_por = ""
            string_estoque = ""
            string_multiplo_venda = ""
            string_desconto = ""
            string_marca = ""
            string_grupos = ""
            string_subgrupos = ""
            string_oferta = ""

            counter = 0

            params = {
                "id_distribuidor": id_distribuidor, 
                "distribuidores": distribuidores,
                "id_cliente": id_cliente
            }

            # Procuras

            ## Procura por campo de busca
            if busca:

                busca = str(busca).upper()

                string_busca = ""

                busca_words = busca.split()

                for index, word in enumerate(busca_words):

                    params.update({
                        f"word_{index}": f"%{word}%"
                    })

                    string_busca += f"""AND (
                        p.descricao_completa COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :word_{index}
                        OR p.sku COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :word_{index}
                        OR p.dun14 COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :word_{index}
                        OR p.id_produto COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :word_{index}
                        OR m.desc_marca COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :word_{index}
                    )"""

            ## Procura por sku
            if sku:

                if not isinstance(sku, (list, tuple, set)):
                    sku = [sku]

                else:
                    sku = list(sku)

                params.update({
                    "sku": sku
                })

                string_sku = "AND p.sku IN :sku"

            ## Procura por id_produto
            if id_produto:

                if not isinstance(id_produto, (list, tuple, set)):
                    id_produto = [id_produto]

                else:
                    id_produto = list(id_produto)

                params.update({
                    "id_produto": id_produto
                })

                string_id_produto = "AND p.id_produto IN :id_produto"

            ## Procura por oferta
            if id_oferta or tipo_oferta:

                string_id_oferta = ""
                string_tipo_oferta = ""

                string_oferta_id_cliente = """
                    INNER JOIN
                    (
                    
                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

                        WHERE
                            oc.id_cliente = :id_cliente
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) oc ON
                        o.id_oferta = oc.id_oferta
                """

                ## Procura por id_oferta
                if id_oferta:

                    if not isinstance(id_oferta, (list, tuple, set)):
                        id_oferta = [id_oferta]

                    else:
                        id_oferta = list(id_oferta)

                    params.update({
                        "id_oferta": id_oferta
                    })

                    string_id_oferta = "AND o.id_oferta IN :id_oferta"

                # Procura por tipo_oferta
                if tipo_oferta:
                    
                    if tipo_oferta == 1:
                        string_tipo_oferta = f"""
                            AND (
                                    o.tipo_oferta = 2 
                                    AND o.id_oferta IN (

                                                            SELECT
                                                                ob.id_oferta

                                                            FROM
                                                                PRODUTO_DISTRIBUIDOR pd

                                                                INNER JOIN PRODUTO p ON
                                                                    pd.id_produto = p.id_produto

                                                                INNER JOIN OFERTA o ON
                                                                    pd.id_distribuidor = o.id_distribuidor

                                                                {string_oferta_id_cliente if id_cliente else ""}

                                                                INNER JOIN OFERTA_BONIFICADO ob ON
                                                                    p.id_produto = ob.id_produto
                                                                    AND o.id_oferta = ob.id_oferta

                                                                INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                                    pd.id_produto = tpp.id_produto
                                                                    AND pd.id_distribuidor = tpp.id_distribuidor

                                                                INNER JOIN TABELA_PRECO tp ON
                                                                    tpp.id_tabela_preco = tp.id_tabela_preco
                                                                    AND tpp.id_distribuidor = tp.id_distribuidor

                                                            WHERE
                                                                pd.estoque > 0
                                                                AND o.id_distribuidor IN :distribuidores
                                                                AND o.tipo_oferta = 2
                                                                AND tp.dt_inicio <= GETDATE()
                                                                AND tp.dt_fim >= GETDATE()
                                                                AND tp.tabela_padrao = 'S'
                                                                AND tp.status = 'A'
                                                                AND ob.status = 'A'
                                                                AND p.status = 'A'
                                                                AND pd.status = 'A'

                                                        )
                                )
                        """
                    
                    elif tipo_oferta == 2:
                        string_tipo_oferta = """
                            AND (
                                                
                                    o.tipo_oferta = 3
                                    AND o.id_oferta IN (
                                                        
                                                            SELECT
                                                                id_oferta

                                                            FROM
                                                                OFERTA_ESCALONADO_FAIXA

                                                            WHERE
                                                                desconto > 0
                                                                AND status = 'A'

                                                        )
                                        
                                )
                        """

                else:

                    string_tipo_oferta = f"""
                       AND (
                                (
                                    
                                    o.tipo_oferta = 2 
                                    AND o.id_oferta IN (

                                                            SELECT
                                                                ob.id_oferta

                                                            FROM
                                                                PRODUTO_DISTRIBUIDOR pd

                                                                INNER JOIN PRODUTO p ON
                                                                    pd.id_produto = p.id_produto

                                                                INNER JOIN OFERTA o ON
                                                                    pd.id_distribuidor = o.id_distribuidor

                                                                {string_oferta_id_cliente if id_cliente else ""}

                                                                INNER JOIN OFERTA_BONIFICADO ob ON
                                                                    p.id_produto = ob.id_produto
                                                                    AND o.id_oferta = ob.id_oferta

                                                                INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                                    pd.id_produto = tpp.id_produto
                                                                    AND pd.id_distribuidor = tpp.id_distribuidor

                                                                INNER JOIN TABELA_PRECO tp ON
                                                                    tpp.id_tabela_preco = tp.id_tabela_preco
                                                                    AND tpp.id_distribuidor = tp.id_distribuidor

                                                            WHERE
                                                                pd.estoque > 0
                                                                AND o.id_distribuidor IN :distribuidores
                                                                AND o.tipo_oferta = 2
                                                                AND tp.dt_inicio <= GETDATE()
                                                                AND tp.dt_fim >= GETDATE()
                                                                AND tp.tabela_padrao = 'S'
                                                                AND tp.status = 'A'
                                                                AND ob.status = 'A'
                                                                AND p.status = 'A'
                                                                AND pd.status = 'A'

                                                        )

                                )
                                    OR
                                (
                                    
                                    o.tipo_oferta = 3
                                    AND o.id_oferta IN (
                                                        
                                                            SELECT
                                                                id_oferta

                                                            FROM
                                                                OFERTA_ESCALONADO_FAIXA

                                                            WHERE
                                                                desconto > 0
                                                                AND status = 'A'

                                                        )
                                
                                )
                           ) 
                    """
           
                string_oferta = f"""
                    INNER JOIN
                    (

                        SELECT
                            o.id_oferta,
                            o.id_distribuidor,
                            o.tipo_oferta,
                            op.id_produto

                        FROM
                            PRODUTO_DISTRIBUIDOR pd

                            INNER JOIN PRODUTO p ON
                                pd.id_produto = p.id_produto

                            INNER JOIN OFERTA o ON
                                pd.id_distribuidor = o.id_distribuidor

                            {string_oferta_id_cliente if id_cliente else ""}

                            INNER JOIN OFERTA_PRODUTO op ON
                                p.id_produto = op.id_produto
                                AND o.id_oferta = op.id_oferta

                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                pd.id_produto = tpp.id_produto
                                AND pd.id_distribuidor = tpp.id_distribuidor

                            INNER JOIN TABELA_PRECO tp ON
                                tpp.id_tabela_preco = tp.id_tabela_preco
                                AND tpp.id_distribuidor = tp.id_distribuidor

                        WHERE
                            pd.estoque > 0
                            AND o.id_distribuidor IN :distribuidores
                            {string_id_oferta}
                            {string_tipo_oferta}
                            AND o.limite_ativacao_oferta > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND tp.tabela_padrao = 'S'
                            AND tp.dt_inicio <= GETDATE()
                            AND tp.dt_fim >= GETDATE()
                            AND op.status = 'A'
                            AND tp.status = 'A'
                            AND p.status = 'A'
                            AND pd.status = 'A'
                            AND o.status = 'A'
                    
                    ) op ON
                        pd.id_produto = op.id_produto
                        AND pd.id_distribuidor = op.id_distribuidor
                """

            ## Procura por tipo, grupo e/ou subgrupo
            if id_tipo or id_grupo or id_subgrupo:

                if id_subgrupo:
                    string_tgs += f"AND s.id_subgrupo = :id_subgrupo "
                    params["id_subgrupo"] = id_subgrupo

                if id_grupo:
                    string_tgs += "AND g.id_grupo = :id_grupo "
                    params["id_grupo"] = id_grupo

                if id_tipo:
                    string_tgs += "AND t.id_tipo = :id_tipo "
                    params["id_tipo"] = id_tipo

            ## Procura por estoque
            if estoque:
                string_estoque = "AND pd.estoque > 0"

            ## Procura por multiplo venda
            if multiplo_venda > 0:
                string_multiplo_venda = "AND pd.multiplo_venda = :multiplo_venda"
                params.update({
                    "multiplo_venda": multiplo_venda,
                })

            # Procura por desconto
            if desconto:
                string_desconto = """
                    INNER JOIN
                    (
                    
                        SELECT
                            op.id_produto,
                            o.id_distribuidor

                        FROM
                            (

                                SELECT
                                    o.id_oferta

                                FROM
                                    OFERTA o

                                WHERE
                                    id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                                UNION

                                SELECT
                                    oc.id_oferta

                                FROM
                                    OFERTA_CLIENTE oc

                                WHERE
                                    oc.id_cliente = :id_cliente
                                    AND oc.status = 'A'
                                    AND oc.d_e_l_e_t_ = 0

                            ) offers

                            INNER JOIN OFERTA o ON
                                offers.id_oferta = o.id_oferta

                            INNER JOIN OFERTA_PRODUTO op ON
                                o.id_oferta = op.id_oferta

                            INNER JOIN OFERTA_DESCONTO od ON
                                o.id_oferta = od.id_oferta

                        WHERE
                            o.tipo_oferta = 1
                            AND o.id_distribuidor IN :distribuidores
                            AND o.limite_ativacao_oferta > 0
                            AND od.desconto > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND o.status = 'A'
                            AND op.status = 'A'
                            AND od.status = 'A'
                    
                    ) odesc ON
                        pd.id_produto = odesc.id_produto
                        AND pd.id_distribuidor = odesc.id_distribuidor
                """

            ## Procura por marca
            if marca:

                if not isinstance(marca, (list, tuple, set)):
                    marca = [marca]

                else:
                    marca = list(marca)

                params.update({
                    "marca": marca
                })

                string_marca = "AND m.id_marca IN :marca"

            ## Procura por grupos e/ou subgrupos
            if grupos:

                string_id_distribuidor_tgs = "AND ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END"

                if not isinstance(grupos, (list, tuple, set)):
                    grupos = [grupos]

                else:
                    grupos = list(grupos)

                params.update({
                    "grupos": grupos
                })

                string_grupos = "AND g.id_grupo IN :grupos"

            if subgrupos:

                string_id_distribuidor_tgs = "AND ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END"

                if not isinstance(subgrupos, (list, tuple, set)):
                    subgrupos = [subgrupos]

                else:
                    subgrupos = list(subgrupos)

                params.update({
                    "subgrupos": subgrupos
                })

                string_subgrupos = "AND s.id_subgrupo IN :subgrupos"

            # Paginação
            params.update({
                "offset": (pagina - 1) * limite,
                "limite": limite
            })

            
            if id_cliente:

                if ordenar_por:

                    ## Ordenacao da busca
                    dict_ordenacao = {
                        1:"ordem_estoque DESC, preco_tabela DESC",
                        2:"ordem_estoque DESC, preco_tabela ASC",
                        3:"ordem_estoque DESC, ordem_rankeamento ASC, descricao_completa",
                        4:"ordem_estoque DESC, ordem_rankeamento ASC, descricao_completa",
                        5:"ordem_estoque DESC, descricao_completa ASC",
                        6:"ordem_estoque DESC, descricao_completa DESC"
                    }

                    string_ordenar_por = dict_ordenacao.get(ordenar_por)

                    query = f"""
                        SELECT
                            id_produto,
                            id_distribuidor,
                            COUNT(*) OVER() count__

                        FROM
                            (
                                SELECT
                                    DISTINCT
                                        p.id_produto,
                                        pd.id_distribuidor,
                                        pd.ranking,
                                        p.descricao_completa,
                                        ppr.preco_tabela,
                                        CASE WHEN pd.estoque > 0 THEN 1 ELSE 0 END ordem_estoque,
                                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

                                FROM 
                                    (
                                        SELECT
                                            tpp.id_produto,
                                            tpp.id_distribuidor,
                                            MIN(tpp.preco_tabela) preco_tabela

                                        FROM
                                            TABELA_PRECO tp

                                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                tp.id_tabela_preco = tpp.id_tabela_preco
                                                AND tp.id_distribuidor = tpp.id_distribuidor

                                        WHERE
                                            tp.id_distribuidor IN :distribuidores
                                            AND (
                                                    (
                                                        tp.tabela_padrao = 'S'
                                                    )
                                                        OR
                                                    (
                                                        tp.id_tabela_preco IN (
                                                                                    SELECT
                                                                                        id_tabela_preco

                                                                                    FROM
                                                                                        TABELA_PRECO_CLIENTE

                                                                                    WHERE
                                                                                        id_distribuidor IN :distribuidores
                                                                                        AND id_cliente = :id_cliente
                                                                                )
                                                    )
                                                )
                                            AND tp.status = 'A'
                                            AND tp.dt_inicio <= GETDATE()
                                            AND tp.dt_fim >= GETDATE()
                                            

                                        GROUP BY
                                            tpp.id_produto,
                                            tpp.id_distribuidor

                                    ) ppr

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        ppr.id_produto = pd.id_produto
                                        AND ppr.id_distribuidor = pd.id_distribuidor

                                    INNER JOIN PRODUTO p ON
                                        pd.id_produto = p.id_produto
                        
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

                                    {string_desconto}

                                    {string_oferta}

                                WHERE
                                    pd.id_distribuidor IN :distribuidores
                                    {string_id_produto}
                                    {string_sku}
                                    {string_tgs}
                                    {string_grupos}
                                    {string_subgrupos}
                                    {string_busca}
                                    {string_estoque}
                                    {string_multiplo_venda}
                                    {string_marca}
                                    {string_id_distribuidor_tgs}
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND ps.status = 'A'
                                    AND s.status = 'A'
                                    AND gs.status = 'A'
                                    AND g.status = 'A'
                                    AND t.status = 'A'
                                    AND m.status = 'A'
                                    AND f.status = 'A'

                            ) FILTER_RESULT

                        ORDER BY
                            {string_ordenar_por}

                        OFFSET
                            :offset ROWS

                        FETCH
                            NEXT :limite ROWS ONLY
                    """

                else:

                    params.update({
                        "full_search": f"% {busca} %",
                    })

                    query = f"""
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
                                        p.descricao_completa,
                                        CASE WHEN pd.estoque > 0 THEN 1 ELSE 0 END ordem_estoque,
                                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento,
                                        CASE
                                            WHEN ' ' + p.descricao_completa + ' ' COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :full_search
                                                THEN 1
                                            ELSE
                                                0
                                        END ordem_busca

                                FROM 
                                    (
                                        SELECT
                                            tpp.id_produto,
                                            tpp.id_distribuidor,
                                            MIN(tpp.preco_tabela) preco_tabela

                                        FROM
                                            TABELA_PRECO tp

                                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                tp.id_tabela_preco = tpp.id_tabela_preco
                                                AND tp.id_distribuidor = tpp.id_distribuidor

                                        WHERE
                                            tp.id_distribuidor IN :distribuidores
                                            AND (
                                                    (
                                                        tp.tabela_padrao = 'S'
                                                    )
                                                        OR
                                                    (
                                                        tp.id_tabela_preco IN (
                                                                                    SELECT
                                                                                        id_tabela_preco

                                                                                    FROM
                                                                                        TABELA_PRECO_CLIENTE

                                                                                    WHERE
                                                                                        id_distribuidor IN :distribuidores
                                                                                        AND id_cliente = :id_cliente
                                                                                )
                                                    )
                                                )
                                            AND tp.status = 'A'
                                            AND tp.dt_inicio <= GETDATE()
                                            AND tp.dt_fim >= GETDATE()
                                            

                                        GROUP BY
                                            tpp.id_produto,
                                            tpp.id_distribuidor

                                    ) ppr

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        ppr.id_produto = pd.id_produto
                                        AND ppr.id_distribuidor = pd.id_distribuidor

                                    INNER JOIN PRODUTO p ON
                                        pd.id_produto = p.id_produto
                        
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

                                    
                                    {string_desconto}

                                    {string_oferta}

                                WHERE
                                    pd.id_distribuidor IN :distribuidores
                                    {string_id_produto}
                                    {string_sku}
                                    {string_tgs}
                                    {string_grupos}
                                    {string_subgrupos}
                                    {string_busca}
                                    {string_estoque}
                                    {string_multiplo_venda}
                                    {string_marca}
                                    {string_id_distribuidor_tgs}
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND ps.status = 'A'
                                    AND s.status = 'A'
                                    AND gs.status = 'A'
                                    AND g.status = 'A'
                                    AND t.status = 'A'
                                    AND m.status = 'A'
                                    AND f.status = 'A'

                            ) p

                        ORDER BY
                            ordem_busca DESC, ordem_rankeamento ASC, descricao_completa

                        OFFSET
                            :offset ROWS

                        FETCH
                            NEXT :limite ROWS ONLY
                    """

            else:

                if ordenar_por:

                    ## Ordenacao da busca
                    dict_ordenacao = {
                        3:"ordem_estoque DESC, ordem_rankeamento ASC, descricao_completa",
                        5:"ordem_estoque DESC, descricao_completa ASC",
                        6:"ordem_estoque DESC, descricao_completa DESC"
                    }

                    string_ordenar_por = dict_ordenacao.get(ordenar_por)

                    query = f"""
                        SELECT
                            id_produto,
                            id_distribuidor,
                            COUNT(*) OVER() count__

                        FROM
                            (
                                SELECT
                                    DISTINCT
                                        p.id_produto,
                                        pd.id_distribuidor,
                                        pd.ranking,
                                        p.descricao_completa,
                                        CASE WHEN pd.estoque > 0 THEN 1 ELSE 0 END ordem_estoque,
                                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

                                FROM 

                                    TABELA_PRECO tp

                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                        tp.id_tabela_preco = tpp.id_tabela_preco
                                        AND tp.id_distribuidor = tpp.id_distribuidor

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        tpp.id_produto = pd.id_produto
                                        AND tpp.id_distribuidor = pd.id_distribuidor

                                    INNER JOIN PRODUTO p ON
                                        pd.id_produto = p.id_produto
                        
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

                                    {string_oferta}

                                WHERE
                                    pd.id_distribuidor IN :distribuidores
                                    {string_id_produto}
                                    {string_sku}
                                    {string_tgs}
                                    {string_grupos}
                                    {string_subgrupos}
                                    {string_busca}
                                    {string_marca}
                                    {string_id_distribuidor_tgs}
                                    AND tp.tabela_padrao = 'S'
                                    AND tp.status = 'A'
                                    AND tp.dt_inicio <= GETDATE()
                                    AND tp.dt_fim >= GETDATE()
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND ps.status = 'A'
                                    AND s.status = 'A'
                                    AND gs.status = 'A'
                                    AND g.status = 'A'
                                    AND t.status = 'A'
                                    AND m.status = 'A'
                                    AND f.status = 'A'

                            ) FILTER_RESULT

                        ORDER BY
                            {string_ordenar_por}

                        OFFSET
                            :offset ROWS

                        FETCH
                            NEXT :limite ROWS ONLY
                    """

                else:

                    params.update({
                        "full_search": f"% {busca} %",
                    })

                    query = f"""
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
                                        p.descricao_completa,
                                        CASE WHEN pd.estoque > 0 THEN 1 ELSE 0 END ordem_estoque,
                                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento,
                                        CASE
                                            WHEN ' ' + p.descricao_completa + ' ' COLLATE SQL_Latin1_General_CP1_CI_AI LIKE :full_search
                                                THEN 1
                                            ELSE
                                                0
                                        END ordem_busca

                                FROM 
                                    TABELA_PRECO tp

                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                        tp.id_tabela_preco = tpp.id_tabela_preco
                                        AND tp.id_distribuidor = tpp.id_distribuidor

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        tpp.id_produto = pd.id_produto
                                        AND tpp.id_distribuidor = pd.id_distribuidor

                                    INNER JOIN PRODUTO p ON
                                        pd.id_produto = p.id_produto
                        
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

                                    {string_oferta}

                                WHERE
                                    pd.id_distribuidor IN :distribuidores
                                    {string_id_produto}
                                    {string_sku}
                                    {string_tgs}
                                    {string_grupos}
                                    {string_subgrupos}
                                    {string_busca}
                                    {string_marca}
                                    {string_id_distribuidor_tgs}
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND ps.status = 'A'
                                    AND s.status = 'A'
                                    AND gs.status = 'A'
                                    AND g.status = 'A'
                                    AND t.status = 'A'
                                    AND m.status = 'A'
                                    AND f.status = 'A'

                            ) p

                        ORDER BY
                            ordem_busca DESC, ordem_rankeamento ASC, descricao_completa

                        OFFSET
                            :offset ROWS

                        FETCH
                            NEXT :limite ROWS ONLY
                    """

            dict_query = {
                "query": query,
                "params": params,
                "raw": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:

                logger.info(f"Produtos nao encontrados")
                return {"status":404,
                        "resposta":{
                            "tipo":"7",
                            "descricao":f"Sem dados para retornar."}}, 200

            produtos = [
                {
                    "id_produto": produto[0],
                    "id_distribuidor": produto[1],
                }
                for produto in response
            ]

            counter = response[0][-1]

            dict_produtos = {
                "prod_dist_list": produtos,
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "image_replace": "150"
            }

            full_data = dj.json_products(**dict_produtos)

        if full_data:

            maximum_all = int(counter)
            maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

            logger.info(f"Produtos encontrados e enviados.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."},
                    "informacoes": {
                        "itens": maximum_all,
                        "paginas": maximum_pages,
                        "pagina_atual": pagina},
                    "dados":full_data}, 200

        logger.info(f"Produtos nao encontrados")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200