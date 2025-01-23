#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class ListarOfertasMarketplace(Resource):

    @logger
    def post(self) -> dict:
        """
        Método POST do Listar Ofertas do Marketplace

        Returns:
            [dict]: Status da transação
        """
        logger.info("Metodo POST do Endpoint Listar Ofertas do Marketplace foi requisitado.")

        id_cliente_token = None
        id_usuario = None

        info = secure.verify_token("user")
        if type(info) is dict:
            id_cliente_token = info.get("id_cliente")
            id_usuario = int(info.get('id_usuario'))

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_oferta", "id_tipo", "id_grupo", "id_subgrupo", 
                            "paginar", "busca", "id_orcamento", "id_cliente", "id_produto",
                            "ordenar", "estoque", "multiplo_venda", "tipo_oferta", "marca", 
                            "grupos", "subgrupos", "desconto"]

        if id_cliente_token:
            necessary_keys.append("id_cliente")
        
        else:
            unnecessary_keys.append("id_cliente")
        
        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_oferta": [list, int],
            "id_produto": [list, str],
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "paginar": bool,
            "id_orcamento": [list, int],
            "ordenar": int,
            "desconto": bool,
            "estoque": bool,
            "multiplo_venda": int,
            "tipo_oferta": int,
            "marca": [list, int],
            "grupos": [list, int],
            "subgrupos": [list, int]
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            no_use_columns = unnecessary_keys,
                                            not_null = necessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = response_data.get("id_cliente") \
                        if response_data.get("id_cliente") else None

        busca = response_data.get("busca") if response_data.get("busca") else None

        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = int(response_data.get("id_grupo")) if response_data.get("id_grupo") else None
        id_subgrupo = int(response_data.get("id_subgrupo")) if response_data.get("id_subgrupo") else None

        id_oferta = response_data.get("id_oferta") if response_data.get("id_oferta") else None

        id_produto = response_data.get("id_produto") if response_data.get("id_produto") else None

        id_orcamento = response_data.get("id_orcamento") if response_data.get("id_orcamento") else None

        paginar = True if response_data.get("paginar") else False

        estoque = True if response_data.get("estoque") and id_usuario else False
        desconto = True if response_data.get("desconto") and id_usuario else False
        multiplo_venda = int(response_data.get("multiplo_venda")) if response_data.get("multiplo_venda") else 0
        tipo_oferta = int(response_data.get("tipo_oferta")) if response_data.get("tipo_oferta") and id_usuario else None
        marca = response_data.get("marca")
        grupos = response_data.get("grupos")
        subgrupos = response_data.get("subgrupos")

        ordenar_por = int(response_data.get("ordenar")) if response_data.get("ordenar") else 1 # padrão mais vendidos
        if (ordenar_por > 3 or ordenar_por < 1):
            ordenar_por = 1

        # Verificando o id_cliente enviado
        if id_usuario:

            if not id_cliente:
                logger.error(f"U:{id_usuario} - Nao enviou o cliente escolhido.")
                return {"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario não enviou cliente escolhido."}}, 403

            if id_cliente not in id_cliente_token:
                logger.error(f"U:{id_usuario} - Usuario tentou realizar acao por cliente nao atrelado ao mesmo.")
                return {"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario tentando realizar ação por cliente não atrelado ao mesmo."}}, 403

        else:
            id_cliente = None
            id_usuario = None

        # Verificando possibilidade de procura por orçamento
        if id_orcamento:

            if not id_usuario or not id_cliente:

                logger.error(f"Usuario tentando procura de oferta por orcamento sem credenciais.")
                return {"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario tentando procura de oferta por orcamento sem credenciais."}}, 403

            if type(id_orcamento) is int:
                id_orcamento = [id_orcamento]

            elif type(id_orcamento) is str:
                id_orcamento = [int(id_orcamento)]

        # Verificando o paginar
        if response_data.get("paginar"):
            paginar = True

        elif id_oferta:
            paginar = True

        elif not (id_tipo or id_grupo or id_subgrupo):
            paginar = True

        elif ((id_tipo or id_grupo) and not id_subgrupo):
            paginar = True 

        if not paginar:
            limite = 999999           

        # Pegando os distribuidores validos do cliente
        if not id_usuario:
            query = """
                SELECT
                    DISTINCT id_distribuidor

                FROM
                    DISTRIBUIDOR

                WHERE
                    status = 'A'
                    AND id_distribuidor = CASE
                                              WHEN :id_distribuidor = 0
                                                  THEN id_distribuidor
                                              ELSE
                                                  :id_distribuidor
                                          END
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
                    c.status = 'A'
                    AND c.status_receita = 'A'
                    AND cd.status = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND d.status = 'A'
                    AND	c.id_cliente = :id_cliente
                    AND d.id_distribuidor = CASE
                                                WHEN :id_distribuidor = 0
                                                    THEN d.id_distribuidor
                                                ELSE
                                                    :id_distribuidor
                                            END
            """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

        query_obj = {
            "query": query,
            "params": params,
            "raw": True
        }

        distribuidores = dm.raw_sql_return(**query_obj)

        distribuidores = [distribuidor[0] for distribuidor in distribuidores]

        # Fazendo as queries
        counter = 0

        params = {
            "id_distribuidor": id_distribuidor,
            "distribuidores": distribuidores,
            "id_cliente": id_cliente,
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        bindparams = ["distribuidores"]

        string_id_distribuidor_tgs = "AND ps.id_distribuidor = :id_distribuidor"
        string_busca = ""
        string_tgs = ""
        string_id_produto = ""
        string_id_oferta = ""
        string_ordenar_por = ""
        string_estoque = ""
        string_multiplo_venda = ""
        string_desconto = ""
        string_ofertas = ""
        string_marca = ""
        string_grupos = ""
        string_subgrupos = ""

        ## Procura por campo de busca
        if busca:

            busca = str(busca).upper().split()

            string_busca = ""

            for index, word in enumerate(busca):

                params.update({
                    f"word_{index}": f"%{word}%"
                })

                string_busca += f"""AND (
                    p.descricao COLLATE Latin1_General_CI_AI LIKE :word_{index}
                    OR p.descricao_completa COLLATE Latin1_General_CI_AI LIKE :word_{index}
                    OR p.sku LIKE :word_{index}
                    OR p.dun14 LIKE :word_{index}
                    OR p.id_produto LIKE :word_{index}
                    OR m.desc_marca COLLATE Latin1_General_CI_AI LIKE :word_{index}
                )"""

        ## Procura por id_produto
        if id_produto:

            if type(id_produto) not in [list, tuple, set]:
                id_produto = [id_produto]

            else:
                id_produto = list(id_produto)

            bindparams.append("id_produto")

            params.update({
                "id_produto": id_produto
            })

            string_id_produto = "AND p.id_produto IN :id_produto"

        ## Procura por tipo, grupo e/ou subgrupo
        if id_tipo or id_grupo or id_subgrupo:

            # Pegando os subgrupos
            if id_subgrupo:
                string_tgs += f"AND s.id_subgrupo = :id_subgrupo "
                params["id_subgrupo"] = id_subgrupo

            if id_grupo:
                string_tgs += "AND g.id_grupo = :id_grupo "
                params["id_grupo"] = id_grupo

            if id_tipo:
                string_tgs += "AND t.id_tipo = :id_tipo "
                params["id_tipo"] = id_tipo

        ## Procura por id_oferta
        if id_oferta:

            if not isinstance(id_oferta, (list, tuple, set)):
                id_oferta = [id_oferta]

            else:
                id_oferta = list(id_oferta)

            bindparams.append("id_oferta")

            params.update({
                "id_oferta": id_oferta
            })

            string_id_oferta = "AND offers.id_oferta IN :id_oferta"

        ## Procura por estoque
        if estoque and id_usuario:
            string_estoque = "AND pe.qtd_estoque > 0"

        ## Procura por multiplo venda
        if multiplo_venda > 0:
            string_multiplo_venda = "AND pd.multiplo_venda = :multiplo_venda"
            params.update({
                "multiplo_venda": multiplo_venda,
            })

        # Procura por desconto
        if desconto and id_usuario:
            string_desconto = "AND odesc.desconto > 0"

        # Procura por tipo_oferta
        if tipo_oferta:
            if tipo_oferta == 1:
                string_ofertas = "AND offers.tipo_oferta = 2"
            
            elif tipo_oferta == 2:
                string_ofertas = "AND offers.tipo_oferta = 3"

        ## Procura por marca
        if marca:

            if type(marca) not in [list, tuple, set]:
                marca = [marca]

            else:
                marca = list(marca)

            bindparams.append("marca")

            params.update({
                "marca": marca
            })

            string_marca = "AND m.id_marca IN :marca"

        if grupos:

            string_id_distribuidor_tgs = "AND ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END"

            if type(grupos) not in [list, tuple, set]:
                grupos = [grupos]

            else:
                grupos = list(grupos)

            bindparams.append("grupos")

            params.update({
                "grupos": grupos
            })

            string_grupos = "AND g.id_grupo IN :grupos"

        if subgrupos:

            string_id_distribuidor_tgs = "AND ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END"

            if type(subgrupos) not in [list, tuple, set]:
                subgrupos = [subgrupos]

            else:
                subgrupos = list(subgrupos)

            bindparams.append("subgrupos")

            params.update({
                "subgrupos": subgrupos
            })

            string_subgrupos = "AND s.id_subgrupo IN :subgrupos"

        ## Ordenacao da busca
        dict_ordenacao = {
            1:"o.ordem, o.descricao_oferta",
            2:"o.data_cadastro DESC, o.data_inicio DESC, o.descricao_oferta",
            3:"o.data_cadastro ASC, o.data_inicio ASC, o.descricao_oferta",
        }

        string_ordenar_por = dict_ordenacao.get(ordenar_por)

        # Query 
        ofertas_query = f"""
            SET NOCOUNT ON;
            
            -- Verificando se a tabela temporaria existe
            IF OBJECT_ID('tempDB..#HOLD_OFERTA_INIT', 'U') IS NOT NULL
            BEGIN
                DROP TABLE #HOLD_OFERTA_INIT
            END;

            IF OBJECT_ID('tempDB..#HOLD_OFERTA', 'U') IS NOT NULL
            BEGIN
                DROP TABLE #HOLD_OFERTA
            END;

            -- Salvando as ofertas por filtros de produto
            SELECT 
                DISTINCT offers_produto.id_oferta

            INTO
                #HOLD_OFERTA_INIT

            FROM 
                PRODUTO p

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto
                                    
                INNER JOIN DISTRIBUIDOR d ON
                    pd.id_distribuidor = d.id_distribuidor

                LEFT JOIN PRODUTO_ESTOQUE pe ON
                    pd.id_produto = pe.id_produto 
                    AND pd.id_distribuidor = pe.id_distribuidor

                INNER JOIN FORNECEDOR f ON
                    pd.id_fornecedor = f.id_fornecedor
                    
                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca
                    AND pd.id_distribuidor = m.id_distribuidor

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

                INNER JOIN 
                (
                    SELECT
                        offers.id_produto,
                        offers.id_distribuidor,
                        offers.id_oferta,
                        offers.tipo_oferta

                    FROM
                        (
                            SELECT
                                op.id_oferta,
                                o.id_distribuidor,
                                op.id_produto,
                                o.tipo_oferta

                            FROM
                                (

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                    WHERE
                                        id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                                    UNION

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                        INNER JOIN OFERTA_CLIENTE oc ON
                                            o.id_oferta = oc.id_oferta

                                    WHERE
                                        oc.id_cliente = :id_cliente
                                        AND oc.status = 'A'
                                        AND oc.d_e_l_e_t_ = 0

                                ) offers

                                INNER JOIN OFERTA o ON
                                    offers.id_oferta = o.id_oferta

                                INNER JOIN OFERTA_PRODUTO op ON
                                    o.id_oferta = op.id_oferta

                            WHERE
                                o.status = 'A'
                                AND o.tipo_oferta IN (2,3)
                                AND op.status = 'A'

                            UNION

                            SELECT
                                ob.id_oferta,
                                o.id_distribuidor,
                                ob.id_produto,
                                2 as tipo_oferta

                            FROM
                                (

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                    WHERE
                                        id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                                    UNION

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                        INNER JOIN OFERTA_CLIENTE oc ON
                                            o.id_oferta = oc.id_oferta

                                    WHERE
                                        oc.id_cliente = :id_cliente
                                        AND oc.status = 'A'
                                        AND oc.d_e_l_e_t_ = 0

                                ) offers

                                INNER JOIN OFERTA o ON
                                    offers.id_oferta = o.id_oferta

                                INNER JOIN OFERTA_BONIFICADO ob ON
                                    o.id_oferta = ob.id_oferta

                            WHERE
                                o.status = 'A'
                                AND ob.status = 'A'

                        ) offers

                    WHERE
                        1 = 1
                        {string_id_oferta}
                        {string_ofertas}
                        
                ) as offers_produto ON
                    pd.id_produto = offers_produto.id_produto
                    AND pd.id_distribuidor = offers_produto.id_distribuidor

                LEFT JOIN	
                (
                    SELECT
                        op.id_produto,
                        o.id_distribuidor,
                        od.desconto

                    FROM
                        (

                            SELECT
                                o.id_oferta

                            FROM
                                OFERTA o

                            WHERE
                                id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                            UNION

                            SELECT
                                o.id_oferta

                            FROM
                                OFERTA o

                                INNER JOIN OFERTA_CLIENTE oc ON
                                    o.id_oferta = oc.id_oferta

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
                        AND o.status = 'A'
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()
                        AND op.status = 'A'
                        AND od.status = 'A'
                
                ) odesc ON
                    pd.id_produto = odesc.id_produto
                    AND pd.id_distribuidor = odesc.id_distribuidor

            WHERE
                1 = 1
                {string_id_produto}
                {string_busca}
                {string_tgs}
                {string_subgrupos}
                {string_grupos}
                {string_estoque}
                {string_multiplo_venda}
                {string_desconto}
                {string_marca}
                AND pd.id_distribuidor IN :distribuidores
                {string_id_distribuidor_tgs}
                AND ps.status = 'A'
                AND s.status  = 'A'
                AND gs.status = 'A'
                AND g.status  = 'A'
                AND t.status  = 'A'
                AND d.status = 'A'
                AND m.status  = 'A'
                AND f.status  = 'A';

            -- Pegando as ofertas validas
            SELECT
                o.id_oferta,
                COUNT(*) OVER() 'count__'

            INTO 
                #HOLD_OFERTA

            FROM
                (
                
                    SELECT
                        DISTINCT o.id_oferta,
                        o.ordem,
                        o.descricao_oferta,
                        o.data_cadastro,
                        o.data_inicio

                    FROM
                        (
                
                            (
                                SELECT
                                    av.id_oferta

                                FROM

                                    (
                                        SELECT
                                            o.id_oferta
                                                                                        
                                        FROM
                                            OFERTA o
                                                                                            
                                            INNER JOIN OFERTA_PRODUTO op ON
                                                o.id_oferta = op.id_oferta

                                            INNER JOIN PRODUTO p ON
                                                op.id_produto = p.id_produto

                                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                                p.id_produto = pd.id_produto
                                                AND o.id_distribuidor = pd.id_distribuidor 
                                                                                                
                                        WHERE
                                            o.tipo_oferta = 2
                                            AND op.status = 'A'
                                            AND p.status = 'A'
                                            AND pd.status = 'A'
                                                                                    
                                    ) AS av
                                                                                    
                                    INNER JOIN (
                                                                                                    
                                                    SELECT
                                                        o.id_oferta
                                                                                        
                                                    FROM
                                                        OFERTA o
                                                                                            
                                                        INNER JOIN OFERTA_BONIFICADO ob ON
                                                            o.id_oferta = ob.id_oferta

                                                        INNER JOIN PRODUTO p ON
                                                            ob.id_produto = p.id_produto

                                                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                                            p.id_produto = pd.id_produto
                                                            AND o.id_distribuidor = pd.id_distribuidor
                                                                                                
                                                    WHERE
                                                        o.tipo_oferta = 2
                                                        AND ob.status = 'A'
                                                        AND p.status = 'A'
                                                        AND pd.status = 'A'
                                                                                                
                                            ) AS bv ON
                                    av.id_oferta = bv.id_oferta
                            )

                            UNION

                            (
                                SELECT
                                    o.id_oferta
                                                                                        
                                FROM
                                    OFERTA o
                                                                                            
                                    INNER JOIN OFERTA_PRODUTO op ON
                                        o.id_oferta = op.id_oferta

                                    INNER JOIN PRODUTO p ON
                                        op.id_produto = p.id_produto

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        p.id_produto = pd.id_produto
                                        AND o.id_distribuidor = pd.id_distribuidor

                                    INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                                        o.id_oferta = oef.id_oferta

                                WHERE
                                    o.tipo_oferta = 3
                                    AND oef.desconto > 0
                                    AND oef.status = 'A'
                                    AND op.status = 'A'
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                            )

                        ) offers

                        INNER JOIN OFERTA o ON
                            offers.id_oferta = o.id_oferta

                        INNER JOIN #HOLD_OFERTA_INIT thoi ON
                            o.id_oferta = thoi.id_oferta

                    WHERE
                        1 = 1
                        AND o.status = 'A'
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()

                ) o

            ORDER BY
                {string_ordenar_por}

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY;

            DROP TABLE #HOLD_OFERTA_INIT;

            -- Pegando as informacoes das ofertas e dos produtos
            SELECT
                DISTINCT o.id_oferta,
                o.tipo_oferta,
                p.id_produto,
                pd.id_distribuidor,
                m.id_marca,
                m.desc_marca AS descricao_marca,
                p.sku,
                p.status as status_produto,
                p.descricao_completa AS descricao_produto,
                p.unidade_embalagem,
                p.quantidade_embalagem,
                p.unidade_venda,
                p.quant_unid_venda,
                pp.preco_tabela,
                pd.multiplo_venda,
                pd.ranking,
                ISNULL(pe.qtd_estoque,0) AS estoque,
                pimg.token,
                odesc.desconto,
                odesc.data_inicio as data_inicio_desconto,
                odesc.data_final as data_final_desconto,
                o.descricao_oferta,
                o.limite_ativacao_oferta AS quantidade_limite,
                o.limite_ativacao_oferta AS quantidade_pontos,
                o.limite_ativacao_cliente AS maxima_ativacao,
                o.data_inicio,
                o.data_final,
                0 AS quantidade_produto,
                0 AS quantidade_bonificado,
                0 AS quantidade,
                0 AS automatica,
                0 total_desconto,
                oef.sequencia,
                oef.faixa,
                oef.desconto as desconto_faixa,
                o.produto_agrupado as "unificada",
                ob.quantidade_bonificada,
                o.ordem,
                CASE WHEN o.ordem = 1 THEN 1 ELSE 0 END AS destaque,
                CASE WHEN o.tipo_oferta = 2 THEN 'W' ELSE NULL END AS tipo_campanha,
                CASE WHEN o.operador = 'Q' THEN necessario_para_ativar ELSE 0 END AS quantidade_ativar,
                CASE WHEN o.operador = 'V' THEN necessario_para_ativar ELSE 0 END AS valor_ativar,
                CASE WHEN o.operador = 'Q' THEN necessario_para_ativar ELSE 0 END AS quantidade_minima,
                CASE WHEN o.operador = 'V' THEN necessario_para_ativar ELSE 0 END AS valor_minimo,
                o.status as status_oferta,
                CASE WHEN o.operador = 'Q' THEN op.quantidade_min_ativacao ELSE 0 END AS quantidade_min_ativacao,
                CASE WHEN o.operador = 'V' THEN op.valor_min_ativacao ELSE 0 END AS valor_min_ativacao,
                ob.quantidade_bonificada,
                ob.status status_bonificado,
                CASE
                    WHEN o.tipo_oferta = 2 AND EXISTS (SELECT 1 FROM OFERTA_PRODUTO WHERE id_oferta = o.id_oferta AND id_produto = p.id_produto AND status = 'A') THEN 1
                    ELSE 0
                END ativador,
                CASE 
                    WHEN o.tipo_oferta = 2 AND EXISTS (SELECT 1 FROM OFERTA_BONIFICADO WHERE id_oferta = o.id_oferta AND id_produto = p.id_produto AND status = 'A') THEN 1
                    ELSE 0
                END bonificado,
                o.data_cadastro,
                tho.count__

            FROM
                (
                    
                    SELECT
                        op.id_oferta,
                        o.id_distribuidor,
                        op.id_produto,
                        o.tipo_oferta

                    FROM
                        #HOLD_OFERTA tho

                        INNER JOIN OFERTA o ON
                            tho.id_oferta = o.id_oferta

                        INNER JOIN OFERTA_PRODUTO op ON
                            o.id_oferta = op.id_oferta

                    WHERE
                        1 = 1
                        AND op.status = 'A'

                    UNION

                    SELECT
                        ob.id_oferta,
                        o.id_distribuidor,
                        ob.id_produto,
                        o.tipo_oferta

                    FROM
                        #HOLD_OFERTA tho

                        INNER JOIN OFERTA o ON
                            tho.id_oferta = o.id_oferta

                        INNER JOIN OFERTA_BONIFICADO ob ON
                            o.id_oferta = ob.id_oferta	

                    WHERE
                        1 = 1
                        AND ob.status = 'A'

                ) offers_produto

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    offers_produto.id_produto = pd.id_produto
                    AND offers_produto.id_distribuidor = pd.id_distribuidor

                INNER JOIN PRODUTO p ON
                    pd.id_produto = p.id_produto
                                            
                LEFT JOIN PRODUTO_ESTOQUE pe ON
                    pd.id_produto = pe.id_produto
                    AND pd.id_distribuidor = pe.id_distribuidor 
                                            
                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca
                    AND pe.id_distribuidor = m.id_distribuidor

                INNER JOIN 
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
                        1 = 1
                        AND tp.id_distribuidor IN :distribuidores
                        AND tp.status = 'A'
                        AND tp.dt_inicio <= GETDATE()
                        AND tp.dt_fim >= GETDATE()
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
                                                                    status = 'A'
                                                                    AND id_distribuidor IN :distribuidores
                                                                    AND id_cliente = :id_cliente
                                                            )
                                )
                            )

                    GROUP BY
                        tpp.id_produto,
                        tpp.id_distribuidor

                ) pp ON
                    pd.id_produto = pp.id_produto
                    AND pd.id_distribuidor = pp.id_distribuidor

                INNER JOIN OFERTA o ON
                    offers_produto.id_oferta = o.id_oferta

                INNER JOIN #HOLD_OFERTA tho ON
                    o.id_oferta = tho.id_oferta

                LEFT JOIN 
                (
                    SELECT
                        op.id_produto,
                        o.id_distribuidor,
                        MAX(od.desconto) desconto,
                        o.data_inicio,
                        o.data_final

                    FROM
                        (

                            SELECT
                                o.id_oferta

                            FROM
                                OFERTA o

                            WHERE
                                id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                            UNION

                            SELECT
                                o.id_oferta

                            FROM
                                OFERTA o

                                INNER JOIN OFERTA_CLIENTE oc ON
                                    o.id_oferta = oc.id_oferta

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
                        AND o.status = 'A'
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()
                        AND op.status = 'A'
                        AND od.status = 'A'

                    GROUP BY
                        op.id_produto,
                        o.id_distribuidor,
                        o.data_inicio,
                        o.data_final

                ) odesc ON
                    p.id_produto = odesc.id_produto
                    AND pd.id_distribuidor = odesc.id_distribuidor

                LEFT JOIN OFERTA_PRODUTO op ON
                    o.id_oferta = op.id_oferta
                    AND op.status = 'A'

                LEFT JOIN OFERTA_BONIFICADO ob ON
                    o.id_oferta = ob.id_oferta
                    AND ob.status = 'A'

                LEFT JOIN OFERTA_ESCALONADO_FAIXA oef ON
                    o.id_oferta = oef.id_oferta
                    AND oef.status = 'A'

                LEFT JOIN PRODUTO_IMAGEM pimg ON
                    p.sku = pimg.sku

            WHERE
                1 = 1

            ORDER BY
                {string_ordenar_por},
                pimg.TOKEN,
                p.descricao_completa,
                oef.sequencia,
                oef.faixa

            -- Deletando as tabelas temporarias
            DROP TABLE #HOLD_OFERTA;
        """

        query_obj = {
            "query": ofertas_query,
            "params": params,
            "bindparams": bindparams,
        }

        ofertas_query = dm.raw_sql_return(**query_obj)

        # Criando o json
        oferta_hold = []

        if not ofertas_query:
            logger.error(f"Dados nao encontrados.")
            return {"status":404,
                    "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200

        counter = ofertas_query[0].get("count__")

        server = os.environ.get("PSERVER_PS").lower()

        if server == "production":
            image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
            
        elif server == "development":
            image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

        else:
            image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

        for oferta in ofertas_query:

            id_oferta = oferta.get('id_oferta')
            tipo_oferta = oferta.get("tipo_oferta")

            imagem = oferta.get("token")
            if imagem:
                imagem = f"{image}/imagens/{imagem}".replace("/auto/", "/150/")

            if tipo_oferta == 2:
                for saved_oferta in oferta_hold:
                    if not saved_oferta.get("id_campanha"):
                        continue

                    if saved_oferta["id_campanha"] == id_oferta:
                        
                        id_produto = oferta.get("id_produto")

                        # Caso o produto já esteja salvo
                        saved_image = False
                        
                        if oferta.get("ativador"):
                            for saved_produto in saved_oferta["produto_ativador"]:
                                if saved_produto.get("id_produto") == id_produto:
                                    if imagem not in saved_produto["imagem"]:
                                        saved_produto["imagem"].append(imagem)
                                    saved_image = True
                                    break

                        if oferta.get("bonificado"):
                            for saved_produto in saved_oferta["produto_bonificado"]:
                                if saved_produto.get("id_produto") == id_produto:
                                    if imagem not in saved_produto["imagem"]:
                                        saved_produto["imagem"].append(imagem)
                                    saved_image = True
                                    break

                        if saved_image:
                            break

                        # Caso não...
                        desconto = {}

                        if id_usuario and oferta.get("desconto") and oferta.get("preco_tabela"):
                    
                            desconto_perc = oferta.get("desconto")
                            if desconto_perc:

                                if desconto_perc > 100:
                                    desconto_perc = 100

                                elif desconto_perc < 0:
                                    desconto_perc = 0

                                preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                                desconto = {
                                    "preco_desconto": round(preco_desconto, 3),
                                    "desconto": desconto_perc,
                                    "data_inicio": oferta.get("data_inicio_desconto"),
                                    "data_final": oferta.get("data_final_desconto")
                                }

                        imagens = []
                        if imagem:
                            imagens.append(imagem)

                        produto_campanha = {
                            "id_campanha": oferta.get("id_oferta"),
                            "id_distribuidor": oferta.get("id_distribuidor"),
                            "id_produto": oferta.get("id_produto"),
                            "sku": oferta.get("sku"),
                            "id_marca": oferta.get("id_marca"),
                            "descricao_marca": oferta.get("descricao_marca"),
                            "descricao_produto": oferta.get("descricao_produto"),
                            "imagem": imagens,
                            "status": oferta.get("status_produto"),
                            "quantidade": oferta.get("quantidade"),
                            "quantidade_minima": oferta.get("quantidade_minima"),
                            "valor_minimo": oferta.get("valor_minimo"),
                            "quantidade_min_ativacao": oferta.get("quantidade_min_ativacao"),
                            "valor_min_ativacao": oferta.get("valor_min_ativacao"),
                            "unidade_embalagem": oferta.get("unidade_embalagem"),
                            "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                            "unidade_venda": oferta.get("unidade_venda"),
                            "quant_unid_venda": oferta.get("quant_unid_venda"),
                            "multiplo_venda": oferta.get("multiplo_venda"),
                        }

                        if id_usuario:
                            produto_campanha.update({
                                "preco_tabela": oferta.get("preco_tabela"),
                                "estoque": oferta.get("estoque"),
                                "desconto": desconto,
                            })

                        if oferta.get("ativador"):       
                            saved_oferta["quantidade_produto"] += 1
                            saved_oferta["produto_ativador"].append(produto_campanha)

                        if oferta.get("bonificado"):
                            produto_campanha_bonificado = produto_campanha.copy()
                            produto_campanha_bonificado["quantidade_bonificada"] = oferta.get("quantidade_bonificada")
                            saved_oferta["produto_bonificado"].append(produto_campanha_bonificado)

                        break

                else:
                    campanha_cabecalho = {
                        "id_campanha": id_oferta,
                        "id_distribuidor": oferta.get("id_distribuidor"),
                        "status": oferta.get("status_oferta"),
                        "tipo_campanha": oferta.get("tipo_campanha"),
                        "descricao": oferta.get("descricao_oferta"),
                        "data_inicio": oferta.get("data_inicio"),
                        "data_final": oferta.get("data_final"),
                        "destaque": oferta.get("destaque"),
                        "quantidade_limite": oferta.get("quantidade_limite"),
                        "quantidade_pontos": oferta.get("quantidade_pontos"),
                        "quantidade_produto": oferta.get("quantidade_produto"),
                        "quantidade_ativar": oferta.get("quantidade_ativar"),
                        "maxima_ativacao": oferta.get("maxima_ativacao"),
                        "quantidade_bonificado": oferta.get("quantidade_bonificado"),
                        "valor_ativar": oferta.get("valor_ativar"),
                        "automatica": False,
                        "unidade_venda": None,
                        "unificada": oferta.get("unificada"),
                        "produto_ativador": [],
                        "produto_bonificado": []
                    }

                    imagens = []

                    imagem = oferta.get("token")
                    if imagem:

                        imagem = f"{image}/imagens/{imagem}".replace("/auto/", "/150/")
                        imagens.append(imagem)

                    desconto = {}

                    if id_usuario and oferta.get("desconto") and oferta.get("preco_tabela"):
                
                        desconto_perc = oferta.get("desconto")
                        if desconto_perc:

                            if desconto_perc > 100:
                                desconto_perc = 100

                            elif desconto_perc < 0:
                                desconto_perc = 0

                            preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                            desconto = {
                                "preco_desconto": round(preco_desconto, 3),
                                "desconto": desconto_perc,
                                "data_inicio": oferta.get("data_inicio_desconto"),
                                "data_final": oferta.get("data_final_desconto")
                            }

                    produto_campanha = {
                        "id_campanha": oferta.get("id_oferta"),
                        "id_distribuidor": oferta.get("id_distribuidor"),
                        "id_produto": oferta.get("id_produto"),
                        "sku": oferta.get("sku"),
                        "id_marca": oferta.get("id_marca"),
                        "descricao_marca": oferta.get("descricao_marca"),
                        "descricao_produto": oferta.get("descricao_produto"),
                        "imagem": imagens,
                        "status": oferta.get("status_produto"),
                        "quantidade": oferta.get("quantidade"),
                        "quantidade_minima": oferta.get("quantidade_minima"),
                        "valor_minimo": oferta.get("valor_minimo"),
                        "quantidade_min_ativacao": oferta.get("quantidade_min_ativacao"),
                        "valor_min_ativacao": oferta.get("valor_min_ativacao"),
                        "unidade_embalagem": oferta.get("unidade_embalagem"),
                        "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                        "unidade_venda": oferta.get("unidade_venda"),
                        "quant_unid_venda": oferta.get("quant_unid_venda"),
                        "multiplo_venda": oferta.get("multiplo_venda"),
                    }

                    if id_usuario:
                        produto_campanha.update({
                            "preco_tabela": oferta.get("preco_tabela"),
                            "estoque": oferta.get("estoque"),
                            "desconto": desconto,
                        })

                    if oferta.get("ativador"):       
                        campanha_cabecalho["quantidade_produto"] += 1
                        campanha_cabecalho["unidade_venda"] = oferta.get("unidade_venda")
                        campanha_cabecalho["produto_ativador"].append(produto_campanha)

                    if oferta.get("bonificado"):
                        produto_campanha_bonificado = produto_campanha.copy()
                        produto_campanha_bonificado["quantidade_bonificada"] = oferta.get("quantidade_bonificada")
                        campanha_cabecalho["produto_bonificado"].append(produto_campanha_bonificado)

                    oferta_hold.append(campanha_cabecalho)

            elif tipo_oferta == 3:
                
                for saved_oferta in oferta_hold:
                    if not saved_oferta.get("id_escalonado"):
                        continue

                    if saved_oferta["id_escalonado"] == id_oferta:
                        
                        for faixa in saved_oferta["faixa_escalonado"]:
                            if faixa["sequencia"] == oferta.get('sequencia'):
                                break
                        
                        else:
                            saved_oferta["faixa_escalonado"].append({
                                "id_escalonado": id_oferta,
                                "id_distribuidor": oferta.get("id_distribuidor"),
                                "sequencia": oferta.get("sequencia"),
                                "faixa": oferta.get("faixa"),
                                "desconto": oferta.get("desconto_faixa"),
                                "unidade_venda": saved_oferta["faixa_escalonado"][0].get("unidade_venda"),
                            })

                        # Caso o produto já esteja salvo
                        id_produto = oferta.get("id_produto")
                        saved_image = False

                        for saved_produto in saved_oferta["produto_escalonado"]:
                            if saved_produto.get("id_produto") == id_produto:
                                if imagem not in saved_produto["imagem"]:
                                    saved_produto["imagem"].append(imagem)
                                saved_image = True
                                break

                        if saved_image:
                            break

                        # Caso não
                        imagens = []
                        if imagem:
                            imagens.append(imagem)

                        desconto = {}

                        if id_usuario and oferta.get("desconto") and oferta.get("preco_tabela"):
                    
                            desconto_perc = oferta.get("desconto")
                            if desconto_perc:

                                if desconto_perc > 100:
                                    desconto_perc = 100

                                elif desconto_perc < 0:
                                    desconto_perc = 0

                                preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                                desconto = {
                                    "preco_desconto": round(preco_desconto, 3),
                                    "desconto": desconto_perc,
                                    "data_inicio": oferta.get("data_inicio_desconto"),
                                    "data_final": oferta.get("data_final_desconto")
                                }

                        produto_escalonado = {
                            "id_escalonado": id_oferta,
                            "id_distribuidor": oferta.get("id_distribuidor"),
                            "id_produto": oferta.get("id_produto"),
                            "sku": oferta.get("sku"),
                            "id_marca": oferta.get("id_marca"),
                            "descricao_marca": oferta.get("descricao_marca"),
                            "descricao_produto": oferta.get("descricao_produto"),
                            "imagem": imagens,
                            "status": oferta.get("status_produto"),
                            "quantidade": oferta.get("quantidade"),
                            "unidade_embalagem": oferta.get("unidade_embalagem"),
                            "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                            "unidade_venda": oferta.get("unidade_venda"),
                            "quant_unid_venda": oferta.get("quant_unid_venda"),
                            "multiplo_venda": oferta.get("multiplo_venda"),
                        }

                        if id_usuario:
                            produto_escalonado.update({
                                "preco_tabela": oferta.get("preco_tabela"),
                                "estoque": oferta.get("estoque"),
                                "desconto": desconto,
                            })

                        saved_oferta["produto_escalonado"].append(produto_escalonado)

                        break

                else:
                    imagens = []
                    if imagem:
                        imagens.append(imagem)

                    desconto = {}

                    if id_usuario and oferta.get("desconto") and oferta.get("preco_tabela"):
                
                        desconto_perc = oferta.get("desconto")
                        if desconto_perc:

                            if desconto_perc > 100:
                                desconto_perc = 100

                            elif desconto_perc < 0:
                                desconto_perc = 0

                            preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                            desconto = {
                                "preco_desconto": round(preco_desconto, 3),
                                "desconto": desconto_perc,
                                "data_inicio": oferta.get("data_inicio_desconto"),
                                "data_final": oferta.get("data_final_desconto")
                            }

                    produto_escalonado = {
                        "id_escalonado": id_oferta,
                        "id_distribuidor": oferta.get("id_distribuidor"),
                        "id_produto": oferta.get("id_produto"),
                        "sku": oferta.get("sku"),
                        "id_marca": oferta.get("id_marca"),
                        "descricao_marca": oferta.get("descricao_marca"),
                        "descricao_produto": oferta.get("descricao_produto"),
                        "imagem": imagens,
                        "status": oferta.get("status_produto"),
                        "quantidade": oferta.get("quantidade"),
                        "unidade_embalagem": oferta.get("unidade_embalagem"),
                        "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                        "unidade_venda": oferta.get("unidade_venda"),
                        "quant_unid_venda": oferta.get("quant_unid_venda"),
                        "multiplo_venda": oferta.get("multiplo_venda"),
                    }

                    if id_usuario:
                        produto_escalonado.update({
                            "preco_tabela": oferta.get("preco_tabela"),
                            "estoque": oferta.get("estoque"),
                            "desconto": desconto,
                        })

                    oferta_hold.append({
                        "id_escalonado": id_oferta,
                        "id_distribuidor": oferta.get("id_distribuidor"),
                        "descricao": oferta.get("descricao_oferta"),
                        "status": oferta.get("status_oferta"),
                        "total_desconto": 0,
                        "unificada": oferta.get("unificada"),
                        "escalonado": [
                            {
                                "id_escalonado": id_oferta,
                                "id_distribuidor": oferta.get("id_distribuidor"),
                                "data_inicio": oferta.get("data_inicio"),
                                "data_final": oferta.get("data_final"),
                                "limite": oferta.get("quantidade_limite"),
                                "status_escalonado": oferta.get("status_oferta"),
                            }
                        ],
                        "faixa_escalonado": [
                            {
                                "id_escalonado": id_oferta,
                                "id_distribuidor": oferta.get("id_distribuidor"),
                                "sequencia": oferta.get("sequencia"),
                                "faixa": oferta.get("faixa"),
                                "desconto": oferta.get("desconto_faixa"),
                                "unidade_venda": oferta.get("unidade_venda"),
                            }
                        ],
                        "produto_escalonado": [produto_escalonado]
                    })

        if oferta_hold:
            msg = {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."}}
            
            if paginar:
                msg['informacoes'] = {
                    "itens": counter,
                    "paginas": counter//limite + (counter % limite > 0),
                    "pagina_atual": pagina
                }

            msg["dados"] = oferta_hold

            logger.info("Dados de ofertas enviados para o marketplace.")
            return msg, 200

        logger.error(f"Dados nao encontrados.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200