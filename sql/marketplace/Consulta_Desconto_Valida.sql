USE [B2BTESTE2]

SET NOCOUNT ON;

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_distribuidor INT = 0;
DECLARE @id_cliente INT = 1;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

-- Mostrando o resultado
SELECT
    DISTINCT RESULT.count__,
    RESULT.id_produto,
    p.sku,
    p.descricao_completa as descricao_produto,
    p.status,
    p.dun14,
    p.tipo_produto,
    p.variante,
    p.volumetria,
    p.unidade_embalagem,
    p.quantidade_embalagem,
    p.unidade_venda,
    p.quant_unid_venda,
    p.tokens_imagem as tokens,
    RESULT.id_distribuidor,
    d.nome_fantasia as descricao_distribuidor,
    pd.agrupamento_variante,
    pd.cod_prod_distr,
    SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
    pd.multiplo_venda,
    pd.ranking,
    pd.unidade_venda as unidade_venda_distribuidor,
    pd.quant_unid_venda as quant_unid_venda_distribuidor,
    pd.giro,
    pd.agrup_familia,
    pd.status as status_distribuidor,
    pd.data_cadastro,
    m.id_marca as marca,
    m.desc_marca as descricao_marca,
    f.id_fornecedor,
    f.desc_fornecedor,
    s.id_subgrupo,
    s.descricao as descricao_subgrupo,
    g.id_grupo,
    g.descricao as descricao_grupo,
    t.id_tipo,
    t.descricao as descricao_tipo,
    ISNULL(pe.qtd_estoque, 0) as estoque,
    ISNULL(RESULT.preco_tabela, 0) as preco_tabela,
    odesc.desconto,
    odesc.data_inicio as data_inicio_desconto,
    odesc.data_final as data_final_desconto,
    offers.id_oferta,
    CASE
        WHEN EXISTS (SELECT 1 FROM OFERTA_PRODUTO WHERE id_produto = RESULT.id_produto AND id_oferta = offers.id_oferta) AND offers.tipo_oferta = 2
            THEN 1
        ELSE
            0
    END ativador,
    CASE
        WHEN EXISTS (SELECT 1 FROM OFERTA_BONIFICADO WHERE id_produto = RESULT.id_produto AND id_oferta = offers.id_oferta) AND offers.tipo_oferta = 2
            THEN 1
        ELSE
            0
    END bonificado,
    CASE
        WHEN EXISTS (SELECT 1 FROM OFERTA_PRODUTO WHERE id_produto = RESULT.id_produto AND id_oferta = offers.id_oferta) AND offers.tipo_oferta = 3
            THEN 1
        ELSE
            0
    END escalonado,
    NULL as detalhes,
    uuv.data_cadastro as data_cadastro_ultimos_vistos,
    CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

FROM
            
    (
        SELECT
            id_produto,
            id_distribuidor,
            preco_tabela,
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
                        uuv.data_cadastro as data_cadastro_ultimos_vistos,
                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

                FROM
                    USUARIO_ULTIMOS_VISTOS uuv
                            
                    INNER JOIN PRODUTO p ON
                        uuv.id_produto = p.id_produto

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto
                        AND uuv.id_distribuidor = pd.id_distribuidor

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

                            INNER JOIN USUARIO_ULTIMOS_VISTOS uuv ON
                                tpp.id_produto = uuv.id_produto
                                AND tpp.id_distribuidor = uuv.id_distribuidor

                        WHERE
                            uuv.id_usuario = 1
                            AND uuv.id_cliente = 1
                            AND tp.id_distribuidor IN (1,2)
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
                                                                        id_cliente = 1
                                                                        AND id_distribuidor IN (1,2)
                                                                        AND status = 'A'
                                                                )
                                    )
                                )

                        GROUP BY
                            tpp.id_produto,
                            tpp.id_distribuidor

                    ) ppr ON
                        pd.id_produto = ppr.id_produto
                        AND pd.id_distribuidor = ppr.id_distribuidor
                                
                    INNER JOIN DISTRIBUIDOR d ON
                        pd.id_distribuidor = d.id_distribuidor
                
                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                    INNER JOIN FORNECEDOR f ON
                        pd.id_fornecedor = f.id_fornecedor

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        pd.id_produto = ps.codigo_produto 
                        AND pd.id_distribuidor = ps.id_distribuidor

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
                                    oc.id_cliente = 1
                                    AND oc.status = 'A'
                                    AND oc.d_e_l_e_t_ = 0

                            ) offers

                            INNER JOIN OFERTA o ON
                                offers.id_oferta = o.id_oferta

                            INNER JOIN OFERTA_PRODUTO op ON
                                o.id_oferta = op.id_oferta

                            INNER JOIN USUARIO_ULTIMOS_VISTOS uuv ON
                                op.id_produto = uuv.id_produto
                                AND o.id_distribuidor = uuv.id_distribuidor

                            INNER JOIN OFERTA_DESCONTO od ON
                                o.id_oferta = od.id_oferta

                        WHERE
                            o.tipo_oferta = 1
                            AND uuv.id_usuario = 1
                            AND uuv.id_cliente = 1
                            AND o.id_distribuidor IN (1,2)
                            AND o.status = 'A'
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND op.status = 'A'
                            AND od.status = 'A'
            
                    ) odesc ON
                        pd.id_produto = odesc.id_produto
                        AND pd.id_distribuidor = odesc.id_distribuidor

                    LEFT JOIN 
                    (
                        SELECT
                            offers.id_oferta,
                            offers_produto.id_produto,
                            offers_produto.tipo_oferta

                        FROM
                            (
                                SELECT
                                    op.id_oferta,
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
                                            oc.id_cliente = 1
                                            AND oc.status = 'A'
                                            AND oc.d_e_l_e_t_ = 0

                                    ) offers

                                    INNER JOIN 
                                    (
                                                
                                        SELECT
                                            op.id_oferta,
                                            op.id_produto

                                        FROM
                                            OFERTA_PRODUTO op

                                        WHERE
                                            op.status = 'A'

                                        UNION

                                        SELECT
                                            ob.id_oferta,
                                            ob.id_produto

                                        FROM
                                            OFERTA_BONIFICADO ob

                                        WHERE
                                            ob.status = 'A'

                                    ) op ON
                                        offers.id_oferta = op.id_oferta

                                    INNER JOIN OFERTA o ON
                                        offers.id_oferta = o.id_oferta

                                    INNER JOIN PRODUTO p ON
                                        op.id_produto = p.id_produto

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        p.id_produto = pd.id_produto
                                        AND o.id_distribuidor = pd.id_distribuidor

                                WHERE
                                    o.tipo_oferta IN (2,3)
                                    AND o.id_distribuidor IN (1,2)
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND o.status = 'A'
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                                        
                            ) offers_produto

                            INNER JOIN 
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
                                                                                                            
                                            WHERE
                                                o.tipo_oferta = 2
                                                AND o.id_distribuidor IN (1,2)
                                                AND o.status = 'A'
                                                AND op.status = 'A'
                                                                                                
                                        ) AS av
                                                                                                
                                        INNER JOIN 
                                        (
                                                                                                                
                                            SELECT
                                                o.id_oferta
                                                                                                    
                                            FROM
                                                OFERTA o
                                                                                                        
                                                INNER JOIN OFERTA_BONIFICADO ob ON
                                                    o.id_oferta = ob.id_oferta
                                                                                                            
                                            WHERE
                                                o.tipo_oferta = 2
                                                AND o.id_distribuidor IN (1,2)
                                                AND o.status = 'A'
                                                AND ob.status = 'A'
                                                                                                            
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

                                        INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                                            o.id_oferta = oef.id_oferta
                                                                                                            
                                    WHERE
                                        o.tipo_oferta = 3
                                        AND o.id_distribuidor IN (1,2)
                                        AND oef.desconto > 0
                                        AND oef.status = 'A'
                                        AND o.status = 'A'
                                        AND op.status = 'A'
                                )

                            ) offers ON
                                offers_produto.id_oferta = offers.id_oferta
            
                    ) offers ON
                        pd.id_produto = offers.id_produto

                WHERE
                    uuv.id_usuario = 1
                    AND uuv.id_cliente = 1
                    AND pd.id_distribuidor IN (1,2)
                    AND ps.status = 'A'
                    AND s.status = 'A'
                    AND gs.status = 'A'
                    AND g.status = 'A'
                    AND t.status = 'A'
                    AND d.status = 'A'
                    AND m.status = 'A'
                    AND f.status = 'A'

            ) FILTER_RESULT

        ORDER BY
            ordem_rankeamento ASC, descricao_completa

        OFFSET
            0 ROWS

        FETCH
            NEXT 20 ROWS ONLY
    )
            
    RESULT
                            
    INNER JOIN PRODUTO p ON
        RESULT.id_produto = p.id_produto

    INNER JOIN USUARIO_ULTIMOS_VISTOS uuv ON
        p.id_produto = uuv.id_produto
        AND RESULT.id_distribuidor = uuv.id_distribuidor

    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        uuv.id_produto = pd.id_produto
        AND uuv.id_distribuidor = pd.id_distribuidor

    INNER JOIN DISTRIBUIDOR d ON
        pd.id_distribuidor = d.id_distribuidor

    INNER JOIN MARCA m ON
        pd.id_marca = m.id_marca
        AND pd.id_distribuidor = m.id_distribuidor

    INNER JOIN FORNECEDOR f ON
        pd.id_fornecedor = f.id_fornecedor

    LEFT JOIN PRODUTO_ESTOQUE pe ON
        pd.id_produto = pe.id_produto 
        AND pd.id_distribuidor = pe.id_distribuidor

    LEFT JOIN 
    (
        SELECT
            op.id_produto,
            o.id_distribuidor,
            MAX(od.desconto) desconto,
            o.data_inicio,
            o.data_final

        FROM
            OFERTA o

            INNER JOIN OFERTA_PRODUTO op ON
                o.id_oferta = op.id_oferta

            INNER JOIN USUARIO_ULTIMOS_VISTOS uuv ON
                op.id_produto = uuv.id_produto
                AND o.id_distribuidor = uuv.id_distribuidor

            INNER JOIN OFERTA_DESCONTO od ON
                o.id_oferta = od.id_oferta

        WHERE
            o.tipo_oferta = 1
            AND uuv.id_usuario = 1
            AND uuv.id_cliente = 1
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
                        
    LEFT JOIN 
    (
        SELECT
            offers.id_oferta,
            offers_produto.id_produto,
            offers_produto.tipo_oferta

        FROM
            (
                SELECT
                    op.id_oferta,
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
                            oc.id_cliente = 1
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) offers

                    INNER JOIN 
                    (
                                
                        SELECT
                            op.id_oferta,
                            op.id_produto

                        FROM
                            OFERTA_PRODUTO op

                        WHERE
                            op.status = 'A'

                        UNION

                        SELECT
                            ob.id_oferta,
                            ob.id_produto

                        FROM
                            OFERTA_BONIFICADO ob

                        WHERE
                            ob.status = 'A'

                    ) op ON
                        offers.id_oferta = op.id_oferta

                    INNER JOIN OFERTA o ON
                        offers.id_oferta = o.id_oferta

                    INNER JOIN PRODUTO p ON
                        op.id_produto = p.id_produto

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto
                        AND o.id_distribuidor = pd.id_distribuidor

                WHERE
                    o.tipo_oferta IN (2,3)
                    AND o.id_distribuidor IN (1,2)
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND o.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                                        
            ) offers_produto

            INNER JOIN 
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
                                                                                            
                            WHERE
                                o.tipo_oferta = 2
                                AND o.id_distribuidor IN (1,2)
                                AND o.status = 'A'
                                AND op.status = 'A'
                                                                                
                        ) AS av
                                                                                
                        INNER JOIN 
                        (
                                                                                                
                            SELECT
                                o.id_oferta
                                                                                    
                            FROM
                                OFERTA o
                                                                                        
                                INNER JOIN OFERTA_BONIFICADO ob ON
                                    o.id_oferta = ob.id_oferta
                                                                                            
                            WHERE
                                o.tipo_oferta = 2
                                AND o.id_distribuidor IN (1,2)
                                AND o.status = 'A'
                                AND ob.status = 'A'
                                                                                            
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

                        INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                            o.id_oferta = oef.id_oferta
                                                                                            
                    WHERE
                        o.tipo_oferta = 3
                        AND o.id_distribuidor IN (1,2)
                        AND oef.desconto > 0
                        AND oef.status = 'A'
                        AND o.status = 'A'
                        AND op.status = 'A'
                )

            ) offers ON
                offers_produto.id_oferta = offers.id_oferta

    ) offers ON
        p.id_produto = offers.id_produto

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
    ps.id_distribuidor != 0
    AND uuv.id_usuario = 1
    AND uuv.id_cliente = 1

ORDER BY
    ordem_rankeamento ASC, descricao_completa;