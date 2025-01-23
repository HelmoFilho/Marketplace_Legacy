USE [B2BTESTE2]

SET NOCOUNT ON;
SET STATISTICS IO ON;

-- Só para prototipar
DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;
DECLARE @id_distribuidor INT = 0;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

DECLARE @distribuidores TABLE (
	id_distribuidor INT
)

IF @id_cliente IS NULL
BEGIN

	INSERT INTO
		@distribuidores

		SELECT
			id_distribuidor

		FROM
			DISTRIBUIDOR

		WHERE
			id_distribuidor = CASE WHEN @id_distribuidor = 0 THEN id_distribuidor ELSE @id_distribuidor END
			AND status = 'A'

END

ELSE
BEGIN

	INSERT INTO
		@distribuidores

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
            AND	c.id_cliente = @id_cliente
			AND d.id_distribuidor = CASE WHEN @id_distribuidor = 0 THEN d.id_distribuidor ELSE @id_distribuidor END
	
END;

SELECT
	DISTINCT orcm.id_orcamento,
    orcm.id_distribuidor,
    p.id_produto,
    p.sku,
    p.descricao_completa as descricao_produto,
    m.desc_marca as descricao_marca,
    ppr.preco_tabela,
	CASE
		WHEN orcmitm.tipo = 'B'
			THEN 0
		ELSE
			orcmitm.preco_venda
	END as preco_venda,
	orcmitm.quantidade,
    p.unidade_embalagem,
    p.quantidade_embalagem,
    p.unidade_venda,
    p.quant_unid_venda,
    pd.multiplo_venda,
    s.id_subgrupo,
    s.descricao as descricao_subgrupo,
    g.id_grupo,
    g.descricao as descricao_grupo,
    t.id_tipo,
    t.descricao as descricao_tipo,
    orcmitm.id_oferta,
    offers.tipo_oferta,
    orcmitm.tipo,
    pe.qtd_estoque as estoque,
    orcmitm.data_criacao,
    odesc.desconto,
	CASE 
		WHEN 
			orcmitm.id_oferta IS NOT NULL 
			AND offers.tipo_oferta = 3 
		THEN
			(
				CASE
					WHEN 
						orcmitm.desconto_escalonado IN (SELECT desconto FROM OFERTA_ESCALONADO_FAIXA WHERE status = 'A' AND id_oferta = orcmitm.id_oferta)
					THEN
						orcmitm.desconto_escalonado
					ELSE
						0
				END
			)
		ELSE
			NULL			
	END as desconto_escalonado,	
	orcmitm.desconto_tipo,
	p.tokens_imagem as tokens,
	orcmitm.ordem

FROM
	ORCAMENTO orcm

	INNER JOIN ORCAMENTO_ITEM orcmitm ON
		orcm.id_orcamento = orcmitm.id_orcamento
		AND orcm.id_distribuidor = orcmitm.id_distribuidor
		AND orcm.id_cliente = orcmitm.id_cliente

	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		orcmitm.id_produto = pd.id_produto
		AND orcmitm.id_distribuidor = pd.id_distribuidor

	INNER JOIN PRODUTO p ON
		pd.id_produto = p.id_produto

	INNER JOIN PRODUTO_ESTOQUE pe ON
		pd.id_produto = pe.id_produto
		AND pd.id_distribuidor = pe.id_distribuidor

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

			INNER JOIN ORCAMENTO_ITEM orcmitm ON
				tpp.id_produto = orcmitm.id_produto
				AND tpp.id_distribuidor = orcmitm.id_distribuidor

		WHERE
			tp.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
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
														TABELA_PRECO_CLIENTE tpc

													WHERE
														id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
														AND id_cliente = @id_cliente
												)
					)
				)
			AND tp.dt_inicio <= GETDATE()
			AND tp.dt_fim >= GETDATE()
			AND orcmitm.status = 'A'
			AND orcmitm.d_e_l_e_t_ = 0
			AND tp.status = 'A'

		GROUP BY
			tpp.id_produto,
			tpp.id_distribuidor

	) ppr ON
		pd.id_produto = ppr.id_produto
		AND pd.id_distribuidor = ppr.id_distribuidor

	INNER JOIN PRODUTO_SUBGRUPO ps ON
        pe.id_produto = ps.codigo_produto 
		AND pe.id_distribuidor = ps.id_distribuidor

    INNER JOIN SUBGRUPO s ON
        ps.id_subgrupo = s.id_subgrupo
        AND ps.id_distribuidor = s.id_distribuidor
                                                                                
    INNER JOIN GRUPO_SUBGRUPO gs ON
        s.id_subgrupo = gs.id_subgrupo

    INNER JOIN GRUPO g ON
        gs.id_grupo = g.id_grupo

    INNER JOIN TIPO t ON
        g.tipo_pai = t.id_tipo

	INNER JOIN MARCA m ON
		pd.id_marca = m.id_marca

	LEFT JOIN 
	(
		SELECT
			op.id_produto,
			o.id_distribuidor,
			MAX(od.desconto) desconto

		FROM
			(

				SELECT
					o.id_oferta

				FROM
					OFERTA o

				WHERE
					o.id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A')

				UNION

				SELECT
					o.id_oferta

				FROM
					OFERTA o

				WHERE
					o.id_oferta IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND id_cliente = @id_cliente)

			) offers

			INNER JOIN OFERTA o ON
				offers.id_oferta = o.id_oferta

			INNER JOIN OFERTA_PRODUTO op ON
				o.id_oferta = op.id_oferta

			INNER JOIN ORCAMENTO_ITEM orcmitm ON
				op.id_produto = orcmitm.id_produto

			INNER JOIN ORCAMENTO orcm ON
				orcmitm.id_orcamento = orcm.id_orcamento

			INNER JOIN OFERTA_DESCONTO od ON
				o.id_oferta = od.id_oferta

		WHERE
			o.tipo_oferta = 1
			AND orcm.id_usuario = @id_usuario
			AND orcm.id_cliente = @id_cliente
			AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
			AND orcm.status = 'A'
			AND orcmitm.status = 'A'
			AND orcm.d_e_l_e_t_ = 0
			AND orcmitm.d_e_l_e_t_ = 0
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
                            oc.id_cliente = @id_cliente
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
                    AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
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
                                AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
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
                                AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
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
                        AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                        AND oef.desconto > 0
                        AND oef.status = 'A'
                        AND o.status = 'A'
                        AND op.status = 'A'
                )

            ) offers ON
                offers_produto.id_oferta = offers.id_oferta

	) offers ON
		p.id_produto = offers.id_produto
		AND orcmitm.id_oferta = offers.id_oferta

WHERE
	orcm.id_usuario = @id_usuario
	AND orcm.id_cliente = @id_cliente
	AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
	AND ps.id_distribuidor != 0
	AND pe.qtd_estoque > 0
	AND orcm.status = 'A'
	AND orcmitm.status = 'A'
	AND orcm.d_e_l_e_t_ = 0
	AND orcmitm.d_e_l_e_t_ = 0
	AND p.status = 'A'
	AND pd.status = 'A'
	AND ps.status = 'A'
	AND s.status = 'A'
	AND gs.status = 'A'
	AND g.status = 'A'
	AND t.status = 'A'

ORDER BY
	orcmitm.data_criacao,
	orcmitm.ordem