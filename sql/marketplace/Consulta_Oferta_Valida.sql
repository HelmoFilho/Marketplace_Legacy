USE [B2BTESTE2]

DECLARE @id_distribuidor INT = 0;
DECLARE @id_cliente INT = 1;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

DECLARE @distribuidores TABLE (id_distribuidor INT);

IF @id_distribuidor = 0 OR @id_distribuidor = 1
INSERT INTO @distribuidores VALUES (1);

IF @id_distribuidor = 0 OR @id_distribuidor = 2
INSERT INTO @distribuidores VALUES (2);

DECLARE @ofertas TABLE (id_oferta INT);

INSERT INTO
	@ofertas

	VALUES
		(13),
		(18),
		(19),
		(21),
		(28),
		(35),
		(39),
		(41),
		(43),
		(44),
		(51),
		(52),
		(55),
		(56),
		(64),
		(77),
		(78),
		(79),
		(82),
		(91);

SET NOCOUNT ON;

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
			AND tp.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
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
														AND id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
														AND id_cliente = @id_cliente
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
					oc.id_cliente = @id_cliente
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
			AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
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
	o.ordem, o.descricao_oferta,
	pimg.TOKEN,
	p.descricao_completa,
	oef.sequencia,
	oef.faixa