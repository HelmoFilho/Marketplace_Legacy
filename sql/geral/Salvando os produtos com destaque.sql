USE [B2BTESTE2]

DELETE FROM PRODUTO_DESTAQUES;

INSERT INTO
	PRODUTO_DESTAQUES
		(
			id_grupo,
			id_distribuidor,
			ordem,
			id_produto
		)

	SELECT
		id_grupo,
		id_distribuidor,
		ordem,
		id_produto

	FROM
		(
	
			SELECT
				DISTINCT
					g.id_grupo,
					pd.id_produto,
					RANK() OVER(PARTITION BY g.id_grupo ORDER BY CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END) ordem,
					g.id_distribuidor

			FROM
				PRODUTO p
                                    
				INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
					p.id_produto = pd.id_produto

				INNER JOIN TABELA_PRECO_PRODUTO tpp ON
					pd.id_produto = tpp.id_produto
					AND pd.id_distribuidor = tpp.id_distribuidor

				INNER JOIN TABELA_PRECO tp ON
					tpp.id_tabela_preco = tp.id_tabela_preco
					AND tpp.id_distribuidor = tp.id_distribuidor

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

			WHERE
				pd.estoque > 0
				AND tp.tabela_padrao = 'S'
				AND	tp.dt_inicio <= GETDATE()
				AND	tp.dt_fim >= GETDATE()
				AND p.status = 'A'
				AND pd.status = 'A'
				AND tp.status = 'A'
				AND m.status = 'A'
				AND f.status = 'A'
				AND t.status = 'A'
				AND g.status = 'A'
				AND gs.status = 'A'
				AND s.status = 'A'
				AND ps.status = 'A'
	
		) pr

	WHERE
		ordem <= 6

	ORDER BY
		id_grupo,
		ordem