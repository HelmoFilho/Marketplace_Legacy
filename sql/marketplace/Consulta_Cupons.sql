USE [B2BTESTE2]

DECLARE @id_cliente INT = 2;
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

-- Validando os cupons com relação aos itens
SELECT
	cp.id_cupom,
	cp.id_distribuidor,
	cp.codigo_cupom,
	cp.titulo,
	cp.descricao,
	cp.data_inicio,
	cp.data_final,
	cp.tipo_cupom,
	tpcp.descricao as descricao_tipo_cupom,
	cp.desconto_valor,
	cp.desconto_percentual,
	cp.valor_limite,
	cp.valor_ativar,
	cp.termos_uso,
	cp.status,
	cp.qt_reutiliza as reutiliza,
	0 oculto,
    CASE WHEN cp.bloqueado = 'N' THEN 0 ELSE 1 END bloqueado,
	cp.tipo_itens,
	tpcpit.descricao as descricao_tipo_itens,
	citns.id_objeto as itens,
	CUPONS.count__

FROM
	(
	
		SELECT
			CUPONS.id_cupom,
			CUPONS.id_distribuidor,
			CUPONS.id_tipo_cupom,
			CUPONS.id_tipo_itens,
			CUPONS_VALIDOS.data_cadastro,
			COUNT(*) OVER() count__

		FROM
			(
				-- Para todos
				SELECT
					DISTINCT cp.id_cupom,
					cp.id_distribuidor,
					cp.tipo_cupom as id_tipo_cupom,
					cp.tipo_itens as id_tipo_itens

				FROM
					CUPOM cp

				WHERE
					cp.tipo_itens = 1

				UNION

				-- Para o produto
				SELECT
					DISTINCT cp.id_cupom,
					cp.id_distribuidor,
					cp.tipo_cupom as id_tipo_cupom,
					cp.tipo_itens as id_tipo_itens

				FROM
					CUPOM cp

					INNER JOIN CUPOM_ITENS citns ON
						cp.id_cupom = citns.id_cupom

					INNER JOIN 
					(
						SELECT
							pd.id_produto,
							0 as id_distribuidor

						FROM
							PRODUTO_DISTRIBUIDOR pd

						WHERE
							status = 'A'
							AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

						UNION

						SELECT
							pd.id_produto,
							pd.id_distribuidor

						FROM
							PRODUTO_DISTRIBUIDOR pd
						
						WHERE
							status = 'A'
							AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

					) pds ON
						cp.id_distribuidor = pds.id_distribuidor
						AND citns.id_objeto = pds.id_produto

					INNER JOIN PRODUTO p ON
						pds.id_produto = p.id_produto

				WHERE
					cp.tipo_itens = 2
					AND citns.status = 'A'
					AND p.status = 'A'

				UNION

				-- Para marcas
				SELECT
					DISTINCT cp.id_cupom,
					cp.id_distribuidor,
					cp.tipo_cupom as id_tipo_cupom,
					cp.tipo_itens as id_tipo_itens

				FROM
					CUPOM cp

					INNER JOIN CUPOM_ITENS citns ON
						cp.id_cupom = citns.id_cupom

					INNER JOIN
					(
						SELECT
							CONVERT(VARCHAR(10), m.id_marca) as id_marca,
							0 as id_distribuidor

						FROM
							MARCA m

						WHERE
							status = 'A'
							AND m.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

						UNION

						SELECT
							CONVERT(VARCHAR(10), m.id_marca) as id_marca,
							m.id_distribuidor

						FROM
							MARCA m

						WHERE
							status = 'A'
							AND m.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

					) m ON
						citns.id_objeto = m.id_marca
						AND cp.id_distribuidor = m.id_distribuidor
			
					INNER JOIN PRODUTO_DISTRIBUIDOR pd ON 
						m.id_marca = pd.id_marca 
				
					INNER JOIN PRODUTO p ON 
						pd.id_produto = p.id_produto  

				WHERE
					cp.tipo_itens = 3
					AND p.status = 'A' 
					AND pd.status = 'A'
					AND citns.status = 'A'

				UNION

				-- Para grupo
				SELECT
					DISTINCT cp.id_cupom,
					cp.id_distribuidor,
					cp.tipo_cupom as id_tipo_cupom,
					cp.tipo_itens as id_tipo_itens

				FROM
					CUPOM cp

					INNER JOIN CUPOM_ITENS citns ON
		 				cp.id_cupom = citns.id_cupom

					INNER JOIN GRUPO g ON
		 				citns.id_objeto = CONVERT(VARCHAR(10), g.id_grupo)
						AND cp.id_distribuidor = g.id_distribuidor

					INNER JOIN TIPO t ON
		 				g.tipo_pai = t.id_tipo

					INNER JOIN GRUPO_SUBGRUPO gs ON
		 				g.id_grupo = gs.id_grupo

					INNER JOIN SUBGRUPO s ON
		 				gs.id_subgrupo = s.id_subgrupo

					INNER JOIN PRODUTO_SUBGRUPO ps ON
		 				s.id_subgrupo = ps.id_subgrupo
		 				AND s.id_distribuidor = ps.id_distribuidor

					INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		 				ps.codigo_produto = pd.id_produto
		 				AND ps.id_distribuidor = pd.id_distribuidor

					INNER JOIN PRODUTO p ON
		 				pd.id_produto = p.id_produto

				WHERE
					cp.tipo_itens = 4
					AND citns.status = 'A'
					AND p.status = 'A'
					AND pd.status = 'A'
					AND t.status = 'A'
					AND g.status = 'A'
					AND gs.status = 'A'
					AND s.status = 'A'
					AND ps.status = 'A'

				UNION

				-- Para subgrupo
				SELECT
					DISTINCT cp.id_cupom,
					cp.id_distribuidor,
					cp.tipo_cupom as id_tipo_cupom,
					cp.tipo_itens as id_tipo_itens

				FROM
					CUPOM cp

					INNER JOIN CUPOM_ITENS citns ON
		 				cp.id_cupom = citns.id_cupom

					INNER JOIN SUBGRUPO s ON
		 				citns.id_objeto = CONVERT(VARCHAR(10), s.id_subgrupo)
						AND cp.id_distribuidor = s.id_distribuidor

					INNER JOIN GRUPO_SUBGRUPO gs ON
		 				s.id_subgrupo = gs.id_subgrupo
						AND s.id_distribuidor = gs.id_distribuidor

					INNER JOIN GRUPO g ON
		 				gs.id_grupo = g.id_grupo

					INNER JOIN TIPO t ON
		 				g.tipo_pai = t.id_tipo

					INNER JOIN PRODUTO_SUBGRUPO ps ON
		 				s.id_subgrupo = ps.id_subgrupo
		 				AND s.id_distribuidor = ps.id_distribuidor

					INNER JOIN PRODUTO p ON
		 				ps.codigo_produto = p.id_produto

					INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		 				p.id_produto = pd.id_produto
		 				AND cp.id_distribuidor = pd.id_distribuidor

				WHERE
					cp.tipo_itens = 5
					AND citns.status = 'A'
					AND p.status = 'A'
					AND pd.status = 'A'
					AND t.status = 'A'
					AND g.status = 'A'
					AND gs.status = 'A'
					AND s.status = 'A'
					AND ps.status = 'A'

			) CUPONS

			INNER JOIN 
			(
				
				SELECT
					cp.id_cupom,
					cp.id_distribuidor,
					tpcp.id_tipo_cupom,
					tpcpit.id_tipo_itens,
					CUPONS.data_cadastro
	
				FROM
					(

                        (
                            SELECT
                                cp.id_cupom,
                                cp.id_distribuidor,
                                cp.data_cadastro

                            FROM
                                CUPOM cp

                            WHERE
                                1 = 1
                                AND oculto = 'N'
                                AND lista_cliente IS NULL
                                AND id_cupom NOT IN (SELECT id_cupom FROM CUPOM_CLIENTE)

                            UNION

                            SELECT
                                cp.id_cupom,
                                cp.id_distribuidor,
                                cpco.data_habilitado as data_cadastro

                            FROM
                                CUPOM cp

                                INNER JOIN CUPOM_CLIENTE_OCULTO cpco ON
                                    cp.id_cupom = cpco.id_cupom

                            WHERE
                                cpco.id_cliente = @id_cliente
                                AND cp.oculto != 'N'
                                AND cp.lista_cliente IS NULL
                                AND cpco.status = 'A'
                        )

                        UNION

                        (
                            SELECT
                                cp.id_cupom,
                                cp.id_distribuidor,
                                cp.data_cadastro

                            FROM
                                CUPOM cp

                                INNER JOIN CUPOM_CLIENTE cpc ON
                                    cp.id_cupom = cpc.id_cupom
                                    AND cp.lista_cliente = cpc.id_lista_cliente

                                INNER JOIN LISTA_SEGMENTACAO_USUARIO lsu ON
                                    cpc.id_lista_cliente = lsu.id_lista
                                    AND cpc.id_cliente = lsu.id_cliente

                                INNER JOIN LISTA_SEGMENTACAO ls ON
                                    lsu.id_lista = ls.id_lista

                            WHERE
                                cp.lista_cliente IS NOT NULL
                                AND cp.oculto = 'N'
                                AND lsu.id_cliente = @id_cliente
                                AND ls.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                                AND ls.d_e_l_e_t_ = 0
                                AND ls.status = 'A'          
                                AND cpc.status = 'A'   

                            UNION

                            SELECT
                                cp.id_cupom,
                                cp.id_distribuidor,
                                cpco.data_habilitado as data_cadastro

                            FROM
                                CUPOM cp

                                INNER JOIN CUPOM_CLIENTE cpc ON
                                    cp.id_cupom = cpc.id_cupom
                                    AND cp.lista_cliente = cpc.id_lista_cliente

                                INNER JOIN LISTA_SEGMENTACAO_USUARIO lsu ON
                                    cpc.id_lista_cliente = lsu.id_lista
                                    AND cpc.id_cliente = lsu.id_cliente

                                INNER JOIN LISTA_SEGMENTACAO ls ON
                                    lsu.id_lista = ls.id_lista

                                INNER JOIN CUPOM_CLIENTE_OCULTO cpco ON
                                    cpc.id_cupom = cpco.id_cupom
                                    AND cpc.id_cliente = cpco.id_cliente

                            WHERE
                                cp.lista_cliente IS NOT NULL
                                AND cp.oculto != 'N'
                                AND lsu.id_cliente = @id_cliente
                                AND ls.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                                AND ls.d_e_l_e_t_ = 0
                                AND cpc.status = 'A'
                                AND ls.status = 'A'     
                                AND cpco.status = 'A'

                        )

                    ) CUPONS

					INNER JOIN CUPOM cp ON
						CUPONS.id_cupom = cp.id_cupom
						AND CUPONS.id_distribuidor = cp.id_distribuidor

					INNER JOIN TIPO_CUPOM tpcp ON
						cp.tipo_cupom = tpcp.id_tipo_cupom

					INNER JOIN TIPO_CUPOM_ITENS tpcpit ON
						cp.tipo_itens = tpcpit.id_tipo_itens

				WHERE
					1 = 1
					AND (
							(
								cp.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
							)
								OR
							(
								cp.id_distribuidor = 0
								AND @id_distribuidor = 0
							)
						)
					AND cp.status = 'A'
					AND cp.bloqueado = 'N'
					AND tpcp.status = 'A'
					AND tpcpit.status = 'A'

			) CUPONS_VALIDOS ON
				CUPONS.id_cupom = CUPONS_VALIDOS.id_cupom
				AND CUPONS.id_distribuidor = CUPONS_VALIDOS.id_distribuidor
				AND CUPONS.id_tipo_cupom = CUPONS_VALIDOS.id_tipo_cupom
				AND CUPONS.id_tipo_itens = CUPONS_VALIDOS.id_tipo_itens

			INNER JOIN CUPOM cp ON
				CUPONS_VALIDOS.id_cupom = cp.id_cupom
				AND CUPONS_VALIDOS.id_distribuidor = cp.id_distribuidor
				AND CUPONS_VALIDOS.id_tipo_cupom = cp.tipo_cupom
				AND CUPONS_VALIDOS.id_tipo_itens = cp.tipo_itens

		ORDER BY
			cp.titulo,
			cp.descricao

		OFFSET
			@offset ROWS

		FETCH
			NEXT @limite ROWS ONLY
			
	) CUPONS

	INNER JOIN CUPOM cp ON
		CUPONS.id_cupom = cp.id_cupom
		AND CUPONS.id_distribuidor = cp.id_distribuidor
		AND CUPONS.id_tipo_cupom = cp.tipo_cupom
		AND CUPONS.id_tipo_itens = cp.tipo_itens

	INNER JOIN TIPO_CUPOM tpcp ON
		cp.tipo_cupom = tpcp.id_tipo_cupom

	INNER JOIN TIPO_CUPOM_ITENS tpcpit ON
		cp.tipo_itens = tpcpit.id_tipo_itens

	INNER JOIN
	(
		SELECT
			DISTINCT cp.id_cupom,
			cp.id_distribuidor,
			NULL as id_objeto

		FROM
			CUPOM cp

		WHERE
			cp.tipo_itens = 1

		UNION

		-- Para o produto
		SELECT
			DISTINCT cp.id_cupom,
			cp.id_distribuidor,
			citns.id_objeto

		FROM
			CUPOM cp

			INNER JOIN CUPOM_ITENS citns ON
				cp.id_cupom = citns.id_cupom

			INNER JOIN 
			(
				SELECT
					pd.id_produto,
					0 as id_distribuidor

				FROM
					PRODUTO_DISTRIBUIDOR pd

				WHERE
					status = 'A'
					AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

				UNION

				SELECT
					pd.id_produto,
					pd.id_distribuidor

				FROM
					PRODUTO_DISTRIBUIDOR pd
						
				WHERE
					status = 'A'
					AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

			) pds ON
				cp.id_distribuidor = pds.id_distribuidor
				AND citns.id_objeto = pds.id_produto

			INNER JOIN PRODUTO p ON
				pds.id_produto = p.id_produto

		WHERE
			cp.tipo_itens = 2
			AND citns.status = 'A'
			AND p.status = 'A'

		UNION

		-- Para marcas
		SELECT
			DISTINCT cp.id_cupom,
			cp.id_distribuidor,
			citns.id_objeto

		FROM
			CUPOM cp

			INNER JOIN CUPOM_ITENS citns ON
				cp.id_cupom = citns.id_cupom

			INNER JOIN
			(
				SELECT
					CONVERT(VARCHAR(10), m.id_marca) as id_marca,
					0 as id_distribuidor

				FROM
					MARCA m

				WHERE
					status = 'A'
					AND m.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

				UNION

				SELECT
					CONVERT(VARCHAR(10), m.id_marca) as id_marca,
					m.id_distribuidor

				FROM
					MARCA m

				WHERE
					status = 'A'
					AND m.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)

			) m ON
				citns.id_objeto = m.id_marca
				AND cp.id_distribuidor = m.id_distribuidor
			
			INNER JOIN PRODUTO_DISTRIBUIDOR pd ON 
				m.id_marca = pd.id_marca 
				
			INNER JOIN PRODUTO p ON 
				pd.id_produto = p.id_produto  

		WHERE
			cp.tipo_itens = 3
			AND p.status = 'A' 
			AND pd.status = 'A'
			AND citns.status = 'A'

		UNION

		-- Para grupo
		SELECT
			DISTINCT cp.id_cupom,
			cp.id_distribuidor,
			citns.id_objeto

		FROM
			CUPOM cp

			INNER JOIN CUPOM_ITENS citns ON
		 		cp.id_cupom = citns.id_cupom

			INNER JOIN GRUPO g ON
		 		citns.id_objeto = CONVERT(VARCHAR(10), g.id_grupo)
				AND cp.id_distribuidor = g.id_distribuidor

			INNER JOIN TIPO t ON
		 		g.tipo_pai = t.id_tipo

			INNER JOIN GRUPO_SUBGRUPO gs ON
		 		g.id_grupo = gs.id_grupo

			INNER JOIN SUBGRUPO s ON
		 		gs.id_subgrupo = s.id_subgrupo

			INNER JOIN PRODUTO_SUBGRUPO ps ON
		 		s.id_subgrupo = ps.id_subgrupo
		 		AND s.id_distribuidor = ps.id_distribuidor

			INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		 		ps.codigo_produto = pd.id_produto
		 		AND ps.id_distribuidor = pd.id_distribuidor

			INNER JOIN PRODUTO p ON
		 		pd.id_produto = p.id_produto

		WHERE
			cp.tipo_itens = 4
			AND citns.status = 'A'
			AND p.status = 'A'
			AND pd.status = 'A'
			AND t.status = 'A'
			AND g.status = 'A'
			AND gs.status = 'A'
			AND s.status = 'A'
			AND ps.status = 'A'

		UNION

		-- Para subgrupo
		SELECT
			DISTINCT cp.id_cupom,
			cp.id_distribuidor,
			citns.id_objeto

		FROM
			CUPOM cp

			INNER JOIN CUPOM_ITENS citns ON
		 		cp.id_cupom = citns.id_cupom

			INNER JOIN SUBGRUPO s ON
		 		citns.id_objeto = CONVERT(VARCHAR(10), s.id_subgrupo)
				AND cp.id_distribuidor = s.id_distribuidor

			INNER JOIN GRUPO_SUBGRUPO gs ON
		 		s.id_subgrupo = gs.id_subgrupo
				AND s.id_distribuidor = gs.id_distribuidor

			INNER JOIN GRUPO g ON
		 		gs.id_grupo = g.id_grupo

			INNER JOIN TIPO t ON
		 		g.tipo_pai = t.id_tipo

			INNER JOIN PRODUTO_SUBGRUPO ps ON
		 		s.id_subgrupo = ps.id_subgrupo
		 		AND s.id_distribuidor = ps.id_distribuidor

			INNER JOIN PRODUTO p ON
		 		ps.codigo_produto = p.id_produto

			INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		 		p.id_produto = pd.id_produto
		 		AND cp.id_distribuidor = pd.id_distribuidor

		WHERE
			cp.tipo_itens = 5
			AND citns.status = 'A'
			AND p.status = 'A'
			AND pd.status = 'A'
			AND t.status = 'A'
			AND g.status = 'A'
			AND gs.status = 'A'
			AND s.status = 'A'
			AND ps.status = 'A'
	) citns ON
		cp.id_cupom = citns.id_cupom
		AND cp.id_distribuidor = citns.id_distribuidor

WHERE
	1 = 1
	
ORDER BY
	CASE WHEN cp.oculto = 'N' THEN 0 ELSE 1 END DESC,
	CUPONS.data_cadastro DESC,
	cp.titulo,
	cp.descricao,
	cp.id_cupom;