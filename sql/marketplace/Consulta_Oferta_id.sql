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

SET NOCOUNT ON;

SELECT
	o.id_oferta,
	COUNT(*) OVER() count__

FROM
	(
	
		SELECT 
			DISTINCT offers_produto.id_oferta

		FROM 
			PRODUTO p

			INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
				p.id_produto = pd.id_produto

			INNER JOIN 
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
							AND o.limite_ativacao_oferta > 0
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

										INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
											op.id_produto = pd.id_produto
											AND o.id_distribuidor = pd.id_distribuidor

										INNER JOIN PRODUTO p ON
											pd.id_produto = p.id_produto

									WHERE
										o.tipo_oferta = 2
										AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
										AND p.status = 'A'
										AND pd.status = 'A'
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

										INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
											ob.id_produto = pd.id_produto
											AND o.id_distribuidor = pd.id_distribuidor

										INNER JOIN PRODUTO p ON
											pd.id_produto = p.id_produto

									WHERE
										o.tipo_oferta = 2
										AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
										AND o.status = 'A'
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

								INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
									o.id_oferta = oef.id_oferta

								INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
									op.id_produto = pd.id_produto
									AND o.id_distribuidor = pd.id_distribuidor

								INNER JOIN PRODUTO p ON
									pd.id_produto = p.id_produto

							WHERE
								o.tipo_oferta = 3
								AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
								AND oef.desconto > 0
								AND oef.status = 'A'
								AND o.status = 'A'
								AND op.status = 'A'
								AND p.status = 'A'
								AND pd.status = 'A'
						)

					) offers ON
						offers_produto.id_oferta = offers.id_oferta
			
			) as offers_produto ON
				pd.id_produto = offers_produto.id_produto
						
			INNER JOIN DISTRIBUIDOR d ON
				pd.id_distribuidor = d.id_distribuidor

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
					AND o.limite_ativacao_oferta > 0
					AND o.data_inicio <= GETDATE()
					AND o.data_final >= GETDATE()
					AND o.status = 'A'
					AND op.status = 'A'
					AND od.status = 'A'
	
			) odesc ON
				pd.id_produto = odesc.id_produto
				AND pd.id_distribuidor = odesc.id_distribuidor

			LEFT JOIN PRODUTO_ESTOQUE pe ON
				pd.id_produto = pe.id_produto 
				AND pd.id_distribuidor = pe.id_distribuidor

		WHERE
			pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
			AND ps.id_distribuidor = @id_distribuidor
			AND ps.status = 'A'
			AND s.status  = 'A'
			AND gs.status = 'A'
			AND g.status  = 'A'
			AND t.status  = 'A'
			AND d.status = 'A'
			AND m.status  = 'A'
			AND f.status  = 'A'
	
	) offers

	INNER JOIN OFERTA o ON
		offers.id_oferta = o.id_oferta

ORDER BY
	o.id_oferta

OFFSET
	@offset ROWS

FETCH
	NEXT @limite ROWS ONLY