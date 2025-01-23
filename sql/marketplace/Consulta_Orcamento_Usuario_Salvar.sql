USE [B2BTESTE2]

DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;
DECLARE @produtos VARCHAR(MAX) = '[{"id_produto": "6042-1", "quantidade": 80, "id_distribuidor": "2", "preco_aplicado": 9.38, "desconto": 10, "id_campanha": null, "id_escalonado": 395, "bonificado": null}, {"id_produto": "6067-1", "quantidade": 48, "id_distribuidor": "2", "preco_aplicado": 5.78, "desconto": 0, "id_campanha": null, "id_escalonado": 319, "bonificado": null},{"id_produto": "4289-11", "quantidade": 80, "id_distribuidor": "1", "preco_aplicado": 7.73, "desconto": 5.4, "id_campanha": null, "id_escalonado": null, "bonificado": null}]'
DECLARE @data_atual DATETIME = GETDATE();

DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT
);

DECLARE @ofertas TABLE (
	id_oferta INT
);

INSERT INTO @ofertas VALUES (395), (319);

INSERT INTO
	@id_distribuidor_hold

	SELECT
		d.id_distribuidor

	FROM
		DISTRIBUIDOR d

		INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
			d.id_distribuidor = cd.id_distribuidor

		INNER JOIN CLIENTE c ON
			cd.id_cliente = c.id_cliente

	WHERE
		1 = 1
        AND	c.id_cliente = @id_cliente
        AND d.id_distribuidor != 0
        AND c.status = 'A'
        AND c.status_receita = 'A'
        AND cd.status = 'A'
        AND cd.d_e_l_e_t_ = 0
        AND d.status = 'A';



SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_JSON','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_JSON
END;

IF Object_ID('tempDB..#HOLD_PRODUTO_ORCAMENTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_PRODUTO_ORCAMENTO
END;

IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_OFERTA
END;

-- Criando as tabelas temporarias
CREATE TABLE #HOLD_JSON
(
	id INT IDENTITY(1,1),
	ordem INT,
	id_produto VARCHAR(100),
	id_distribuidor INT,
	quantidade INT,
	id_escalonado INT,
	id_campanha INT,
	bonificado BIT,
	preco_aplicado DECIMAL(18,2),
	desconto DECIMAL(18,2)
);

CREATE TABLE #HOLD_PRODUTO_ORCAMENTO
(
	ordem INT,
	id_distribuidor INT,
	id_orcamento INT,
	id_cliente INT,
	id_oferta INT,
	id_produto VARCHAR(100),
	status CHAR(1),
	quantidade INT,
	tipo CHAR(1),
	preco_venda DECIMAL(18,2),
	preco_tabela DECIMAL(18,2),
	preco_desconto DECIMAL(18,2),
	desconto DECIMAL(18,2),
	desconto_escalonado DECIMAL(18,2),
	desconto_tipo INT,
	data_criacao DATETIME,
	d_e_l_e_t_ INT,
	id_erro INT
);

-- Declarações de variáveis globais

--## Variáveis de apoio
DECLARE @count_produto INT;
DECLARE @max_produto INT;

DECLARE @ordem INT;
DECLARE @id_produto VARCHAR(100);
DECLARE @id_distribuidor INT;
DECLARE @quantidade INT;
DECLARE @id_campanha INT;
DECLARE @id_escalonado INT;
DECLARE @bonificado BIT;
DECLARE @preco_aplicado DECIMAL(18,2);
DECLARE @desconto_interno DECIMAL(8,2);

DECLARE @id_orcamento INT;

DECLARE @preco_tabela DECIMAL(18,2);
DECLARE @preco_desconto DECIMAL(18,2);
DECLARE @desconto_tipo_1 DECIMAL(8,2);

DECLARE @tipo VARCHAR(1) = 'V';
DECLARE @id_oferta INT;
DECLARE @tipo_oferta INT;
DECLARE @desconto_escalonado DECIMAL(8,2);

DECLARE @desconto_tipo INT;
DECLARE @desconto_aplicado DECIMAL(18,2);

--## Variaveis de erro
DECLARE @id_erro INT = 0;

--## Variaveis tabelas
DECLARE @id_orcamento_hold TABLE (
	id_orcamento INT,
	id_distribuidor INT,
	data_cricao DATETIME
);

-- Pegando os dados do JSON
INSERT INTO
	#HOLD_JSON
		(
			ordem,
			id_produto,
			id_distribuidor,
			quantidade,
			id_escalonado,
			id_campanha,
			bonificado,
			preco_aplicado,
			desconto
		)

	SELECT
		ROW_NUMBER() OVER(PARTITION BY nda.id_distribuidor ORDER BY nda.ordem_nat) ordem,
		nda.id_produto,
		nda.id_distribuidor,
		nda.quantidade,
		nda.id_escalonado,
		nda.id_campanha,
		nda.bonificado,
		nda.preco_aplicado,
		nda.desconto

	FROM
		(
			
			SELECT
				ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as ordem_nat,
				id_produto,
				id_distribuidor,
				quantidade,
				id_escalonado,
				id_campanha,
				bonificado,
				preco_aplicado,
				desconto

			FROM
				OPENJSON(@produtos)

			WITH
				(
					id_produto VARCHAR(100),
					id_distribuidor INT,
					quantidade INT,
					id_escalonado INT,
					id_campanha INT,
					bonificado BIT,
					preco_aplicado DECIMAL(18,2),
					desconto DECIMAL(18,2)
				)

		) nda

		INNER JOIN PRODUTO p ON
			nda.id_produto = p.id_produto

		INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
			nda.id_produto = pd.id_produto
			AND nda.id_distribuidor = pd.id_distribuidor

		INNER JOIN
		(
			SELECT
				p.id_produto,
				MIN(tpp.preco_tabela) preco_tabela

			FROM
				PRODUTO p

				INNER JOIN TABELA_PRECO_PRODUTO tpp ON
					p.id_produto = tpp.id_produto

				INNER JOIN TABELA_PRECO tp ON
					tpp.id_tabela_preco = tp.id_tabela_preco

			WHERE
				tp.status = 'A'

			GROUP BY
				p.id_produto

		) tpp ON
			pd.id_produto = tpp.id_produto

		INNER JOIN PRODUTO_ESTOQUE pe ON
			pd.id_produto = pe.id_produto
			AND pd.id_distribuidor = pe.id_distribuidor

	WHERE
		1 = 1
		AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
		AND pe.qtd_estoque > 0
		AND p.status = 'A'
		AND pd.status = 'A'
		AND LEN(p.sku) > 0
		
	ORDER BY
		ordem_nat;

-- Salvando ofertas validas
SELECT
	DISTINCT offers.id_oferta,
	offers.id_distribuidor,
	offers.tipo_oferta

INTO
	#HOLD_OFERTA

FROM
	(
		SELECT
			o.id_oferta,
			o.id_distribuidor,
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

			INNER JOIN OFERTA o ON
				offers.id_oferta = o.id_oferta

		WHERE
			o.tipo_oferta IN (2,3)
			AND o.id_oferta IN (SELECT id_oferta FROM @ofertas)
			AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
			AND o.status = 'A'
			AND o.data_inicio <= @data_atual
			AND o.data_final >= @data_atual
			
	) offers

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
                                                                
						INNER JOIN PRODUTO p ON
							op.id_produto = p.id_produto

						INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
							p.id_produto = pd.id_produto
							AND o.id_distribuidor = pd.id_distribuidor
                                                                
					WHERE
						o.tipo_oferta = 2
						AND o.id_oferta IN (SELECT id_oferta FROM @ofertas)
						AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
						AND o.status = 'A'
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
									AND o.id_oferta IN (SELECT id_oferta FROM @ofertas)
									AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
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

				INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
					o.id_oferta = oef.id_oferta
                                                            
				INNER JOIN OFERTA_PRODUTO op ON
					o.id_oferta = op.id_oferta
                                                                
				INNER JOIN PRODUTO p ON
					op.id_produto = p.id_produto

				INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
					p.id_produto = pd.id_produto
					AND o.id_distribuidor = pd.id_distribuidor
                                                                
			WHERE
				o.tipo_oferta = 3
				AND o.id_oferta IN (SELECT id_oferta FROM @ofertas)
				AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
				AND o.status = 'A'
				AND oef.desconto > 0
				AND oef.status = 'A'
				AND op.status = 'A'
				AND p.status = 'A'
				AND pd.status = 'A'
		)

	) offers_produto ON
		offers.id_oferta = offers_produto.id_oferta;

-- Pegando os orcamentos ativos
INSERT INTO
	@id_orcamento_hold

	SELECT
		id_orcamento,
		id_distribuidor,
		data_criacao

	FROM
		ORCAMENTO

	WHERE
		id_usuario = @id_usuario
		AND id_cliente = @id_cliente
		AND d_e_l_e_t_ = 0
		AND status = 'A'
		AND id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

-- Realizando o loop nos produtos
SELECT
	@max_produto = MAX(id),
	@count_produto = MIN(id)

FROM
	#HOLD_JSON;

WHILE @count_produto <= @max_produto
BEGIN

	-- Pegando as informações
	SET @id_erro = 0;

	SELECT
		TOP 1
			@ordem = ordem,
			@id_produto = id_produto,
			@id_distribuidor = id_distribuidor,
			@quantidade = quantidade,
			@id_campanha = id_campanha,
			@id_escalonado = id_escalonado,
			@bonificado = CASE WHEN ISNULL(bonificado, 0) = 0 THEN 0 ELSE 1 END,
			@preco_aplicado = preco_aplicado,
			@desconto_interno = desconto

	FROM
		#HOLD_JSON

	WHERE
		id = @count_produto;

	-- Verificando se o distribuidor existe
	IF NOT EXISTS (SELECT 1 FROM @id_distribuidor_hold	WHERE id_distribuidor = @id_distribuidor)
		SET @id_erro = 1;

	-- Verificando se existe um orcamento para aquele distribuidor
	SET @id_orcamento = (SELECT TOP 1 id_orcamento FROM @id_orcamento_hold WHERE id_distribuidor = @id_distribuidor ORDER BY data_cricao);

	-- Verificando o estoque atual do produto
	IF @id_erro = 0
	BEGIN

		IF ISNULL((SELECT TOP 1 pe.qtd_estoque 
					FROM PRODUTO p 
						INNER JOIN PRODUTO_ESTOQUE pe ON p.id_produto = pe.id_produto 
					WHERE p.id_produto = @id_produto), 0) <= 0

			SET @id_erro = 2;

	END

	-- Mostrando o preço de tabela do produto
	SET @preco_tabela = NULL;
	SET @preco_desconto = NULL;

	SET @preco_tabela = (

							SELECT
								TOP 1 MIN(tpp.preco_tabela) preco_tabela

							FROM
								TABELA_PRECO tp

								INNER JOIN TABELA_PRECO_PRODUTO tpp ON
									tp.id_tabela_preco = tpp.id_tabela_preco
									AND tp.id_distribuidor = tpp.id_distribuidor

							WHERE
								1 = 1
								AND tpp.id_produto = @id_produto
								AND tp.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
								AND tp.status = 'A'
								AND tp.dt_inicio <= @data_atual
								AND tp.dt_fim >= @data_atual
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
																			AND id_distribuidor = @id_distribuidor
																			AND id_cliente = @id_cliente
																	)
										)
									)

							GROUP BY
								tpp.id_produto,
								tpp.id_distribuidor
						);

	IF @preco_tabela IS NULL
		SET @id_erro = 3;

	-- Verificando se o produto possui algum desconto
	SET @desconto_tipo_1 = NULL;

	IF @id_erro = 0
	BEGIN

		SET @desconto_tipo_1 = (
									SELECT
										MAX(od.desconto)

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
										AND op.id_produto = @id_produto
										AND o.id_distribuidor = @id_distribuidor
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
							   );

		IF ISNULL(@desconto_tipo_1, 0) <= 0	  SET @desconto_tipo_1 = 0;
		ELSE IF @desconto_tipo_1 > 100        SET @desconto_tipo_1 = 100;

		-- Alterando o preco desconto
		IF @desconto_tipo_1 > 0
		BEGIN
			SET @preco_desconto = @preco_tabela * (1 - (@desconto_tipo_1/100));
		END

		ELSE
			SET @preco_desconto = 0;

	END

	-- Verificando qual o tipo oferta
	SET @tipo = NULL;
	SET @desconto_escalonado = NULL;

	SET @id_oferta = CASE
						 WHEN ISNULL(@id_campanha, 0) > 0 THEN @id_campanha
						 WHEN ISNULL(@id_escalonado, 0) > 0 THEN @id_escalonado
						 ELSE NULL
					 END;

	SET @tipo_oferta = CASE
						   WHEN ISNULL(@id_campanha, 0) > 0 THEN 2
						   WHEN ISNULL(@id_escalonado, 0) > 0 THEN 3
						   ELSE NULL
					   END;

	IF @id_erro = 0 AND @id_oferta IS NOT NULL
	BEGIN

		-- Verificando se a oferta e valida
		IF NOT EXISTS (SELECT id_oferta FROM #HOLD_OFERTA WHERE id_oferta = @id_oferta AND tipo_oferta = @tipo_oferta AND id_distribuidor = @id_distribuidor)
			SET @id_erro = 4;

		-- Verificando se o produto tem a oferta informada
		IF @id_erro = 0 AND NOT EXISTS (
											SELECT
												TOP 1 1

											FROM
												OFERTA_PRODUTO

											WHERE
												id_produto = @id_produto
												AND id_oferta = @id_oferta
												AND status = 'A'
									   )
		BEGIN

			IF @tipo_oferta = 2 AND @bonificado = 1
			BEGIN

				IF NOT EXISTS (
									SELECT
										TOP 1 1

									FROM
										OFERTA_BONIFICADO

									WHERE
										id_produto = @id_produto
										AND id_oferta = @id_oferta
										AND status = 'A'
							  )
				BEGIN
					SET @id_erro = 5;
				END
				
			END

			ELSE
			BEGIN
				SET @id_erro = 5;
			END

		END
		
	END

	-- Verificando o tipo da oferta
	SET @tipo = CASE WHEN @id_oferta IS NOT NULL AND @tipo_oferta = 2 AND @bonificado = 1 THEN 'B' ELSE 'V' END;

	-- Salvando o desconto do escalonado
	IF @id_erro = 0 AND @id_oferta IS NOT NULL AND @tipo_oferta = 3
	BEGIN
		IF @desconto_interno IN (SELECT desconto FROM OFERTA_ESCALONADO_FAIXA WHERE id_oferta = @id_oferta AND status = 'A')
			SET @desconto_escalonado = @desconto_interno;

			IF ISNULL(@desconto_escalonado, 0) <= 0	  SET @desconto_escalonado = 0;
			ELSE IF @desconto_escalonado > 100        SET @desconto_escalonado = 100;
	END

	-- Verificando o desconto aplicado
	SET @desconto_aplicado = NULL;
	SET @desconto_tipo = NULL;

	IF @id_erro = 0
	BEGIN

		IF @desconto_tipo_1 > 0 OR @desconto_escalonado > 0
		BEGIN
			
			IF ISNULL(@desconto_tipo_1, 0) = ISNULL(@desconto_escalonado, 0)
			BEGIN 
				SET @desconto_aplicado = @desconto_tipo_1;
				SET @desconto_tipo = 1;
			END

			ELSE IF ISNULL(@desconto_tipo_1, 0) > ISNULL(@desconto_escalonado, 0)
			BEGIN
				SET @desconto_aplicado = @desconto_tipo_1;
				SET @desconto_tipo = 1;
			END

			ELSE
			BEGIN
				SET @desconto_aplicado = @desconto_escalonado;
				SET @desconto_tipo = 2;
			END

		END

	END	

	-- Salvando o resultado
	IF @id_erro = 0
	BEGIN
		
		IF NOT EXISTS (
							SELECT
								TOP 1 1

							FROM
								#HOLD_PRODUTO_ORCAMENTO

							WHERE
								id_distribuidor = @id_distribuidor
								AND id_produto = @id_produto
								AND tipo = @tipo							
								AND id_oferta = ISNULL(@id_oferta, -1) -- Porque pode ser null
					  )

		BEGIN

			INSERT INTO
				#HOLD_PRODUTO_ORCAMENTO
					(
						ordem,
						id_distribuidor,
						id_orcamento,
						id_cliente,
						id_oferta,
						id_produto,
						status,
						quantidade,
						tipo,
						preco_venda,
						preco_tabela,
						preco_desconto,
						desconto_tipo,
						desconto,
						desconto_escalonado,
						data_criacao,
						d_e_l_e_t_
					)

				VALUES
					(
						@ordem,
						@id_distribuidor,
						@id_orcamento,
						@id_cliente,
						@id_oferta,
						@id_produto,
						'A',
						@quantidade,
						@tipo,
						@preco_aplicado,
						@preco_tabela,
						@preco_desconto,
						@desconto_tipo,
						@desconto_tipo_1,
						@desconto_escalonado,
						@data_atual,
						0
					);

		END

	END

	ELSE IF @id_erro > 0
	BEGIN

		INSERT INTO
			#HOLD_PRODUTO_ORCAMENTO
				(
					id_produto,
					id_distribuidor,
					id_erro
				)

			VALUES
				(
					@id_produto,
					@id_distribuidor,
					@id_erro
				);

	END

	-- Incrementando o contador
	SET @count_produto = @count_produto + 1;
		

END

-- Mostrando o resultado
SELECT
	ordem,
	id_orcamento,
	id_cliente,
	id_produto,
	id_distribuidor,
	quantidade,
	tipo,
	preco_tabela,
	preco_venda,
	desconto_tipo,
	CASE WHEN id_oferta = -1 THEN NULL ELSE id_oferta END as id_oferta,
	CASE WHEN id_oferta = -1 THEN NULL ELSE desconto_escalonado END as desconto_escalonado,
	CASE WHEN desconto = 0 THEN NULL ELSE desconto END as desconto,
	CASE WHEN desconto = 0 THEN NULL ELSE preco_desconto END as preco_desconto,
	data_criacao,
	status,
	d_e_l_e_t_,
	id_erro

FROM
	#HOLD_PRODUTO_ORCAMENTO;

-- Deleta tabelas temporarias
DROP TABLE #HOLD_JSON;
DROP TABLE #HOLD_PRODUTO_ORCAMENTO;
DROP TABLE #HOLD_OFERTA;