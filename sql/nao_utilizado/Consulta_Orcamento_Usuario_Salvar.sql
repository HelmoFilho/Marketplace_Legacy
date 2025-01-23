USE [B2BTESTE2]

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

IF Object_ID('tempDB..#HOLD_PRODUTO_OFERTA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_PRODUTO_OFERTA
END;

IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_OFERTA
END;

-- Criando as tabelas temporarias
CREATE TABLE #HOLD_JSON
(
	id INT IDENTITY(1,1),
	id_produto VARCHAR(100),
	id_distribuidor INT,
	quantidade INT,
	id_escalonado INT,
	id_campanha INT,
	bonificado BIT,
	preco_aplicado DECIMAL(18,2)
);

CREATE TABLE #HOLD_PRODUTO_ORCAMENTO
(
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
	data_criacao VARCHAR(30),
	d_e_l_e_t_ INT,
	id_erro INT

);

-- Declarações de variáveis globais
--## Variaveis de entrada
DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;
DECLARE @produtos VARCHAR(MAX) = '[{"id_produto": "1001-1", "quantidade": "1", "id_distribuidor": "1"}, {"id_produto": "3332-1", "quantidade": 1, "id_distribuidor": "1", "id_campanha": 451, "bonificado": 0}, {"id_produto": "7609-1", "quantidade": "1", "id_distribuidor": "2"}]'

--## Variáveis de apoio
DECLARE @date DATETIME = GETDATE();
DECLARE @count_produto INT;
DECLARE @max_produto INT;
DECLARE @id_produto VARCHAR(100);
DECLARE @id_distribuidor INT;
DECLARE @quantidade INT;
DECLARE @id_campanha INT;
DECLARE @id_escalonado INT;
DECLARE @bonificado BIT;
DECLARE @id_orcamento INT;
DECLARE @tabela_preco VARCHAR(20);

DECLARE @quantidade_query INT;
DECLARE @preco_tabela DECIMAL(18,3);
DECLARE @preco_venda DECIMAL(18,3);
DECLARE @preco_desconto DECIMAL(18,3);
DECLARE @preco_aplicado DECIMAL(18,3);
DECLARE @estoque INT;
DECLARE @desconto DECIMAL(8,3);
DECLARE @tipo VARCHAR(1) = 'V';
DECLARE @id_oferta INT;
DECLARE @tipo_oferta INT;

--## Variaveis de erro
DECLARE @id_erro INT = 0;

--## Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT
);

DECLARE @id_orcamento_hold TABLE (
	id_orcamento INT,
	id_distribuidor INT
);

-- Salvando os distribuidores validos do cliente
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
		c.status = 'A'
		AND cd.status = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND d.status = 'A';
	
-- Pegando os produtos
SELECT
	p.id_produto,
	pd.id_distribuidor

INTO
	#HOLD_PRODUTO_OFERTA	

FROM
	PRODUTO p

	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		p.id_produto = pd.id_produto

WHERE
	LEN(p.sku) > 0
	AND p.status = 'A'
	AND pd.status = 'A'
	AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

-- Pegando os dados do JSON
INSERT INTO
	#HOLD_JSON

	SELECT
		id_produto,
		id_distribuidor,
		quantidade,
		id_escalonado,
		id_campanha,
		bonificado,
		preco_aplicado

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
			preco_aplicado DECIMAL(18,2)
		);

-- Salvando ofertas validas
SELECT
	DISTINCT o.id_oferta,
	o.tipo_oferta

INTO
	#HOLD_OFERTA

FROM
	OFERTA o

	INNER JOIN OFERTA_PRODUTO op ON
		o.id_oferta = op.id_oferta

	INNER JOIN #HOLD_PRODUTO_OFERTA thpo ON
		op.id_produto = thpo.id_produto
		AND o.id_distribuidor = thpo.id_distribuidor
		
WHERE
	LEN(op.id_produto) > 0
	AND o.tipo_oferta IN (2,3)
	AND op.status = 'A'
	AND o.status = 'A'
	AND o.data_cadastro <= GETDATE()
	AND o.data_inicio <= GETDATE()
	AND o.data_final >= GETDATE();

-- Verificando se a oferta é válida
IF EXISTS(SELECT 1 FROM #HOLD_OFERTA)
BEGIN

	-- Salvando ofertas bonificadas
	DELETE FROM
		#HOLD_OFERTA

	WHERE
		id_oferta NOT IN (
							SELECT
								tho.id_oferta

							FROM
								
								#HOLD_OFERTA tho

								INNER JOIN OFERTA_BONIFICADO ob ON
									tho.id_oferta = ob.id_oferta
						 )
		AND tipo_oferta = 2;
		

	-- Salvando ofertas escalonada
	DELETE FROM
		#HOLD_OFERTA

	WHERE
		id_oferta NOT IN (
							SELECT
								tho.id_oferta

							FROM
								
								#HOLD_OFERTA tho

								INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
									tho.id_oferta = oef.id_oferta
						 )
		AND tipo_oferta = 3;

END

-- Pegando os orcamentos ativos
INSERT INTO
	@id_orcamento_hold

	SELECT
		id_orcamento,
		id_distribuidor

	FROM
		ORCAMENTO

	WHERE
		status = 'A'
		AND d_e_l_e_t_ = 0
		AND id_usuario = @id_usuario
		AND id_cliente = @id_cliente
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
	SET @date = (SELECT DATEADD(millisecond, 10, @date));

	SELECT
		TOP 1
			@id_produto = id_produto,
			@id_distribuidor = id_distribuidor,
			@quantidade = quantidade,
			@id_campanha = id_campanha,
			@id_escalonado = id_escalonado,
			@bonificado = bonificado,
			@preco_aplicado = preco_aplicado

	FROM
		#HOLD_JSON

	WHERE
		id = @count_produto;

	-- Normalizando o bonificado
	IF @bonificado IS NULL OR @bonificado = 0
	BEGIN
		SET @bonificado = 0;
	END

	ELSE
	BEGIN
		SET @bonificado = 1;
	END

	-- Verificando se o distribuidor existe
	IF NOT EXISTS (
						SELECT
							TOP 1 1

						FROM
							DISTRIBUIDOR 

						WHERE
							id_distribuidor = @id_distribuidor
				  )
	BEGIN

		SET @id_erro = 1;

	END

	-- Verificando se o cliente pode adicionar produtos daquele distribuidor
	IF @id_erro = 0 AND NOT EXISTS (
										SELECT
											TOP 1 1

										FROM
											@id_distribuidor_hold

										WHERE
											id_distribuidor = @id_distribuidor
								   )
	BEGIN

		SET @id_erro = 2;

	END

	-- Verificando se o produto pertence ao distribuidor
	IF @id_erro = 0 AND NOT EXISTS (
										SELECT
											TOP 1 1

										FROM
											#HOLD_PRODUTO_OFERTA

										WHERE
											id_produto = @id_produto
											AND id_distribuidor = @id_distribuidor
								   )
	BEGIN

		SET @id_erro = 3;

	END

	-- Verificando se existe um orcamento para aquele distribuidor
	SET @id_orcamento = (SELECT TOP 1 id_orcamento FROM @id_orcamento_hold WHERE id_distribuidor = @id_distribuidor);

	IF @id_orcamento IS NULL AND @id_erro = 0
	BEGIN

		INSERT INTO
			ORCAMENTO
				(
					id_usuario,
					id_cliente,
					id_distribuidor,
					data_criacao,
					status,
					d_e_l_e_t_
				)

			VALUES
				(
					@id_usuario,
					@id_cliente,
					@id_distribuidor,
					GETDATE(),
					'A',
					0
				);

		COMMIT;

		SET @id_orcamento = SCOPE_IDENTITY();
		
		INSERT INTO
			@id_orcamento_hold
				(
					id_distribuidor,
					id_orcamento
				)
				
			VALUES
				(
					@id_distribuidor,
					@id_orcamento
				);

	END	

	-- Mostrando o preço de tabela do produto
	SET @preco_tabela = NULL;
	SET @preco_desconto = NULL;
	SET @preco_venda = NULL;

	SET @tabela_preco = (
							SELECT
								tpc.id_tabela_preco
                                                        
							FROM
								TABELA_PRECO_CLIENTE tpc
                                                            
								INNER JOIN TABELA_PRECO tp ON
									tpc.id_tabela_preco = tp.id_tabela_preco
									AND tpc.id_distribuidor = tp.id_distribuidor
                                                                
							WHERE
								tpc.id_distribuidor = @id_distribuidor
								AND tpc.id_cliente = @id_cliente
								AND tp.status = 'A'
								AND @date >= tp.dt_inicio
								AND @date <= tp.dt_fim
						);

	IF @tabela_preco IS NOT NULL AND @id_erro = 0
	BEGIN

		SET @preco_tabela = (
								SELECT
									TOP 1 tpp.preco_tabela

								FROM
									TABELA_PRECO_PRODUTO tpp

									INNER JOIN TABELA_PRECO tp ON
										tpp.id_tabela_preco = tp.id_tabela_preco
										AND tpp.id_distribuidor = tp.id_distribuidor

								WHERE           
									tp.id_tabela_preco = @tabela_preco
									AND tpp.id_distribuidor = @id_distribuidor
									AND tpp.id_produto = @id_produto
									AND tp.status = 'A'
							); 

	END

	-- Pegando o preco padrao
	IF @preco_tabela IS NULL AND @id_erro = 0
	BEGIN

		SET @preco_tabela = (
								SELECT
									TOP 1 tpp.preco_tabela

								FROM
									PRODUTO p

									INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
										p.id_produto = pd.id_produto

									INNER JOIN TABELA_PRECO_PRODUTO tpp ON
										p.id_produto = tpp.id_produto
										AND pd.id_distribuidor = tpp.id_distribuidor

									INNER JOIN TABELA_PRECO tp ON
										tpp.id_tabela_preco = tp.id_tabela_preco
										AND tpp.id_distribuidor = tp.id_distribuidor

								WHERE           
									tp.tabela_padrao = 'S'
									AND pd.id_distribuidor = @id_distribuidor
									AND p.id_produto = @id_produto
									AND tp.status = 'A'
							);

		IF @preco_tabela IS NULL
		BEGIN
			SET @id_erro = 4;
		END

	END

	-- Verificando se o produto possui algum desconto
	IF @id_erro = 0
	BEGIN

		SET @desconto = (
							SELECT
								TOP 1 od.desconto
                        
							FROM
								OFERTA o
                        
								INNER JOIN OFERTA_DESCONTO od ON
									o.id_oferta = od.id_oferta
                            
								INNER JOIN OFERTA_PRODUTO op ON
									od.id_oferta = op.id_oferta
                            
								INNER JOIN PRODUTO p ON
									op.id_produto = p.id_produto
                            
								INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
									p.id_produto = pd.id_produto
									AND o.id_distribuidor = pd.id_distribuidor
                            
							WHERE
								o.status = 'A'
								AND od.status = 'A'
								AND op.status = 'A'
								AND p.id_produto = @id_produto
								AND pd.id_distribuidor = @id_distribuidor

							ORDER BY
								od.desconto DESC
						);

		IF @desconto < 0 OR @desconto IS NULL
		BEGIN
			SET @desconto = 0;
		END

		ELSE IF @desconto > 100
		BEGIN
			SET @desconto = 100;
		END

		-- Alterando o preco de venda
		SET @preco_desconto = @preco_tabela * (1 - (@desconto/100));

	END

	-- Verificando o estoque atual do produto
	IF @id_erro = 0
	BEGIN

		SET @estoque = (
							SELECT
								TOP 1 pe.qtd_estoque

							FROM
								PRODUTO p

								INNER JOIN PRODUTO_ESTOQUE pe ON
									p.id_produto = pe.id_produto

							WHERE
								p.id_produto = @id_produto
					   );

		IF @estoque < 0 OR @estoque IS NULL
		BEGIN
			SET @estoque = 0;
		END

	END

	-- Verificando qual o tipo oferta
	SET @tipo = NULL;
	SET @id_oferta = NULL;

	IF @id_erro = 0 AND (@id_campanha IS NOT NULL OR @id_escalonado IS NOT NULL)
	BEGIN

		IF @id_campanha IS NOT NULL AND @id_campanha > 0
		BEGIN
			SET @id_oferta = @id_campanha;
			SET @tipo_oferta = 2;
		END

		ELSE IF @id_escalonado IS NOT NULL AND @id_escalonado > 0
		BEGIN
			SET @id_oferta = @id_escalonado;
			SET @tipo_oferta = 3;
		END

		-- Verificando se a oferta e valida
		IF NOT EXISTS (SELECT id_oferta FROM #HOLD_OFERTA WHERE id_oferta = @id_oferta)
		BEGIN
			SET @id_erro = 5;
		END

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

			IF @id_campanha IS NOT NULL AND @bonificado = 1
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
					SET @id_erro = 6;
				END
				
			END

			ELSE
			BEGIN
				SET @id_erro = 6;
			END

		END
		
	END

	-- Verificando o tipo da oferta
	IF @id_erro = 0 AND @id_oferta IS NOT NULL AND @tipo_oferta = 2 AND @bonificado = 1
	BEGIN
		SET @tipo = 'B';
	END

	ELSE
	BEGIN
		SET @tipo = 'V';
	END

	-- Verificando preco_aplicado
	IF @id_erro = 0
	BEGIN

		IF @preco_aplicado IS NULL
		BEGIN
			
			IF @desconto > 0 AND @preco_desconto IS NOT NULL
			BEGIN
				SET @preco_aplicado = @preco_desconto;
			END

			ELSE
			BEGIN
				SET @preco_aplicado = @preco_tabela;
			END

		END

		SET @preco_venda = @preco_aplicado;

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
								AND id_oferta = ISNULL(@id_oferta, -1)
					  )

		BEGIN

			INSERT INTO
				#HOLD_PRODUTO_ORCAMENTO
					(
						id_produto,
						id_distribuidor,
						id_orcamento,
						id_cliente,
						id_oferta,
						status,
						quantidade,
						tipo,
						preco_venda,
						preco_tabela,
						preco_desconto,
						data_criacao,
						d_e_l_e_t_
					)

				VALUES
					(
						@id_produto,
						@id_distribuidor,
						@id_orcamento,
						@id_cliente,
						ISNULL(@id_oferta, -1),
						'A',
						@quantidade,
						@tipo,
						@preco_venda,
						@preco_tabela,
						@preco_desconto,
						(SELECT CONVERT(VARCHAR(30), @date, 21) AS [YYYY-MM-DD HH:MM:SS.mmm]),
						0
					);

		END

	END

	ELSE
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
	id_produto,
	id_distribuidor,
	id_orcamento,
	id_cliente,
	CASE WHEN id_oferta = -1 THEN NULL ELSE id_oferta END as id_oferta,
	status,
	quantidade,
	tipo,
	preco_venda,
	preco_tabela,
	preco_desconto,
	data_criacao,
	d_e_l_e_t_,
	id_erro

FROM
	#HOLD_PRODUTO_ORCAMENTO;

-- Deleta tabelas temporarias
DROP TABLE #HOLD_JSON;
DROP TABLE #HOLD_PRODUTO_ORCAMENTO;
DROP TABLE #HOLD_PRODUTO_OFERTA;
DROP TABLE #HOLD_OFERTA;