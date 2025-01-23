USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_PRODUTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_PRODUTO
END;

IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_OFERTA
END;

IF Object_ID('tempDB..#HOLD_PRODUTO_ORCAMENTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_PRODUTO_ORCAMENTO
END;

IF Object_ID('tempDB..#HOLD_DATA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_DATA
END;

IF Object_ID('tempDB..#HOLD_ORCAMENTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_ORCAMENTO
END;

-- Criando tabelas temporarias
CREATE TABLE #HOLD_PRODUTO_ORCAMENTO
(
	id INT IDENTITY(1,1),
	id_distribuidor INT,
	id_orcamento INT,
	id_cliente INT,
	id_oferta INT,
	id_produto VARCHAR(100) COLLATE Latin1_General_CI_AS,
	quantidade INT,
	tipo CHAR(1) COLLATE Latin1_General_CI_AS,
	preco_venda DECIMAL(18,2),
	preco_tabela DECIMAL(18,2),
	preco_desconto DECIMAL(18,3),
	data_criacao DATETIME
);

CREATE TABLE #HOLD_DATA
(
	id_produto VARCHAR(50),
	id_distribuidor INT,
	id_campanha INT,
	id_escalonado INT,
	preco_tabela DECIMAL(18,3),
	preco_desconto DECIMAL(18,3),
	preco_aplicado DECIMAL(18,3),
	quantidade INT,
	estoque INT,
	desconto DECIMAL(8,3),
	categorizacao VARCHAR(MAX),
	sku VARCHAR(50),
	descricao_produto VARCHAR(MAX),
	descricao_marca VARCHAR(100),
	imagem VARCHAR(MAX),
	status VARCHAR(1),
	unidade_embalagem VARCHAR(20),
	quantidade_embalagem INT,
	unidade_venda VARCHAR(20),
	quant_unid_venda INT,
	bonificado BIT
);

CREATE TABLE #HOLD_ORCAMENTO
(
	id_orcamento INT,
	id_distribuidor INT,
	dados VARCHAR(MAX)
);

-- Declarações de variáveis globais
--## Variaveis de entrada
DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;
DECLARE @id_distribuidor INT;
DECLARE @id_orcamento_input VARCHAR(MAX) = NULL;
DECLARE @image_server VARCHAR(1000) = 'https://midiasmarketplace-dev.guarany.com.br';
DECLARE @image_replace VARCHAR(100) = '150';

--## Variaveis de controle
DECLARE @id_orcamento INT;
DECLARE @tabela_preco VARCHAR(20);
DECLARE @id_produto VARCHAR(100);
DECLARE @tipo CHAR(1);
DECLARE @id_oferta INT;
DECLARE @tipo_oferta INT;
DECLARE @id_escalonado INT;
DECLARE @id_campanha INT;
DECLARE @quantidade INT;
DECLARE @max_orcamento INT;
DECLARE @count_orcamento INT;
DECLARE @max_produto INT;
DECLARE @count_produto INT;
DECLARE @imagem VARCHAR(MAX);
DECLARE @categorizacao VARCHAR(MAX);
DECLARE @max_count_tipo INT;
DECLARE @count_tipo INT;
DECLARE @max_count_grupo INT;
DECLARE @count_grupo INT;
DECLARE @actual_tipo INT;
DECLARE @actual_grupo INT;
DECLARE @tipo_cat VARCHAR(MAX);
DECLARE @grupo_cat VARCHAR(MAX);
DECLARE @subgrupo_cat VARCHAR(MAX);
DECLARE @preco_tabela DECIMAL(18,3);
DECLARE @preco_desconto DECIMAL(18,3);
DECLARE @preco_aplicado DECIMAL(18,3);
DECLARE @estoque INT;
DECLARE @desconto DECIMAL(8,3);
DECLARE @bonificado BIT;
DECLARE @orcamento_hold VARCHAR(MAX);

--## Variaveis tabelas
DECLARE @id_tipo TABLE (
	posicao INT IDENTITY(1,1),
	id INT
);

DECLARE @id_grupo TABLE (
	posicao INT IDENTITY(1,1),
	id INT
);

DECLARE @id_subgrupo TABLE (
	id INT
);

DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT
);

DECLARE @id_orcamento_hold TABLE (
	id INT IDENTITY(1,1),
	id_orcamento INT,
	id_distribuidor INT,
	data_criacao DATETIME
);

-- Salvando os id_distribuidores atralados ao cliente
INSERT INTO
	@id_distribuidor_hold

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
		AND cd.status = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND d.status = 'A'
		AND d.id_distribuidor = CASE
									WHEN ISNULL(@id_distribuidor, 0) != 0
										THEN @id_distribuidor
									ELSE
										d.id_distribuidor
								END;

-- Pegando os produtos dos distribuidores
SELECT
	p.id_produto,
	pd.id_distribuidor

INTO
	#HOLD_PRODUTO

FROM
	PRODUTO p

	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		p.id_produto = pd.id_produto

WHERE
	p.status = 'A'
	AND pd.status = 'A'
	AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

-- Salvando as ofertas validas
SELECT
	DISTINCT o.id_oferta,
	o.tipo_oferta,
	o.id_distribuidor

INTO
	#HOLD_OFERTA

FROM
	OFERTA o

	INNER JOIN OFERTA_PRODUTO op ON
		o.id_oferta = op.id_oferta

	INNER JOIN #HOLD_PRODUTO thp ON
		op.id_produto = thp.id_produto
		AND o.id_distribuidor = thp.id_distribuidor
		
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

-- Pegar os orcamentos do usuario que possuem produtos
IF LEN(@id_orcamento_input) > 0
BEGIN

	INSERT INTO
		@id_orcamento_hold
			(
				id_orcamento,
				id_distribuidor,
				data_criacao
			)

		SELECT
			DISTINCT o.id_orcamento,
			o.id_distribuidor,
			o.data_criacao

		FROM
			ORCAMENTO o

			INNER JOIN ORCAMENTO_ITEM oi ON
				o.id_orcamento = oi.id_orcamento
				AND o.id_distribuidor = oi.id_distribuidor

			INNER JOIN #HOLD_PRODUTO thp ON
				oi.id_produto = thp.id_produto

			INNER JOIN (
							SELECT
								nda.value

							FROM
								STRING_SPLIT(@id_orcamento_input, ',') nda

							WHERE
								LEN(nda.value) > 0
					   ) as nda ON
				o.id_orcamento = nda.value

		WHERE
			o.id_cliente = @id_cliente
			AND o.id_usuario = @id_usuario
			AND o.d_e_l_e_t_ = 0
			AND oi.d_e_l_e_t_ = 0
			AND oi.status = 'A'
			AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)

		ORDER BY
			o.data_criacao;

END

ELSE
BEGIN

	INSERT INTO
		@id_orcamento_hold
			(
				id_orcamento,
				id_distribuidor,
				data_criacao
			)

		SELECT
			DISTINCT o.id_orcamento,
			o.id_distribuidor,
			o.data_criacao

		FROM
			ORCAMENTO o

			INNER JOIN ORCAMENTO_ITEM oi ON
				o.id_orcamento = oi.id_orcamento
				AND o.id_distribuidor = oi.id_distribuidor

			INNER JOIN #HOLD_PRODUTO thp ON
				oi.id_produto = thp.id_produto

		WHERE
			o.d_e_l_e_t_ = 0
			AND o.status = 'A'
			AND oi.d_e_l_e_t_ = 0
			AND oi.status = 'A'
			AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
			AND o.id_cliente = @id_cliente
			AND o.id_usuario = @id_usuario
		
		ORDER BY
			o.data_criacao;

END

-- Loop nos orcamentos
SELECT
	@max_orcamento = MAX(id),
	@count_orcamento = MIN(id)

FROM
	@id_orcamento_hold;

WHILE @count_orcamento <= @max_orcamento
BEGIN

	-- Pegando o orcamento atual
	SELECT
		@id_orcamento = id_orcamento,
		@id_distribuidor = id_distribuidor

	FROM
		@id_orcamento_hold

	WHERE
		id = @count_orcamento;

	-- Pegando os produtos do orcamento
	TRUNCATE TABLE #HOLD_PRODUTO_ORCAMENTO;
	TRUNCATE TABLE #HOLD_DATA

	INSERT INTO
		#HOLD_PRODUTO_ORCAMENTO
			(
				id_distribuidor,
				id_orcamento,
				id_cliente,
				id_oferta,
				id_produto,
				quantidade,
				tipo,
				preco_venda,
				preco_tabela,
				preco_desconto
			)

		SELECT
			oi.id_distribuidor,
			oi.id_orcamento,
			oi.id_cliente,
			oi.id_oferta,
			oi.id_produto,
			oi.quantidade,
			oi.tipo,
			oi.preco_venda,
			oi.preco_tabela,
			oi.preco_desconto

		FROM
			ORCAMENTO_ITEM oi

			INNER JOIN #HOLD_PRODUTO thp ON
				oi.id_produto = thp.id_produto
				AND oi.id_distribuidor = thp.id_distribuidor

		WHERE
			oi.d_e_l_e_t_ = 0
			AND oi.status = 'A'
			AND oi.id_distribuidor = @id_distribuidor
			AND oi.id_orcamento = @id_orcamento
		
		ORDER BY
			oi.data_criacao;

	-- Loop nos produtos
	SELECT
		@max_produto = MAX(id),
		@count_produto = MIN(id)

	FROM
		#HOLD_PRODUTO_ORCAMENTO;

	WHILE @count_produto <= @max_produto
	BEGIN

		-- Pegando o produto atual
		SELECT
			@id_produto = id_produto,
			@tipo = tipo,
			@id_oferta = id_oferta,
			@quantidade = quantidade,
			@preco_aplicado = preco_venda

		FROM
			#HOLD_PRODUTO_ORCAMENTO

		WHERE
			id = @count_produto;

		-- Pegando o tipo-grupo-subgrupo
		SET @categorizacao = '';

		---- Pegando o tipo
		DELETE FROM
			@id_tipo;

		INSERT INTO
			@id_tipo

			SELECT
				DISTINCT t.id_tipo

			FROM
				TIPO t

			WHERE
				t.status = 'A'
				AND t.id_distribuidor = @id_distribuidor
			
			ORDER BY
				t.id_tipo;

		-- Loop para pegar grupos
		SELECT
			@max_count_tipo = MAX(posicao),
			@count_tipo = MIN(posicao)

		FROM
			@id_tipo;

		WHILE @count_tipo <= @max_count_tipo
		BEGIN

			SET @actual_tipo = (SELECT TOP 1 id FROM @id_tipo WHERE posicao = @count_tipo);
			
			-- Pegando os grupos
			DELETE FROM
				@id_grupo;

			INSERT INTO
				@id_grupo

				SELECT
					DISTINCT g.id_grupo

				FROM
					GRUPO g

				WHERE
					g.status = 'A'
					AND g.id_distribuidor = @id_distribuidor
					AND g.tipo_pai = @actual_tipo
					
				ORDER BY
					g.id_grupo;

			-- Loop para o subgrupo
			SELECT
				@max_count_grupo = MAX(posicao),
				@count_grupo = MIN(posicao)

			FROM
				@id_grupo;

			WHILE @count_grupo < @max_count_grupo
			BEGIN
				
				SET @actual_grupo = (SELECT TOP 1 id FROM @id_grupo WHERE posicao = @count_grupo);

				-- Pegando os subgrupos
				DELETE FROM
					@id_subgrupo;

				INSERT INTO
					@id_subgrupo

					SELECT
						DISTINCT s.id_subgrupo

					FROM
						GRUPO_SUBGRUPO gs

						INNER JOIN SUBGRUPO s ON
							gs.id_subgrupo = s.id_subgrupo
							AND gs.id_distribuidor = s.id_distribuidor
						
						INNER JOIN PRODUTO_SUBGRUPO ps ON
							s.id_subgrupo = ps.id_subgrupo
							AND s.id_distribuidor = ps.id_distribuidor

					WHERE
						gs.status = 'A'
						AND s.status = 'A'
						AND ps.status = 'A'
						AND gs.id_grupo = @actual_grupo
						AND gs.id_distribuidor = @id_distribuidor
						AND ps.codigo_produto = @id_produto
						
					ORDER BY
						s.id_subgrupo;

				IF EXISTS (SELECT TOP 1 1 FROM @id_subgrupo)
				BEGIN
					
					SET @subgrupo_cat = (
											SELECT 
												'{"id_subgrupo": ' + CONVERT(VARCHAR(10),id) + '},' 
												
											FROM 
												@id_subgrupo 

											FOR
												XML PATH('')
										);

					SET @subgrupo_cat = SUBSTRING(@subgrupo_cat, 1, LEN(@subgrupo_cat) - 1);
					
					SET @grupo_cat = ('{"id_grupo": ' + CONVERT(VARCHAR(10),@actual_grupo) + ', "subgrupo": [' + @subgrupo_cat + ']},'); 
					SET @grupo_cat = SUBSTRING(@grupo_cat, 1, LEN(@grupo_cat) - 1);
					
					SET @tipo_cat = ('{"id_tipo": ' + CONVERT(VARCHAR(10),@actual_tipo) + ', "grupo": [' + @grupo_cat + ']},'); 

					SET @categorizacao = @categorizacao + @tipo_cat;

				END

				-- incrementando o contador
				SET @count_grupo = @count_grupo + 1;

			END

			-- incrementando o contador
			SET @count_tipo = @count_tipo + 1;

		END

		IF LEN(@categorizacao) <= 0
		BEGIN
			SET @categorizacao = '[]';
		END

		ELSE
		BEGIN
			SET @categorizacao = '[' + SUBSTRING(@categorizacao, 1, LEN(@categorizacao)- 1) + ']';
		END

		-- Verificando se o cliente está associado a uma tabela de preço diferente
		SET @preco_tabela = NULL;
		SET @preco_desconto = NULL;

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
									AND GETDATE() >= tp.dt_inicio
									AND GETDATE() <= tp.dt_fim
							);

		-- Mostrando o preço de tabela do produto
		IF @tabela_preco IS NOT NULL
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
		IF @preco_tabela IS NULL
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

		END

		-- Verificando se o produto possui algum desconto
		IF @preco_tabela IS NOT NULL
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
			IF @desconto = 0 OR @desconto = 100
			BEGIN
				SET @preco_desconto = 0;
			END
	
			ELSE IF @desconto > 0 AND @desconto < 100
			BEGIN
				SET @preco_desconto = @preco_tabela * (1 - (@desconto/100));
			END

		END

		-- Verificando a oferta
		SET @bonificado = 0;
		SET @id_campanha = NULL;
		SET @id_escalonado = NULL;

		If @tipo = 'B' AND  @id_oferta IS NULL
		BEGIN
			SET @id_produto = NULL;
		END

		IF @id_oferta IS NOT NULL
		BEGIN

			-- Verificando se a oferta e valida
			IF NOT EXISTS (SELECT id_oferta FROM #HOLD_OFERTA WHERE id_oferta = @id_oferta)
			BEGIN
				SET @id_oferta = NULL;
			END

			-- Verificando se o produto tem a oferta informada
			IF @tipo = 'V'
			BEGIN

				IF NOT EXISTS (
									SELECT
										TOP 1 id_produto

									FROM
										OFERTA_PRODUTO

									WHERE
										id_oferta = @id_oferta
										AND id_produto = @id_produto
										AND status = 'A'
							  )
				BEGIN
					SET @id_oferta = NULL;
				END

			END

			ELSE
			BEGIN

				SET @bonificado = 1;
				
				IF NOT EXISTS (
									SELECT
										TOP 1 id_produto

									FROM
										OFERTA_BONIFICADO

									WHERE
										id_oferta = @id_oferta
										AND id_produto = @id_produto
										AND status = 'A'
							  )
				BEGIN
					SET @id_oferta = NULL;
					SET @id_produto = NULL;
				END

			END

		END

		-- Verificando o tipo da oferta
		IF @id_oferta IS NOT NULL
		BEGIN

			SET @tipo_oferta = (SELECT TOP 1 tipo_oferta FROM #HOLD_OFERTA WHERE id_oferta = @id_oferta)

			IF @tipo_oferta = 2
			BEGIN
				SET @id_campanha = @id_oferta
			END

			ELSE
			BEGIN
				SET @id_escalonado = @id_oferta
			END

		END

		-- Pegando as imagens do produto
		SET @imagem = (
						
							SELECT
								'"' + CONVERT(VARCHAR(100), @image_server) + '/imagens/' +  REPLACE(pimg.token, '/auto/', '/' + @image_replace + '/') + '",' AS 'data()'

							FROM
								PRODUTO_IMAGEM pimg

								INNER JOIN PRODUTO p ON
									pimg.sku = p.sku

							WHERE
								p.id_produto = @id_produto

							ORDER BY
								pimg.imagem
		
							FOR 
								XML PATH('')
					  );

		SET @imagem = '[' + SUBSTRING(@imagem, 1, LEN(@imagem) - 1) + ']';

		-- Salvando o produto no orcamento
		IF @id_produto IS NOT NULL
		BEGIN

			INSERT INTO
				#HOLD_DATA
					(
						id_produto,
						id_distribuidor,
						id_campanha,
						id_escalonado,
						preco_tabela,
						preco_desconto,
						preco_aplicado,
						quantidade,
						estoque,
						desconto,
						categorizacao,
						sku,
						descricao_produto,
						descricao_marca,
						imagem,
						status,
						unidade_embalagem,
						quantidade_embalagem,
						unidade_venda,
						quant_unid_venda,
						bonificado
					)

				VALUES
					(
						@id_produto,
						@id_distribuidor,
						@id_campanha,
						@id_escalonado,
						@preco_tabela,
						@preco_desconto,
						@preco_aplicado,
						@quantidade,
						NULL,
						@desconto,
						@categorizacao,
						NULL,
						NULL,
						NULL,
						@imagem,
						'A',
						NULL,
						NULL,
						NULL,
						NULL,
						@bonificado
					)

		END

		-- Incrementando o contador
		SET @count_produto = @count_produto + 1

	END

	IF EXISTS (SELECT TOP 1 1 FROM #HOLD_DATA)
	BEGIN

		-- Completando os campos
		UPDATE
			#HOLD_DATA

		SET
			estoque = ISNULL(pe.qtd_estoque, 0),
			sku = p.sku,
			descricao_produto = p.descricao_completa,
			descricao_marca = m.desc_marca,
			unidade_embalagem = p.unidade_embalagem,
			quantidade_embalagem = p.quantidade_embalagem,
			unidade_venda = pd.unidade_venda,
			quant_unid_venda = pd.quant_unid_venda

		FROM
			#HOLD_DATA thd

			LEFT JOIN PRODUTO_ESTOQUE pe ON
				thd.id_produto = pe.id_produto
				AND thd.id_distribuidor = pe.id_distribuidor

			INNER JOIN PRODUTO p ON
				pe.id_produto = p.id_produto

			INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
				p.id_produto = pd.id_produto
				AND pe.id_distribuidor = pd.id_distribuidor

			INNER JOIN MARCA m ON
				pd.id_marca = m.id_marca

			LEFT JOIN PRODUTO_IMAGEM pimg ON
				p.sku = pimg.sku;

		-- Salvando o orcamento
		INSERT INTO
			#HOLD_ORCAMENTO
				(
					id_orcamento,
					id_distribuidor,
					dados
				)

			VALUES
				(
					@id_orcamento,
					@id_distribuidor,
					(SELECT * FROM #HOLD_DATA FOR JSON AUTO, INCLUDE_NULL_VALUES)
				);
			

	END

	-- Incrementando o contador
	SET @count_orcamento = @count_orcamento + 1

END

-- Mostrando o resultado
SELECT
	id_orcamento,
	id_distribuidor,
	dados as itens

FROM
	#HOLD_ORCAMENTO;

-- Deleta tabelas temporarias
DROP TABLE #HOLD_PRODUTO;
DROP TABLE #HOLD_OFERTA;
DROP TABLE #HOLD_PRODUTO_ORCAMENTO;
DROP TABLE #HOLD_DATA;
DROP TABLE #HOLD_ORCAMENTO