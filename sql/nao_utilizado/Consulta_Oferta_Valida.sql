USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_OFERTA
END;

IF OBJECT_ID('tempDB..#HOLD_PRE_OFERTA', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_PRE_OFERTA
END;

IF OBJECT_ID('tempDB..#HOLD_PRODUTO', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_PRODUTO
END;

IF OBJECT_ID('tempDB..#HOLD_STRING', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_STRING
END;

-- Criando a tabela temporaria de palavras
CREATE TABLE #HOLD_STRING
(
	id INT IDENTITY(1,1),
	string VARCHAR(100)
);

-- Criando a tabela temporaria de oferta
CREATE TABLE #HOLD_OFERTA
(
	id_oferta INT,
	tipo_oferta INT,
	id_distribuidor INT,
	ordem INT,
	descricao_oferta VARCHAR(1000) 
);

-- Variaveis globais
---- Variaveis de entrada
DECLARE @id_distribuidor INT = 0;
DECLARE @id_cliente INT = 1;
DECLARE @id_usuario INT = 1;
DECLARE @id_tipo INT = NULL;
DECLARE @id_grupo INT = NULL;
DECLARE @id_subgrupo INT = NULL;
DECLARE @busca VARCHAR(1000) = NULL;
DECLARE @id_produto VARCHAR(MAX) = '4289-11,6341-1,4480-1';
DECLARE @id_oferta VARCHAR(MAX) = NULL;
DECLARE @id_orcamento VARCHAR(MAX) = NULL;
DECLARE @paginar INT = 1;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

---- Variaveis de apoio
DECLARE @max_count INT;
DECLARE @actual_count INT = 1;
DECLARE @word VARCHAR(1000);

---- Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT
);

DECLARE @id_orcamento_hold TABLE (
	id_orcamento INT
);

-- Pegando distribuidores válidos para o cliente
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
		c.id_cliente = CASE
						   WHEN @id_cliente IS NULL
							   THEN c.id_cliente
						   ELSE
							   @id_cliente
					   END
		AND d.id_distribuidor = CASE
									WHEN @id_distribuidor = 0
										THEN d.id_distribuidor
									ELSE
										@id_distribuidor
								END
		AND c.status = 'A'
		AND c.status_receita = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND cd.status = 'A'
		AND d.status = 'A';

-- Salvando os produtos validos
SELECT
	p.id_produto,
	p.sku,
	pd.id_distribuidor

INTO
	#HOLD_PRODUTO

FROM
	PRODUTO p
                        
    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        p.id_produto = pd.id_produto

WHERE
	LEN(p.sku) > 0
	AND p.id_produto IS NOT NULL
	AND p.status = 'A'
	AND pd.status = 'A'
	AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

-- Filtrando por...

-- ## tipo/grupo/subgrupo
IF (@id_tipo IS NOT NULL) OR (@id_grupo IS NOT NULL) OR (@id_subgrupo IS NOT NULL)
BEGIN
	DELETE FROM
		#HOLD_PRODUTO

	WHERE
		id_produto NOT IN (
								SELECT
									p.id_produto

								FROM
									PRODUTO p
                            
									INNER JOIN PRODUTO_SUBGRUPO ps ON
										p.id_produto = ps.codigo_produto
                            
									INNER JOIN SUBGRUPO s ON
										ps.id_distribuidor = s.id_distribuidor 
										AND ps.id_subgrupo = s.id_subgrupo 
                            
									INNER JOIN GRUPO_SUBGRUPO gs ON
										s.id_distribuidor = gs.id_distribuidor 
										AND ps.id_subgrupo = gs.id_subgrupo 
                            
									INNER JOIN GRUPO g ON
										gs.id_grupo = g.id_grupo 
                            
									INNER JOIN TIPO t ON
										g.tipo_pai = t.id_tipo
									
								WHERE
									ps.status = 'A'
									AND s.status = 'A'
									AND gs.status = 'A'
									AND g.status = 'A'
									AND t.status = 'A'
									AND t.id_tipo = CASE
														WHEN @id_tipo is NULL
															THEN t.id_tipo
														ELSE
															@id_tipo
													END
									AND g.id_grupo = CASE
															WHEN @id_grupo IS NULL
																THEN g.id_grupo
															ELSE
																@id_grupo
														END
									AND s.id_subgrupo = CASE
															WHEN @id_subgrupo IS NULL
																THEN s.id_subgrupo
															ELSE
																@id_subgrupo
														END
									AND ps.id_distribuidor = CASE
																 WHEN @id_distribuidor = 0
																	 THEN ps.id_distribuidor
																 ELSE
																	 @id_distribuidor
															 END

						  )
END

-- ## Marca ou descricao de produto
ELSE IF LEN(@busca) > 0
BEGIN

	-- Removendo espaco em branco adicionais e salvando as palavras individualmente
	INSERT INTO
		#HOLD_STRING
			(
				string
			)

		SELECT
			VALUE

		FROM
			STRING_SPLIT(TRIM(@busca), ' ')

		WHERE
			LEN(VALUE) > 0;

	-- Realizando o filtro
	SET @max_count = (SELECT MAX(id) FROM #HOLD_STRING) + 1;

	WHILE @actual_count < @max_count
	BEGIN

		-- Pegando palavra atual
		SET @word = '%' + (SELECT TOP 1 string FROM #HOLD_STRING WHERE id = @actual_count) + '%';

		-- Fazendo query na palavra atual
		DELETE FROM
			#HOLD_PRODUTO

		WHERE
			id_produto NOT IN (
									SELECT
									thp.id_produto

									FROM
										#HOLD_PRODUTO thp

										INNER JOIN PRODUTO p ON
											thp.id_produto = p.id_produto

										INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
											p.id_produto = pd.id_produto

										INNER JOIN MARCA m ON
											pd.id_marca = m.id_marca
											AND pd.id_distribuidor = m.id_distribuidor

									WHERE
										p.descricao_completa COLLATE Latin1_General_CI_AI LIKE @word
										OR m.desc_marca COLLATE Latin1_General_CI_AI LIKE @word
										
								)
			

		-- Verificando se ainda existem registros
		IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
		BEGIN
			SET @actual_count = @actual_count + 1;
		END

		ELSE
		BEGIN
			SET @actual_count = @max_count;
		END

	END

END

-- ## Orcamento
ELSE IF LEN(@id_orcamento) > 0 AND @id_usuario IS NOT NULL AND @id_cliente IS NOT NULL
BEGIN

	-- Pegando os orcamentos validos
	INSERT INTO
		@id_orcamento_hold

		SELECT
			VALUE

		FROM
			STRING_SPLIT(TRIM(@id_orcamento), ',')

		WHERE
			LEN(VALUE) > 0;

	-- Removendo os que não estão no orcamento
	DELETE FROM
		#HOLD_PRODUTO

	WHERE
		id_produto NOT IN (
								SELECT
									thp.id_produto

								FROM
									#HOLD_PRODUTO thp

									INNER JOIN ORCAMENTO_ITEM oi ON
										thp.id_produto = oi.id_produto
										AND thp.id_distribuidor = oi.id_distribuidor

									INNER JOIN ORCAMENTO o ON
										oi.id_orcamento = o.id_orcamento

								WHERE
								    o.id_cliente = @id_cliente
									AND o.id_usuario = @id_usuario
									AND o.id_orcamento IN (SELECT id_orcamento FROM @id_orcamento_hold)
						  );

END

-- ## ID de produto
ELSE IF LEN(@id_produto) > 0
BEGIN

	DELETE FROM
		#HOLD_PRODUTO

	WHERE
		id_produto NOT IN (
								SELECT
									VALUE

								FROM
									STRING_SPLIT(TRIM(@id_produto), ',')

								WHERE
									LEN(VALUE) > 0
							)

END

-- Verificando se produtos foram encontrados
IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
BEGIN

	-- Salvando pre-ofertas
	SELECT
		DISTINCT o.id_oferta,
		o.tipo_oferta,
		o.id_distribuidor,
		o.ordem,
		o.descricao_oferta

	INTO
		#HOLD_PRE_OFERTA

	FROM
		OFERTA o

		INNER JOIN OFERTA_PRODUTO op ON
			o.id_oferta = op.id_oferta

		INNER JOIN #HOLD_PRODUTO thp ON
			op.id_produto = thp.id_produto
			AND o.id_distribuidor = thp.id_distribuidor
		
	WHERE
		op.id_produto IS NOT NULL
		AND o.tipo_oferta IN (2,3)
		AND op.status = 'A'
		AND o.status = 'A'
		AND o.data_cadastro <= GETDATE()
		AND o.data_inicio <= GETDATE()
		AND o.data_final >= GETDATE()
		AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

	IF EXISTS(SELECT 1 FROM #HOLD_PRE_OFERTA)
	BEGIN

		-- Salvando ofertas bonificadas
		INSERT INTO
			#HOLD_OFERTA

			SELECT
				DISTINCT thpo.id_oferta,
				thpo.tipo_oferta,
				thpo.id_distribuidor,
				thpo.ordem,
				thpo.descricao_oferta

			FROM
				#HOLD_PRE_OFERTA thpo

				INNER JOIN OFERTA_BONIFICADO ob ON
					thpo.id_oferta = ob.id_oferta

			WHERE
				ob.status = 'A'
				AND ob.id_produto IS NOT NULL
				AND thpo.tipo_oferta = 2;

		-- Salvando ofertas escalonada
		INSERT INTO
			#HOLD_OFERTA
	
			SELECT
				DISTINCT thpo.id_oferta,
				thpo.tipo_oferta,
				thpo.id_distribuidor,
				thpo.ordem,
				thpo.descricao_oferta

			FROM
				#HOLD_PRE_OFERTA thpo

				INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
					thpo.id_oferta = oef.id_oferta

			WHERE
				oef.status = 'A'
				AND thpo.tipo_oferta = 3;

		-- Definindo a Paginacao
		IF @id_subgrupo IS NOT NULL AND @paginar = 0
		BEGIN
			SET @offset = 0
			SET @limite = (SELECT COUNT(*) FROM #HOLD_OFERTA)

			IF @limite <= 0
			BEGIN
				SET @limite = 1
			END
		END

	END

END

-- Filtrando por id_oferta
IF @id_oferta IS NOT NULL AND LEN(@id_oferta) > 0
BEGIN
	
	DELETE FROM
		#HOLD_OFERTA

	WHERE
		id_oferta NOT IN (
							SELECT
								VALUE

							FROM
								STRING_SPLIT(@id_oferta, ' ')

							WHERE
								LEN(VALUE) > 0
						 )

END

-- Mostrando os resultados
SELECT
	id_oferta,
	tipo_oferta,
	id_distribuidor,
	ordem,
	descricao_oferta,
	COUNT(*) OVER() as COUNT__

FROM
	#HOLD_OFERTA

ORDER BY
	ordem ASC,
	descricao_oferta ASC

OFFSET
	@offset ROWS

FETCH
	NEXT @limite ROWS ONLY;

-- Deleta tabelas tempor�rias
DROP TABLE #HOLD_OFERTA;
DROP TABLE #HOLD_STRING;

IF OBJECT_ID('tempDB..#HOLD_PRE_OFERTA', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_PRE_OFERTA
END;

IF OBJECT_ID('tempDB..#HOLD_PRODUTO', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_PRODUTO
END;