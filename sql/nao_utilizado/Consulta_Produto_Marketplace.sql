USE [B2BTESTE2]

SET NOCOUNT ON;
SET STATISTICS IO OFF;

-- Verificando se a tabela tempor�ria existe
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

CREATE TABLE #HOLD_PRODUTO
(
	id_produto VARCHAR(100),
	id_distribuidor INT,
	ranking INT,
	descricao_completa VARCHAR(MAX)
);

-- Declarações de variaveis globais
---- Variaveis de entrada
DECLARE @id_distribuidor INT = 0;
DECLARE @id_cliente INT = 2;

DECLARE @id_cliente_token VARCHAR(MAX) = NULL;

DECLARE @busca VARCHAR(MAX) = NULL;

DECLARE @id_produto VARCHAR(MAX) = NULL;

DECLARE @id_tipo INT = 1;
DECLARE @id_grupo INT = 23;
DECLARE @id_subgrupo INT = 108;

DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

---- Variaveis de apoio
DECLARE @stream_string VARCHAR(MAX);
DECLARE @null_search BIT = 0;

---- Variaveis tabelas
DECLARE @distribuidor_hold TABLE (
	id_distribuidor INT
);

DECLARE @id_subgrupo_hold TABLE (
	id_subgrupo INT
);

-- Pegando os clientes do token
IF LEN(@id_cliente_token) > 0
BEGIN

	IF NOT EXISTS (
						SELECT
							c.id_cliente

						FROM
							STRING_SPLIT(@id_cliente_token, ',') nda

							INNER JOIN CLIENTE c ON
								nda.value = c.id_cliente

						WHERE
							nda.value = @id_cliente
				  )
	BEGIN
		SET @null_search = 1;
	END

END

-- Pegando os distribuidores validos
IF @id_cliente IS NULL
BEGIN

	INSERT INTO
		@distribuidor_hold

		SELECT
			DISTINCT id_distribuidor

		FROM
			DISTRIBUIDOR

		WHERE
			status = 'A';

END

ELSE
BEGIN
	
	INSERT INTO
		@distribuidor_hold

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
			AND	c.id_cliente = @id_cliente
			AND d.id_distribuidor = CASE
										WHEN @id_distribuidor = 0
											THEN d.id_distribuidor
										ELSE
											@id_distribuidor
									END;

END

-- Salvando os produtos validos
IF @null_search = 0
BEGIN
	INSERT INTO
		#HOLD_PRODUTO

		SELECT
			p.id_produto,
			pd.id_distribuidor,
			pd.ranking,
			p.descricao_completa

		FROM
			PRODUTO p
                        
			INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
				p.id_produto = pd.id_produto

		WHERE
			LEN(p.sku) > 0
			AND p.status = 'A'
			AND pd.status = 'A'
			AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidor_hold);

END

-- Filtrando por...

-- ## Campo de busca ou por ids de produto
IF @busca IS NOT NULL OR @id_produto IS NOT NULL
BEGIN

	-- ## Campo de busca
	IF @busca IS NOT NULL
	BEGIN
		SET @stream_string = @busca
	END

	-- ## id de produto
	ELSE
	BEGIN
		SET @stream_string = @id_produto
	END

	-- Removendo espa�o em branco adicionais e salvando as palavras individualmente
	INSERT INTO
		#HOLD_STRING
			(
				string
			)

		SELECT
			VALUE

		FROM
			STRING_SPLIT(TRIM(@stream_string), ' ')

		WHERE
			VALUE != ' ';

	-- ## Filtrando pelo campo de busca
	IF @busca IS NOT NULL
	BEGIN

		DECLARE @max_count INT = (SELECT MAX(id) FROM #HOLD_STRING) + 1;
		DECLARE @actual_count INT = 1;
		DECLARE @word VARCHAR(100);

		WHILE @actual_count < @max_count
		BEGIN

			-- Pegando palavra atual
			SET @word = '%' + (SELECT TOP 1 string FROM #HOLD_STRING WHERE id = @actual_count) + '%'

			-- Fazendo query na palavra atual
			DELETE FROM
				#HOLD_PRODUTO

			WHERE
				id_produto NOT IN (
										SELECT
											p.id_produto

										FROM
											PRODUTO p

										WHERE
											p.descricao COLLATE Latin1_General_CI_AI LIKE @word
											OR p.descricao_completa COLLATE Latin1_General_CI_AI LIKE @word
											OR p.sku LIKE @word
											OR p.dun14 LIKE @word
											OR p.id_produto LIKE @word
								  )
			

			-- Verificando se ainda existem registros
			IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
			BEGIN
				SET @actual_count = @actual_count + 1
			END

			ELSE
			BEGIN
				SET @actual_count = @max_count
			END

		END
	END

	-- Filtrando por id produto
	ELSE
	BEGIN
		
		DELETE FROM
			#HOLD_PRODUTO

		WHERE
			id_produto NOT IN (
									SELECT
										string

									FROM
										#HOLD_STRING
							  )
	END

END

-- ## tipo/grupo/subgrupo
-- Filtrando pelo tipo/grupo/subgrupo
IF (@id_tipo IS NOT NULL) OR (@id_grupo IS NOT NULL) OR (@id_subgrupo IS NOT NULL)
BEGIN

	-- Pegando os subgrupos validos
	INSERT INTO
		@id_subgrupo_hold

		SELECT
			s.id_subgrupo

		FROM
			PRODUTO_SUBGRUPO ps

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
			ps.id_distribuidor = @id_distribuidor
			AND s.id_subgrupo = CASE
                                    WHEN @id_subgrupo IS NOT NULL
                                        THEN @id_subgrupo
                                    ELSE
                                        s.id_subgrupo
                                END
			AND g.id_grupo = CASE
                                 WHEN @id_grupo IS NOT NULL
                                     THEN @id_grupo
                                 ELSE
                                     g.id_grupo
                             END
			AND t.id_tipo = CASE
                                WHEN @id_tipo IS NOT NULL
                                    THEN @id_tipo
                                ELSE
                                    t.id_tipo
                            END
			AND t.status = 'A'
            AND g.status = 'A'
            AND gs.status = 'A'
            AND s.status = 'A'
            AND ps.status = 'A';
			
    -- Removendo produtos que não estão nos subgrupos
    DELETE FROM
        #HOLD_PRODUTO

    WHERE
        id_produto NOT IN (
                                SELECT
                                    ps.codigo_produto

                                FROM
									PRODUTO_SUBGRUPO ps

                                WHERE
									ps.id_subgrupo IN (SELECT id_subgrupo FROM @id_subgrupo_hold) 
                          );

END

-- Mostra o Resultado Final
SELECT
	id_produto,
	id_distribuidor,
	ranking,
	descricao_completa,
	COUNT(*) OVER() as 'count__'

FROM
	#HOLD_PRODUTO

ORDER BY
	CASE WHEN ranking > 0 THEN 1 ELSE 0 END DESC,
	ranking ASC,
	descricao_completa  

OFFSET
	@offset ROWS

FETCH
	NEXT @limite ROWS ONLY;

-- Deleta tabelas tempor�rias
DROP TABLE #HOLD_PRODUTO;
DROP TABLE #HOLD_STRING;