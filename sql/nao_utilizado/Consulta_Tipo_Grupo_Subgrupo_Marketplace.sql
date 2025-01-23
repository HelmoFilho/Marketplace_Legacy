USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificanto existencia das tabelas temporarias
IF OBJECT_ID('tempDB..#HOLD_DATA','U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_DATA;
END

-- Criando as tabelas temporarias
CREATE TABLE #HOLD_DATA
(
	id INT IDENTITY(1,1),
	id_tipo INT,
	descricao VARCHAR(1000),
	grupos VARCHAR(MAX)
);

-- Declarações de variaveis globais
---- Variaveis de entrada
DECLARE @id_distribuidor INT = 0;

---- Variaveis de controle
DECLARE @max_tipo INT;
DECLARE @count_tipo INT;
DECLARE @max_grupo INT;
DECLARE @count_grupo INT;
DECLARE @descri_tipo VARCHAR(200);
DECLARE @descri_grupo VARCHAR(200);
DECLARE @id_tipo INT;
DECLARE @id_grupo INT;
DECLARE @id_subgrupo INT;
DECLARE @id_tipo_string VARCHAR(MAX);
DECLARE @id_grupo_string VARCHAR(MAX);
DECLARE @id_subgrupo_string VARCHAR(MAX)
DECLARE @hold_result VARCHAR(MAX);

---- Variaveis tabelas
DECLARE @hold_id_tipo TABLE (
	id INT IDENTITY(1,1),
	id_tipo INT,
	descricao VARCHAR(200)
);

DECLARE @hold_id_grupo TABLE (
	id INT IDENTITY(1,1),
	id_grupo INT,
	descricao VARCHAR(200)
);

DECLARE @hold_grupo TABLE (
	grupo VARCHAR(MAX)
);

DECLARE @hold_id_subgrupo TABLE (
	id_subgrupo INT,
	descricao VARCHAR(200),
	ranking INT,
	ordem BIT
);

-- Pegando os id de tipo válidos
INSERT INTO
	@hold_id_tipo
		(
			id_tipo,
			descricao
		)

SELECT
	t.id_tipo,
	t.descricao

FROM
	TIPO t

WHERE
	t.id_distribuidor = @id_distribuidor
	AND t.status = 'A'

ORDER BY
	CASE WHEN ISNULL(t.ranking, 0) = 0 THEN 0 ELSE 1 END DESC,
	t.ranking ASC,
	t.descricao;

IF EXISTS (SELECT TOP 1 1 FROM @hold_id_tipo)
BEGIN

	DELETE FROM
		@hold_grupo;
	
	-- Loop do tipo
	SELECT
		@max_tipo = MAX(id),
		@count_tipo = MIN(id)

	FROM	
		@hold_id_tipo;

	WHILE @count_tipo <= @max_tipo
	BEGIN

		SELECT
			@id_tipo = id_tipo,
			@descri_tipo = RTRIM(descricao)

		FROM 
			@hold_id_tipo 
			
		WHERE 
			id = @count_tipo;
		
		-- Pegando os grupos atrelados ao tipo
		DELETE FROM
			@hold_id_grupo;

		INSERT INTO
			@hold_id_grupo
				(
					id_grupo,
					descricao
				)

		SELECT
			g.id_grupo,
			g.descricao

		FROM
			GRUPO g

		WHERE
			g.id_distribuidor = @id_distribuidor
			AND g.tipo_pai = @id_tipo
			AND g.status = 'A'

		ORDER BY
			CASE WHEN ISNULL(g.ranking, 0) = 0 THEN 0 ELSE 1 END DESC,
			g.ranking ASC,
			g.descricao;

		IF EXISTS (SELECT TOP 1 1 FROM @hold_id_grupo)
		BEGIN

			-- Loop do grupo
			SELECT
				@max_grupo = MAX(id),
				@count_grupo = MIN(id)

			FROM	
				@hold_id_grupo;

			WHILE @count_grupo <= @max_grupo
			BEGIN
				
				SELECT
					@id_grupo = id_grupo,
					@descri_grupo = RTRIM(descricao)

				FROM 
					@hold_id_grupo 
				
				WHERE 
					id = @count_grupo;

				-- Pegando os grupos atrelados ao tipo
				DELETE FROM
					@hold_id_subgrupo;

				INSERT INTO
					@hold_id_subgrupo
						(
							id_subgrupo,
							descricao,
							ranking,
							ordem
						)

				SELECT
					DISTINCT s.id_subgrupo,
					s.descricao,
					s.ranking,
					CASE WHEN ISNULL(s.ranking, 0) = 0 THEN 0 ELSE 1 END

				FROM
					PRODUTO p

					INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
						p.id_produto = pd.id_produto

					INNER JOIN PRODUTO_SUBGRUPO ps ON
						pd.id_produto = ps.codigo_produto

					INNER JOIN SUBGRUPO s ON
						ps.id_subgrupo = s.id_subgrupo
						AND ps.id_distribuidor = s.id_distribuidor

					INNER JOIN GRUPO_SUBGRUPO gs ON
						s.id_subgrupo = gs.id_subgrupo

				WHERE
					ps.id_distribuidor = @id_distribuidor
					AND gs.id_grupo = @id_grupo
					AND LEN(p.sku) > 0
					AND gs.status = 'A'
					AND s.status = 'A'
					AND ps.status = 'A'
					AND pd.status = 'A'
					AND p.status = 'A'

				ORDER BY
					CASE WHEN ISNULL(s.ranking, 0) = 0 THEN 0 ELSE 1 END DESC,
					s.ranking ASC,
					s.descricao;

				IF EXISTS (SELECT TOP 1 1 FROM @hold_id_subgrupo)
				BEGIN

					-- Criando o json do subgrupo
					SET @id_subgrupo_string = (
													SELECT
														id_subgrupo,
														RTRIM(descricao) AS descricao_subgrupo

													FROM
														@hold_id_subgrupo

													ORDER BY
														descricao

													FOR
														JSON AUTO
											   );

					SET @id_subgrupo_string = REPLACE(@id_subgrupo_string, '\/', '/');

					SET @id_grupo_string = '{"id_grupo":' + CONVERT(VARCHAR(10), @id_grupo) +
											',"descricao_grupo":"' + @descri_grupo + '","subgrupo": ' + 
											@id_subgrupo_string + '}';

					INSERT INTO
						@hold_grupo

						VALUES
							(
								@id_grupo_string
							);
					
				END

				SET @count_grupo = @count_grupo + 1;
			END

		END

		IF EXISTS (SELECT TOP 1 1 FROM @hold_grupo)
		BEGIN

			-- Criando o json do subgrupo
			SET @id_grupo_string = (
										SELECT
											grupo + ','

										FROM
											@hold_grupo

										FOR
											XML PATH('')
									);

			DELETE FROM @hold_grupo;

			SET @id_grupo_string = SUBSTRING(@id_grupo_string, 1, LEN(@id_grupo_string) - 1)
			SET @id_grupo_string = '[' + @id_grupo_string + ']';

			INSERT INTO
				#HOLD_DATA
					(
						id_tipo,
						descricao,
						grupos
					)

				VALUES
					(
						@id_tipo,
						@descri_tipo,
						@id_grupo_string
					);

		END

		SET @count_tipo = @count_tipo + 1;
	END
	
END

-- Mostrando o resultado final
SELECT
	thd.id_tipo,
	thd.descricao as descricao_tipo,
	t.ranking,
	thd.grupos as grupo

FROM
	#HOLD_DATA thd

	INNER JOIN TIPO t ON
		thd.id_tipo = t.id_tipo

ORDER BY
	thd.id;

-- Deletando as tabelas temporarias
DROP TABLE #HOLD_DATA;