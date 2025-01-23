USE [B2BTESTE2]

SET NOCOUNT OFF;
SET STATISTICS IO ON;

-- Verificando se a tabela tempor�ria existe
IF OBJECT_ID('tempDB..#HOLD_PRODUTO', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_PRODUTO
END;

IF OBJECT_ID('tempDB..#HOLD_DATA', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_DATA
END;

-- Criando as tabelas
CREATE TABLE #HOLD_PRODUTO
(
	id_produto VARCHAR(100),
	id_distribuidor INT,
	ranking INT,
	descricao_completa VARCHAR(MAX)
);

CREATE TABLE #HOLD_DATA
(
	id_tipo INT,
	id_grupo INT,
	id_subgrupo INT,
	id_distribuidor INT
);

-- Declaracao de variaveis globais
--## Variaveis de entrada
DECLARE @id_tipo INT = NULL;
DECLARE @id_grupo INT = NULL;

DECLARE @id_distribuidor VARCHAR(MAX) = '0'

DECLARE @check_produto BIT = 1;

DECLARE @status CHAR(1) = NULL;

DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

--## Variaveis de controle
DECLARE @check_distribuidor BIT = 0;

--## Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT,
	nome_fantasia VARCHAR(1000)
);

DECLARE @id_tipo_hold TABLE (
	id_tipo INT,
	id_distribuidor INT
);

DECLARE @id_grupo_hold TABLE (
	id_grupo INT,
	id_distribuidor INT
);

DECLARE @id_subgrupo_hold TABLE (
	id_subgrupo INT,
	id_distribuidor INT
);

-- Verificando os distribuidores do usuario
INSERT INTO
	@id_distribuidor_hold

	SELECT
		nda.value,
		UPPER(RTRIM(d.nome_fantasia))

	FROM
		STRING_SPLIT(@id_distribuidor, ',') as nda
		
		INNER JOIN DISTRIBUIDOR d ON
			nda.value = d.id_distribuidor
			
	WHERE
		d.status = 'A';

IF 0 IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
BEGIN
	SET @check_distribuidor = 1;
END

-- Verificando o que se quer
IF @check_produto = 0
BEGIN

	-- Subgrupos
	IF @id_grupo IS NOT NULL
	BEGIN

		INSERT INTO
			@id_subgrupo_hold
				(
					id_subgrupo,
					id_distribuidor
				)

			SELECT
				s.id_subgrupo,
				s.id_distribuidor

			FROM
				SUBGRUPO s

				INNER JOIN GRUPO_SUBGRUPO gs ON
					s.id_subgrupo = gs.id_subgrupo
					AND s.id_distribuidor = gs.id_distribuidor

				INNER JOIN GRUPO g ON
					gs.id_grupo = g.id_grupo
					AND gs.id_distribuidor = g.id_distribuidor
	
				INNER JOIN TIPO t ON
					g.tipo_pai = t.id_tipo
					AND g.id_distribuidor = t.id_distribuidor

			WHERE
				s.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
				AND g.id_grupo = @id_grupo
				AND t.id_tipo = CASE WHEN @id_tipo IS NULL THEN t.id_tipo ELSE @id_tipo END
				AND t.status = CASE WHEN @status IS NULL THEN t.status ELSE @status END
				AND g.status = CASE WHEN @status IS NULL THEN g.status ELSE @status END
				AND gs.status = CASE WHEN @status IS NULL THEN gs.status ELSE @status END
				AND s.status = CASE WHEN @status IS NULL THEN s.status ELSE @status END;		

	END

	-- Grupos
	ELSE IF @id_tipo IS NOT NULL
	BEGIN

		INSERT INTO
			@id_grupo_hold
				(
					id_grupo,
					id_distribuidor
				)

			SELECT
				g.id_grupo,
				g.id_distribuidor

			FROM
				GRUPO g
	
				INNER JOIN TIPO t ON
					g.tipo_pai = t.id_tipo
					AND g.id_distribuidor = t.id_distribuidor

			WHERE
				g.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
				AND g.tipo_pai = @id_tipo
				AND t.status = CASE WHEN @status IS NULL THEN t.status ELSE @status END
				AND g.status = CASE WHEN @status IS NULL THEN g.status ELSE @status END;

	END

	-- Tipos
	ELSE
	BEGIN

		INSERT INTO
			@id_tipo_hold
				(
					id_tipo,
					id_distribuidor
				)

			SELECT
				t.id_tipo,
				t.id_distribuidor

			FROM
				TIPO t

			WHERE
				t.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
				AND t.status = CASE WHEN @status IS NULL THEN t.status ELSE @status END;

	END

END

ELSE
BEGIN

	-- Salvando os produtos
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
			p.status = 'A'
			AND pd.status = 'A'
			AND (
					(
						@check_distribuidor = 1
					)
						OR
					(
						@check_distribuidor = 0
						AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
					)
				);

	-- Verificando o que deve ser enviado
	INSERT INTO
		#HOLD_DATA
			(
				id_tipo,
				id_grupo,
				id_subgrupo,
				id_distribuidor
			)

		SELECT
			t.id_tipo,
			g.id_grupo,
			s.id_subgrupo,
			ps.id_distribuidor

		FROM
			PRODUTO_SUBGRUPO ps
			
			INNER JOIN SUBGRUPO s ON
				ps.id_subgrupo = s.id_subgrupo
				AND ps.id_distribuidor = s.id_distribuidor

			INNER JOIN GRUPO_SUBGRUPO gs ON
				s.id_subgrupo = gs.id_subgrupo
				AND s.id_distribuidor = gs.id_distribuidor

			INNER JOIN GRUPO g ON
				gs.id_grupo = g.id_grupo
	
			INNER JOIN TIPO t ON
				g.tipo_pai = t.id_tipo

		WHERE
			ps.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
			AND t.id_tipo = CASE WHEN @id_tipo IS NULL THEN t.id_tipo ELSE @id_tipo END
			AND g.id_grupo = CASE WHEN @id_grupo IS NULL THEN g.id_grupo ELSE @id_grupo END
			AND t.status = CASE WHEN @status IS NULL THEN t.status ELSE @status END
			AND g.status = CASE WHEN @status IS NULL THEN g.status ELSE @status END
			AND gs.status = CASE WHEN @status IS NULL THEN gs.status ELSE @status END
			AND s.status = CASE WHEN @status IS NULL THEN s.status ELSE @status END
			AND ps.status = CASE WHEN @status IS NULL THEN ps.status ELSE @status END;

	IF @id_grupo IS NOT NULL
	BEGIN

		INSERT INTO
			@id_subgrupo_hold
				(
					id_subgrupo,
					id_distribuidor
				)

			SELECT
				DISTINCT thd.id_subgrupo,
				thd.id_distribuidor

			FROM
				#HOLD_DATA thd;				

	END

	ELSE IF @id_tipo IS NOT NULL
	BEGIN

		INSERT INTO
			@id_grupo_hold
				(
					id_grupo,
					id_distribuidor
				)

			SELECT
				DISTINCT thd.id_grupo,
				thd.id_distribuidor

			FROM
				#HOLD_DATA thd;

	END

	ELSE
	BEGIN

		INSERT INTO
			@id_tipo_hold
				(
					id_tipo,
					id_distribuidor
				)

			SELECT
				DISTINCT thd.id_tipo,
				thd.id_distribuidor

			FROM
				#HOLD_DATA thd;

	END

END

-- Visualizando o resultado
IF EXISTS (SELECT TOP 1 1 FROM @id_subgrupo_hold)
BEGIN

	SELECT
		vish.id_subgrupo,
		UPPER(RTRIM(s.descricao)) as descricao,
		vish.id_distribuidor,
		vidh.nome_fantasia,
		s.status,
		s.data_cadastro

	FROM
		@id_subgrupo_hold as vish

		INNER JOIN SUBGRUPO s ON
			vish.id_subgrupo = s.id_subgrupo
			AND vish.id_distribuidor = s.id_distribuidor

		INNER JOIN @id_distribuidor_hold vidh ON
			s.id_distribuidor = vidh.id_distribuidor

	ORDER BY
		s.descricao;

END

ELSE IF EXISTS (SELECT TOP 1 1 FROM @id_grupo_hold)
BEGIN

	SELECT
		vigh.id_grupo,
		UPPER(RTRIM(g.descricao)) as descricao,
		vigh.id_distribuidor,
		vidh.nome_fantasia,
		g.tipo_pai,
		g.status,
		g.data_cadastro

	FROM
		@id_grupo_hold as vigh

		INNER JOIN GRUPO g ON
			vigh.id_grupo = g.id_grupo
			AND vigh.id_distribuidor = g.id_distribuidor

		INNER JOIN @id_distribuidor_hold vidh ON
			g.id_distribuidor = vidh.id_distribuidor

	ORDER BY
		g.descricao;

END

ELSE 
BEGIN

	SELECT
		vith.id_tipo,
		UPPER(RTRIM(t.descricao)) as descricao,
		vith.id_distribuidor,
		vidh.nome_fantasia,
		t.status,
		t.padrao,
		t.data_cadastro

	FROM
		@id_tipo_hold as vith

		INNER JOIN TIPO t ON
			vith.id_tipo = t.id_tipo
			AND vith.id_distribuidor = t.id_distribuidor

		INNER JOIN @id_distribuidor_hold vidh ON
			t.id_distribuidor = vidh.id_distribuidor

	ORDER BY
		t.descricao;

END

-- Deleta tabelas temporarias
DROP TABLE #HOLD_PRODUTO;
DROP TABLE #HOLD_DATA;