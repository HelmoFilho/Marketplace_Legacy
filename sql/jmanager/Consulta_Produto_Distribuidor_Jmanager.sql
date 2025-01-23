USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporária existe
IF Object_ID('tempDB..#HOLD_PRODUTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_PRODUTO
END;

IF OBJECT_ID('tempDB..#HOLD_STRING', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_STRING
END;

IF OBJECT_ID('tempDB..#HOLD_DISTRIBUIDOR', 'U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_DISTRIBUIDOR
END;

-- Criando a tabela temporaria de palavras
CREATE TABLE #HOLD_STRING
(
	id INT IDENTITY(1,1),
	string VARCHAR(100)
);
            
-- Declarando variáveis de controle
DECLARE @status VARCHAR(1) = 'A';

DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';

DECLARE @id_distribuidor VARCHAR(MAX) = '0';
DECLARE @check_distribuidor BIT = 0;

DECLARE @id_tipo INT = NULL;
DECLARE @id_grupo INT = NULL;
DECLARE @id_subgrupo INT = NULL;

DECLARE @objeto VARCHAR(MAX) = NULL;

DECLARE @offset INT = 0;
DECLARE @limite INT = 1000;

-- Pegando os ids de distribuidores
SELECT
	VALUE as id_distribuidor

INTO
	#HOLD_DISTRIBUIDOR

FROM
	STRING_SPLIT(@id_distribuidor, ',')

WHERE
	LEN(VALUE) > 0;

IF 0 IN (SELECT id_distribuidor FROM #HOLD_DISTRIBUIDOR)
BEGIN
	SET @check_distribuidor = 1;
END

-- Realizando a query
SELECT 
    pd.id_distribuidor,
    pd.id_produto
    
INTO
	#HOLD_PRODUTO

FROM
    PRODUTO p 
                
    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        p.id_produto = pd.id_produto

WHERE
    pd.status = CASE
                    WHEN @status IS NOT NULL
                        THEN @status
                    ELSE
                        pd.status
                END
	AND p.status = CASE
					   WHEN @status IS NOT NULL
						   THEN @status
					   ELSE
						   p.status
				   END
	AND LEN(p.sku) > 0
    AND pd.data_cadastro BETWEEN @date_1 AND @date_2
	AND (
			(@check_distribuidor = 1)
				OR
			(@check_distribuidor = 0 AND pd.id_distribuidor IN (SELECT id_distribuidor FROM #HOLD_DISTRIBUIDOR))
		);

-- Filtrando pelo objeto
IF @objeto IS NOT NULL OR LEN(@objeto) > 0
BEGIN

	-- Removendo espacos em branco adicionais e salvando as palavras individualmente
	INSERT INTO
		#HOLD_STRING
			(
				string
			)

		SELECT
			VALUE

		FROM
			STRING_SPLIT(TRIM(@objeto), ' ')

		WHERE
			VALUE != ' '
			AND LEN(VALUE) > 0

	-- Setando os contadores
	DECLARE @max_count INT = (SELECT MAX(id) FROM #HOLD_STRING) + 1
	DECLARE @actual_count INT = 1
	DECLARE @word VARCHAR(100)

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
										thp.id_produto

									FROM
										#HOLD_PRODUTO thp

										INNER JOIN PRODUTO p ON
											thp.id_produto = p.id_produto

									WHERE
										p.descricao_completa COLLATE Latin1_General_CI_AI LIKE @word
										OR p.sku LIKE @word
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

-- Filtrando pelo tipo/grupo/subgrupo
IF (@id_tipo IS NOT NULL) OR (@id_grupo IS NOT NULL) OR (@id_subgrupo IS NOT NULL)
BEGIN
	
	DELETE FROM
		#HOLD_PRODUTO

	WHERE
		id_produto NOT IN (
								SELECT
									thp.id_produto

								FROM
									#HOLD_PRODUTO thp

									INNER JOIN PRODUTO_SUBGRUPO ps ON
										thp.id_produto = ps.codigo_produto

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
									t.status = 'A'
									AND g.status = 'A'
									AND gs.status = 'A'
									AND s.status = 'A'
									AND ps.status = 'A'
									AND t.id_tipo = CASE
														WHEN @id_tipo IS NOT NULL
															THEN @id_tipo
														ELSE
															t.id_tipo
													END
									AND g.id_grupo = CASE
														 WHEN @id_grupo IS NOT NULL
															 THEN @id_grupo
														 ELSE
															 g.id_grupo
													 END
									AND s.id_subgrupo = CASE
															WHEN @id_subgrupo IS NOT NULL
																THEN @id_subgrupo
															ELSE
																s.id_subgrupo
														END
							)

END

-- Vendo o resultado
SELECT
    pd.id_distribuidor,
    d.nome_fantasia as nome_distribuidor,
    pd.id_produto,
    pd.agrupamento_variante,
    pd.cod_prod_distr,
    SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
    pd.multiplo_venda,
    pd.ranking,
    pd.unidade_venda,
    pd.quant_unid_venda,
    pd.giro,
    pd.agrup_familia,
    pd.status,
    p.descricao_completa as descr_completa_distr,
    p.sku,    
    pe.qtd_estoque as quantidade_estoque,
    p.volumetria,
    p.variante,
    p.unidade_embalagem,
    p.quantidade_embalagem,
    p.dun14,
    p.tipo_produto,
	t.id_tipo as cod_tipo,
    t.descricao as descr_tipo,
    g.id_grupo as cod_grupo,
    g.descricao as descr_grupo,
    s.id_subgrupo as cod_subgrupo,
    s.descricao as descr_subgrupo,
    m.id_marca as cod_marca,
    m.desc_marca as descr_marca,
    f.id_fornecedor as cod_fornecedor,
    f.desc_fornecedor as descri_fornecedor,
    pd.data_cadastro,
    COUNT(*) OVER() AS 'count__'

FROM
    #HOLD_PRODUTO as thp

	INNER JOIN PRODUTO p ON
		thp.id_produto = p.id_produto
                
    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        p.id_produto = pd.id_produto

	INNER JOIN MARCA m ON
		pd.id_marca = m.id_marca

	INNER JOIN FORNECEDOR f ON
		pd.id_fornecedor = f.id_fornecedor

	INNER JOIN PRODUTO_ESTOQUE pe ON
		p.id_produto = pe.id_produto

	INNER JOIN DISTRIBUIDOR d ON
		pd.id_distribuidor = d.id_distribuidor

	INNER JOIN SUBGRUPO s ON
		pd.id_subgrupo = s.id_subgrupo
		AND pd.id_distribuidor = s.id_distribuidor
		
	INNER JOIN GRUPO g ON
		pd.id_grupo = g.id_grupo

	INNER JOIN TIPO t ON
		g.tipo_pai = t.id_tipo

ORDER BY
    thp.id_distribuidor ASC,
	CASE WHEN pd.ranking > 0 THEN 1 ELSE 0 END DESC,
	pd.ranking,
    p.data_cadastro ASC

OFFSET
    @offset ROWS

FETCH
    NEXT @limite ROWS ONLY;

-- Deleta a tabela temporaria
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_PRODUTO;
DROP TABLE #HOLD_DISTRIBUIDOR;