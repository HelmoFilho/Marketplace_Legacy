-- USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
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

-- Variaveis de controle
DECLARE @id_distribuidor INT = 0;
DECLARE @id_lista INT = 3;
DECLARE @objeto VARCHAR(MAX) = NULL;
DECLARE @data_1 VARCHAR(10) = '1900-01-01'
DECLARE @data_2 VARCHAR(10) = '3000-01-01'
DECLARE @status VARCHAR(1) = 'A'
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

-- Salvando os produtos da lista
SELECT
	p.id_produto,
    p.sku,
    p.descricao_completa as descricao_produto,
    pd.cod_prod_distr,
    pd.agrupamento_variante,
    SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
    lpr.status

INTO
	#HOLD_PRODUTO

FROM
	LISTA_PRODUTOS lp
                
    INNER JOIN LISTA_PRODUTOS_RELACAO lpr ON
        lp.id_lista = lpr.id_lista
                    
    INNER JOIN PRODUTO p ON
        lpr.sku = p.sku

    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        p.id_produto = pd.id_produto

WHERE
	lp.d_e_l_e_t_ = 0
	AND p.sku IS NOT NULL
	AND LEN(p.sku) > 0
    AND lp.status = @status
    AND lpr.status = @status
    AND p.status = @status
    AND pd.status = @status
    AND lp.id_lista = @id_lista
    AND lp.id_distribuidor = @id_distribuidor
    AND pd.id_distribuidor = CASE
                                WHEN @id_distribuidor = 0
                                    THEN pd.id_distribuidor
                                ELSE
                                    @id_distribuidor
                             END
    AND lp.data_criacao BETWEEN @data_1 AND @data_2

-- Filtrando pelo objeto
IF @objeto IS NOT NULL
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
			VALUE != ' ';

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

-- Mostra o Resultado Final
SELECT
	*,
	COUNT(*) OVER() AS 'COUNT__'

FROM
	#HOLD_PRODUTO

ORDER BY
	descricao_produto ASC

OFFSET
	@offset ROWS

FETCH
	NEXT @limite ROWS ONLY;

-- Deleta tabelas temporarias
DROP TABLE #HOLD_PRODUTO;
DROP TABLE #HOLD_STRING;