-- USE [B2BTESTE2]

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

-- Criando a tabela temporaria de palavras
CREATE TABLE #HOLD_STRING
(
	id INT IDENTITY(1,1),
	string VARCHAR(100)
);

-- Declarando variáveis locais
DECLARE @count_pendente INT;
DECLARE @count_aprovado INT;
DECLARE @count_reprovado INT;
DECLARE @count_total INT;
            
-- Declarando variáveis de controle
DECLARE @status VARCHAR(1) = 'A';
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;
DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';
DECLARE @objeto VARCHAR(MAX) = 'IMPALA';

-- Realizando a query
SELECT
    ap.id_distribuidor,
    ap.id_produto,
    ap.agrupamento_variante,
    ap.descr_reduzida_distr,
    ap.descr_completa_distr,
    ap.cod_prod_distr,
    ap.cod_frag_distr,
    ap.sku,
    ap.dun14,
    ap.status,
    ap.tipo_produto,
    ap.cod_marca,
    ap.descr_marca,
    ap.cod_grupo,
    ap.descr_grupo,
    ap.cod_subgrupo,
    ap.descr_subgrupo,
    ap.variante,
    ap.multiplo_venda,
    ap.unidade_venda,
    ap.quant_unid_venda,
    ap.unidade_embalagem,
    ap.quantidade_embalagem,
    ap.ranking,
    ap.giro,
    ap.agrup_familia,
    ap.volumetria,
    ap.cod_fornecedor,
    ap.descri_fornecedor,
    ap.dt_insert,
    ap.dt_update,
    ap.status_aprovado,
    ap.usuario_aprovado,
    ap.url_imagem,
    ap.motivo_reprovado,
    d.nome_fantasia
                
INTO 
    #HOLD_PRODUTO

FROM
    API_PRODUTO ap

    INNER JOIN DISTRIBUIDOR d ON
        ap.id_distribuidor = d.id_distribuidor

WHERE
    ap.sku IS NOT NULL 
	AND ap.sku != ''
	AND LEN(ap.sku) > 0
    AND ap.dt_insert BETWEEN @date_1 AND @date_2
    AND (
            (0 IN (0))
                OR
            (0 NOT IN (0) AND ap.id_distribuidor IN (0))
        );

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

										INNER JOIN API_PRODUTO ap ON
											thp.id_produto = ap.id_produto

									WHERE
										ap.descr_completa_distr COLLATE Latin1_General_CI_AI LIKE @word
										OR ap.sku LIKE @word
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

-- Pegando os valores do contadores
SET @count_aprovado  = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'A');
SET @count_pendente  = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'P');
SET @count_reprovado = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'R');
SET @count_total = @count_aprovado + @count_pendente + @count_reprovado;

-- Vendo o resultado
SELECT
    hr.*,
    @count_total as "count__",
    @count_aprovado as "count_aprovado",
    @count_pendente as "count_pendente",
    @count_reprovado as "count_reprovado"

FROM
    #HOLD_PRODUTO as hr

WHERE
    hr.status_aprovado = @status

ORDER BY
    hr.dt_insert ASC

OFFSET
    @offset ROWS

FETCH
    NEXT @limite ROWS ONLY;

-- Deleta a tabela temporaria
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_PRODUTO;