-- USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporária existe
IF Object_ID('tempDB..#HOLD_CLIENTE','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_CLIENTE
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
            
-- Declarando variáveis de controle
DECLARE @status VARCHAR(1) = 'A';
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;
DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';
DECLARE @objeto VARCHAR(MAX) = '07458188000165';

-- Realizando a query
SELECT 
    c.id_cliente,
    c.status,
    c.cnpj,
    c.razao_social,
    c.nome_fantasia,
    c.endereco,
    c.endereco_num,
    c.endereco_complemento,
    c.bairro,
    c.cep,
    c.cidade,
    c.estado,
    c.telefone,
    c.data_cadastro,
    c.data_aprovacao,
    c.status_receita

INTO
	#HOLD_CLIENTE

FROM
    CLIENTE c

WHERE
    c.id_cliente in (1,2)
    AND c.data_cadastro BETWEEN @date_1 AND @date_2
    AND c.status = CASE
                        WHEN @status IS NOT NULL
                            THEN @status
                        ELSE
                            c.status
                   END

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
			#HOLD_CLIENTE

		WHERE
			id_cliente NOT IN (
									SELECT
										id_cliente

									FROM
										#HOLD_CLIENTE thc

									WHERE
										thc.cnpj LIKE @word
										OR thc.razao_social COLLATE Latin1_General_CI_AI LIKE @word
										OR thc.nome_fantasia COLLATE Latin1_General_CI_AI LIKE @word
								)

		-- Verificando se ainda existem registros
		IF EXISTS(SELECT 1 FROM #HOLD_CLIENTE)
		BEGIN
			SET @actual_count = @actual_count + 1
		END

		ELSE
		BEGIN
			SET @actual_count = @max_count
		END

	END

END

-- Vendo o resultado
SELECT
    thc.*,
    COUNT(*) OVER() AS 'count__'

FROM
    #HOLD_CLIENTE as thc

ORDER BY
    thc.nome_fantasia ASC

OFFSET
    @offset ROWS

FETCH
    NEXT @limite ROWS ONLY;

-- Deleta a tabela temporaria
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_CLIENTE;