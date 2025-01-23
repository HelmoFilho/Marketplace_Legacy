-- USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporária existe
IF Object_ID('tempDB..#HOLD_USUARIO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_USUARIO
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
DECLARE @id_usuario INT = '1';
DECLARE @status VARCHAR(1) = 'A';
-- DECLARE @offset INT = 0;
-- DECLARE @limite INT = 20;
DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';
DECLARE @nome VARCHAR(MAX) = 'JOSÉ HELMO SILVA DOS SANTOS FILHO';

-- Realizando a query
SELECT 
    DISTINCT u.id_usuario,
    u.nome,
    u.cpf,
    u.status,
    u.email,
    u.telefone,
    u.data_cadastro,
    u.aceite_termos,
    u.data_aceite

INTO
	#HOLD_USUARIO

FROM
    USUARIO u

WHERE
	u.id_usuario = CASE
						WHEN @id_usuario IS NULL
							THEN u.id_usuario
						ELSE
							@id_usuario
				   END
    AND u.status = CASE
                        WHEN @status IS NOT NULL
                            THEN @status
                        ELSE
                            u.status
                   END
    AND u.data_cadastro BETWEEN @date_1 AND @date_2

-- Filtrando pelo objeto
IF @nome IS NOT NULL
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
			STRING_SPLIT(TRIM(@nome), ' ')

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
			#HOLD_USUARIO

		WHERE
			id_usuario NOT IN (
									SELECT
										id_usuario

									FROM
										#HOLD_USUARIO

									WHERE
										nome COLLATE Latin1_General_CI_AI LIKE @word
								)

		-- Verificando se ainda existem registros
		IF EXISTS(SELECT 1 FROM #HOLD_USUARIO)
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
    thu.*,
    COUNT(*) OVER() AS 'count__'

FROM
    #HOLD_USUARIO as thu

ORDER BY
    thu.id_usuario ASC

--OFFSET
--    @offset ROWS

--FETCH
--    NEXT @limite ROWS ONLY;

-- Deleta a tabela temporaria
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_USUARIO;