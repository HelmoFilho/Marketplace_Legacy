USE [B2BTESTE2]

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

CREATE TABLE #HOLD_USUARIO
(
	id INT IDENTITY(1,1),
    id_usuario int,
	nome varchar(100) COLLATE Latin1_General_CI_AS NULL,
	cpf nchar(11) COLLATE Latin1_General_CI_AS NULL,
	status char(1) COLLATE Latin1_General_CI_AS NULL,
	email varchar(50) COLLATE Latin1_General_CI_AS NULL,
	telefone nchar(20) COLLATE Latin1_General_CI_AS NULL,
	data_cadastro date NULL,
	aceite_termos char(1) COLLATE Latin1_General_CI_AS NULL,
	data_aceite datetime NULL
);
                        
-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_usuario INT = NULL;
DECLARE @status VARCHAR(1) = NULL;
DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';
DECLARE @nome VARCHAR(MAX) = NULL;
DECLARE @id_distribuidor VARCHAR(MAX) = NULL;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

--## Variaveis de apoio
DECLARE @max_count INT;
DECLARE @actual_count INT;
DECLARE @word VARCHAR(100);

--## Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT
);

-- Pegando os distribuidores



-- Realizando a query
INSERT INTO
	#HOLD_USUARIO
		(
			id_usuario,
			nome,
			cpf,
			status,
			email,
			telefone,
			data_cadastro,
			aceite_termos,
			data_aceite
		)

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

	FROM
		USUARIO u
	
		INNER JOIN USUARIO_CLIENTE uc ON
			u.id_usuario = uc.id_usuario

		INNER JOIN CLIENTE c ON
			uc.id_cliente = c.id_cliente

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
		AND u.d_e_l_e_t_ = 0
		AND uc.d_e_l_e_t_ = 0;

-- Filtrando pelo objeto
IF LEN(@nome) > 0
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
	SELECT 
		@max_count = MAX(id),
		@actual_count = MIN(id)
		
	FROM 
		#HOLD_STRING

    WHILE @actual_count <= @max_count
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
    thu.id_usuario,
    thu.nome,
    thu.cpf,
    thu.status,
    thu.email,
    thu.telefone,
    thu.data_cadastro,
    thu.aceite_termos,
    thu.data_aceite,
    COUNT(*) OVER() AS 'count__'

FROM
    #HOLD_USUARIO as thu

ORDER BY
    thu.id_usuario ASC

-- Deleta a tabela temporaria
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_USUARIO;