USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporária existe
IF Object_ID('tempDB..#HOLD_DISTRIBUIDOR','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_DISTRIBUIDOR
END;

IF Object_ID('tempDB..#HOLD_CLIENTE','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_CLIENTE
END;

IF Object_ID('tempDB..#HOLD_TEMP_CLIENTE','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_TEMP_CLIENTE
END;

IF OBJECT_ID('tempDB..#HOLD_STRING', 'U') IS NOT NULL
BEGIN
    DROP TABLE #HOLD_STRING
END;

IF OBJECT_ID('tempDB..#HOLD_USUARIO', 'U') IS NOT NULL
BEGIN
    DROP TABLE #HOLD_USUARIO
END;

-- Criando tabelas temporarias
CREATE TABLE #HOLD_STRING
(
    id INT IDENTITY(1,1),
    string VARCHAR(100)
);

CREATE TABLE #HOLD_USUARIO
(
    id_usuario INT,
	nome VARCHAR(100) COLLATE Latin1_General_CI_AS NULL,
	cpf NCHAR(11) COLLATE Latin1_General_CI_AS NULL,
	status CHAR(1) COLLATE Latin1_General_CI_AS NULL,
	email VARCHAR(50) COLLATE Latin1_General_CI_AS NULL,
	telefone NCHAR(20) COLLATE Latin1_General_CI_AS NULL,
	data_cadastro date NULL,
	aceite_termos CHAR(1) COLLATE Latin1_General_CI_AS NULL,
	data_aceite DATETIME NULL
);

CREATE TABLE #HOLD_DISTRIBUIDOR
(
    id_distribuidor INT,
	nome_fantasia VARCHAR(1000),
	razao_social VARCHAR(1000)
);

CREATE TABLE #HOLD_CLIENTE
(
	id INT IDENTITY(1,1),
	id_cliente INT,
    status CHAR(1),
    cnpj VARCHAR(20),
    razao_social VARCHAR(1000),
    nome_fantasia VARCHAR(1000),
    endereco VARCHAR(1000),
    endereco_num INT,
    endereco_complemento VARCHAR(1000),
    bairro VARCHAR(1000),
    cep VARCHAR(8),
    cidade VARCHAR(50),
    estado VARCHAR(2),
    telefone VARCHAR(20),
    data_cadastro DATE,
    data_aprovacao DATE,
    status_receita CHAR(1),
    relacao_cliente_distribuidor VARCHAR(MAX),
	relacao_cliente_usuario VARCHAR(MAX)
);

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_cliente INT;
DECLARE @id_usuario INT;
DECLARE @id_distribuidor VARCHAR(MAX) = '0';
DECLARE @status VARCHAR(1) = NULL;
DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';
DECLARE @busca VARCHAR(MAX) = NULL;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

--## Variaveis de apoio
DECLARE @check_distribuidor BIT = 0;
DECLARE @json_usuario VARCHAR(MAX);
DECLARE @json_distribuidor VARCHAR(MAX);
DECLARE @max_count INT;
DECLARE @actual_count INT;
DECLARE @word VARCHAR(MAX);
DECLARE @actual_cliente INT;
DECLARE @count INT;

--## Variaveis tabelas
DECLARE @distribuidor_input TABLE (
	id_distribuidor INT
);

DECLARE @hold_json_distribuidor TABLE (
	id_distribuidor INT,
	nome_fantasia VARCHAR(1000),
	razao_social VARCHAR(1000),
	status_cliente_distribuidor CHAR(1)
);

DECLARE @hold_json_usuario TABLE (
	id_usuario INT,
	nome VARCHAR(100) COLLATE Latin1_General_CI_AS NULL,
	cpf NCHAR(11) COLLATE Latin1_General_CI_AS NULL,
	status CHAR(1) COLLATE Latin1_General_CI_AS NULL,
	email VARCHAR(50) COLLATE Latin1_General_CI_AS NULL,
	telefone NCHAR(20) COLLATE Latin1_General_CI_AS NULL,
	data_cadastro date NULL,
	aceite_termos CHAR(1) COLLATE Latin1_General_CI_AS NULL,
	data_aceite DATETIME NULL,
	status_usuario_cliente CHAR(1)
);

-- Salvando os distribuidores
INSERT INTO
	@distribuidor_input

	SELECT
		VALUE

	FROM
		STRING_SPLIT(@id_distribuidor, ',')

	WHERE
		LEN(VALUE) > 0;

IF 0 IN (SELECT id_distribuidor FROM @distribuidor_input)
BEGIN
	SET @check_distribuidor = 1;
END

INSERT INTO
	#HOLD_DISTRIBUIDOR

	SELECT
		id_distribuidor,
		RTRIM(UPPER(nome_fantasia)) as nome_fantasia,
		RTRIM(UPPER(razao_social)) as razao_social

	FROM
		DISTRIBUIDOR

	WHERE
		(
			(
				@check_distribuidor = 1
			)
				OR
			(
				@check_distribuidor = 0
				AND id_distribuidor IN (SELECT id_distribuidor FROM @distribuidor_input)
			)
		)
		AND id_distribuidor != 0
		AND status = 'A';

-- Salvando os clientes temporariamentes
SELECT
	DISTINCT c.id_cliente,
	c.cnpj,
    c.razao_social,
    c.nome_fantasia

INTO
	#HOLD_TEMP_CLIENTE

FROM
	CLIENTE c

	INNER JOIN USUARIO_CLIENTE uc ON
		c.id_cliente = uc.id_cliente

	INNER JOIN USUARIO u ON
		uc.id_usuario = u.id_usuario
	
WHERE
	c.id_cliente = CASE
					   WHEN @id_cliente IS NULL
						   THEN c.id_cliente
					   ELSE
						   @id_cliente
				   END
	AND u.id_usuario = CASE
						   WHEN @id_usuario IS NULL
							   THEN u.id_usuario
						   ELSE
							   @id_usuario
					   END
	AND c.status =  CASE
					    WHEN @status IS NULL
						    THEN c.status
					    ELSE
							@status
				    END
	AND c.data_cadastro BETWEEN @date_1 AND @date_2
	AND uc.d_e_l_e_t_ = 0
	AND u.d_e_l_e_t_ = 0;


-- Filtrando pelo campo de busca
IF LEN(@busca) > 0
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
            STRING_SPLIT(TRIM(@busca), ' ')

        WHERE
            LEN(VALUE) > 0;

    -- Setando os contadores
	SELECT 
		@max_count = MAX(id),
		@actual_count = MIN(id)
		
	FROM 
		#HOLD_STRING;

    WHILE @actual_count <= @max_count
    BEGIN

        -- Pegando palavra atual
        SET @word = '%' + (SELECT TOP 1 string FROM #HOLD_STRING WHERE id = @actual_count) + '%'

        -- Fazendo query na palavra atual
        DELETE FROM
            #HOLD_TEMP_CLIENTE

        WHERE
            id_cliente NOT IN (
                                    SELECT
                                        thtc.id_cliente

                                    FROM
                                        #HOLD_TEMP_CLIENTE thtc

                                    WHERE
                                        thtc.razao_social COLLATE Latin1_General_CI_AI LIKE @word
										OR thtc.nome_fantasia COLLATE Latin1_General_CI_AI LIKE @word
										OR thtc.cnpj LIKE @word
                              )

        -- Verificando se ainda existem registros
        IF EXISTS(SELECT 1 FROM #HOLD_TEMP_CLIENTE)
        BEGIN
            SET @actual_count = @actual_count + 1;
        END

        ELSE
        BEGIN
            SET @actual_count = @max_count + 1;
        END

    END

END

-- Salvando os clientes
INSERT INTO
	#HOLD_CLIENTE
		(
			id_cliente
		)

	SELECT
		id_cliente

	FROM
		#HOLD_TEMP_CLIENTE

	ORDER BY
		id_cliente

	OFFSET
		@offset ROWS

	FETCH
		NEXT @limite ROWS ONLY;


SET @count = (SELECT COUNT(*) FROM #HOLD_TEMP_CLIENTE);

-- Salvando os usuarios dos clientes
INSERT INTO
	#HOLD_USUARIO
		(
			id_usuario
		)

	SELECT
		DISTINCT u.id_usuario

	FROM
		USUARIO u

		INNER JOIN USUARIO_CLIENTE uc ON
			u.id_usuario = uc.id_usuario

		INNER JOIN #HOLD_CLIENTE thc ON
			uc.id_cliente = thc.id_cliente

	WHERE
		uc.d_e_l_e_t_ = 0
		AND u.d_e_l_e_t_ = 0;

UPDATE
	#HOLD_USUARIO

SET
	nome = UPPER(RTRIM(u.nome)),
	cpf = RTRIM(u.cpf),
	status = UPPER(RTRIM(u.status)),
	email = UPPER(RTRIM(u.email)),
	telefone = RTRIM(u.telefone),
	data_cadastro = RTRIM(u.data_cadastro),
	aceite_termos = UPPER(RTRIM(u.aceite_termos)),
	data_aceite = RTRIM(u.aceite_termos)
	
FROM
	#HOLD_USUARIO thu,	
	USUARIO u

WHERE
	thu.id_usuario = u.id_usuario;

-- Fazendo o loop de verificacao
SELECT 
	@max_count = MAX(id),
	@actual_count = MIN(id)
		
FROM 
	#HOLD_CLIENTE;

WHILE @actual_count <= @max_count
BEGIN

	-- Pegando o cliente atual
	SET @actual_cliente = (SELECT TOP 1 id_cliente FROM #HOLD_CLIENTE WHERE id = @actual_count);

	-- Salvando os usuarios e distribuidores do cliente
	DELETE FROM @hold_json_distribuidor;
	DELETE FROM @hold_json_usuario;

	INSERT INTO
		@hold_json_usuario

		SELECT
			DISTINCT thu.id_usuario,
			thu.nome,
			thu.cpf,
			thu.status,
			thu.email,
			thu.telefone,
			thu.data_cadastro,
			thu.aceite_termos,
			thu.data_aceite,
			uc.status as status_usuario_cliente

		FROM
			#HOLD_USUARIO thu

			INNER JOIN USUARIO_CLIENTE uc ON
				thu.id_usuario = uc.id_usuario

			INNER JOIN #HOLD_CLIENTE thc ON
				uc.id_cliente = thc.id_cliente

		WHERE
			thc.id_cliente = @actual_cliente
			AND uc.d_e_l_e_t_ = 0;

	SET @json_usuario = (
							SELECT
								*

							FROM
								@hold_json_usuario

							ORDER BY
								nome

							FOR
								JSON AUTO
						);

	INSERT INTO 
		@hold_json_distribuidor

		SELECT
			DISTINCT thd.id_distribuidor,
			thd.nome_fantasia,
			thd.razao_social,
			cd.status as status_cliente_distribuidor

		FROM
			#HOLD_CLIENTE thc

			INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
				thc.id_cliente = cd.id_cliente

			INNER JOIN #HOLD_DISTRIBUIDOR thd ON
				cd.id_distribuidor = thd.id_distribuidor

		WHERE
			thc.id_cliente = @actual_cliente
			AND cd.d_e_l_e_t_ = 0;


	SET @json_distribuidor = (
									SELECT
										*

									FROM
										@hold_json_distribuidor

									ORDER BY
										id_distribuidor

									FOR
										JSON AUTO
							 );

	IF @json_usuario IS NULL
	BEGIN
		SET @json_usuario = '[]';
	END

	IF @json_distribuidor IS NULL
	BEGIN
		SET @json_distribuidor = '[]';
	END

	-- Atualizando o registro do cliente
	UPDATE
		#HOLD_CLIENTE

	SET
		relacao_cliente_usuario = @json_usuario,
		relacao_cliente_distribuidor = @json_distribuidor

	WHERE
		id_cliente = @actual_cliente;

	-- Incrementando o contador
	SET @actual_count = @actual_count + 1;

END

-- Mostrando o resultado
SELECT
	c.id_cliente,
	c.status,
	RTRIM(c.cnpj) as cnpj,
	RTRIM(UPPER(c.razao_social)) as razao_social,
	RTRIM(UPPER(c.nome_fantasia)) as nome_fantasia,
	RTRIM(UPPER(c.endereco)) as endereco,
	c.endereco_num,
	RTRIM(UPPER(c.endereco_complemento)) as endereco_complemento,
	RTRIM(UPPER(c.bairro)) as bairro,
	RTRIM(UPPER(c.cep)) as cep,
	RTRIM(UPPER(c.cidade)) as cidade,
	RTRIM(UPPER(c.estado)) as estado,
	RTRIM(UPPER(c.telefone)) as telefone,
	c.data_cadastro,
	c.data_aprovacao,
	RTRIM(UPPER(c.status_receita)) as status_receita,
	thc.relacao_cliente_usuario as usuarios,
	thc.relacao_cliente_distribuidor as distribuidores,
	@count as 'count__'

FROM
	CLIENTE c

	INNER JOIN #HOLD_CLIENTE thc ON
		c.id_cliente = thc.id_cliente

ORDER BY
	thc.razao_social;

-- Deletando as tabelas temporarias
DROP TABLE #HOLD_DISTRIBUIDOR;
DROP TABLE #HOLD_CLIENTE;
DROP TABLE #HOLD_TEMP_CLIENTE;
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_USUARIO;