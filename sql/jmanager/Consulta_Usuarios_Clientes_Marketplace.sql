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

IF Object_ID('tempDB..#HOLD_TEMP_USUARIO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_TEMP_USUARIO
END;

IF OBJECT_ID('tempDB..#HOLD_STRING', 'U') IS NOT NULL
BEGIN
    DROP TABLE #HOLD_STRING
END;

IF OBJECT_ID('tempDB..#HOLD_DATA', 'U') IS NOT NULL
BEGIN
    DROP TABLE #HOLD_DATA
END;

IF OBJECT_ID('tempDB..#HOLD_CLIENTE_DISTRIBUIDOR', 'U') IS NOT NULL
BEGIN
    DROP TABLE #HOLD_CLIENTE_DISTRIBUIDOR
END;

-- Criando tabelas temporarias
CREATE TABLE #HOLD_STRING
(
    id INT IDENTITY(1,1),
    string VARCHAR(100)
);

CREATE TABLE #HOLD_DATA
(
	id INT IDENTITY(1,1),
    id_usuario INT,
	nome VARCHAR(100) COLLATE Latin1_General_CI_AS NULL,
	cpf NCHAR(11) COLLATE Latin1_General_CI_AS NULL,
	status CHAR(1) COLLATE Latin1_General_CI_AS NULL,
	email VARCHAR(50) COLLATE Latin1_General_CI_AS NULL,
	telefone NCHAR(20) COLLATE Latin1_General_CI_AS NULL,
	data_cadastro date NULL,
	aceite_termos CHAR(1) COLLATE Latin1_General_CI_AS NULL,
	data_aceite DATETIME NULL,
	clientes VARCHAR(MAX)
);

CREATE TABLE #HOLD_CLIENTE_DISTRIBUIDOR
(
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
    status_usuario_cliente CHAR(1),
    relacao_cliente_distribuidor VARCHAR(MAX)
);

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_usuario INT = NULL;
DECLARE @status VARCHAR(1) = NULL;
DECLARE @date_1 VARCHAR(10) = '1900-01-01';
DECLARE @date_2 VARCHAR(10) = '3000-01-01';
DECLARE @nome VARCHAR(MAX) = NULL;
DECLARE @id_distribuidor VARCHAR(MAX) = '0';
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

--## Variaveis de apoio
DECLARE @max_count INT;
DECLARE @actual_count INT;
DECLARE @max_cliente INT;
DECLARE @actual_cliente INT;
DECLARE @word VARCHAR(100);
DECLARE @cliente_json VARCHAR(MAX);
DECLARE @distribuidor_json VARCHAR(MAX);
DECLARE @id_cliente INT;
DECLARE @check_distribuidor BIT = 0;
DECLARE @total_count INT;

--## Variaveis tabelas
DECLARE @id_cliente_hold TABLE (
	id INT IDENTITY(1,1),
	id_cliente INT,
	data_aprovacao DATETIME
);

DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT,
	nome_fantasia VARCHAR(1000),
	razao_social VARCHAR(1000),
	status_cliente_distribuidor CHAR(1)
);

DECLARE @hold_usuario TABLE (
	id INT IDENTITY(1,1),
    id_usuario INT,
	nome VARCHAR(1000),
	count__ INT
);

DECLARE @distribuidor_input TABLE (
	id_distribuidor INT
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

SELECT
	id_distribuidor,
	RTRIM(UPPER(nome_fantasia)) as nome_fantasia,
	RTRIM(UPPER(razao_social)) as razao_social

INTO
	#HOLD_DISTRIBUIDOR

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

-- Salvando os clientes válidos
SELECT
	id_cliente,
    status,
    cnpj,
    RTRIM(UPPER(razao_social)) razao_social,
    RTRIM(UPPER(nome_fantasia)) nome_fantasia,
    RTRIM(UPPER(endereco)) endereco,
    RTRIM(UPPER(endereco_num)) endereco_num,
    RTRIM(UPPER(endereco_complemento)) endereco_complemento,
    bairro,
    cep,
    cidade,
    estado,
    telefone,
    data_cadastro,
    data_aprovacao,
    status_receita

INTO
	#HOLD_CLIENTE

FROM
	CLIENTE
	
WHERE
	LEN(razao_social) > 0;

IF @check_distribuidor = 0
BEGIN

	DELETE FROM
		#HOLD_CLIENTE

	WHERE
		id_cliente NOT IN (
								SELECT
									thc.id_cliente

								FROM
									#HOLD_CLIENTE thc

									INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
										thc.id_cliente = cd.id_cliente

									INNER JOIN #HOLD_DISTRIBUIDOR thd ON
										cd.id_distribuidor = thd.id_distribuidor

								WHERE
									cd.d_e_l_e_t_ = 0
						  );

END

-- Pegando os usuario
SELECT 
    DISTINCT u.id_usuario,
    u.nome

INTO
	#HOLD_TEMP_USUARIO

FROM
    USUARIO u
                
    INNER JOIN USUARIO_CLIENTE uc ON
        u.id_usuario = uc.id_usuario

    INNER JOIN #HOLD_CLIENTE thc ON
		uc.id_cliente = thc.id_cliente

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

-- Verificando por nome
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
            #HOLD_TEMP_USUARIO

        WHERE
            id_usuario NOT IN (
                                    SELECT
                                        id_usuario

                                    FROM
                                        #HOLD_TEMP_USUARIO

                                    WHERE
                                        nome COLLATE Latin1_General_CI_AI LIKE @word
                                )

        -- Verificando se ainda existem registros
        IF EXISTS(SELECT 1 FROM #HOLD_TEMP_USUARIO)
        BEGIN
            SET @actual_count = @actual_count + 1
        END

        ELSE
        BEGIN
            SET @actual_count = @max_count
        END

    END

END

-- Salvando o resto das informações do usuario
INSERT INTO
	@hold_usuario
		(
			id_usuario,
			nome,
			count__
		)

	SELECT
		DISTINCT id_usuario,
		nome,
		COUNT(*) OVER() 'count__'

	FROM
		#HOLD_TEMP_USUARIO

	ORDER BY
		nome
	
	OFFSET
		@offset ROWS
	
	FETCH
		NEXT @limite ROWS ONLY;

-- Loop no usuario
SELECT 
	@max_count = MAX(id),
	@actual_count = MIN(id),
	@total_count = MAX(count__)
		
FROM 
	@hold_usuario;

WHILE @actual_count <= @max_count
BEGIN
	
	SET @id_usuario = (SELECT TOP 1 id_usuario FROM @hold_usuario WHERE	id = @actual_count);

	-- Salvando os clientes 
	DELETE FROM @id_cliente_hold;

	INSERT INTO
		@id_cliente_hold
			(
				id_cliente,
				data_aprovacao
			)

		SELECT
			DISTINCT thc.id_cliente,
			uc.data_aprovacao

		FROM
			#HOLD_CLIENTE thc

			INNER JOIN USUARIO_CLIENTE uc ON
				thc.id_cliente = uc.id_cliente

			INNER JOIN USUARIO u ON
				uc.id_usuario = u.id_usuario

		WHERE
			uc.d_e_l_e_t_ = 0
			AND u.id_usuario = @id_usuario

		ORDER BY
			uc.data_aprovacao;

	-- Loop nos clientes
	SELECT
		@max_cliente = MAX(id),
		@actual_cliente = MIN(id)

	FROM
		@id_cliente_hold;

	TRUNCATE TABLE #HOLD_CLIENTE_DISTRIBUIDOR;

	WHILE @actual_cliente <= @max_cliente
	BEGIN

		SET @id_cliente = (SELECT TOP 1 id_cliente FROM @id_cliente_hold WHERE id = @actual_cliente);

		-- Salvando os distribuidores
		DELETE FROM @id_distribuidor_hold;

		INSERT INTO
			@id_distribuidor_hold
				(
					id_distribuidor,
					nome_fantasia,
					razao_social,
					status_cliente_distribuidor					
				)

			SELECT
				thd.id_distribuidor,
				thd.nome_fantasia,
				thd.razao_social,
				cd.status

			FROM
				#HOLD_CLIENTE thc

				INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
					thc.id_cliente = cd.id_cliente

				INNER JOIN #HOLD_DISTRIBUIDOR thd ON
					cd.id_distribuidor = thd.id_distribuidor

			WHERE
				cd.d_e_l_e_t_ = 0
				AND cd.status = 'A'
				AND thc.id_cliente = @id_cliente
			
			ORDER BY
				cd.data_aprovacao;
				
		-- Salvando o json
		SET @distribuidor_json = (
										SELECT
											id_distribuidor as distribuidor,
											nome_fantasia,
											razao_social,
											status_cliente_distribuidor

										FROM
											@id_distribuidor_hold

										FOR
											JSON AUTO										
								 );

		IF @distribuidor_json IS NULL
		BEGIN
			SET @distribuidor_json = '[]';
		END

		-- Salvando o cliente
		INSERT INTO
			#HOLD_CLIENTE_DISTRIBUIDOR
				(
					id_cliente,
					status,
					cnpj,
					razao_social,
					nome_fantasia,
					endereco,
					endereco_num,
					endereco_complemento,
					bairro,
					cep,
					cidade,
					estado,
					telefone,
					data_cadastro,
					data_aprovacao,
					status_receita,
					status_usuario_cliente,
					relacao_cliente_distribuidor
				)

			SELECT
				TOP 1 thc.id_cliente,
				thc.status,
				thc.cnpj,
				thc.razao_social,
				thc.nome_fantasia,
				thc.endereco,
				thc.endereco_num,
				thc.endereco_complemento,
				thc.bairro,
				thc.cep,
				thc.cidade,
				thc.estado,
				thc.telefone,
				thc.data_cadastro,
				thc.data_aprovacao,
				thc.status_receita,
				uc.status,
				@distribuidor_json

			FROM
				#HOLD_CLIENTE thc

				INNER JOIN USUARIO_CLIENTE uc ON
					thc.id_cliente = uc.id_cliente

			WHERE
				uc.d_e_l_e_t_ = 0
				AND uc.id_usuario = @id_usuario
				AND uc.id_cliente = @id_cliente;

		-- Incrementando o contador
		SET @actual_cliente = @actual_cliente + 1;

	END

	SET @cliente_json = (
							SELECT
								id_cliente,
								status,
								cnpj,
								razao_social,
								nome_fantasia,
								endereco,
								endereco_num,
								endereco_complemento,
								bairro,
								cep,
								cidade,
								estado,
								telefone,
								data_cadastro,
								data_aprovacao,
								status_receita,
								status_usuario_cliente,
								relacao_cliente_distribuidor

							FROM
								#HOLD_CLIENTE_DISTRIBUIDOR

							FOR
								JSON AUTO
						);

	-- Salvando o usuario
	INSERT INTO
		#HOLD_DATA
			(
				id_usuario,
				nome,
				cpf,
				status,
				email,
				telefone,
				data_cadastro,
				aceite_termos,
				data_aceite,
				clientes
			)

		SELECT
			id_usuario,
			UPPER(RTRIM(nome)),
			cpf,
			status,
			UPPER(RTRIM(email)),
			telefone,
			data_cadastro,
			aceite_termos,
			data_aceite,
			@cliente_json

		FROM
			USUARIO

		WHERE
			id_usuario = @id_usuario


	-- Incrementando o contador
	SET @actual_count = @actual_count + 1;

END

-- Mostrando o resultado final
SELECT
	id_usuario,
	nome,
	cpf,
	status,
	email,
	telefone,
	data_cadastro,
	aceite_termos,
	data_aceite,
	clientes,
	@total_count as 'count__'

FROM
	#HOLD_DATA
	
ORDER BY
	id_usuario;	

-- Deletando as tabelas temporarias
DROP TABLE #HOLD_DISTRIBUIDOR;
DROP TABLE #HOLD_CLIENTE;
DROP TABLE #HOLD_TEMP_USUARIO;
DROP TABLE #HOLD_STRING;
DROP TABLE #HOLD_DATA;
DROP TABLE #HOLD_CLIENTE_DISTRIBUIDOR;