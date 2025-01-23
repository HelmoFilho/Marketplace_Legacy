USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificanto existencia das tabelas temporarias
IF OBJECT_ID('tempDB..#HOLD_DATA','U') IS NOT NULL
BEGIN
	DROP TABLE #HOLD_DATA;
END

-- Criando tabela de dados
CREATE TABLE #HOLD_DATA
(
	id INT, -- id_forma_pagamento
	nome VARCHAR(100),
	condicoes VARCHAR(MAX),
	cartoes VARCHAR(MAX),
	padrao CHAR(1)
);

-- Declarações de variaveis globais
---- Variaveis de entrada
DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;
DECLARE @id_distribuidor INT = 1;

---- Variaveis de controle
DECLARE @id_grupo_pagamento INT;
DECLARE @id_formapgto INT;
DECLARE @id_condpgto INT;
DECLARE @count_padrao INT;
DECLARE @desc_formapgto VARCHAR(200);
DECLARE @count_formapgto INT;
DECLARE @max_formapgto INT;
DECLARE @formapgto_padrao CHAR(1);
DECLARE @hold_json_condpgto VARCHAR(MAX);
DECLARE @hold_json_cartoes VARCHAR(MAX);

---- Variaveis tabelas
DECLARE @hold_cond_pagamento TABLE (
	id_condpgto INT,
	desc_condpgto VARCHAR(200),
	percentual DECIMAL(9,2),
	id_formapgto INT,
	padrao CHAR(1)
);

DECLARE @hold_forma_pagamento TABLE (
	id INT IDENTITY(1,1),
	id_formapgto INT,
	desc_formapgto VARCHAR(200)
);

DECLARE @hold_cartoes_usuario TABLE (
	id_maxipago VARCHAR(50),
	numero_cartao VARCHAR(50),
	bandeira VARCHAR(50),
	cvv_check BIT
);

-- Verificando o grupo de pagamento
SET @id_grupo_pagamento = (
								SELECT
									TOP 1 id_grupo

								FROM
									CLIENTE_GRUPO_PAGTO

								WHERE
									id_cliente = @id_cliente
									AND status = 'A'
						  );

IF @id_grupo_pagamento IS NULL
BEGIN

	SET @id_grupo_pagamento = (

									SELECT
										TOP 1 id_grupo

									FROM
										GRUPO_PAGTO

									WHERE
										status = 'A'
										AND padrao = 'S'
							  );

END

-- Verificando condicoes e formas de pagamento acessíveis para o cliente
INSERT INTO
	@hold_cond_pagamento
		(
			id_condpgto,
			desc_condpgto,
			percentual,
			id_formapgto,
			padrao
		)

	SELECT
		cp.id_condpgto,
		cp.descricao,
		cp.percentual,
		fp.id_formapgto,
		gpi.padrao

	FROM
		GRUPO_PAGTO_ITEM gpi

		INNER JOIN CONDICAO_PAGAMENTO cp ON 
			gpi.id_condpgto = cp.id_condpgto

		INNER JOIN FORMA_PAGAMENTO fp ON
			cp.id_formapgto = fp.id_formapgto

	WHERE
		gpi.id_grupo = @id_grupo_pagamento
		AND gpi.id_distribuidor = @id_distribuidor
		AND gpi.status = 'A'
		AND cp.status = 'A'
		AND fp.status = 'A'

	ORDER BY
		gpi.id_condpgto;


-- Setando o padrao
SET @count_padrao = (SELECT COUNT(*) FROM @hold_cond_pagamento WHERE padrao = 'S');

IF @count_padrao = 0
BEGIN
	SET @id_condpgto = (SELECT MIN(id_condpgto) FROM @hold_cond_pagamento);
END

ELSE
BEGIN
	SET @id_condpgto = (SELECT MIN(id_condpgto) FROM @hold_cond_pagamento WHERE padrao = 'S');
END

UPDATE
	@hold_cond_pagamento

SET
	padrao = CASE WHEN id_condpgto = @id_condpgto THEN 'S' ELSE 'N' END;

-- Salvando os ids de formas de pagamento
INSERT INTO
	@hold_forma_pagamento
		(
			id_formapgto,
			desc_formapgto
		)

	SELECT
		DISTINCT fp.id_formapgto,
		fp.descricao

	FROM
		@hold_cond_pagamento vhcd

		INNER JOIN FORMA_PAGAMENTO fp ON
			vhcd.id_formapgto = fp.id_formapgto
		
	ORDER BY
		fp.id_formapgto;

-- FOR das formas de pagamento
SELECT
	@max_formapgto = MAX(id),
	@count_formapgto = MIN(id)

FROM
	@hold_forma_pagamento;

WHILE @count_formapgto <= @max_formapgto
BEGIN

	-- Pegando o id e descricao da forma de pagamento
	SELECT
		TOP 1 
			@id_formapgto = id_formapgto,
			@desc_formapgto = desc_formapgto

	FROM
		@hold_forma_pagamento

	WHERE
		id = @count_formapgto;

	-- Verificando se é a forma de pagamento padrao
	IF EXISTS (SELECT 1 FROM @hold_cond_pagamento WHERE id_formapgto = @id_formapgto AND padrao = 'S')
	BEGIN
		SET @formapgto_padrao = 'S';
	END

	ELSE
	BEGIN
		SET @formapgto_padrao = 'N';
	END

	-- Criando o JSON da forma de pagamento
	SET @hold_json_condpgto = (
									SELECT
										id_condpgto,
										RTRIM(desc_condpgto) as descricao,
										percentual,
										CASE WHEN padrao = 'S' THEN 1 ELSE 0 END as padrao

									FROM
										@hold_cond_pagamento

									WHERE
										id_formapgto = @id_formapgto

									FOR
										JSON AUTO
								);

	-- Salvando os cartões do cliente
	SET @hold_json_cartoes = NULL;

	IF @id_formapgto = 2
	BEGIN

		-- Salvando os cartões do usuario
		INSERT INTO
			@hold_cartoes_usuario

			SELECT
                id_maxipago,
                numero_cartao,
                bandeira,
				CASE WHEN LEN(cvv) > 0 THEN 1 ELSE 0 END as cvv_check

            FROM
                USUARIO_CARTAO_DE_CREDITO

            WHERE
                id_usuario = @id_usuario
                AND id_cliente = @id_cliente

            ORDER BY
                data_criacao;

		IF EXISTS (SELECT TOP 1 1 FROM @hold_cartoes_usuario)
		BEGIN
			SET @hold_json_cartoes = (SELECT * FROM @hold_cartoes_usuario FOR JSON AUTO);
		END

		ELSE
		BEGIN
			SET @hold_json_cartoes = '[]';
		END

	END

	-- Salvando os dados finais
	IF LEN(@hold_json_condpgto) > 0
	BEGIN

		INSERT INTO
			#HOLD_DATA
				(
					id,
					nome,
					condicoes,
					padrao,
					cartoes
				)

			VALUES
				(
					@id_formapgto,
					RTRIM(@desc_formapgto),
					REPLACE(@hold_json_condpgto, '"', '&%$'),
					@formapgto_padrao,
					REPLACE(@hold_json_cartoes, '"', '&%$')
				);				

	END

	-- Incrementando o contador
	SET @count_formapgto = @count_formapgto + 1;

END

-- Mostrando o resultado final
SELECT
	id,
	nome,
	condicoes,
	CASE WHEN padrao = 'S' THEN 1 ELSE 0 END as padrao,
	cartoes

FROM
	#HOLD_DATA

ORDER BY
	id;

-- Deleta tabelas temporarias
DROP TABLE #HOLD_DATA;