USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_DATA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_DATA
END;

IF Object_ID('tempDB..#HOLD_FAVORITO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_FAVORITO
END;

-- Criando a tabela temporária
CREATE TABLE #HOLD_DATA
(
	id INT IDENTITY(1,1),
	id_produto VARCHAR(1000),
	id_distribuidor INT
);

CREATE TABLE #HOLD_FAVORITO
(
	id_usuario INT,
	id_distribuidor INT,
	codigo_produto VARCHAR(50),
	sku VARCHAR(50),
	data_cadastro DATETIME,
	id_erro INT
);

-- Declarações de variáveis globais
---- Variaveis de entrada
DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;
DECLARE @produtos VARCHAR(MAX) = '[{"id_produto": "100-1", "id_distribuidor": 1}]';

---- Variaveis de controle
DECLARE @count_produto INT;
DECLARE @max_produto INT;
DECLARE @id_produto VARCHAR(50);
DECLARE @id_distribuidor INT;
DECLARE @date DATETIME = GETDATE();
DECLARE @id_erro INT = 0;

-- Salvando os id de distribuidor
INSERT INTO
	#HOLD_DATA
		(
			id_produto,
			id_distribuidor
		)

	SELECT
		id_produto,
		id_distribuidor

	FROM
		OPENJSON(@produtos)
	
	WITH
		(
			id_produto VARCHAR(200)   '$.id_produto',  
			id_distribuidor INT       '$.id_distribuidor'
		);

-- FOR do produto
SELECT
	@max_produto = MAX(id),
	@count_produto = MIN(id)

FROM
	#HOLD_DATA;

WHILE @count_produto <= @max_produto
BEGIN

	-- Pegando id_produto e id_distribuidor atual
	SELECT
		@id_produto = id_produto,
		@id_distribuidor = id_distribuidor,
		@id_erro = 0

	FROM
		#HOLD_DATA

	WHERE
		id = @count_produto;

	SET @date = (SELECT DATEADD(millisecond, 10, @date));

	-- Verificando se o usuario pode adicionar aquele produto
	IF NOT EXISTS (
						SELECT
							TOP 1 c.id_cliente

						FROM
							CLIENTE c

							INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
								c.id_cliente = cd.id_cliente

							INNER JOIN DISTRIBUIDOR d ON
								cd.id_distribuidor = d.id_distribuidor

						WHERE
							c.id_cliente = @id_cliente
							AND d.id_distribuidor = @id_distribuidor
							AND c.status = 'A'
							AND cd.status = 'A'
							AND cd.d_e_l_e_t_ = 0
							AND d.status = 'A'
				  )
	BEGIN

		SET @id_erro = 1;

	END

	-- Verificando se o distribuidor possui o produto
	IF @id_erro = 0
	BEGIN

		IF NOT EXISTS (
							SELECT
								TOP 1 p.id_produto

							FROM
								PRODUTO p

								INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
									p.id_produto = pd.id_produto

							WHERE
								p.id_produto = @id_produto
								AND pd.id_distribuidor = @id_distribuidor
					  )

		BEGIN
			
			SET @id_erro = 2;

		END

	END

	-- Salvando o resultado
	IF @id_erro = 0
	BEGIN

		IF NOT EXISTS (
							SELECT
								codigo_produto

							FROM
								#HOLD_FAVORITO

							WHERE
								codigo_produto = @id_produto
					  )
		BEGIN
			INSERT INTO
				#HOLD_FAVORITO
					(
						id_usuario,
						id_distribuidor,
						codigo_produto,
						sku,
						data_cadastro
					)

				SELECT
					TOP 1 @id_usuario,
					pd.id_distribuidor,
					p.id_produto,
					p.sku,
					@date

				FROM
					PRODUTO p

					INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
						p.id_produto = pd.id_produto

				WHERE
					p.id_produto = @id_produto
					AND pd.id_distribuidor = @id_distribuidor;
		END

	END

	ELSE
	BEGIN

		INSERT INTO
			#HOLD_FAVORITO
				(
					id_distribuidor,
					codigo_produto,
					id_erro
				)

			VALUES
				(
					@id_distribuidor,
					@id_produto,
					@id_erro
				);

	END
	
	-- Incrementando o contador
	SET @count_produto = @count_produto + 1;

END

-- Mostrando o resultado final
SELECT
	id_usuario,
	id_distribuidor,
	codigo_produto,
	sku,
	data_cadastro,
	id_erro

FROM
	#HOLD_FAVORITO
	
ORDER BY
	data_cadastro;

-- Deletando tabelas temporarias
DROP TABLE #HOLD_DATA;
DROP TABLE #HOLD_FAVORITO;