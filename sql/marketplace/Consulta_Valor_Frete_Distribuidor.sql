USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_DISTRIBUIDOR','U') IS NOT NULL 
BEGIN
    DROP TABLE #FRETE_DISTRIBUIDOR
END;

-- Criando a tabela temporaria
CREATE TABLE #FRETE_DISTRIBUIDOR
(
	id_distribuidor INT,
	valor_frete DECIMAL(18,2) DEFAULT 0,
	valor_minimo_frete DECIMAL(18,2) DEFAULT 0,
	valor_minimo_pedido DECIMAL(18,2) DEFAULT 0,
	maximo_item_pedido INT DEFAULT 0
);

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_cliente INT = ISNULL(1,0);

--## Variaveis de controle
DECLARE @id_distribuidor INT;
DECLARE @used_id_distribuidor INT;
DECLARE @max_count INT;
DECLARE @actual_count INT;

--## Variaveis tabelas
DECLARE @distribuidor_cliente TABLE (
	id INT IDENTITY(1,1),
	id_distribuidor INT
);

DECLARE @hold_frete TABLE (
	id_distribuidor INT,
	valor_frete DECIMAL(18,2),
	valor_minimo_frete DECIMAL(18,2),
	valor_minimo_pedido DECIMAL(18,2),
	maximo_item_pedido DECIMAL(18,2)
);

-- Pegando os distribuidores do cliente
INSERT INTO
	@distribuidor_cliente
		(
			id_distribuidor
		)

	SELECT
		DISTINCT d.id_distribuidor

	FROM
		DISTRIBUIDOR d

		INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
			d.id_distribuidor = cd.id_distribuidor

		INNER JOIN CLIENTE c ON
			cd.id_cliente = c.id_cliente

	WHERE
		LEN(c.nome_fantasia) > 0
		AND c.id_cliente = @id_cliente
		AND c.status = 'A'
		AND cd.status = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND cd.data_aprovacao <= GETDATE()
		AND d.status = 'A'
		AND d.id_distribuidor != 0

	ORDER BY
		d.id_distribuidor;

-- Pegando os valores de frete
INSERT INTO
	@hold_frete
		(
			id_distribuidor,
			valor_frete,
			valor_minimo_frete,
			valor_minimo_pedido,
			maximo_item_pedido
		)

	SELECT
		id_distribuidor,
		valor_frete,
		valor_minimo_frete,
		valor_minimo_pedido,
		maximo_item_pedido

	FROM	
		PARAMETRO_DISTRIBUIDOR

	WHERE
		id_distribuidor = 0
		OR id_distribuidor IN (SELECT id_distribuidor FROM @distribuidor_cliente);

-- Realizando o FOR
SELECT
	@max_count = MAX(id),
	@actual_count = MIN(id)

FROM
	@distribuidor_cliente;

WHILE @actual_count <= @max_count
BEGIN

	SET @id_distribuidor = (SELECT TOP 1 id_distribuidor FROM @distribuidor_cliente WHERE id = @actual_count);

	-- Verificar o frete
	IF NOT EXISTS (
						SELECT
							TOP 1 1

						FROM
							@hold_frete

						WHERE
							id_distribuidor = @id_distribuidor
				  )
	BEGIN
		SET @used_id_distribuidor = 0;
	END

	ELSE
	BEGIN
		SET @used_id_distribuidor = @id_distribuidor;
	END

	INSERT INTO
		#FRETE_DISTRIBUIDOR
			(
				id_distribuidor,
				valor_frete,
				valor_minimo_frete,
				valor_minimo_pedido,
				maximo_item_pedido
			)

		SELECT
			@id_distribuidor,
			valor_frete,
			valor_minimo_frete,
			valor_minimo_pedido,
			maximo_item_pedido

		FROM
			@hold_frete

		WHERE
			id_distribuidor = @used_id_distribuidor;

	-- Incrementando o contador
	SET @actual_count = @actual_count + 1;

END

-- Mostrando o resultado
SELECT
	id_distribuidor,
	valor_frete,
	valor_minimo_frete,
	valor_minimo_pedido,
	maximo_item_pedido

FROM
	#FRETE_DISTRIBUIDOR

ORDER BY
	id_distribuidor;

-- Deletando a tabela temporaria
DROP TABLE #FRETE_DISTRIBUIDOR;