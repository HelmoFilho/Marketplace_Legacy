USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando existencia das tabelas temporarias
IF Object_ID('tempDB..#HOLD_DATA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_DATA
END;

-- Criando tabelas temporarias
CREATE TABLE #HOLD_DATA
(
	id_distribuidor INT,
	limite_credito DECIMAL(13,3),
	credito DECIMAL(13,3),
	pedido_minimo DECIMAL(13,3)
)

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_cliente VARCHAR(MAX) = 1;
DECLARE @id_usuario VARCHAR(MAX) = 1;
DECLARE @id_distribuidor VARCHAR(MAX) = ISNULL(NULL,'0');

--## Variaveis de apoio
DECLARE @check_distribuidor BIT = 0;
DECLARE @saldo DECIMAL(13,3);
DECLARE @saldo_negativo DECIMAL(13,3);
DECLARE @limite_credito DECIMAL(13,3);
DECLARE @pedido_minimo DECIMAL(13,3);
DECLARE @max_count INT;
DECLARE @actual_count INT;

--## Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
    id_distribuidor INT
);

DECLARE @distribuidor_cliente TABLE (
	id INT IDENTITY(1,1),
    id_distribuidor INT
);

DECLARE @parametros_hold TABLE (
	id_distribuidor INT,
	limite_credito DECIMAL(13,3),
	pedido_minimo DECIMAL(13,3)
);

-- Pegando os id da string
INSERT INTO
    @id_distribuidor_hold
        (
            id_distribuidor
        )

    SELECT
        nda.value

    FROM
        STRING_SPLIT(@id_distribuidor, ',') nda

    WHERE
        LEN(nda.value) > 0;

IF 0 IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
BEGIN
    SET @check_distribuidor = 1;
END

-- Verificando os distribuidores do cliente
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
            cd.id_cliente  = c.id_cliente

    WHERE
        c.id_cliente = @id_cliente
		AND d.id_distribuidor != 0
        AND c.status = 'A'
        AND cd.status = 'A'
        AND cd.d_e_l_e_t_ = 0
        AND cd.data_aprovacao <= GETDATE()
        AND d.status = 'A'
        AND (
                (
                    @check_distribuidor = 1
                )
                    OR
                (
                    @check_distribuidor = 0
                    AND d.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
                )
            );

-- Pegando o limite de credito
INSERT INTO
	@parametros_hold
		(
			id_distribuidor,
			limite_credito,
			pedido_minimo
		)

SELECT
	DISTINCT pc.id_distribuidor,
    pc.limite_credito,
	pc.valor_minimo_pedido

FROM
    PARAMETRO_CLIENTE pc

	INNER JOIN @distribuidor_cliente vdc ON
		pc.id_distribuidor = vdc.id_distribuidor

WHERE
	pc.id_cliente = @id_cliente

ORDER BY
    pc.id_distribuidor;

-- Realizando o for
SELECT
	@max_count = MAX(id),
	@actual_count = MIN(id)

FROM
	@distribuidor_cliente;

WHILE @actual_count <= @max_count
BEGIN

	SET @id_distribuidor = (SELECT TOP 1 id_distribuidor FROM @distribuidor_cliente WHERE id = @actual_count);

	SELECT
		TOP 1 @limite_credito = ISNULL(limite_credito,0),
		@pedido_minimo = ISNULL(pedido_minimo,0)

	FROM
		@parametros_hold

	WHERE
		id_distribuidor = @id_distribuidor;

	-- Pegando o saldo negativo
	SELECT
		@saldo_negativo = ISNULL(SUM(valor), 0)

	FROM
		PEDIDO

	WHERE
		id_cliente = @id_cliente
		AND id_distribuidor = @id_distribuidor
		AND data_finalizacao IS NULL
		AND d_e_l_e_t_ = 0
		AND forma_pagamento = 1;

	SET @saldo = @limite_credito - @saldo_negativo;

	-- Salvando o saldo 
	IF NOT EXISTS (
						SELECT 
							TOP 1 1 
						
						FROM 
							PARAMETRO_CLIENTE 
							
						WHERE 
							id_distribuidor = @id_distribuidor 
							AND id_cliente = @id_cliente
				  )
	BEGIN

		BEGIN TRANSACTION;

		INSERT INTO
			PARAMETRO_CLIENTE
				(
					id_cliente,
					id_distribuidor,
					limite_credito,
					saldo_limite,
					valor_minimo_pedido
				)

			VALUES
				(
					@id_cliente,
					@id_distribuidor,
					0,
					0 - @saldo_negativo,
					0
				);

		COMMIT;

	END

	ELSE
	BEGIN

		BEGIN TRANSACTION;

		UPDATE
			PARAMETRO_CLIENTE

		SET
			saldo_limite = @saldo

		WHERE
			id_cliente = @id_cliente
			AND id_distribuidor = @id_distribuidor;

		COMMIT;
		
	END

	-- Salvado os dados
	IF NOT EXISTS (SELECT TOP 1 1 FROM #HOLD_DATA WHERE id_distribuidor = @id_distribuidor)
	BEGIN

		BEGIN TRANSACTION;

		INSERT INTO
			#HOLD_DATA
				(
					id_distribuidor,
					limite_credito,
					credito,
					pedido_minimo
				)

			VALUES
				(
					@id_distribuidor,
					@limite_credito,
					@saldo,
					@pedido_minimo
				)

		COMMIT;
	END
	
	-- Incrementando o contador
	SET @actual_count = @actual_count + 1;

END

-- Mostrando o resultado
SELECT
	id_distribuidor,
	limite_credito,
	credito as credito_atual,
	pedido_minimo

FROM
	#HOLD_DATA
	
ORDER BY
	id_distribuidor;

-- Deletando tabelas temporarias
DROP TABLE #HOLD_DATA;