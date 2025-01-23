USE [B2BTESTE2]

SET NOCOUNT ON;

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_cliente VARCHAR(MAX) = 1;
DECLARE @id_distribuidor VARCHAR(MAX) = ISNULL(NULL,'0');

--## Variaveis de apoio
DECLARE @check_distribuidor BIT = 0;

--## Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
    id_distribuidor INT
);

DECLARE @distribuidor_cliente TABLE (
    id_distribuidor INT
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

-- Mostrando o resultado
SELECT
	
	
FROM
	CLIENTE c

	INNER JOIN PARAMETRO_CLIENTE pc ON
		c.id_cliente = pc.id_cliente
	
	INNER JOIN @distribuidor_cliente vdc ON
		pc.id_distribuidor = vdc.id_distribuidor;