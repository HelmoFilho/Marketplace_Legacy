USE [B2BTESTE2]

SET NOCOUNT ON;

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_distribuidor INT = 0;
DECLARE @id_cliente INT = 1;
DECLARE @offset INT = 0;
DECLARE @limite INT = 20;

--## Variaveis tabelas
DECLARE @id_distribuidor_hold TABLE (
	id_distribuidor INT
);

-- Pegando distribuidores v�lidos para o cliente
INSERT INTO
	@id_distribuidor_hold

	SELECT
		DISTINCT d.id_distribuidor

	FROM
		CLIENTE c

		INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
			c.id_cliente = cd.id_cliente

		INNER JOIN DISTRIBUIDOR d ON
			cd.id_distribuidor = d.id_distribuidor

	WHERE
		c.id_cliente = @id_cliente
		AND d.id_distribuidor = CASE
									WHEN @id_distribuidor = 0
										THEN d.id_distribuidor
									ELSE
										@id_distribuidor
								END
		AND c.status = 'A'
		AND c.status_receita = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND cd.status = 'A'
		AND d.status = 'A';

-- Mostrando o resultado
SELECT
	p.id_produto,
	pd.id_distribuidor,
	o.ordem,
	o.descricao_oferta,
	pd.ranking,
	p.descricao_completa

FROM
	OFERTA o

	INNER JOIN OFERTA_DESCONTO od ON
		o.id_oferta = od.id_oferta

	INNER JOIN OFERTA_PRODUTO op ON
		od.id_oferta = op.id_oferta

	INNER JOIN PRODUTO p ON
		op.id_produto = p.id_produto

	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		p.id_produto = pd.id_produto
		AND o.id_distribuidor = pd.id_distribuidor

WHERE
	LEN(p.sku) > 0
	AND p.id_produto IS NOT NULL
	AND p.status = 'A'
	AND pd.status = 'A'
	AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold)
	AND o.status = 'A'
	AND o.data_cadastro <= GETDATE()
	AND o.data_inicio <= GETDATE()
	AND o.data_final >= GETDATE()
	AND op.status = 'A'
	AND od.status = 'A'
	
ORDER BY
	o.ordem,
	o.descricao_oferta,
	CASE WHEN pd.ranking > 0 THEN 1 ELSE 0 END DESC,
	pd.ranking,
	p.descricao_completa
	
OFFSET
	@offset ROWS

FETCH
	NEXT @limite ROWS ONLY;