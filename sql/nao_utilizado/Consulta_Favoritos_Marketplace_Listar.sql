USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_FAVORITO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_FAVORITO
END;

-- Criando a tabela temporária
CREATE TABLE #HOLD_FAVORITO
(
	id_distribuidor INT,
	id_produto VARCHAR(50),
	data_cadastro DATETIME
);

-- Declarações de variáveis globais
---- Variaveis de entrada
DECLARE @id_usuario INT = 1;
DECLARE @id_cliente INT = 1;

---- Variaveis de controle
INSERT INTO
	#HOLD_FAVORITO

	SELECT
		DISTINCT uf.id_distribuidor,
		uf.codigo_produto,
		uf.data_cadastro

	FROM
		USUARIO_FAVORITO uf

		INNER JOIN PRODUTO p ON
			uf.codigo_produto = p.id_produto

		INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
			p.id_produto = pd.id_produto

	WHERE
		uf.id_usuario = @id_usuario;


-- Removendo os produtos do distribuidor que não estão atrelados ao cliente
DELETE FROM
	#HOLD_FAVORITO

WHERE
	id_distribuidor NOT IN (
								SELECT
									d.id_distribuidor

								FROM
									CLIENTE c

									INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
										c.id_cliente = cd.id_cliente

									INNER JOIN DISTRIBUIDOR d ON
										cd.id_distribuidor = d.id_distribuidor

								WHERE
									c.id_cliente = @id_cliente
									AND c.status = 'A'
									AND cd.status = 'A'
									AND cd.d_e_l_e_t_ = 0
									AND d.status = 'A'
						   )

-- Mostrando os resultados
SELECT
	id_produto,
	id_distribuidor

FROM
	#HOLD_FAVORITO

ORDER BY
	data_cadastro DESC

-- Deletando tabelas temporarias
DROP TABLE #HOLD_FAVORITO;