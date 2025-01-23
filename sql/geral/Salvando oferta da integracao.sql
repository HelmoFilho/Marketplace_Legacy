USE [B2BTESTE2]

--Salvando o cabecalho da oferta
INSERT INTO
	OFERTA
		(
			id_oferta_api,
			descricao_oferta,
			id_distribuidor,
			tipo_oferta,
			ordem,
			operador,
			data_inicio,
			data_final,
			necessario_para_ativar,
			limite_ativacao_cliente,
			limite_ativacao_oferta,
			produto_agrupado,
			status,
			data_cadastro,
			usuario_cadastro
		)

	SELECT
		id_oferta_api,
		descricao_oferta,
		id_distribuidor,
		tipo_oferta,
		ordem,
		operador,
		data_inicio,
		data_final,
		necessario_para_ativar,
		limite_ativacao_cliente,
		limite_ativacao_oferta,
		produto_agrupado,
		status,
		GETDATE() as data_cadastro,
		usuario_cadastro

	FROM
		API_OFERTA ao

	WHERE
		NOT EXISTS (
						SELECT
							1

						FROM
							OFERTA o

						WHERE
							o.id_oferta_api = ao.id_oferta_api
							AND o.id_distribuidor = ao.id_distribuidor
				   )

-- Salvando os id_oferta nas tabelas de api

-- # Api oferta
UPDATE
	ao

SET
	id_oferta = o.id_oferta

FROM
	API_OFERTA ao

	INNER JOIN OFERTA o ON
		ao.id_oferta_api = o.id_oferta_api

WHERE
	ao.id_oferta IS NULL

-- # Api oferta Bonificado
UPDATE
	aob

SET
	id_oferta = o.id_oferta,
	id_produto = p.id_produto

FROM
	API_OFERTA_BONIFICADO aob

	INNER JOIN OFERTA o ON
		aob.id_oferta_api = o.id_oferta_api

	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		aob.id_produto_api = pd.cod_prod_distr
		AND aob.id_distribuidor = pd.id_distribuidor

	INNER JOIN PRODUTO p ON
		pd.id_produto = p.id_produto

WHERE
	aob.id_oferta IS NULL

-- # Api oferta Cliente
UPDATE
	aoc

SET
	id_oferta = o.id_oferta,
	id_cliente = c.id_cliente

FROM
	API_OFERTA_CLIENTE aoc

	INNER JOIN OFERTA o ON
		aoc.id_oferta_api = o.id_oferta_api

	INNER JOIN CLIENTE c ON
		aoc.cod_cliente = c.chave_integracao

WHERE
	aoc.id_oferta IS NULL

-- # Api oferta Produto
UPDATE
	aop

SET
	id_oferta = o.id_oferta,
	id_produto = p.id_produto

FROM
	API_OFERTA_PRODUTO aop

	INNER JOIN OFERTA o ON
		aop.id_oferta_api = o.id_oferta_api

	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
		aop.id_produto_api = pd.cod_prod_distr
		AND aop.id_distribuidor = pd.id_distribuidor

	INNER JOIN PRODUTO p ON
		pd.id_produto = p.id_produto

WHERE
	aop.id_oferta IS NULL

-- # Api oferta Escalonado Faixa
UPDATE
	aoef

SET
	id_oferta = o.id_oferta

FROM
	API_OFERTA_ESCALONADO_FAIXA aoef

	INNER JOIN OFERTA o ON
		aoef.id_oferta_api = o.id_oferta_api

WHERE
	aoef.id_oferta IS NULL

-- Salvando as ofertas do cliente
INSERT INTO
	OFERTA_CLIENTE
		(
			id_oferta,
			id_cliente,
			data_cadastro,
			status,
			d_e_l_e_t_
		)

	SELECT
		aoe.id_oferta,
		aoe.id_cliente,
		GETDATE() as data_cadastro,
		aoe.status,
		0 d_e_l_e_t_

	FROM
		API_OFERTA_CLIENTE aoe

	WHERE
		NOT EXISTS (
						SELECT
							1

						FROM
							OFERTA_CLIENTE

						WHERE
							id_cliente = aoe.id_cliente
							AND id_oferta = aoe.id_oferta
				   )
		AND aoe.id_cliente IS NOT NULL

-- Salvando os bonificados da oferta
INSERT INTO
	OFERTA_BONIFICADO
		(
			id_oferta,
			id_produto,
			quantidade_bonificada,
			status
		)

	SELECT
		aob.id_oferta,
		aob.id_produto,
		aob.quantidade_bonificada,
		aob.status

	FROM
		API_OFERTA_BONIFICADO aob

	WHERE
		aob.id_produto IS NOT NULL
		AND NOT EXISTS (
							SELECT
								1

							FROM
								OFERTA_BONIFICADO

							WHERE
								id_oferta = aob.id_oferta
								AND id_produto = aob.id_produto
					   )

-- Salvando os produtos da oferta
INSERT INTO
	OFERTA_PRODUTO
		(
			id_oferta,
			id_produto,
			quantidade_min_ativacao,
			valor_min_ativacao,
			status
		)

	SELECT
		aop.id_oferta,
		aop.id_produto,
		aop.quantidade_min_ativacao,
		aop.valor_min_ativacao,
		aop.status

	FROM
		API_OFERTA_PRODUTO aop

	WHERE
		aop.id_produto IS NOT NULL
		AND NOT EXISTS (
							SELECT
								1

							FROM
								OFERTA_PRODUTO

							WHERE
								id_produto = aop.id_produto
								AND id_oferta = aop.id_oferta
					   )

-- Salvando as faixas de escalonamentos de oferta
INSERT INTO
	OFERTA_ESCALONADO_FAIXA
		(
			id_oferta,
			sequencia,
			faixa,
			desconto,
			status
		)

	SELECT
		aoef.id_oferta,
		aoef.sequencia,
		aoef.faixa,
		aoef.desconto,
		aoef.status

	FROM
		API_OFERTA_ESCALONADO_FAIXA aoef

	WHERE
		NOT EXISTS (
						SELECT
							1

						FROM
							OFERTA_ESCALONADO_FAIXA

						WHERE
							id_oferta = aoef.id_oferta
							AND sequencia = aoef.sequencia
				   )