USE [B2BTESTE2]

IF Object_ID('tempDB..#API_RANKING_SUBGRUPO','U') IS NOT NULL
BEGIN
    DROP TABLE #API_RANKING_SUBGRUPO;
END

IF Object_ID('tempDB..#API_RANKING_GRUPO','U') IS NOT NULL
BEGIN
    DROP TABLE #API_RANKING_GRUPO;
END

IF Object_ID('tempDB..#API_RANKING_TIPO','U') IS NOT NULL
BEGIN
    DROP TABLE #API_RANKING_TIPO;
END

-- Subgrupo
SELECT
	DISTINCT 
		RANK() OVER(PARTITION BY ps.id_distribuidor ORDER BY AVG(CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END)) as subgrupo_ranking,
		ps.id_subgrupo,
		ps.id_distribuidor

INTO
	#API_RANKING_SUBGRUPO

FROM
	(

		SELECT
			id_produto,
			id_distribuidor,
			ranking

		FROM
			PRODUTO_DISTRIBUIDOR

		WHERE
			status = 'A'

		UNION

		SELECT
			id_produto,
			0 as id_distribuidor,
			ranking

		FROM
			PRODUTO_DISTRIBUIDOR

		WHERE
			status = 'A'

	) pd

	INNER JOIN PRODUTO_SUBGRUPO ps ON
		pd.id_produto = ps.codigo_produto
		AND pd.id_distribuidor = ps.id_distribuidor

GROUP BY
	ps.id_distribuidor,
	ps.id_subgrupo

ORDER BY
	ps.id_distribuidor,
	subgrupo_ranking;


UPDATE
	s

SET
	s.ranking = tars.subgrupo_ranking

FROM
	SUBGRUPO s

	INNER JOIN #API_RANKING_SUBGRUPO tars ON
		s.id_subgrupo = tars.id_subgrupo
		AND s.id_distribuidor = tars.id_distribuidor;

-- Grupo
SELECT
	DISTINCT 
		RANK() OVER(PARTITION BY g.id_distribuidor ORDER BY AVG(subgrupo_ranking)) as grupo_ranking,
		g.id_grupo,
		g.id_distribuidor

INTO
	#API_RANKING_GRUPO

FROM
	#API_RANKING_SUBGRUPO tars

	INNER JOIN GRUPO_SUBGRUPO gs ON
		gs.id_subgrupo = tars.id_subgrupo
		AND gs.id_distribuidor = tars.id_distribuidor

	INNER JOIN GRUPO g ON
		gs.id_grupo = g.id_grupo
		AND gs.id_distribuidor = g.id_distribuidor
	
GROUP BY
	g.id_distribuidor,
	g.id_grupo

ORDER BY
	g.id_distribuidor,
	grupo_ranking;


UPDATE
	g

SET
	g.ranking = targ.grupo_ranking

FROM
	GRUPO g

	INNER JOIN #API_RANKING_GRUPO targ ON
		g.id_grupo = targ.id_grupo
		AND g.id_distribuidor = targ.id_distribuidor;

-- Tipo
SELECT
	DISTINCT 
		RANK() OVER(PARTITION BY t.id_distribuidor ORDER BY AVG(grupo_ranking)) as tipo_ranking,
		t.id_tipo,
		t.id_distribuidor

INTO
	#API_RANKING_TIPO

FROM
	#API_RANKING_GRUPO targ

	INNER JOIN GRUPO g ON
		targ.id_grupo = g.id_grupo
		AND targ.id_distribuidor = g.id_distribuidor

	INNER JOIN TIPO t ON
		g.tipo_pai = t.id_tipo
		AND g.id_distribuidor = t.id_distribuidor
	
GROUP BY
	t.id_distribuidor,
	t.id_tipo

ORDER BY
	t.id_distribuidor,
	tipo_ranking;


UPDATE
	t

SET
	t.ranking = tart.tipo_ranking

FROM
	Tipo t

	INNER JOIN #API_RANKING_TIPO tart ON
		t.id_tipo = tart.id_tipo
		AND t.id_distribuidor = tart.id_distribuidor;


DROP TABLE #API_RANKING_SUBGRUPO;
DROP TABLE #API_RANKING_GRUPO;
DROP TABLE #API_RANKING_TIPO;