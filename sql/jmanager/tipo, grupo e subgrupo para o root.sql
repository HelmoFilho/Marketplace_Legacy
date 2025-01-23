USE [B2BTESTE2]

SET NOCOUNT ON;

DECLARE @id_tipo INT = NULL;

-- Tipo
SET @id_tipo = (SELECT TOP 1 id_tipo FROM TIPO WHERE padrao = 'S' and id_distribuidor = 0)
IF @id_tipo IS NULL
BEGIN

INSERT INTO 
    TIPO
        (
            id_distribuidor,
            descricao,
            status,
            padrao
        )

    VALUES
        (
            0,
            'CATEGORIAS',
            'A',
            'S'
        );

SET @id_tipo = (SELECT SCOPE_IDENTITY());

END


-- Grupo
INSERT INTO
GRUPO
    (
        id_distribuidor,
        tipo_pai,
        cod_grupo_distribuidor,
        descricao,
        status
    )

SELECT
    DISTINCT
        0 as id_distribuidor,
        @id_tipo as tipo_pai,
        g.cod_grupo_distribuidor,
        (SELECT TOP 1 descricao FROM GRUPO WHERE cod_grupo_distribuidor = g.cod_grupo_distribuidor),
        'A' as status

FROM
    GRUPO g

WHERE
    NOT EXISTS (
                    SELECT
                        1

                    FROM
                        GRUPO

                    WHERE
                        id_distribuidor = 0
                        AND cod_grupo_distribuidor = g.cod_grupo_distribuidor
            );

-- Subgrupo
INSERT INTO
SUBGRUPO
    (
        id_distribuidor,
        cod_subgrupo_distribuidor,
        descricao,
        status
    )

SELECT
    DISTINCT
        0 as id_distribuidor,
        s.cod_subgrupo_distribuidor,
        (SELECT TOP 1 descricao FROM SUBGRUPO WHERE cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor),
        'A' as status

FROM
    SUBGRUPO s

WHERE
    NOT EXISTS (
                    SELECT
                        1

                    FROM
                        SUBGRUPO

                    WHERE
                        id_distribuidor = 0
                        AND cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor
            );


-- Grupo Subgrupo
INSERT INTO
GRUPO_SUBGRUPO
    (
        id_distribuidor,
        id_grupo,
        id_subgrupo,
        status
    )

SELECT
    0 as id_distribuidor,
    g.id_grupo,
    s.id_subgrupo,
    gs.status

FROM
    (

        SELECT
            DISTINCT
                g.cod_grupo_distribuidor,
                'A' as status,
                s.cod_subgrupo_distribuidor

        FROM
            SUBGRUPO s

            INNER JOIN GRUPO_SUBGRUPO gs ON
                s.id_subgrupo = gs.id_subgrupo
                AND s.id_distribuidor = gs.id_distribuidor

            INNER JOIN GRUPO g ON
                gs.id_grupo = g.id_grupo
                AND gs.id_distribuidor = g.id_distribuidor

        WHERE
            s.id_distribuidor != 0	

    ) gs                           

    INNER JOIN SUBGRUPO s ON
        gs.cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor

    INNER JOIN GRUPO g ON
        gs.cod_grupo_distribuidor = g.cod_grupo_distribuidor
        AND s.id_distribuidor = g.id_distribuidor

WHERE
    s.id_distribuidor = 0;


-- Produto Subgrupo
INSERT INTO
PRODUTO_SUBGRUPO
    (
        codigo_produto,
        sku,
        id_distribuidor,
        id_subgrupo,
        status
    )

SELECT
    ps.codigo_produto,
    ps.sku,
    s.id_distribuidor,
    s.id_subgrupo,
    ps.status_produto_subgrupo

FROM
    (

        SELECT
            DISTINCT
                s.cod_subgrupo_distribuidor,
                ps.status as status_produto_subgrupo,
                ps.codigo_produto,
                ps.sku

        FROM
            SUBGRUPO s

            INNER JOIN PRODUTO_SUBGRUPO ps ON
                s.id_subgrupo = ps.id_subgrupo
                AND s.id_distribuidor = ps.id_distribuidor

        WHERE
            s.id_distribuidor != 0	

    ) ps

    INNER JOIN SUBGRUPO s ON
        ps.cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor

WHERE
    s.id_distribuidor = 0