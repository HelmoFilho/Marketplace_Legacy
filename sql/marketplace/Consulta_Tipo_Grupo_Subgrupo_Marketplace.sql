USE [B2BTESTE2]

DECLARE @id_cliente INT = 1;
DECLARE @id_distribuidor INT = 0;

DECLARE @distribuidores TABLE (
	id_distribuidor INT
)

IF @id_cliente IS NULL
BEGIN

	INSERT INTO
		@distribuidores

		SELECT
			id_distribuidor

		FROM
			DISTRIBUIDOR

		WHERE
			id_distribuidor = CASE WHEN @id_distribuidor = 0 THEN id_distribuidor ELSE @id_distribuidor END
			AND status = 'A'

END

ELSE
BEGIN

	INSERT INTO
		@distribuidores

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
			AND d.id_distribuidor = CASE WHEN @id_distribuidor = 0 THEN d.id_distribuidor ELSE @id_distribuidor END
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND cd.d_e_l_e_t_ = 0
            AND d.status = 'A'
	
END;

SELECT
    t.id_tipo,
    t.descricao descricao_tipo,
    g.id_grupo,
    g.descricao descricao_grupo,
    s.id_subgrupo,
    s.descricao descricao_subgrupo

FROM
    (

        SELECT
            DISTINCT t.id_tipo,
            g.id_grupo,
            s.id_subgrupo

        FROM
            (

                SELECT
                    DISTINCT
                        pd.id_produto

                FROM
                    PRODUTO p
                                
                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        pd.id_produto = tpp.id_produto
                        AND pd.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN TABELA_PRECO tp ON
                        tpp.id_tabela_preco = tp.id_tabela_preco
                        AND tpp.id_distribuidor = tp.id_distribuidor

                    INNER JOIN FORNECEDOR f ON
		                pd.id_fornecedor = f.id_fornecedor

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor


                WHERE
                    pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                    AND (

                            (
                                tp.TABELA_PADRAO = 'S'
                            )
                                OR
                            (
                                @id_cliente IS NOT NULL
                                AND tp.ID_TABELA_PRECO = (
                                                                        
                                                            SELECT
                                                                tpc.id_tabela_preco
            
                                                            FROM
                                                                TABELA_PRECO_CLIENTE tpc
            
                                                            WHERE
                                                                ID_CLIENTE = @id_cliente
                                                                AND tpc.ID_DISTRIBUIDOR IN (SELECT id_distribuidor FROM @distribuidores)
            
                                                        )
                            )

                        )
                    AND	tp.dt_inicio <= GETDATE()
                    AND	tp.dt_fim >= GETDATE()
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND tp.status = 'A'
                    AND m.status = 'A'
	                AND f.status = 'A'
                                
            ) pd

            INNER JOIN PRODUTO_SUBGRUPO ps ON
                pd.id_produto = ps.codigo_produto 

            INNER JOIN SUBGRUPO s ON
                ps.id_subgrupo = s.id_subgrupo
                AND ps.id_distribuidor = s.id_distribuidor
                                                                        
            INNER JOIN GRUPO_SUBGRUPO gs ON
                s.id_subgrupo = gs.id_subgrupo

            INNER JOIN GRUPO g ON
                gs.id_grupo = g.id_grupo

            INNER JOIN TIPO t ON
                g.tipo_pai = t.id_tipo

        WHERE
            ps.id_distribuidor = @id_distribuidor
            AND t.status = 'A'
            AND g.status = 'A'
            AND gs.status = 'A'
            AND s.status = 'A'
            AND ps.status = 'A'

    ) tgs

    INNER JOIN TIPO t ON
        tgs.id_tipo = t.id_tipo

    INNER JOIN GRUPO g ON
        tgs.id_grupo = g.id_grupo

    INNER JOIN SUBGRUPO s ON
        tgs.id_subgrupo = s.id_subgrupo

ORDER BY
    CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END,
    t.descricao,
    CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END,
    g.descricao,
    CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END,
    s.descricao