SET NOCOUNT ON;

USE [B2BTESTE2]

DECLARE @produto_input VARCHAR(MAX) = '[{"id_distribuidor": 1, "id_produto": "100-1"}, {"id_distribuidor": 1, "id_produto": "4289-11"},{"id_distribuidor": 1, "id_produto": "853-6"}, {"id_distribuidor": 1, "id_produto": "3835-1"}]';
DECLARE @id_cliente INT = 1;
DECLARE @bool_marketplace BIT = 0;
DECLARE @image_server VARCHAR(1000) = 'https://midiasmarketplace-dev.guarany.com.br';
DECLARE @image_replace VARCHAR(100) = '150';

DECLARE @distribuidores TABLE (id_distribuidor INT);
INSERT INTO @distribuidores VALUES (1),(2);

-- Fazendo a query
SELECT
    DISTINCT RESULT.id, 
    RESULT.id_produto,
    p.sku,
    p.descricao_completa as descricao_produto,
    p.status,
    p.dun14,
    p.tipo_produto,
    p.variante,
    p.volumetria,
    p.unidade_embalagem,
    p.quantidade_embalagem,
    p.unidade_venda,
    p.quant_unid_venda,
    pimg.token,
    RESULT.id_distribuidor,
    d.nome_fantasia as descricao_distribuidor,
    pd.agrupamento_variante,
    pd.cod_prod_distr,
    SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
    pd.multiplo_venda,
    pd.ranking,
    pd.unidade_venda as unidade_venda_distribuidor,
    pd.quant_unid_venda as quant_unid_venda_distribuidor,
    pd.giro,
    pd.agrup_familia,
    pd.status as status_distribuidor,
    pd.data_cadastro,
    m.id_marca as marca,
    m.desc_marca as descricao_marca,
    f.id_fornecedor,
    f.desc_fornecedor,
    s.id_subgrupo,
    s.descricao as descricao_subgrupo,
    g.id_grupo,
    g.descricao as descricao_grupo,
    t.id_tipo,
    t.descricao as descricao_tipo,
    ISNULL(pe.qtd_estoque, 0) as estoque,
    ISNULL(RESULT.preco_tabela, 0) as preco_tabela,
    odesc.desconto,
    odesc.data_inicio as data_inicio_desconto,
    odesc.data_final as data_final_desconto,
    offers.id_oferta,
    CASE
		WHEN offers.id_oferta IS NULL 
			THEN NULL
        WHEN EXISTS (SELECT 1 FROM OFERTA_PRODUTO WHERE id_produto = RESULT.id_produto AND id_oferta = offers.id_oferta) AND offers.tipo_oferta = 2
            THEN 1
        ELSE
            0
    END ativador,
    CASE
		WHEN offers.id_oferta IS NULL 
			THEN NULL
        WHEN EXISTS (SELECT 1 FROM OFERTA_BONIFICADO WHERE id_produto = RESULT.id_produto AND id_oferta = offers.id_oferta) AND offers.tipo_oferta = 2
            THEN 1
        ELSE
            0
    END bonificado,
    CASE
		WHEN offers.id_oferta IS NULL 
			THEN NULL
        WHEN EXISTS (SELECT 1 FROM OFERTA_PRODUTO WHERE id_produto = RESULT.id_produto AND id_oferta = offers.id_oferta) AND offers.tipo_oferta = 3
            THEN 1
        ELSE
            0
    END escalonado,
    pdet.detalhes

FROM
    (

		SELECT
			thip.id,
			tpp.id_produto,
			tpp.id_distribuidor,
			MIN(tpp.preco_tabela) preco_tabela

		FROM
			(

				SELECT
					nda.id,
					pd.id_produto,
					pd.id_distribuidor

				FROM
					PRODUTO p

					INNER JOIN PRODUTO_DISTRIBUIDOR pd ON 
						p.id_produto = pd.id_produto

					INNER JOIN 
					(

						SELECT
							ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as id,
							id_produto,
							id_distribuidor

						FROM
							OPENJSON(@produto_input)
                    
						WITH
							(
								id_produto VARCHAR(200)   '$.id_produto',  
								id_distribuidor INT       '$.id_distribuidor'
							)

					) nda ON
						pd.id_produto = nda.id_produto
						AND pd.id_distribuidor = nda.id_distribuidor

				WHERE
					pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
					AND (
							(
								@bool_marketplace = 1
							)
								OR
							(
								@bool_marketplace = 0
								AND p.status = 'A'
								AND pd.status = 'A'
							)
						)
			
			) thip

            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                thip.id_produto = tpp.id_produto
				AND thip.id_distribuidor = tpp.id_distribuidor

            INNER JOIN TABELA_PRECO tp ON
                tpp.ID_DISTRIBUIDOR = tp.ID_DISTRIBUIDOR
                AND tpp.ID_TABELA_PRECO = tp.ID_TABELA_PRECO

		WHERE
			tp.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
			AND tp.status = 'A'
			AND tp.dt_inicio <= GETDATE()
			AND tp.dt_fim >= GETDATE()
			AND (
					(
						tp.tabela_padrao = 'S'
					)
						OR
					(
						tp.id_tabela_preco IN (
													SELECT
														id_tabela_preco

													FROM
														TABELA_PRECO_CLIENTE

													WHERE
														status = 'A'
														AND id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
														AND id_cliente = @id_cliente
												)
					)
				)

		GROUP BY
			thip.id,
			tpp.id_produto,
			tpp.id_distribuidor

    ) RESULT

    INNER JOIN PRODUTO p ON
        RESULT.id_produto = p.id_produto

    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        p.id_produto = pd.id_produto
        AND RESULT.id_distribuidor = pd.id_distribuidor

    LEFT JOIN PRODUTO_IMAGEM pimg ON
        p.sku = pimg.sku

    INNER JOIN DISTRIBUIDOR d ON
        pd.id_distribuidor = d.id_distribuidor

    INNER JOIN MARCA m ON
        pd.id_marca = m.id_marca
        AND pd.id_distribuidor = m.id_distribuidor

    INNER JOIN FORNECEDOR f ON
        pd.id_fornecedor = f.id_fornecedor

    LEFT JOIN PRODUTO_ESTOQUE pe ON
        pd.id_produto = pe.id_produto 
        AND pd.id_distribuidor = pe.id_distribuidor

    LEFT JOIN PRODUTO_DETALHES pdet ON
        p.id_produto = pdet.id_produto

    LEFT JOIN (
					SELECT
						op.id_produto,
						o.id_distribuidor,
						MAX(od.desconto) desconto,
						o.data_inicio,
						o.data_final

					FROM
						(

							SELECT
								o.id_oferta

							FROM
								OFERTA o

							WHERE
								id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

							UNION

							SELECT
								o.id_oferta

							FROM
								OFERTA o

								INNER JOIN OFERTA_CLIENTE oc ON
									o.id_oferta = oc.id_oferta

							WHERE
								oc.id_cliente = @id_cliente
								AND oc.status = 'A'
								AND oc.d_e_l_e_t_ = 0

						) offers

						INNER JOIN OFERTA o ON
							offers.id_oferta = o.id_oferta

						INNER JOIN OFERTA_PRODUTO op ON
							o.id_oferta = op.id_oferta

						INNER JOIN OFERTA_DESCONTO od ON
							o.id_oferta = od.id_oferta

					WHERE
						o.tipo_oferta = 1
						AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
						AND o.status = 'A'
						AND o.data_inicio <= GETDATE()
						AND o.data_final >= GETDATE()
						AND op.status = 'A'
						AND od.status = 'A'

					GROUP BY
						op.id_produto,
						o.id_distribuidor,
						o.data_inicio,
						o.data_final

                ) odesc ON
        p.id_produto = odesc.id_produto
        AND pd.id_distribuidor = odesc.id_distribuidor

    LEFT JOIN OFERTA_PRODUTO op ON
        RESULT.id_produto = op.id_produto
                                
    LEFT JOIN 
    (
        SELECT
            offers.id_oferta,
            offers_produto.id_produto,
            offers_produto.tipo_oferta

        FROM
            (
                SELECT
                    op.id_oferta,
                    op.id_produto,
                    o.tipo_oferta

                FROM
                    (

                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                        UNION

                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                            INNER JOIN OFERTA_CLIENTE oc ON
                                o.id_oferta = oc.id_oferta

                        WHERE
                            oc.id_cliente = @id_cliente
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) offers

                    INNER JOIN 
                    (
                                    
                        SELECT
                            op.id_oferta,
                            op.id_produto

                        FROM
                            OFERTA_PRODUTO op

                        WHERE
                            op.status = 'A'

                        UNION

                        SELECT
                            ob.id_oferta,
                            ob.id_produto

                        FROM
                            OFERTA_BONIFICADO ob

                        WHERE
                            ob.status = 'A'

                    ) op ON
                        offers.id_oferta = op.id_oferta

                    INNER JOIN OFERTA o ON
                        offers.id_oferta = o.id_oferta

                    INNER JOIN PRODUTO p ON
                        op.id_produto = p.id_produto

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto
                        AND o.id_distribuidor = pd.id_distribuidor

                WHERE
                    o.tipo_oferta IN (2,3)
                    AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND o.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                                            
            ) offers_produto

            INNER JOIN 
            (

                (
                    SELECT
                        av.id_oferta

                    FROM

                        (
                            SELECT
                                o.id_oferta
                                                                                        
                            FROM
                                OFERTA o
                                                                                            
                                INNER JOIN OFERTA_PRODUTO op ON
                                    o.id_oferta = op.id_oferta
                                                                                                
                            WHERE
                                o.tipo_oferta = 2
                                AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                                AND o.status = 'A'
                                AND op.status = 'A'
                                                                                    
                        ) AS av
                                                                                    
                        INNER JOIN 
                        (
                                                                                                    
                            SELECT
                                o.id_oferta
                                                                                        
                            FROM
                                OFERTA o
                                                                                            
                                INNER JOIN OFERTA_BONIFICADO ob ON
                                    o.id_oferta = ob.id_oferta
                                                                                                
                            WHERE
                                o.tipo_oferta = 2
                                AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                                AND o.status = 'A'
                                AND ob.status = 'A'
                                                                                                
                        ) AS bv ON
                            av.id_oferta = bv.id_oferta
                )

                UNION

                (
                    SELECT
                        o.id_oferta
                                                                                        
                    FROM
                        OFERTA o
                                                                                            
                        INNER JOIN OFERTA_PRODUTO op ON
                            o.id_oferta = op.id_oferta

                        INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                            o.id_oferta = oef.id_oferta
                                                                                                
                    WHERE
                        o.tipo_oferta = 3
                        AND o.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidores)
                        AND oef.desconto > 0
                        AND oef.status = 'A'
                        AND o.status = 'A'
                        AND op.status = 'A'
                )

            ) offers ON
                offers_produto.id_oferta = offers.id_oferta

    ) offers ON
        p.id_produto = offers.id_produto

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
    ps.id_distribuidor != 0

ORDER BY
    RESULT.id;