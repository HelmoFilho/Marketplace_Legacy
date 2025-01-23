SELECT
	*

FROM
	(
		SELECT
                DISTINCT av.id_produto
                        
            FROM
                (
                            
                    SELECT
                        o.id_oferta,
                        o.tipo_oferta,
                        o.id_distribuidor,
                        o.ordem,
                        o.descricao_oferta,
						p.id_produto
                                
                    FROM
                        OFERTA o
                                    
                        INNER JOIN OFERTA_PRODUTO op ON
                            o.id_oferta = op.id_oferta
                                        
                        INNER JOIN PRODUTO p ON
                            op.id_produto = p.id_produto
                                    
                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto
                            AND o.id_distribuidor = pd.id_distribuidor
                                        
                    WHERE
                        o.tipo_oferta = 2
                        AND op.id_produto IS NOT NULL
                        AND o.id_distribuidor = 1
                        AND op.status = 'A'
                        AND o.status = 'A'
                        AND p.status = 'A'
                        AND pd.status = 'A'
                        AND o.data_cadastro <= GETDATE()
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()
                            
                ) AS av
                            
                INNER JOIN 
                (
                                            
                    SELECT
                        o.id_oferta,
                        o.tipo_oferta,
                        o.id_distribuidor,
                        o.ordem,
                        o.descricao_oferta,
						p.id_produto
                                
                    FROM
                        OFERTA o
                                    
                        INNER JOIN OFERTA_BONIFICADO ob ON
                            o.id_oferta = ob.id_oferta
                                        
                        INNER JOIN PRODUTO p ON
                            ob.id_produto = p.id_produto
                                    
                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto
                            AND o.id_distribuidor = pd.id_distribuidor
                                        
                    WHERE
                        o.tipo_oferta = 2
                        AND ob.id_produto IS NOT NULL
						AND o.id_distribuidor = 1
                        AND ob.status = 'A'
                        AND o.status = 'A'
                        AND p.status = 'A'
                        AND pd.status = 'A'
                        AND o.data_cadastro <= GETDATE()
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()	
                                        
                ) AS bv ON
                    av.id_oferta = bv.id_oferta
	) as A

	INNER JOIN (
					SELECT
						p.id_produto

					FROM
						PRODUTO p

						INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
							p.id_produto = pd.id_produto

						INNER JOIN OFERTA_PRODUTO op ON
							pd.id_produto = op.id_produto

						INNER JOIN OFERTA o ON
							op.id_oferta = o.id_oferta
							AND pd.id_distribuidor = o.id_distribuidor

					WHERE
						o.status = 'A'
						AND o.data_final >= GETDATE()
						AND o.tipo_oferta = 3
						AND o.id_distribuidor = 1
						AND op.status = 'A'
						AND pd.status = 'A'
						AND p.status = 'A'
			   ) as B ON
		A.id_produto = B.id_produto