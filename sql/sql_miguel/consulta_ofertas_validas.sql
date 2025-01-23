SELECT
    ov.id_oferta,
    ov.tipo_oferta,
    COUNT(*) OVER() AS 'COUNT__'
    
FROM
    (
        SELECT
            DISTINCT av.id_oferta,
            av.tipo_oferta,
            av.id_distribuidor,
            av.ordem,
            av.descricao_oferta
        
        FROM
            -- Pega os ativadores ativos (oferta tipo 2) e os escalonado (oferta tipo 3)
            (
                SELECT
                    o.id_oferta,
                    o.tipo_oferta,
                    o.id_distribuidor,
                    o.ordem,
                    o.descricao_oferta
                
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
                    o.tipo_oferta in (2,3)
                    AND op.id_produto IS NOT NULL
                    AND o.id_distribuidor = pd.id_distribuidor
                    AND op.status = 'A'
                    AND o.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
            ) AS av -- Ativadores Validos
                    
            -- Pega os Bonificados ativos (oferta tipo 2)
            INNER JOIN		
            (
                SELECT
                    o.id_oferta,
                    o.tipo_oferta,
                    o.id_distribuidor,
                    o.ordem,
                    o.descricao_oferta
                
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
                    AND o.id_distribuidor = o.id_distribuidor
                    AND ob.status = 'A'
                    AND o.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
            ) AS bv ON -- Bonificados Validos
            
            av.id_oferta = bv.id_oferta
    ) AS ov -- Ofertas Validas
    
ORDER BY
    ov.id_distribuidor ASC,
    ov.ordem ASC,
    ov.descricao_oferta ASC

OFFSET
    0 ROWS

FETCH
    NEXT 20 ROWS ONLY