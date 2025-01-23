USE [B2BTESTE2]

SET NOCOUNT ON;

-- Criando tabela temporaria
IF Object_ID('tempDB..#API_PRODUTO_TMP','U') IS NOT NULL
BEGIN
    DROP TABLE #API_PRODUTO_TMP;
END

IF Object_ID('tempDB..#API_UNIQUE_AGRUPAMENTO','U') IS NOT NULL
BEGIN
    DROP TABLE #API_UNIQUE_AGRUPAMENTO;
END

-- Salvando informações na tabela temporária
SELECT 
    * 
                    
INTO 
    #API_PRODUTO_TMP
                                    
FROM 
    API_PRODUTO
                                    
WHERE 
    STATUS_APROVADO IN ('P')
    AND sku IS NOT NULL
                                    
ORDER BY 
    AGRUPAMENTO_VARIANTE, 
    COD_FRAG_DISTR, 
    COD_PROD_DISTR;

-- Salvando os agrupamentos unicos
SELECT
    AGRUPAMENTO_VARIANTE,
    COD_FRAG_DISTR,
    COD_PROD_DISTR,
    ID_DISTRIBUIDOR,
    ROW_NUMBER() OVER(PARTITION BY ID_DISTRIBUIDOR, ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR) ORDER BY COD_PROD_DISTR) ORDER_FRAG

INTO
    #API_UNIQUE_AGRUPAMENTO

FROM
    #API_PRODUTO_TMP A;	

-- Salvando informações do fornecedor
INSERT INTO 
	FORNECEDOR
		(
			cod_fornecedor_distribuidor,
			desc_fornecedor,
			status
		)
                                        
	SELECT
		cod_fornecedor,
		descri_fornecedor,
		status

	FROM
		(
		
			SELECT  
				DISTINCT
					cod_fornecedor,
					descri_fornecedor,
					'A' as status,
					A.ID_DISTRIBUIDOR
                                        
			FROM 
				(
					SELECT
						DISTINCT 
							COD_FORNECEDOR,
							ID_DISTRIBUIDOR,
							DESCRI_FORNECEDOR

					FROM
						#API_PRODUTO_TMP 

				) A
                                        
			WHERE NOT EXISTS (  SELECT 
									1 
								FROM 
									FORNECEDOR B
								WHERE 
									B.cod_fornecedor_distribuidor = COD_FORNECEDOR)
		
		) A
							
	ORDER BY
		A.ID_DISTRIBUIDOR;


-- Salvando TIPO caso não exista
INSERT INTO 
	TIPO
		(
			id_distribuidor,
			descricao,
			status,
			padrao	
		)
                                        
	SELECT
		id_distribuidor,
		descricao,
		status,
		padrao

	FROM
		(
		
			SELECT	
				DISTINCT
					A.id_distribuidor,
					'CATEGORIAS' descricao,
					'A' status,
					'S' PADRAO
                                        
			FROM 
				(
					SELECT
						DISTINCT
							ID_DISTRIBUIDOR

					FROM
						#API_PRODUTO_TMP

					WHERE
						id_distribuidor NOT IN (SELECT id_distribuidor FROM TIPO WHERE padrao = 'S')
				) A
		
		) A
		
	ORDER BY
		A.ID_DISTRIBUIDOR;


-- Salvando GRUPO caso não exista
INSERT INTO 
	GRUPO
		(
			id_distribuidor,
			tipo_pai,
			descricao,
			cod_grupo_distribuidor,
			status
		)

	SELECT
		id_distribuidor,
		tipo_pai,
		descricao,
		cod_grupo,
		status

	FROM
		(
		
			SELECT  
				DISTINCT
					id_distribuidor,
					(

						SELECT 
							MAX(ID_TIPO)

						FROM 
							TIPO C 

						WHERE 
							C.PADRAO='S' AND C.id_distribuidor = A.ID_DISTRIBUIDOR

					) tipo_pai,
					descr_grupo descricao,
					cod_grupo,
					'A' STATUS

			FROM 
				#API_PRODUTO_TMP A
		
			WHERE
				NOT EXISTS (SELECT 1 FROM GRUPO WHERE cod_grupo = A.cod_grupo AND id_distribuidor = A.id_distribuidor)
		
		) A
		
	ORDER BY
		A.ID_DISTRIBUIDOR,
		A.COD_GRUPO;

-- Salvando SUBGRUPO caso não exista
INSERT INTO 
	SUBGRUPO
		(
			id_distribuidor,
			descricao,
			cod_subgrupo_distribuidor,
			status
		)

	SELECT
		id_distribuidor,
		descr_subgrupo,
		cod_subgrupo,
		STATUS

	FROM
		(
		
			SELECT  
				DISTINCT
					id_distribuidor,
					descr_subgrupo,
					cod_subgrupo,
					'A' STATUS

			FROM 
				#API_PRODUTO_TMP A
		
			WHERE
				NOT EXISTS (SELECT 1 FROM SUBGRUPO s WHERE s.cod_subgrupo_distribuidor = A.cod_subgrupo AND s.id_distribuidor = A.id_distribuidor)
		
		) A
		
	ORDER BY
		A.ID_DISTRIBUIDOR,
		A.COD_SUBGRUPO;
                                        

-- Salvando GRUPO_SUBGRUPO caso não exista
INSERT INTO 
	GRUPO_SUBGRUPO
		(
			id_distribuidor,
			id_grupo,
			id_subgrupo,
			status
		)

	SELECT
		ID_DISTRIBUIDOR,
		id_grupo,
		id_subgrupo,
		STATUS

	FROM
		(
		
			SELECT  
				DISTINCT
					A.ID_DISTRIBUIDOR,
					g.id_grupo,
					s.id_subgrupo,
					'A' STATUS
			FROM 
				(
					SELECT
						DISTINCT 
							ID_DISTRIBUIDOR,
							COD_GRUPO,
							COD_SUBGRUPO

					FROM
						#API_PRODUTO_TMP
				) A

				INNER JOIN GRUPO g ON
					A.id_distribuidor = g.id_distribuidor
					AND A.cod_grupo = g.cod_grupo_distribuidor

				INNER JOIN SUBGRUPO s ON
					A.id_distribuidor = s.id_distribuidor
					AND A.cod_subgrupo = s.cod_subgrupo_distribuidor

			WHERE 
				NOT EXISTS ( 
								SELECT 
									1 
								FROM 
									GRUPO_SUBGRUPO GB
								WHERE 
									GB.id_subgrupo = s.id_subgrupo
									AND GB.id_grupo = g.id_grupo
									AND GB.id_distribuidor = A.ID_DISTRIBUIDOR 
						   )
		
		) A
				   
	ORDER BY
		A.ID_DISTRIBUIDOR,
		A.id_grupo,
		A.id_subgrupo;
                                                                

-- Salvando MARCA caso não exista
INSERT INTO 
	MARCA
		(
			id_distribuidor,
			desc_marca,
			cod_marca_distribuidor,
			status
		)

	SELECT
		id_distribuidor,
		descr_marca,
		cod_marca,
		status

	FROM
		(
		
			SELECT  
				DISTINCT
					id_distribuidor,
					descr_marca,
					cod_marca,
					status

			FROM 
				#API_PRODUTO_TMP A

			WHERE 
				NOT EXISTS ( 
								SELECT 
									1 
								FROM 
									MARCA B
								WHERE 
									B.cod_marca_distribuidor = A.cod_marca
									AND B.id_distribuidor = A.ID_DISTRIBUIDOR 
							)
		
		) A
					
	ORDER BY
		A.ID_DISTRIBUIDOR,
		A.COD_MARCA;


-- Salvando PRODUTO_DISTRIBUIDOR_ID caso não exista
INSERT 
    INTO PRODUTO_DISTRIBUIDOR_ID
SELECT  
    DISTINCT
        ID_DISTRIBUIDOR,
        ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR) AGRUPAMENTO_VARIANTE,
        GETDATE() DATA_CADASTRO
FROM 
    (
        SELECT
            DISTINCT 
                ID_DISTRIBUIDOR,
                AGRUPAMENTO_VARIANTE,
                COD_PROD_DISTR

        FROM
            #API_PRODUTO_TMP
    ) A
WHERE NOT EXISTS ( 
                    SELECT 
                        1 
                    FROM 
                        PRODUTO_DISTRIBUIDOR_ID B
                    WHERE 
                        B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                        AND B.id_distribuidor = A.ID_DISTRIBUIDOR );


-- Salvando PRODUTO_DISTRIBUIDOR caso não exista
INSERT INTO 
	PRODUTO_DISTRIBUIDOR
		(
			id_produto,
			id_distribuidor,
			agrupamento_variante,
			cod_prod_distr,
			multiplo_venda,
			ranking,
			unidade_venda,
			quant_unid_venda,
			giro,
			agrup_familia,
			id_marca,
			id_fornecedor,
			status
		)

	SELECT  
		(
			SELECT 
				CONVERT(VARCHAR, ID_PRODUTO) 
			FROM 
				PRODUTO_DISTRIBUIDOR_ID B
			WHERE 
				B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
				AND B.id_distribuidor = A.ID_DISTRIBUIDOR) 
		+'-'+ 
		(
			SELECT
				CONVERT(VARCHAR, (

										(
											SELECT 
												COUNT(*)
											FROM 
												PRODUTO_DISTRIBUIDOR C 
											WHERE 
												C.id_distribuidor = A.ID_DISTRIBUIDOR 
												AND C.AGRUPAMENTO_VARIANTE=ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR) 
										)

										+

										(

											SELECT
												ORDER_FRAG
                        
											FROM
												#API_UNIQUE_AGRUPAMENTO D

											WHERE
												D.id_distribuidor = A.ID_DISTRIBUIDOR 
												AND D.COD_PROD_DISTR = A.COD_PROD_DISTR
										)
                                        
								)
					)

		) id_produto,
		id_distribuidor,
		agrupamento_variante,
		cod_prod_distr,
		multiplo_venda,
		ranking,
		unidade_venda,
		quant_unid_venda,
		giro,
		agrup_familia,
		(

			SELECT
				TOP 1 id_marca

			FROM
				MARCA

			WHERE
				cod_marca_distribuidor = A.cod_marca
				AND id_distribuidor = A.id_distribuidor
				
		) id_marca,
		(

			SELECT
				TOP 1 id_fornecedor

			FROM
				FORNECEDOR

			WHERE
				cod_fornecedor_distribuidor = A.cod_fornecedor
				
		) id_fornecedor,
		status

	FROM 
		#API_PRODUTO_TMP A

	WHERE NOT EXISTS ( 
						SELECT 
							1 
						FROM 
							PRODUTO_DISTRIBUIDOR B
						WHERE B.AGRUPAMENTO_VARIANTE = ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
							AND B.id_distribuidor = A.ID_DISTRIBUIDOR
							AND B.COD_PROD_DISTR = A.COD_PROD_DISTR);


-- Salvando PRODUTO caso não exista
INSERT INTO 
	PRODUTO
		(
			ID_PRODUTO,
			ID_AGRUPAMENTO,
			DESCRICAO,
			DESCRICAO_COMPLETA,
			SKU,
			DUN14,
			STATUS,
			TIPO_PRODUTO,
			VARIANTE,
			UNIDADE_VENDA,
			QUANT_UNID_VENDA,
			UNIDADE_EMBALAGEM,
			QUANTIDADE_EMBALAGEM,
			VOLUMETRIA,
			DT_INSERT,
			DT_UPDATE,
			DATA_CADASTRO
		)
SELECT 
    DISTINCT 
    (
        SELECT 
            ID_PRODUTO
        FROM 
            PRODUTO_DISTRIBUIDOR C 
        WHERE 
            C.id_distribuidor = A.ID_DISTRIBUIDOR
                AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                AND C.COD_PROD_DISTR=A.COD_PROD_DISTR)
    ID_PRODUTO,
    AGRUPAMENTO_VARIANTE ID_AGRUPAMENTO,
    DESCR_REDUZIDA_DISTR DESCRICAO,
    DESCR_COMPLETA_DISTR DESCRICAO_COMPLETA,
    SKU,
    DUN14,
    STATUS,
    TIPO_PRODUTO,
    VARIANTE,
    UNIDADE_VENDA,
    QUANT_UNID_VENDA,
    UNIDADE_EMBALAGEM,
    QUANTIDADE_EMBALAGEM,
    VOLUMETRIA,
    DT_INSERT,
    DT_UPDATE,
    GETDATE() DATA_CADASTRO
FROM 
    #API_PRODUTO_TMP A
WHERE NOT EXISTS ( 
                    SELECT 
                        1 
                    FROM 
                        PRODUTO B
                    WHERE 
                        B.ID_PRODUTO IN (
                                            SELECT 
                                                ID_PRODUTO
                                            FROM 
                                                PRODUTO_DISTRIBUIDOR C 
                                            WHERE 
                                                C.id_distribuidor = A.ID_DISTRIBUIDOR
                                                    AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                                    AND C.COD_PROD_DISTR=A.COD_PROD_DISTR) );


-- Salvando PRODUTO_SUBGRUPO caso não exista
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
    DISTINCT
        (SELECT 
            ID_PRODUTO
        FROM 
            PRODUTO_DISTRIBUIDOR C 
        WHERE 
            C.id_distribuidor = A.ID_DISTRIBUIDOR
            AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
            AND C.COD_PROD_DISTR=A.COD_PROD_DISTR)
        CODIGO_PRODUTO,
        SKU,
        A.ID_DISTRIBUIDOR,
        s.id_subgrupo,
        'A' STATUS
FROM 
    (
        SELECT
            DISTINCT 
                ID_DISTRIBUIDOR,
                COD_PROD_DISTR,
                AGRUPAMENTO_VARIANTE,
                SKU,
                COD_SUBGRUPO

        FROM
            #API_PRODUTO_TMP
    ) A

	INNER JOIN SUBGRUPO s ON
		A.id_distribuidor = s.id_distribuidor
		AND A.cod_subgrupo = s.cod_subgrupo_distribuidor

WHERE NOT EXISTS ( 
                    SELECT 
                        1 
                    FROM 
                        PRODUTO_SUBGRUPO ps 
                    WHERE 
                        ps.id_subgrupo = s.id_subgrupo
                        AND ps.sku = A.SKU
                        AND ps.id_distribuidor = A.ID_DISTRIBUIDOR 
                        AND ps.codigo_produto = (
                                                    SELECT 
                                                        ID_PRODUTO
                                                    FROM 
                                                        PRODUTO_DISTRIBUIDOR C 
                                                    WHERE 
                                                        C.id_distribuidor = A.ID_DISTRIBUIDOR
                                                        AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                                        AND C.COD_PROD_DISTR=A.COD_PROD_DISTR))

-- Update na tabela API_PRODUTO
UPDATE 
    A
SET
    A.ID_PRODUTO = CASE
                        WHEN (SELECT TOP 1 id_produto FROM #API_PRODUTO_TMP) IS NOT NULL
                            THEN A.ID_PRODUTO
                        ELSE (SELECT ID_PRODUTO
                                FROM PRODUTO_DISTRIBUIDOR C 
                                WHERE C.id_distribuidor = B.ID_DISTRIBUIDOR
                                    AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(B.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, B.ID_DISTRIBUIDOR)+'-'+B.COD_PROD_DISTR)
                                    AND C.COD_PROD_DISTR=B.COD_PROD_DISTR)
                    END,
    A.STATUS_APROVADO = 'A',
    A.USUARIO_APROVADO = 'HELMOFILHO09@GMAIL.COM'
FROM
    API_PRODUTO AS A
    INNER JOIN 
        #API_PRODUTO_TMP AS B
            ON A.COD_PROD_DISTR = B.COD_PROD_DISTR
            AND A.ID_DISTRIBUIDOR=B.ID_DISTRIBUIDOR
WHERE 
    A.STATUS_APROVADO IN ('P');


DROP TABLE #API_PRODUTO_TMP
DROP TABLE #API_UNIQUE_AGRUPAMENTO;