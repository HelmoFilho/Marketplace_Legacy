USE [B2BTESTE2]

DECLARE @email VARCHAR(100) = 'HELMOFILHO09@GMAIL.COM';
DECLARE @codigo_produto VARCHAR(100) = '0000702';

SET NOCOUNT ON;

DECLARE @id_tipo INT = NULL;
DECLARE @id_grupo INT = NULL;
DECLARE @id_subgrupo INT = NULL;
DECLARE @id_fornecedor INT = NULL;
DECLARE @id_marca INT = NULL;
DECLARE @id_distribuidor INT = 1;

-- Criando tabela temporaria
IF Object_ID('tempDB..#API_PRODUTO_TMP','U') IS NOT NULL
BEGIN
    DROP TABLE #API_PRODUTO_TMP;
END

-- Salvando informações na tabela temporária
SELECT 
    TOP 1 * 
    INTO #API_PRODUTO_TMP
                                        
FROM 
    API_PRODUTO
                                        
WHERE 
    COD_PROD_DISTR = @codigo_produto
    AND ID_DISTRIBUIDOR = @id_distribuidor
    AND STATUS_APROVADO IN ('P', 'R')
    AND sku IS NOT NULL
                                        
ORDER BY 
    AGRUPAMENTO_VARIANTE, 
    COD_FRAG_DISTR, 
    COD_PROD_DISTR;

PRINT 'id_distribuidor => ' + CONVERT(VARCHAR(100), @id_distribuidor);

-- Salvando informações do fornecedor
SET @id_fornecedor = (SELECT TOP 1 id_fornecedor FROM FORNECEDOR WHERE
	cod_fornecedor_distribuidor = (SELECT TOP 1 cod_fornecedor FROM #API_PRODUTO_TMP))

IF @id_fornecedor IS NULL
BEGIN
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
			'A'

		FROM 
			#API_PRODUTO_TMP A

	SET @id_fornecedor = (SELECT SCOPE_IDENTITY());
END

PRINT 'id_fornecedor => ' + CONVERT(VARCHAR(100), @id_fornecedor);

-- Salvando TIPO caso não exista
SET @id_tipo = (SELECT TOP 1 id_tipo FROM TIPO WHERE padrao = 'S' and id_distribuidor = @id_distribuidor)
IF @id_tipo IS NULL
BEGIN

	INSERT INTO 
		TIPO
			(
				id_distribuidor,
				descricao,
				status,
				padrao,
				ranking
			)
                                            
		SELECT	
			id_distribuidor,
			'CATEGORIAS' as descricao,
			'A' as status,
			'S' padrao,
			(SELECT ISNULL(MAX(RANKING), 0) + 1 FROM TIPO) as ranking
	
		FROM 
			#API_PRODUTO_TMP A;

	SET @id_tipo = (SELECT SCOPE_IDENTITY());
END

PRINT 'id_tipo => ' + CONVERT(VARCHAR(100), @id_tipo);

-- Salvando GRUPO caso não exista
SET @id_grupo = (SELECT TOP 1 id_grupo FROM GRUPO WHERE id_distribuidor = @id_distribuidor 
	AND cod_grupo_distribuidor = (SELECT TOP 1 cod_grupo FROM #API_PRODUTO_TMP))

IF @id_grupo IS NULL
BEGIN
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
		@id_tipo as tipo_pai,
		descr_grupo,
		cod_grupo,
		'A'

	FROM 
		#API_PRODUTO_TMP A;

	SET @id_grupo = (SELECT SCOPE_IDENTITY());
END

PRINT 'id_grupo => ' + CONVERT(VARCHAR(100), @id_grupo);

-- Salvando SUBGRUPO caso não exista
SET @id_subgrupo = (SELECT TOP 1 id_subgrupo FROM SUBGRUPO WHERE id_distribuidor = (SELECT TOP 1 id_distribuidor FROM #API_PRODUTO_TMP)
	AND cod_subgrupo_distribuidor = (SELECT TOP 1 cod_subgrupo FROM #API_PRODUTO_TMP))

IF @id_subgrupo IS NULL
BEGIN
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
		'A'

	FROM 
		#API_PRODUTO_TMP A;
		
	SET @id_subgrupo = (SELECT SCOPE_IDENTITY());
END
                                            
PRINT 'id_subgrupo => ' + CONVERT(VARCHAR(100), @id_subgrupo);

-- Salvando GRUPO_SUBGRUPO caso não exista
IF NOT EXISTS (SELECT TOP 1 1 FROM GRUPO_SUBGRUPO WHERE id_grupo = @id_grupo AND id_subgrupo = @id_subgrupo)

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
			@id_grupo id_grupo,
			@id_subgrupo id_subgrupo,
			'A' STATUS
		FROM 
			#API_PRODUTO_TMP A;
                                                                    

-- Salvando MARCA caso não exista
SET @id_marca = (SELECT TOP 1 id_marca FROM MARCA WHERE id_distribuidor = @id_distribuidor
	AND cod_marca_distribuidor = (SELECT TOP 1 id_marca FROM #API_PRODUTO_TMP))

IF @id_marca IS NULL
BEGIN

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
			descr_marca desc_marca,
			cod_marca,
			'A' STATUS

		FROM 
			#API_PRODUTO_TMP A;

	SET @id_marca = (SELECT SCOPE_IDENTITY());
END

PRINT 'id_marca => ' + CONVERT(VARCHAR(100), @id_marca);

-- Salvando PRODUTO_DISTRIBUIDOR_ID caso não exista
INSERT 
    INTO PRODUTO_DISTRIBUIDOR_ID
SELECT  
    ID_DISTRIBUIDOR,
    ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR) AGRUPAMENTO_VARIANTE,
    GETDATE() DATA_CADASTRO
FROM 
    #API_PRODUTO_TMP A
WHERE NOT EXISTS ( 
                    SELECT 
                        1 
                    FROM 
                        PRODUTO_DISTRIBUIDOR_ID B
                    WHERE 
                        B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                        AND B.id_distribuidor = A.ID_DISTRIBUIDOR );


-- Salvando PRODUTO_DISTRIBUIDOR caso não exista
INSERT 
    INTO PRODUTO_DISTRIBUIDOR
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
            AND B.id_distribuidor = A.ID_DISTRIBUIDOR
	) 
    +'-'+ 
    (
        SELECT 
            CONVERT(VARCHAR, COUNT(*)+1) 
        FROM 
            PRODUTO_DISTRIBUIDOR C 
        WHERE 
            C.id_distribuidor = A.ID_DISTRIBUIDOR AND C.AGRUPAMENTO_VARIANTE=ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR) 
	) 
	as id_produto,
    id_distribuidor,
    agrupamento_variante,
	cod_prod_distr,
	multiplo_venda,
	ranking,
    unidade_venda,
	quant_unid_venda,
    giro,
    agrup_familia,
    @id_marca id_marca,
    @id_fornecedor id_fornecedor,
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
INSERT
    INTO PRODUTO_SUBGRUPO
SELECT  
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
    ID_DISTRIBUIDOR,
    @id_subgrupo id_subgrupo,
    'A' STATUS
FROM 
    #API_PRODUTO_TMP A
WHERE NOT EXISTS ( 
                    SELECT 
                        1 
                    FROM 
                        PRODUTO_SUBGRUPO ps 
                    WHERE 
                        ps.id_subgrupo = @id_subgrupo
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
    A.USUARIO_APROVADO = @email
FROM
    API_PRODUTO AS A
    INNER JOIN 
        #API_PRODUTO_TMP AS B
            ON A.COD_PROD_DISTR = B.COD_PROD_DISTR
            AND A.ID_DISTRIBUIDOR=B.ID_DISTRIBUIDOR
WHERE 
    A.COD_PROD_DISTR = @codigo_produto
    AND A.ID_DISTRIBUIDOR = @id_distribuidor
    AND A.STATUS_APROVADO IN ('P', 'R');


DROP TABLE #API_PRODUTO_TMP