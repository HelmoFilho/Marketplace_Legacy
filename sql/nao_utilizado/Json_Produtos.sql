USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se as tabelas temporarias existe
IF Object_ID('tempDB..#HOLD_ID_PRODUTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_ID_PRODUTO
END;

IF Object_ID('tempDB..#HOLD_PRODUTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_PRODUTO
END;

IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_OFERTA
END;

IF Object_ID('tempDB..#HOLD_INFO_PRODUTO_DISTRIBUIDOR','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_INFO_PRODUTO_DISTRIBUIDOR
END;

IF Object_ID('tempDB..#HOLD_INFO_PRODUTO','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_INFO_PRODUTO
END;


-- Criacao de tabelas temporarias
CREATE TABLE #HOLD_ID_PRODUTO
(
	id INT IDENTITY(1,1),
	id_produto VARCHAR(50),
	id_distribuidor INT
);

CREATE TABLE #HOLD_OFERTA
(
	id_oferta INT,
	tipo_oferta INT,
	id_distribuidor INT
);

CREATE TABLE #HOLD_INFO_PRODUTO_DISTRIBUIDOR
(
	id_produto VARCHAR(50),
	id_distribuidor INT,
	descricao_distribuidor VARCHAR(MAX),
	agrupamento_variante VARCHAR(20),
	cod_prod_distr VARCHAR(20),
	cod_frag_distr VARCHAR(20),
	id_fornecedor VARCHAR(20),
	desc_fornecedor VARCHAR(1000),
	multiplo_venda INT,
	ranking INT,
	unidade_venda VARCHAR(20),
	quant_unid_venda INT,
	giro VARCHAR(20),
	agrup_familia VARCHAR(20),
	id_tipo VARCHAR(MAX),
	id_grupo VARCHAR(MAX),
	id_subgrupo VARCHAR(MAX),
	status VARCHAR(1),
	data_cadastro DATE,
	estoque INT,
	preco_tabela DECIMAL(18,3),
	desconto VARCHAR(1000),
	ofertas VARCHAR(MAX),
	detalhes VARCHAR(MAX)
);

CREATE TABLE #HOLD_INFO_PRODUTO
(
	descricao_produto VARCHAR(MAX),
    sku VARCHAR(50),
	status VARCHAR(1),
    marca INT,
    descricao_marca VARCHAR(100),
    dun14 VARCHAR(20),
    tipo_produto VARCHAR(1000),
    variante VARCHAR(200),
    volumetria VARCHAR(20),
    unidade_embalagem VARCHAR(20),
    quantidade_embalagem INT,
    unidade_venda VARCHAR(20),
    quant_unid_venda INT,
    imagem VARCHAR(MAX),
    distribuidores VARCHAR(MAX)
);

-- Declara��es de variaveis globais
-- ## Variaveis de entrada
DECLARE @produto_input VARCHAR(MAX) = '[{"id_distribuidor": 1, "id_produto": "100-1"}]';
DECLARE @id_cliente INT = 1;
DECLARE @bool_marketplace BIT = 0;
DECLARE @image_server VARCHAR(1000) = 'https://midiasmarketplace-dev.guarany.com.br';
DECLARE @image_replace VARCHAR(100) = '150';

-- ## Variaveis de controle
DECLARE @actual_product INT;
DECLARE @max_product INT;
DECLARE @id_distribuidor INT;
DECLARE @id_produto VARCHAR(50);
DECLARE @distribuidor_nome_fantasia VARCHAR(100);
DECLARE @tgs_erro BIT = 0;
DECLARE @hold_tipo VARCHAR(MAX);
DECLARE @hold_grupo VARCHAR(MAX);
DECLARE @hold_subgrupo VARCHAR(MAX);
DECLARE @preco_tabela DECIMAL(18,3);
DECLARE @preco_venda DECIMAL(18,3);
DECLARE @desconto_hold DECIMAL(18,3);
DECLARE @desconto_json_hold NVARCHAR(MAX);
DECLARE @hold_bonificada VARCHAR(MAX);
DECLARE @hold_oferta VARCHAR(MAX);
DECLARE @distribuidores VARCHAR(MAX);
DECLARE @imagens VARCHAR(MAX);
DECLARE @full_json NVARCHAR(MAX);

-- ## Variaveis tabelas
DECLARE @distribuidor_hold TABLE (
	id_distribuidor INT
);
DECLARE @subgrupo TABLE (
	id INT, 
	descricao VARCHAR(200)
);
DECLARE @grupo TABLE (
	id INT, 
	descricao VARCHAR(200)
);
DECLARE @tipo TABLE (
	id INT, 
	descricao VARCHAR(200)
);
DECLARE @prod_distr TABLE (
	ordem INT IDENTITY(1,1),
	id_produto VARCHAR(50),
	id_distribuidor INT
);
DECLARE @desconto TABLE (
	preco_desconto DECIMAL(18,3),
	desconto DECIMAL(18,3),
	data_inicio DATETIME,
	data_final DATETIME
);
DECLARE @oferta_ativador TABLE (
	id INT
);
DECLARE @oferta_bonificado TABLE (
	id INT
);
DECLARE @oferta_escalonado TABLE (
	id INT
);
DECLARE @oferta TABLE (
	bonificada VARCHAR(MAX),
	escalonada VARCHAR(MAX)
);
DECLARE @bonificada TABLE (
	produto_ativador  VARCHAR(MAX),
	produto_bonificado VARCHAR(MAX)
);

-- Verificando de quais distribuidores o cliente pode ver
INSERT INTO
	@distribuidor_hold

	SELECT
		DISTINCT d.id_distribuidor

	FROM
		CLIENTE c

		INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
			c.id_cliente = cd.id_cliente

		INNER JOIN DISTRIBUIDOR d ON
			cd.id_distribuidor = d.id_distribuidor

	WHERE
		c.status = 'A'
		AND cd.status = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND d.status = 'A'
		AND c.id_cliente = CASE
							   WHEN @id_cliente IS NULL
								   THEN c.id_cliente
							   ELSE
								   @id_cliente
						   END;

-- Verificando ofertas validas

-- ## Salvando os produtos que podem estar em ofertas
SELECT
	p.id_produto,
	pd.id_distribuidor

INTO
	#HOLD_PRODUTO

FROM
	PRODUTO p
                        
    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        p.id_produto = pd.id_produto

WHERE
	p.status = 'A'
	AND pd.status = 'A'
	AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @distribuidor_hold);

-- Verificando se produtos foram encontrados
IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
BEGIN

	-- Salvando pre-ofertas
	INSERT INTO
		#HOLD_OFERTA

	SELECT
		DISTINCT o.id_oferta,
		o.tipo_oferta,
		o.id_distribuidor

	FROM
		OFERTA o

		INNER JOIN OFERTA_PRODUTO op ON
			o.id_oferta = op.id_oferta

		INNER JOIN #HOLD_PRODUTO thp ON
			op.id_produto = thp.id_produto
			AND o.id_distribuidor = thp.id_distribuidor
		
	WHERE
		op.id_produto IS NOT NULL
		AND o.tipo_oferta IN (2,3)
		AND op.status = 'A'
		AND o.status = 'A'
		AND o.data_cadastro <= GETDATE()
		AND o.data_inicio <= GETDATE()
		AND o.data_final >= GETDATE();

	IF EXISTS(SELECT 1 FROM #HOLD_OFERTA)
	BEGIN

		-- Salvando ofertas bonificadas
		DELETE FROM
			#HOLD_OFERTA

		WHERE
			id_oferta NOT IN (
								SELECT
									tho.id_oferta

								FROM
								
									#HOLD_OFERTA tho

									INNER JOIN OFERTA_BONIFICADO ob ON
										tho.id_oferta = ob.id_oferta
								)
			AND tipo_oferta = 2;

		-- Salvando ofertas escalonada
		DELETE FROM
			#HOLD_OFERTA

		WHERE
			id_oferta NOT IN (
								SELECT
									tho.id_oferta

								FROM
								
									#HOLD_OFERTA tho

									INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
										tho.id_oferta = oef.id_oferta
								)
			AND tipo_oferta = 3;

	END

END

-- Salvando os id de distribuidor e produto do input
INSERT INTO
	@prod_distr
		(
			id_produto,
			id_distribuidor
		)

	SELECT
		id_produto,
		id_distribuidor

	FROM
		OPENJSON(@produto_input)
	
	WITH
		(
			id_produto VARCHAR(200)   '$.id_produto',  
			id_distribuidor INT       '$.id_distribuidor'
		);

-- Verificando os produtos do input que podem ser vistos
INSERT INTO
	#HOLD_ID_PRODUTO
		(
			id_produto,
			id_distribuidor
		)

	SELECT
		vpd.id_produto,
		vpd.id_distribuidor

	FROM
		@prod_distr as vpd

		INNER JOIN #HOLD_PRODUTO thp ON
			vpd.id_produto = thp.id_produto
			AND vpd.id_distribuidor = thp.id_distribuidor
			
	ORDER BY
		vpd.ordem ASC;

-- For do codigo de produto
SET @max_product = (SELECT MAX(id) FROM #HOLD_ID_PRODUTO);
SET @actual_product = (SELECT MIN(id) FROM #HOLD_ID_PRODUTO);

WHILE @actual_product <= @max_product
BEGIN

	-- Pegando o id_distribuidor e id_produto
	SELECT
		@id_produto = tip.id_produto,
		@id_distribuidor = tip.id_distribuidor,
		@distribuidor_nome_fantasia = RTRIM(d.nome_fantasia)
	
	FROM
		#HOLD_ID_PRODUTO tip

		INNER JOIN DISTRIBUIDOR d ON
			tip.id_distribuidor = d.id_distribuidor
	
	WHERE
		id = @actual_product;

	-- Limpando tgs
	DELETE FROM
		@subgrupo;

	DELETE FROM
		@grupo;

	DELETE FROM
		@tipo;

	SET @tgs_erro = 0;

	-- Pegando os subgrupos
	INSERT INTO
		@subgrupo

		SELECT
			DISTINCT s.id_subgrupo,
			RTRIM(s.descricao)

		FROM
			PRODUTO p

			INNER JOIN PRODUTO_SUBGRUPO ps ON
				p.id_produto = ps.codigo_produto

			INNER JOIN SUBGRUPO s ON
				ps.id_subgrupo = s.id_subgrupo
				AND ps.id_distribuidor = s.id_distribuidor

		WHERE
			ps.status = 'A'
			AND s.status = 'A'
			AND ps.id_distribuidor = @id_distribuidor
			AND p.id_produto = @id_produto;

	IF EXISTS(SELECT TOP 1 1 FROM @subgrupo)
	BEGIN
		
		-- Pegando os grupos
		INSERT INTO
			@grupo

			SELECT
				DISTINCT g.id_grupo,
				RTRIM(g.descricao)

			FROM
				GRUPO g

				INNER JOIN GRUPO_SUBGRUPO gs ON
					g.id_grupo = gs.id_grupo
					AND g.id_distribuidor = gs.id_distribuidor

			WHERE
				g.status = 'A'
				AND gs.status = 'A'
				AND g.id_distribuidor = @id_distribuidor
				AND gs.id_subgrupo IN (SELECT id FROM @subgrupo);

		IF EXISTS(SELECT TOP 1 1 FROM @grupo)
		BEGIN

			-- Pegando os tipos
			INSERT INTO
				@tipo

				SELECT
					DISTINCT t.id_tipo,
					RTRIM(t.descricao)

				FROM
					TIPO t

					INNER JOIN GRUPO g ON
						t.id_tipo = g.tipo_pai

				WHERE
					t.status = 'A'
					AND g.id_grupo IN (SELECT id FROM @grupo);

			IF NOT EXISTS (SELECT TOP 1 1 FROM @tipo)
			BEGIN
				SET @tgs_erro = 1;
			END

		END

		ELSE
		BEGIN
			SET @tgs_erro = 1;
		END

	END

	ELSE
	BEGIN
		SET @tgs_erro = 1;
	END

	-- Configuranto t-g-s para json
	IF @tgs_erro = 1
	BEGIN
		SET @hold_tipo = '[]';
		SET @hold_grupo = '[]';
		SET @hold_subgrupo = '[]';
	END

	ELSE
	BEGIN
		SET @hold_tipo = REPLACE((SELECT * FROM @tipo FOR JSON AUTO), '\/', '/');
		SET @hold_grupo = REPLACE((SELECT * FROM @grupo FOR JSON AUTO), '\/', '/');
		SET @hold_subgrupo = REPLACE((SELECT * FROM @subgrupo FOR JSON AUTO), '\/', '/');
	END

	-- Verifica��o do pre�o do produto
	IF @id_cliente IS NOT NULL
	BEGIN
		
		-- ## Verifica se o cliente tem pre�o �nico atrelado a ele
		IF @bool_marketplace = 1 AND EXISTS (
												SELECT 
													TOP 1 1
                                    
												FROM 
													TABELA_PRECO_CLIENTE 
                                    
												WHERE 
													id_cliente = @id_cliente 
													AND id_distribuidor = @id_distribuidor
											)
		BEGIN
                        
			SET @preco_tabela = (
									SELECT
										TOP 1 tpp.preco_tabela
                            
									FROM
										PRODUTO p
                                
										INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
											p.id_produto = pd.id_produto
                            
										INNER JOIN TABELA_PRECO_PRODUTO tpp ON
											p.id_produto = tpp.id_produto
											AND pd.id_distribuidor = tpp.id_distribuidor
                                
										INNER JOIN TABELA_PRECO tp ON
											tpp.id_tabela_preco = tp.id_tabela_preco
											AND tpp.id_distribuidor = tp.id_distribuidor
                                
										INNER JOIN TABELA_PRECO_CLIENTE tpc ON
											tp.id_tabela_preco = tpc.id_tabela_preco
											AND tp.id_distribuidor = tpc.id_distribuidor

									WHERE
										pd.id_distribuidor = @id_distribuidor
										AND p.id_produto = @id_produto
										AND tp.status = 'A'
										AND tpc.id_cliente = @id_cliente
								);
                            
		END

		-- ## Caso o cliente n�o tenha pre�o �nico ou for o jmanager
		ELSE
		BEGIN 
                        
			SET @preco_tabela = (
									SELECT
										TOP 1 tpp.preco_tabela
                            
									FROM
										PRODUTO p
                                
										INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
											p.id_produto = pd.id_produto
                            
										INNER JOIN TABELA_PRECO_PRODUTO tpp ON
											p.id_produto = tpp.id_produto
											AND pd.id_distribuidor = tpp.id_distribuidor
                                
										INNER JOIN TABELA_PRECO tp ON
											tpp.id_tabela_preco = tp.id_tabela_preco
											AND tpp.id_distribuidor = tp.id_distribuidor

									WHERE
										pd.id_distribuidor = @id_distribuidor
										AND p.id_produto = @id_produto
										AND tp.status = 'A'
										AND tp.tabela_padrao = 'S'
								);
                        
		END

	END

	-- Verificando se o produto possui desconto
	IF (@id_cliente IS NOT NULL AND @bool_marketplace = 1) OR @bool_marketplace = 0
	BEGIN
		DELETE FROM
		@desconto;

		INSERT INTO	
			@desconto
				(
					desconto,
					data_inicio,
					data_final
				)

			SELECT
				TOP 1 od.desconto,
				o.data_inicio,
				o.data_final
                        
			FROM
				OFERTA o
                        
				INNER JOIN OFERTA_DESCONTO od ON
					o.id_oferta = od.id_oferta
                            
				INNER JOIN OFERTA_PRODUTO op ON
					od.id_oferta = op.id_oferta
                            
			WHERE
				o.status = 'A'
				AND od.status = 'A'
				AND op.status = 'A'
				AND o.tipo_oferta = 1
				AND o.data_cadastro <= GETDATE()
				AND o.data_inicio <= GETDATE()
				AND o.data_final >= GETDATE()
				AND op.id_produto = @id_produto
				AND o.id_distribuidor = @id_distribuidor

			ORDER BY
				od.desconto DESC;

		-- Pegando o valor do desconto
		IF EXISTS (SELECT TOP 1 1 FROM @desconto)
		BEGIN

			-- Criando o preco de venda
			SET @desconto_hold = (SELECT TOP 1 desconto FROM @desconto);

			IF @desconto_hold <= 0 OR @desconto_hold IS NULL
			BEGIN
				SET @preco_venda = @preco_tabela;
				SET @desconto_hold = 0;
			END
					
			ELSE IF @desconto_hold >= 100
			BEGIN
				SET @preco_venda = 0;
				SET @desconto_hold = 100;
			END

			ELSE
			BEGIN
				SET @preco_venda = @preco_tabela * (1 - (@desconto_hold/100))
			END

			-- Atualizando a tabela de desconto
			UPDATE
				@desconto

			SET
				desconto = @desconto_hold,
				preco_desconto = @preco_venda;
						
			SET @desconto_json_hold = (SELECT TOP 1 * FROM @desconto FOR JSON AUTO);
			SET @desconto_json_hold = SUBSTRING(@desconto_json_hold, 2, LEN(@desconto_json_hold) - 2);
		END

		ELSE
		BEGIN
			SET @desconto_json_hold = '{}';
		END				
				
	END

	-- Verificando se o produto participando de alguma oferta
	DELETE FROM
		@oferta_ativador;

	DELETE FROM
		@oferta_bonificado;

	DELETE FROM
		@oferta_escalonado;

	DELETE FROM
		@bonificada;

	DELETE FROM
		@oferta;

	-- ## Ativador da oferta bonificado
	INSERT INTO
		@oferta_ativador

		SELECT
			DISTINCT o.id_oferta

		FROM
            OFERTA o
                        
            INNER JOIN OFERTA_PRODUTO op ON
                o.id_oferta = op.id_oferta

			INNER JOIN #HOLD_OFERTA tho ON
				o.id_oferta = tho.id_oferta
                            
        WHERE
            o.tipo_oferta = 2
            AND op.id_produto = @id_produto
            AND o.id_distribuidor = @id_distribuidor;

	-- ## Bonificado da oferta bonificado
	INSERT INTO
		@oferta_bonificado

		SELECT
            DISTINCT o.id_oferta
					
        FROM
            OFERTA o
                        
            INNER JOIN OFERTA_BONIFICADO ob ON
                o.id_oferta = ob.id_oferta

			INNER JOIN #HOLD_OFERTA tho ON
				o.id_oferta = tho.id_oferta                           
                            
        WHERE
            o.tipo_oferta = 2
            AND ob.id_produto = @id_produto
            AND o.id_distribuidor = @id_distribuidor;

	IF EXISTS (SELECT id FROM @oferta_ativador)
	BEGIN
		INSERT INTO
			@bonificada

			SELECT 
				'[' + (SELECT STRING_AGG (id, ',') from @oferta_ativador) + ']',
				'[' + (SELECT STRING_AGG (id, ',') from @oferta_bonificado) + ']';

	END

	SET @hold_bonificada = (SELECT * FROM @bonificada FOR JSON AUTO);

	-- ## Escalonado da oferta escalonada
	INSERT INTO
		@oferta_escalonado

		SELECT
            DISTINCT o.id_oferta
                    
        FROM
            OFERTA o
                        
            INNER JOIN OFERTA_PRODUTO op ON
                o.id_oferta = op.id_oferta
                            
            INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                op.id_oferta = oef.id_oferta
                            
            INNER JOIN PRODUTO p ON
                op.id_produto = p.id_produto
                        
            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                p.id_produto = pd.id_produto
                AND o.id_distribuidor = pd.id_distribuidor
                            
        WHERE
            o.tipo_oferta = 3
            AND op.id_produto = @id_produto
            AND o.id_distribuidor = @id_distribuidor;

	-- ## Salvando as ofertas
	INSERT INTO
		@oferta

		SELECT
			SUBSTRING(@hold_bonificada, 2, LEN(@hold_bonificada) - 2),
			'{"produto_escalonado": [' + (SELECT STRING_AGG (id, ',') from @oferta_escalonado) + ']}'

	SET @hold_oferta = REPLACE((SELECT * FROM @oferta FOR JSON AUTO), '\"', '"');
	SET @hold_oferta = REPLACE(REPLACE(@hold_oferta, '"{', '{'), '}"', '}');
	SET @hold_oferta = SUBSTRING(@hold_oferta, 2, LEN(@hold_oferta) - 2);

	-- Salvando as informa��es
	INSERT INTO
		#HOLD_INFO_PRODUTO_DISTRIBUIDOR
			(
				id_produto,
				id_distribuidor,
				descricao_distribuidor,
				agrupamento_variante,
				cod_prod_distr,
				cod_frag_distr,
				id_tipo,
				id_grupo,
				id_subgrupo,
				id_fornecedor,
				desc_fornecedor,
				multiplo_venda,
				ranking,
				unidade_venda,
				quant_unid_venda,
				giro,
				agrup_familia,
				status,
				data_cadastro
			)

		SELECT
            TOP 1 @id_produto,
			@id_distribuidor,
			@distribuidor_nome_fantasia,
			pd.agrupamento_variante,
            pd.cod_prod_distr,
            SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
			@hold_tipo,
			@hold_grupo,
			@hold_subgrupo,
			f.id_fornecedor,
            f.desc_fornecedor,
            pd.multiplo_venda,
            pd.ranking,
            pd.unidade_venda,
            pd.quant_unid_venda,
            pd.giro,
            pd.agrup_familia,  
            pd.status,
            pd.data_cadastro

        FROM
            PRODUTO_DISTRIBUIDOR pd

            INNER JOIN FORNECEDOR f ON
                pd.id_fornecedor = f.id_fornecedor

		WHERE
			pd.id_produto = @id_produto
			AND pd.id_distribuidor = @id_distribuidor;

	-- Adicionando informa��es necess�rias para jmanager ou clientes logados
	IF (@id_cliente IS NOT NULL AND @bool_marketplace = 1) OR @bool_marketplace = 0
	BEGIN

		UPDATE
			#HOLD_INFO_PRODUTO_DISTRIBUIDOR

		SET
			desconto = @desconto_json_hold,
			preco_tabela = @preco_tabela,
			ofertas = @hold_oferta,
			estoque = (	
							SELECT
								TOP 1
									CASE
										WHEN qtd_estoque <= 0 OR qtd_estoque IS NULL
											THEN 0
										ELSE
											qtd_estoque
									END

							FROM
								PRODUTO_ESTOQUE

							WHERE
								id_produto = @id_produto
						);
	END

	-- Salvando o json do distribuidor
	SET @distribuidores = (SELECT TOP 1 * FROM #HOLD_INFO_PRODUTO_DISTRIBUIDOR FOR JSON AUTO, INCLUDE_NULL_VALUES);
	SET @distribuidores = REPLACE(@distribuidores, '\"', '"');
	SET @distribuidores = REPLACE(REPLACE(@distribuidores, '"[', '['), ']"', ']');
	SET @distribuidores = REPLACE(REPLACE(@distribuidores, '"{', '{'), '}"', '}');
	SET @distribuidores = REPLACE(@distribuidores, '"', '&%$');


	DELETE FROM
		#HOLD_INFO_PRODUTO_DISTRIBUIDOR;

	-- Pegando as imagens do produto
	SET @imagens = (
						
						SELECT
							'"' + CONVERT(VARCHAR(100), @image_server) + '/imagens/' +  REPLACE(pimg.token, '/auto/', '/' + @image_replace + '/') + '",' AS 'data()'

						FROM
							PRODUTO_IMAGEM pimg

							INNER JOIN PRODUTO p ON
								pimg.sku = p.sku

						WHERE
							p.id_produto = @id_produto

						ORDER BY
							pimg.imagem
		
						FOR 
							XML PATH('')
					);

	IF LEN(@imagens) > 0
	BEGIN
		SET @imagens = '[' + SUBSTRING(@imagens, 1, LEN(@imagens) - 1) + ']';
	END

	ELSE
	BEGIN
		SET @imagens = '[]';
	END

	-- Salvando o produto completo
	INSERT INTO
		#HOLD_INFO_PRODUTO
			(
				sku,
				descricao_produto,
				status,
				marca,
				descricao_marca,
				dun14,
				tipo_produto,
				variante,
				volumetria,
				unidade_embalagem,
				quantidade_embalagem,
				unidade_venda,
				quant_unid_venda,
				imagem,
				distribuidores
			)

		SELECT
            TOP 1 p.sku,
            p.descricao_completa,
            p.status,
            m.id_marca,
            RTRIM(UPPER(m.desc_marca)),
            p.dun14,
            p.tipo_produto,
            p.variante,
            p.volumetria,
            p.unidade_embalagem,
            p.quantidade_embalagem,
            p.unidade_venda,
            p.quant_unid_venda,
			@imagens,
			@distribuidores
				
        FROM
            PRODUTO p

            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                p.id_produto = pd.id_produto 

            INNER JOIN MARCA m ON
                pd.id_marca = m.id_marca 
                AND pd.id_distribuidor = m.id_distribuidor

        WHERE
			p.id_produto = @id_produto
			AND (
					(
						@bool_marketplace = 1
						AND p.status = 'A'
						AND pd.status = 'A'
						AND m.status = 'A'
					)
						OR
					(
						@bool_marketplace = 0
					)
				)
                
        ORDER BY
            pd.id_distribuidor ASC;

	-- Incrementando o contador
	SET @actual_product = @actual_product + 1;

END

-- Mostrando o resultado
SELECT
	sku,
	descricao_produto,
	status,
	marca,
	descricao_marca,
	dun14,
	tipo_produto,
	variante,
	volumetria,
	unidade_embalagem,
	quantidade_embalagem,
	unidade_venda,
	quant_unid_venda,
	imagem,
	distribuidores

FROM
	#HOLD_INFO_PRODUTO;
	
-- Deletando tabelas temporarias
DROP TABLE #HOLD_ID_PRODUTO;
DROP TABLE #HOLD_PRODUTO;
DROP TABLE #HOLD_OFERTA;
DROP TABLE #HOLD_INFO_PRODUTO_DISTRIBUIDOR;
DROP TABLE #HOLD_INFO_PRODUTO;