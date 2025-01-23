USE [B2BTESTE2]

SET NOCOUNT ON;

-- Verificando se a tabela temporaria existe
IF Object_ID('tempDB..#HOLD_DISTRIBUIDOR','U') IS NOT NULL 
BEGIN
    DROP TABLE #HOLD_DISTRIBUIDOR
END;

-- Criando tabelas tempor�rias
CREATE TABLE #HOLD_DISTRIBUIDOR
(
	id INT IDENTITY(1,1),
	id_distribuidor INT UNIQUE,
	nome_fantasia VARCHAR(1000),
    razao_social VARCHAR(1000),
    cor_a VARCHAR(50),
    cor_b VARCHAR(50),
    cor_c VARCHAR(50),
    imagem VARCHAR(MAX)
);

-- Variaveis globais
--## Variaveis de entrada
DECLARE @id_cliente INT = ISNULL(NULL,0);
DECLARE @image_route VARCHAR(1000) = 'https://midiasmarketplace-dev.guarany.com.br/imagens/fotos/auto/distribuidores';

--## Variaveis de apoio
DECLARE @imagem VARCHAR(MAX);
DECLARE @nome_fantasia VARCHAR(MAX);
DECLARE @actual_count INT;
DECLARE @max_count INT;

--## Variaveis tabelas
DECLARE @hold_string TABLE (
	string VARCHAR(MAX)
);

DECLARE @hold_imagem TABLE (
	imagem VARCHAR(MAX)
);

-- Salvando o distribuidor 0
INSERT INTO
	#HOLD_DISTRIBUIDOR
		(
			id_distribuidor,
			nome_fantasia,
			razao_social,
			cor_a,
			cor_b,
			cor_c,
			imagem
		)

	SELECT
		TOP 1 d.id_distribuidor,
		d.nome_fantasia,
		d.razao_social,
		d.cor_a,
		d.cor_b,
		d.cor_c,
		d.imagem

	FROM
		DISTRIBUIDOR d

	WHERE
		d.status = 'A'
		AND d.id_distribuidor = 0;

-- Salvando os outros distribuidores
INSERT INTO
	#HOLD_DISTRIBUIDOR
		(
			id_distribuidor,
			nome_fantasia,
			razao_social,
			cor_a,
			cor_b,
			cor_c,
			imagem
		)

	SELECT
		DISTINCT d.id_distribuidor,
		d.nome_fantasia,
		d.razao_social,
		d.cor_a,
		d.cor_b,
		d.cor_c,
		d.imagem
            
	FROM
		DISTRIBUIDOR d

		INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
			d.id_distribuidor = cd.id_distribuidor

		INNER JOIN CLIENTE c ON
			cd.id_cliente = c.id_cliente
            
	WHERE
		c.status = 'A'
		AND cd.status = 'A'
		AND cd.d_e_l_e_t_ = 0
		AND cd.data_aprovacao <= GETDATE()
		AND d.status = 'A'                
		AND c.id_cliente = CASE WHEN @id_cliente = 0 THEN c.id_cliente ELSE @id_cliente END
		AND d.id_distribuidor != 0

	ORDER BY
		d.id_distribuidor ASC;

-- Realizando o for
SELECT
	@max_count = MAX(id),
	@actual_count = MIN(id)

FROM
	#HOLD_DISTRIBUIDOR;

WHILE @actual_count <= @max_count
BEGIN

	SELECT 
		TOP 1 
			@imagem = imagem,
			@nome_fantasia = nome_fantasia
			
	FROM 
		#HOLD_DISTRIBUIDOR 
			
	WHERE 
		id = @actual_count;

	-- Split do nome_fantasia
	DELETE FROM @hold_string;

	INSERT INTO
		@hold_string

		SELECT
			value

		FROM
			STRING_SPLIT(LOWER(@nome_fantasia), ' ')

		WHERE
			LEN(value) > 0;

	SET @nome_fantasia = (SELECT string + '-' FROM @hold_string FOR XML PATH(''));
	SET @nome_fantasia = SUBSTRING(@nome_fantasia, 1, LEN(@nome_fantasia) - 1);
	
	-- Split de imagem
	SET @imagem = REPLACE(REPLACE(@imagem, '.', ' '), ',', ' ');

	DELETE FROM @hold_imagem;

	INSERT INTO
		@hold_imagem
			(
				imagem
			)

		SELECT
			@image_route + '/' + @nome_fantasia + '/' + value

		FROM
			STRING_SPLIT(LOWER(@imagem), ' ')

		WHERE
			LEN(value) > 0
			AND value LIKE '%[0-9]%'

		ORDER BY
			value ASC;

	SET @imagem = (SELECT imagem + ',' FROM @hold_imagem FOR XML PATH(''));
	SET @imagem = SUBSTRING(@imagem, 1, LEN(@imagem) - 1);

	-- Atualizando dados de imagem
	UPDATE
		#HOLD_DISTRIBUIDOR

	SET
		imagem = @imagem

	WHERE
		id = @actual_count;

	-- Incrementando o contador
	SET @actual_count = @actual_count + 1;

END

-- Mostrando o resultado
SELECT
	id_distribuidor,
	nome_fantasia,
    razao_social,
    cor_a,
    cor_b,
    cor_c,
    imagem

FROM
	#HOLD_DISTRIBUIDOR

ORDER BY
	id_distribuidor;

-- Deletando as tabelas temporarias
DROP TABLE #HOLD_DISTRIBUIDOR;