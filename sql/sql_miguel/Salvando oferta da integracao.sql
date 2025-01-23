USE [B2BTESTE2]
GO

/****** Object:  Table [dbo].[PRODUTO_DISTRIBUIDOR_ID]    Script Date: 17/02/2022 16:02:41 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--TRUNCATE TABLE API_OFERTA ;

PRINT 'SALVANDO OFERTA_ID'
INSERT INTO OFERTA_ID
	SELECT  ID_DISTRIBUIDOR,
			ID_OFERTA_API,
			GETDATE() DATA_CADASTRO
	FROM API_OFERTA A
	WHERE NOT EXISTS ( SELECT 1 FROM OFERTA_ID B
						WHERE B.ID_OFERTA_API = A.ID_OFERTA_API
						  AND B.id_distribuidor = A.ID_DISTRIBUIDOR )


PRINT 'ATUALIZADO API_OFERTA'
UPDATE A
SET
    A.ID_OFERTA = (SELECT ID_OFERTA
					FROM OFERTA_ID C 
					WHERE C.id_distribuidor = A.ID_DISTRIBUIDOR
						AND C.ID_OFERTA_API = A.ID_OFERTA_API)
FROM API_OFERTA A
WHERE ID_OFERTA IS NULL
	  ;


SELECT * FROM OFERTA_ID
SELECT * FROM API_OFERTA
SELECT * FROM OFERTA

--TRUNCATE TABLE OFERTA_ID
--TRUNCATE TABLE API_OFERTA
--TRUNCATE TABLE OFERTA


INSERT INTO [dbo].OFERTA
           ([id_oferta]
           ,[descricao_oferta]
           ,[id_distribuidor]
           ,[tipo_oferta]
           ,[ordem]
           ,[operador]
           ,[data_inicio]
           ,[data_final]
           ,[necessario_para_ativar]
           ,[limite_ativacao_cliente]
           ,[limite_ativacao_oferta]
           ,[produto_agrupado]
           ,[status]
           ,[data_cadastro]
           ,[usuario_cadastro]
           ,[data_atualizacao]
           ,[usuario_atualizacao])
	SELECT   [id_oferta]
			,[descricao_oferta]
			,[id_distribuidor]
			,[tipo_oferta]
			,[ordem]
			,[operador]
			,[data_inicio]
			,[data_final]
			,[necessario_para_ativar]
			,[limite_ativacao_cliente]
			,[limite_ativacao_oferta]
			,[produto_agrupado]
			,[status]
			,[data_cadastro]
			,[usuario_cadastro]
			,[data_atualizacao]
			,[usuario_atualizacao]
	FROM API_OFERTA A
	WHERE NOT EXISTS (SELECT 1 FROM OFERTA B WHERE A.ID_OFERTA=B.ID_OFERTA)

GO


INSERT INTO [dbo].[OFERTA_BONIFICADO]
           ([id_oferta]
           ,[ID_PRODUTO]
           ,[quantidade_bonificada]
           ,[status])
     VALUES
           (<id_oferta, int,>
           ,<ID_PRODUTO, varchar(20),>
           ,<quantidade_bonificada, int,>
           ,<status, char(1),>)
		   ;

SELECT  [id_oferta]
           ,[ID_PRODUTO]
           ,[ID_PRODUTO_API]
           ,[ID_OFERTA_API]
           ,[quantidade_bonificada]
           ,[status]
		   FROM [dbo].[API_OFERTA_BONIFICADO]


