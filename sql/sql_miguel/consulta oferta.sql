SELECT [id_oferta] id_campanha
      ,[descricao_oferta] descricao
      ,[id_distribuidor] id_distribuidor
      ,[tipo_oferta] tipo_campanha /*TEM QUE TRATAR PARA ENVIAR W, B, Q, X (Provavelmente COMPRE GANHA eh B, e escalonado eh X)*/
	  ,CASE WHEN ordem=1 THEN 1 ELSE 0 END destaque
      ,[data_inicio] data_inicio
      ,[data_final] data_final
	  .
      ,[limite_ativacao_cliente] maxima_ativacao
      ,[limite_ativacao_oferta] quantidade_limite
      ,[limite_ativacao_oferta] quantidade_pontos
	  ,/*somar os produtos ativadores*/ 0 quantidade_produto
      ,[status] status
	  ,1 automatica

	  /*DA TABELA QUE NAO PRECISA ENVIAR POR ENQUANTO*/
      ,[produto_agrupado]
      ,[data_cadastro]
      ,[usuario_cadastro]
      ,[data_atualizacao]
      ,[usuario_atualizacao]

	  /*O QUE ELE TEM QUE NAO TEMOS*/
	  --quantidade_falta
	  --quantidade_bonificado
	  --ativada

	  --produto_bonificado[]
	  --produto_ativador[]
  FROM [dbo].[OFERTA]
  WHERE tipo_oferta in (2)
  ORDER BY ORDEM, descricao_oferta

--SELECT * FROM [dbo].[OFERTA_TIPO]

--produto_bonificado: ProdutoBonificado[];
--produto_ativador: ProdutoAtivador[];
--automatica: boolean;
--quantidade_falta?: number;
--quantidade_bonificado: number;
--ativada?: boolean;