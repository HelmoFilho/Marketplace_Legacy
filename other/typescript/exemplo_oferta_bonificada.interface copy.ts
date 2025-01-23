this._campaignOffersMock = [
    {
      id_campanha: 1, //
      id_distribuidor: 1, //
      status: Status.ATIVO, //
      tipo_campanha: 'W', //
      descricao: //
        'Compre 8 UN SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA para ganhar 1 UN SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA TRADICIONAL',
      data_inicio: '2022-01-01', //
      data_final: '2022-12-31', //
      destaque: false, // 
      quantidade_limite: 20, //
      quantidade_pontos: 20, //
      quantidade_produto: 1, //
      quantidade_ativar: 8, //
      maxima_ativacao: 16, //
      quantidade_bonificado: 0, 
      valor_ativar: 0, //
      unidade_venda: 'UN', //
      produto_bonificado: [
        {
          id_campanha: 1, //
          id_distribuidor: 1, //
          id_produto: '225-1', //
          sku: '7896279120784', //
          descricao_marca: 'VITA CAPILI', //
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML TRADICIONAL', //
          imagem: [ //
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120784/shampoo-anticaspa-vita-capili-2x1-150ml-tradicional/1',
          ],
          status: Status.ATIVO, //
          preco_tabela: 0, //
          quantidade: 0, //
          estoque: 5, //
          unidade_embalagem: 'CX', //
          quantidade_embalagem: 12, //
          unidade_venda: 'UN', //
          quant_unid_venda: 1, //
        },
      ],
      produto_ativador: [
        {
          id_campanha: 1, //
          id_distribuidor: 1, //
          id_produto: '225-2', //
          sku: '7896279120791', //
          descricao_marca: 'VITA CAPILI', //
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',//
          imagem: [//
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120791/shampoo-anticaspa-vita-capili-2x1-150ml-limpeza-profunda/1',
          ],
          status: Status.ATIVO,//
          preco_tabela: 6.85,//
          quantidade: 0,//
          estoque: 50, //
          quantidade_minima: 8, //
          valor_minimo: 120, //
          desconto: 0, //
          unidade_embalagem: 'CX',//
          quantidade_embalagem: 12,//
          unidade_venda: 'UN',//
          quant_unid_venda: 1,//
        },
      ],
      automatica: false,
    },
    {
      id_campanha: 2,
      id_distribuidor: 1,
      status: Status.ATIVO,
      tipo_campanha: 'W',
      descricao:
        'Compre 12 UN de qualquer SHAMPOO ANTICASPA VITA CAPILI para ganhar 2 UN SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',
      data_inicio: '2022-01-01',
      data_final: '2022-12-31',
      destaque: false,
      quantidade_limite: 5,
      quantidade_pontos: 5,
      quantidade_produto: 2,
      quantidade_ativar: 12,
      quantidade_bonificado: 0,
      valor_ativar: 0,
      maxima_ativacao: 24,
      unidade_venda: 'UN',
      produto_bonificado: [
        {
          id_campanha: 2,
          id_distribuidor: 1,
          id_produto: '225-1',
          sku: '7896279120784',
          descricao_marca: 'VITA CAPILI',
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML TRADICIONAL',
          imagem: [
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120784/shampoo-anticaspa-vita-capili-2x1-150ml-tradicional/1',
          ],
          status: Status.ATIVO,
          preco_tabela: 0,
          quantidade: 0,
          estoque: 50,
          unidade_embalagem: 'CX',
          quantidade_embalagem: 12,
          unidade_venda: 'UN',
          quant_unid_venda: 1,
        },
      ],
      produto_ativador: [
        {
          id_campanha: 2,
          id_distribuidor: 1,
          id_produto: '225-2',
          sku: '7896279120791',
          descricao_marca: 'VITA CAPILI',
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',
          imagem: [
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120791/shampoo-anticaspa-vita-capili-2x1-150ml-limpeza-profunda/1',
          ],
          status: Status.ATIVO,
          preco_tabela: 6.85,
          quantidade: 0,
          estoque: 50,
          quantidade_minima: 6,
          valor_minimo: 180,
          desconto: 0,
          unidade_embalagem: 'CX',
          quantidade_embalagem: 12,
          unidade_venda: 'UN',
          quant_unid_venda: 1,
        },
        {
          id_campanha: 2,
          id_distribuidor: 1,
          id_produto: '225-1',
          sku: '7896279120784',
          descricao_marca: 'VITA CAPILI',
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML TRADICIONAL',
          imagem: [
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120784/shampoo-anticaspa-vita-capili-2x1-150ml-tradicional/1',
          ],
          status: Status.ATIVO,
          preco_tabela: 6.85,
          quantidade: 0,
          estoque: 50,
          quantidade_minima: 0,
          valor_minimo: 180,
          desconto: 0,
          unidade_embalagem: 'CX',
          quantidade_embalagem: 12,
          unidade_venda: 'UN',
          quant_unid_venda: 1,
        },
      ],
      automatica: false,
    },
    {
      id_campanha: 1,
      id_distribuidor: 2,
      status: Status.ATIVO,
      tipo_campanha: 'W',
      descricao:
        'Compre 8 UN SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA para ganhar 1 UN SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA TRADICIONAL',
      data_inicio: '2022-01-01',
      data_final: '2022-12-31',
      destaque: false,
      quantidade_limite: 20,
      quantidade_pontos: 20,
      quantidade_produto: 1,
      quantidade_ativar: 8,
      maxima_ativacao: 16,
      quantidade_bonificado: 0,
      valor_ativar: 0,
      unidade_venda: 'UN',
      produto_bonificado: [
        {
          id_campanha: 1,
          id_distribuidor: 2,
          id_produto: '225-1',
          sku: '7896279120784',
          descricao_marca: 'VITA CAPILI',
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML TRADICIONAL',
          imagem: [
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120784/shampoo-anticaspa-vita-capili-2x1-150ml-tradicional/1',
          ],
          status: Status.ATIVO,
          preco_tabela: 0,
          quantidade: 0,
          estoque: 5,
          unidade_embalagem: 'CX',
          quantidade_embalagem: 12,
          unidade_venda: 'UN',
          quant_unid_venda: 1,
        },
      ],
      produto_ativador: [
        {
          id_campanha: 1,
          id_distribuidor: 2,
          id_produto: '225-2',
          sku: '7896279120791',
          descricao_marca: 'VITA CAPILI',
          descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',
          imagem: [
            'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120791/shampoo-anticaspa-vita-capili-2x1-150ml-limpeza-profunda/1',
          ],
          status: Status.ATIVO,
          preco_tabela: 6.85,
          quantidade: 0,
          estoque: 50,
          quantidade_minima: 8,
          valor_minimo: 120,
          desconto: 0,
          unidade_embalagem: 'CX',
          quantidade_embalagem: 12,
          unidade_venda: 'UN',
          quant_unid_venda: 1,
        },
      ],
      automatica: false,
    },
  ];