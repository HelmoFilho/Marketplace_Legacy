this._steppedOffersMock = [
  {
    id_escalonado: 1,
    id_distribuidor: 1,
    status: Status.ATIVO,
    escalonado: [
      {
        id_escalonado: 1,
        id_distribuidor: 1,
        data_inicio: '2022-01-01',
        data_final: '2022-12-31',
        limite: 24,
        status_escalonado: Status.ATIVO,
      },
    ],
    faixa_escalonado: [
      {
        id_escalonado: 1,
        id_distribuidor: 1,
        sequencia: 1,
        faixa: 12,
        desconto: 5,
        unidade_venda: 'UN',
      },
    ],
    produto_escalonado: [
      {
        id_escalonado: 1,
        id_distribuidor: 1,
        id_produto: '225-2',
        sku: '7896279120791',
        descricao_marca: 'VITA CAPILI',
        descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',
        imagem: [
          'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120791/shampoo-anticaspa-vita-capili-2x1-150ml-limpeza-profunda/1',
        ],
        preco_tabela: 6.85,
        preco_desconto: 0,
        quantidade: 0,
        estoque: 50,
        status: Status.ATIVO,
        unidade_embalagem: 'CX',
        quantidade_embalagem: 12,
        unidade_venda: 'UN',
        quant_unid_venda: 1,
      },
    ],
    total_desconto: 0,
    unificada: false,
  },
  {
    id_escalonado: 2,
    id_distribuidor: 1,
    status: Status.ATIVO,
    escalonado: [
      {
        id_escalonado: 2,
        id_distribuidor: 1,
        data_inicio: '2022-01-01',
        data_final: '2022-12-31',
        limite: 24,
        status_escalonado: Status.ATIVO,
      },
    ],
    faixa_escalonado: [
      {
        id_escalonado: 2,
        id_distribuidor: 1,
        sequencia: 2,
        faixa: 24,
        desconto: 10,
        unidade_venda: 'UN',
      },
      {
        id_escalonado: 2,
        id_distribuidor: 1,
        sequencia: 1,
        faixa: 12,
        desconto: 5,
        unidade_venda: 'UN',
      },
    ],
    produto_escalonado: [
      {
        id_escalonado: 2,
        id_distribuidor: 1,
        id_produto: '225-2',
        sku: '7896279120791',
        descricao_marca: 'VITA CAPILI',
        descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',
        imagem: [
          'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120791/shampoo-anticaspa-vita-capili-2x1-150ml-limpeza-profunda/1',
        ],
        preco_tabela: 6.85,
        preco_desconto: 0,
        quantidade: 0,
        estoque: 24,
        status: Status.ATIVO,
        unidade_embalagem: 'CX',
        quantidade_embalagem: 12,
        unidade_venda: 'UN',
        quant_unid_venda: 1,
      },
      {
        id_escalonado: 2,
        id_distribuidor: 1,
        id_produto: '225-1',
        sku: '7896279120784',
        descricao_marca: 'VITA CAPILI',
        descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML TRADICIONAL',
        imagem: [
          'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120784/shampoo-anticaspa-vita-capili-2x1-150ml-tradicional/1',
        ],
        preco_tabela: 6.85,
        preco_desconto: 0,
        quantidade: 0,
        estoque: 24,
        status: Status.ATIVO,
        unidade_embalagem: 'CX',
        quantidade_embalagem: 12,
        unidade_venda: 'UN',
        quant_unid_venda: 1,
      },
    ],
    total_desconto: 0,
    unificada: true,
  },
  {
    id_escalonado: 3,
    id_distribuidor: 1,
    status: Status.ATIVO,
    escalonado: [
      {
        id_escalonado: 3,
        id_distribuidor: 1,
        data_inicio: '2022-01-01',
        data_final: '2022-12-31',
        limite: 24,
        status_escalonado: Status.ATIVO,
      },
    ],
    faixa_escalonado: [
      {
        id_escalonado: 3,
        id_distribuidor: 1,
        sequencia: 2,
        faixa: 24,
        desconto: 10,
        unidade_venda: 'UN',
      },
      {
        id_escalonado: 3,
        id_distribuidor: 1,
        sequencia: 1,
        faixa: 12,
        desconto: 5,
        unidade_venda: 'UN',
      },
      {
        id_escalonado: 3,
        id_distribuidor: 1,
        sequencia: 1,
        faixa: 36,
        desconto: 15,
        unidade_venda: 'UN',
      },
    ],
    produto_escalonado: [
      {
        id_escalonado: 3,
        id_distribuidor: 1,
        id_produto: '80-1',
        sku: '7898588112078',
        descricao_marca: 'TAIFF',
        descricao_produto: 'SECADOR TAIFF STYLE PRO 2000V',
        imagem: [
          'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7898588112078/secador-taiff-style-pro-2000v/1',
        ],
        preco_tabela: 243.92,
        preco_desconto: 0,
        quantidade: 0,
        estoque: 0,
        status: Status.ATIVO,
        unidade_embalagem: 'CX',
        quantidade_embalagem: 1,
        unidade_venda: 'UN',
        quant_unid_venda: 1,
      },
      {
        id_escalonado: 3,
        id_distribuidor: 1,
        id_produto: '81-1',
        sku: '7898588112214',
        descricao_marca: 'TAIFF',
        descricao_produto: 'ESCOVA TAIFF STYLE SECA E ALISAR',
        imagem: [
          'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7898588112214/escova-taiff-style-seca-e-alisar/1',
        ],
        preco_tabela: 280.6,
        preco_desconto: 0,
        quantidade: 0,
        estoque: 50,
        status: Status.ATIVO,
        unidade_embalagem: 'CX',
        quantidade_embalagem: 1,
        unidade_venda: 'UN',
        quant_unid_venda: 1,
      },
    ],
    total_desconto: 0,
    unificada: false,
  },
  {
    id_escalonado: 1,
    id_distribuidor: 2,
    status: Status.ATIVO,
    escalonado: [
      {
        id_escalonado: 1,
        id_distribuidor: 2,
        data_inicio: '2022-01-01',
        data_final: '2022-12-31',
        limite: 24,
        status_escalonado: Status.ATIVO,
      },
    ],
    faixa_escalonado: [
      {
        id_escalonado: 1,
        id_distribuidor: 2,
        sequencia: 1,
        faixa: 12,
        desconto: 5,
        unidade_venda: 'UN',
      },
    ],
    produto_escalonado: [
      {
        id_escalonado: 1,
        id_distribuidor: 2,
        id_produto: '225-2',
        sku: '7896279120791',
        descricao_marca: 'VITA CAPILI',
        descricao_produto: 'SHAMPOO ANTICASPA VITA CAPILI 2X1 150ML LIMPEZA PROFUNDA',
        imagem: [
          'https://midiasmarketplace-dev.guarany.com.br/imagens/produto/auto/7896279120791/shampoo-anticaspa-vita-capili-2x1-150ml-limpeza-profunda/1',
        ],
        preco_tabela: 6.85,
        preco_desconto: 0,
        quantidade: 0,
        estoque: 50,
        status: Status.ATIVO,
        unidade_embalagem: 'CX',
        quantidade_embalagem: 12,
        unidade_venda: 'UN',
        quant_unid_venda: 1,
      },
    ],
    total_desconto: 0,
    unificada: false,
  },
];