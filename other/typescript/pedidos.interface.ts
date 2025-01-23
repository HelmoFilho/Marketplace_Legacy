export interface DefaultResponse {
    status: number;
    resposta: Resposta;
}

export interface SalvarPedidoRequest {
    id_cliente: number;
    id_distribuidor: number;
    pedido: Pedido;
}

export interface SalvarPedidoResponse extends DefaultResponse {
dados: {
    id_cliente: number;
    id_distribuidor: number;
    id_orcamento: number;
    id_pedido: number;
    status_pedido: StatusPedido;
    status_pagamento: StatusPagamento;
}
}

export interface Pedido {
    id_pedido?: number;
    id_orcamento?: number;
    id_usuario: number;
    cliente: Cliente;
    itens: ListaItem[];
    qtde_itens: number;
    subtotal: number;
    desconto: number;
    bonus: number;
    total: number;
    metodo_pagamento: MetodoPagamento;
    data_pedido: string;
    status_pedido?: StatusPedido;
    status_pagamento?: StatusPagamento;
    codigo_status?: number;
    percent?: number;
}

export interface MetodoPagamento {
    id: number;
    nome: string;
    id_condpgto?: number;
    descricao?: string;
    id_maxipago?: string;
    numero_cartao?: string;
    bandeira?: string;
    observacao?: string;
}

export enum StatusPedido {
    ENVIADO = 'Enviado ao distribuidor',
    FATURADO = 'Faturado',
    SEPARADO = 'Produtos separado',
    TRANSPORTE = 'Em transporte/trânsito',
    ENTREGUE = 'Entregue',
    PENDENTE = 'Pendente',
    CANCELADO = 'Cancelado',
}
  
export enum StatusPagamento {
    EMPROCESSAMENTO = 'Em processamento',
    PROCESSADO = 'Processado',
    PENDENTE = 'Pendente',
    CANCELADO = 'Cancelado',
}