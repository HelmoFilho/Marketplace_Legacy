export interface Financeiro {
    id_pedido: string;
    id_distribuidor: number;
    data_vencimento: string;
    dias_atraso?: number;
    parcelas?: number;
    forma_pagamento: string;
    condicao_pagamento?: CondicaoPagamento;
    nota_fiscal: string;
    valor: number;
    saldo: number;
    status: StatusBoleto;
    id_cliente: number;
    cliente: Cliente;
}

export enum CondicaoPagamento {
    AVISTA = 'À vista',
    PRAZO = 'Prazo',
}

export enum StatusBoleto {
    ABERTO = 'Aberto',
    PAGO = 'Pago',
    ATRASADO = 'Em atraso',
}