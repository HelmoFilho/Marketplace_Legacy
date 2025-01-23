export interface ClienteMetodosPagamentoRequest {
    id_distribuidor: number;
    id_cliente: number;
}
  
export interface ClienteMetodosPagamentoResponse extends DefaultResponse {
    informacoes?: Informacoes;
    dados?: ClienteMetodosPagamento[];
}
  
export interface ClienteMetodosPagamento {
    id: number;
    nome: string;
    prazos?: number[];
    cartoes?: ClientCard[];
}
  
export interface ClientCard {
    cartao: string;
    validade?: string;
    token_cartao?: string;
    parcelas: number[];
}