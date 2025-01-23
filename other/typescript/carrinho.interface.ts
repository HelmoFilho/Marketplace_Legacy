import { BaseProduto, Ofertas, Produto } from '.';

export interface Cesta {
  [key: number]: ListaItem[];
}

export interface CestaOfertas {
  [key: number]: Ofertas;
}

export interface OrderSummary {
  subtotal: number;
  discount: number;
  bonus: number;
  total: number;
}

export interface Item {
  id_campanha?: number;
  id_escalonado?: number;
  produto: BaseProduto | Produto;
  preco_tabela: number;
  preco_desconto?: number;
  quantidade: number;
  estoque: number;
  desconto?: number;
}

export interface Carrinho {
  id_orcamento?: number;
  id_usuario?: number;
  id_cliente?: number;
  data_criacao?: string;
  itens: ListaItem[];
}

export interface ListaItem extends BaseProduto {
  id_distribuidor: number;
  id_campanha?: number;
  id_escalonado?: number;
  preco_tabela: number;
  preco_desconto?: number;
  quantidade: number;
  estoque: number; // TODO: tornar obrigatório?
  bonificado?: boolean;
  desconto?: number;
}
