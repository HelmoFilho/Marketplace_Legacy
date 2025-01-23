import { BaseProduto, DefaultResponse, Produto, Status } from ".";

export interface Ofertas {
  campanha: OfertaCampanha[];
  escalonado: OfertaEscalonado[]
}

export interface CampanhaRequest {
  id_distribuidor: number;
}

export interface CampanhaResponse extends DefaultResponse {
  dados?: OfertaCampanha[];
}

export interface EscalonadoRequest {
  id_distribuidor: number;
}

export interface EscalonadoResponse extends DefaultResponse {
  dados?: OfertaEscalonado[];
}

export interface OfertaCampanha {
  id_campanha: number;
  id_distribuidor: number;
  status: Status;
  tipo_campanha: string; // W, B, Q, X TODO: verificar descrição dos tipos
  descricao: string;
  data_inicio: string;
  data_final: string;
  destaque: boolean;
  quantidade_limite: number;
  quantidade_pontos: number;
  quantidade_produto: number;
  quantidade_ativar: number;
  valor_ativar: number;
  maxima_ativacao: number;
  unidade_venda: string;
  produto_bonificado: ProdutoBonificado[];
  produto_ativador: ProdutoAtivador[];
  automatica: boolean;
  quantidade_falta?: number;
  quantidade_bonificado: number;
  ativada?: boolean;
}

export interface ProdutoBonificado extends BaseProduto {
  id_campanha: number;
  id_distribuidor: number;
  preco_tabela: number;
  quantidade: number;
  estoque: number;
}

export interface ProdutoAtivador extends BaseProduto {
  id_campanha: number;
  id_distribuidor: number;
  preco_tabela: number;
  quantidade: number;
  estoque: number;
  quantidade_minima: number;
  valor_minimo: number;
  desconto: number;
}



export interface OfertaEscalonado {
  id_escalonado: number;
  id_distribuidor: number;
  status: Status;
  escalonado: Escalonado[];
  faixa_escalonado: FaixaEscalonado[];
  produto_escalonado: ProdutoEscalonado[];
  total_desconto: number;
  unificada: boolean;
  ativada?: boolean;
}

export interface Escalonado {
  id_escalonado: number;
  id_distribuidor: number;
  data_inicio: string;
  data_final: string;
  limite: number;
  status_escalonado: Status; 
}

export interface FaixaEscalonado {
  id_escalonado: number;
  id_distribuidor: number;
  sequencia: number; // 1 tenho que comprar 30 produtos, 2 tem que 40 produtos para o desconto
  faixa: number; // é a quantidade para atingir a desconto da sequencio
  desconto: number; // desconto da sequencia
  unidade_venda: string;
}

export interface ProdutoEscalonado extends BaseProduto  {
  id_escalonado: number;
  id_distribuidor: number;
  preco_tabela: number;
  preco_desconto: number;
  quantidade: number;
  estoque: number;
}

