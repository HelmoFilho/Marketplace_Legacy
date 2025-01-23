/**
 * Documentação:
 * http://192.168.1.168:5753/documentation/listar_tipo_grupo_subgrupo_marketplace
 * http://192.168.1.168:5753/documentation/listar_produtos_marketplace
 *
 */

import { DefaultResponse } from '.';

export interface Tipo {
  id_tipo: number;
  descricao_tipo: string;
  grupo: Grupo[];
}

export interface Grupo {
  id_grupo: number;
  descricao_grupo: string;
  subgrupo: Subgrupo[];
}

export interface Subgrupo {
  id_subgrupo: number;
  descricao_subgrupo: string;
}

export interface Description {
  id: number;
  descricao: string;
}

export interface Informacoes {
  itens?: number;
  paginas?: number;
  pagina_atual?: number;
}

// export interface TipoGrupos {
//   id_distribuidor: number;
//   id_tipo: number;
//   descricao: string;
//   status: Status;
//   data_cadastro: string;
// }

// export interface GrupoProdutos {
//   id_distribuidor: number;
//   tipo_pai: number;
//   id_grupo: number;
//   descricao: string;
//   status: Status;
//   data_cadastro: string;
// }

// export interface SubgrupoProdutos {
//   id_distribuidor: number;
//   grupo_pai: number[];
//   id_subgrupo: number;
//   descricao: string;
//   status: Status;
//   data_cadastro: string;
// }

export interface GrupoProdutosRequest {
  id_distribuidor: number;
}

export interface Catalogo {
  // tipo: TipoGrupos[];
  // grupo: GrupoProdutos[];
  // subgrupo: SubgrupoProdutos[];
  tipo: Tipo[]
}

export interface GrupoProdutosResponse extends DefaultResponse {
  dados?: Tipo[];
}

export interface ProdutoRequest {
  id_distribuidor: number;
  id_tipo: number;
  id_grupo: number;
  id_subgrupo: number | null;
  pagina?: number | null;
}

export interface ProdutoResponse extends DefaultResponse {
  informacoes?: Informacoes;
  dados?: Produto[];
}

export interface BaseProduto {
  id_produto: string;
  sku: string;
  dun14?: string;
  marca?: number;
  descricao_marca: string;
  tipo_produto?: string;
  descricao_produto: string;
  imagem: string[] | [];
  quant_unid_venda?: number;
  quantidade_embalagem?: number;
  unidade_embalagem?: string;
  unidade_venda?: string;
  variante?: string;
  volumetria?: string;
  status: Status;
}
export interface Produto extends BaseProduto {
  distribuidores: ProdutoDistribuidor[] | [];
}

export interface ProdutoDistribuidor {
  id_distribuidor: number;
  id_tipo: Description[];
  id_grupo: Description[];
  id_subgrupo: Description[];
  cod_frag_distr: string;
  cod_prod_distr: string;
  data_cadastro: string;
  agrup_familia: string;
  agrupamento_variante: string;
  desc_fornecedor: string;
  descricao_distribuidor: string;
  quant_unid_venda: number;
  unidade_venda: string;
  preco_tabela: number;
  giro: string;
  estoque: number;
  multiplo_venda: number;
  oferta: Object,
  ranking: number;
  status: Status;
}

export enum Status {
  ATIVO = 'A',
  INATIVO = 'I',
}
