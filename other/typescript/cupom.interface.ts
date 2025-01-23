import { DefaultResponse, Informacoes, Status } from '.';

export interface CupomRequest {
  id_distribuidor: number;
  pagina?: number | null;
}

export interface CupomResponse extends DefaultResponse {
  informacoes?: Informacoes;
  dados?: Cupom[];
}

export interface Cupom {
  id_distribuidor: number;
  id_cupom: number;
  tipo_cupom: TipoCupom;
  codigo_cupom: string;
  titulo: string;
  descricao: string;
  data_inicio: string;
  data_final: string;
  desconto_valor: number;
  desconto_percentual: number;
  valor_limite: number;
  valor_ativar: number;
  termos_uso: string;
  status: Status;
  reutiliza: number;
  oculto: boolean;
  bloqueado: boolean;
  motivo_bloqueio: string | null;
  tipo_itens: TipoItens;
  itens: number[]; // array com lista de ids (grupo ou subgrupo ou marca)
  ativado?: boolean;
}

export enum TipoCupom {
  FRETE = 1,
  PERCENTUAL = 2,
  VALOR = 3,
}

export enum TipoItens {
  TUDO = 0,
  GRUPO = 1,
  SUBGRUPO = 2,
  MARCA = 3,
}