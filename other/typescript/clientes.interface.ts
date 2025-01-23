export interface Cliente {
    id_cliente: number;
    id_usuario: number;
    razao_social: string;
    nome_fantasia: string;
    cnpj: number;
    status: Status;
    endereco: string;
    endereco_num: number;
    endereco_complemento: string;
    bairro: string;
    cep: number;
    cidade: string;
    estado: string;
    telefone: string;
    status_receita: Status;
    distribuidor?: Distribuidor[];
    limites?: Limites[]
  }
  
  export interface Limites {
    id_distribuidor: number;
    limite_credito: number;
    credito_atual: number;
    pedido_minimo: number;
  }