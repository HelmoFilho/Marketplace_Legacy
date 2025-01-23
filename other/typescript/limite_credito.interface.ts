export interface LimitesClienteRequest {
    id_cliente: number;
    id_distribuidor: number[];
}
export interface LimitesClienteResponse {
    id_cliente: number;
    limites: Limites[];
}