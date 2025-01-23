#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class FormasPagamento(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Formas de Pagamentos do Cliente

        Returns:
            [dict]: Status da transação
        """

        # Pegando e validando os dados da request
        response_data = dm.get_request(values_upper = True)

        necessary_keys = ["id_cliente", "id_distribuidor"]

        correct_types = {
            "id_distribuidor": int, "id_cliente": int
        }

        # Checa chaves inválidas e faltantes, valores nulos e se o usuario não existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys,
                                          not_null = necessary_keys,
                                          correct_types = correct_types)):
            
            return validity

        # Pegando os dados necessarios
        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = int(response_data.get("id_cliente"))
        id_usuario = global_info.get("id_usuario")

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        # Verificando se o cliente pode comprar
        cliente_query = """
            SELECT
                TOP 1 id_cliente

            FROM
                CLIENTE

            WHERE
                status = 'A'
                AND status_receita = 'A'
        """

        params = {
            "id_cliente": id_cliente
        }

        cliente_query = dm.raw_sql_return(cliente_query, params = params, raw = True, first = True)

        if not cliente_query:
            logger.error(f"Cliente nao pode comprar devido status da receita.")
            return {"status":403,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Cliente sem permissão de comprar devidos a problemas com cnpj."}}, 403

        # Checando se o cliente pode comprar do distribuidor informado
        cd_query = """
            SELECT
                c.id_cliente

            FROM
                CLIENTE c

                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                    c.id_cliente = cd.id_cliente

                INNER JOIN DISTRIBUIDOR d ON
                    cd.id_distribuidor = d.id_distribuidor

            WHERE
                c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND cd.data_aprovacao <= GETDATE()
                AND d.status = 'A'
                AND c.id_cliente = :id_cliente
                AND d.id_distribuidor = :id_distribuidor
        """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

        cd_query = dm.raw_sql_return(cd_query, params = params, first = True, raw = True)

        if not cd_query:
            logger.error(f"Cliente nao pode comprar do D:{id_distribuidor}.")
            return {"status":403,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Cliente sem permissão de comprar do distribuidor informado."}}, 403

        # Pegando formas de pagamento
        dict_formas_pagamento = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

        dados = dj.json_formas_pagamento(**dict_formas_pagamento)

        if not dados:
            logger.info(f"Cliente nao possui formas de pagamento disponiveis.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Sem formas de pagamento disponiveis."}}, 200

        logger.info("Formas de pagamento do cliente enviadas")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Formas de pagamento enviadas."}, 
                "dados": dados}, 200