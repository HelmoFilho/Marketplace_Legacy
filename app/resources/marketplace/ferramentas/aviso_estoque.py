#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class AvisoEstoque(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Aviso de Estoque

        Returns:
            [dict]: Status da transação
        """

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["id_produto", "id_cliente"]

        correct_types = {
            "id_cliente": int
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity

        id_usuario = global_info.get("id_usuario")
        id_cliente = int(response_data["id_cliente"])

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        id_produto = response_data["id_produto"]

        # Checando se já não existe notificação criada para o produto
        query = """
            SELECT
                TOP 1 id_notificacao

            FROM
                NOTIFICACAO_ESTOQUE

            WHERE
                id_usuario = :id_usuario
                AND id_produto = :id_produto
                AND status = 'P'
        """

        params = {
            "id_usuario": id_usuario,
            "id_produto": id_produto,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
            "first": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if response:

            id_notificacao = response[0]

            update_notificacao = {
                "id_notificacao": id_notificacao,
                "status": 'C',
            }

            key_columns = ["id_notificacao"]

            dm.raw_sql_update("NOTIFICACAO_ESTOQUE", update_notificacao, key_columns)
            
            logger.info(f"Notificação de estoque {id_notificacao} para o produto {id_produto} cancelada pelo usuario.")
            return {"status": 200,
                    "resposta": {
                        "tipo": "1", 
                        "descricao": "Notificação cancelada."}}, 200

        # Verificando o produto
        query = """
            SELECT
                TOP 1 pd.id_distribuidor,
                p.status,
                pd.status

            FROM
                PRODUTO p

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto

            WHERE
                p.id_produto = :id_produto
        """

        params = {
            "id_produto": id_produto
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
            "first": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error(f"Produto {id_produto} não cadastrado na base.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Produto não cadastrado na base."}}, 200

        id_distribuidor = response[0]
        p_status = response[1]
        pd_status = response[2]

        if p_status != 'A' or pd_status != 'A':
            logger.error(f"Produto {id_produto} com status inválido.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Produto com status inválido."}}, 200

        # Verificando os distribuidores do cliente
        query = """
            SELECT
                TOP 1 1

            FROM
                CLIENTE c

                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                    c.id_cliente = cd.id_cliente

                INNER JOIN DISTRIBUIDOR d ON
                    cd.id_distribuidor = d.id_distribuidor

            WHERE
                d.id_distribuidor = :id_distribuidor
                AND	c.id_cliente = :id_cliente
                AND d.id_distribuidor != 0
                AND c.status = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'
        """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
            "first": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error(f"Cliente não pode ver produtos do distribuidor {id_distribuidor}.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Cliente não pode ver produtos do distribuidor dono do produto."}}, 200

        new_notificacao = {
            "id_usuario": id_usuario,
            "id_produto": id_produto,
            "status": "P",
            "data_criacao": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

        dm.core_sql_insert("NOTIFICACAO_ESTOQUE", new_notificacao)

        logger.info(f"Notificação de estoque para o produto {id_produto} criada.")
        return {"status": 200,
                "resposta": {
                    "tipo": "1", 
                    "descricao": "Notificação criada."}}, 200