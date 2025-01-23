#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class AlteracaoStatusRelacaoUsuarioCliente(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1])
    def post(self) -> dict:
        """
        Método POST do Alteração de Status de Relação do Usuario-Cliente

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_usuario", "id_cliente", "status"]

        correct_types = {
            "id_usuario": int,
            "id_cliente": int,
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = necessary_keys, 
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            return validity

        id_usuario = int(response_data["id_usuario"])
        id_cliente = int(response_data["id_cliente"])

        status = str(response_data.get("status")).upper()
        status = status if status in ["A", "I", "P"] else None

        # Verificando o cliente
        cliente_query = f"""
            SELECT
                TOP 1 status
            
            FROM
                CLIENTE

            WHERE
                id_cliente = :id_cliente
        """

        params = {
            "id_cliente": id_cliente
        }

        dict_query = {
            "query": cliente_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Cliente {id_cliente} nao existe")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cliente não existente."}}, 200

        status_cliente = response[0]
        if status_cliente != 'A':
            logger.error(f"Cliente {id_cliente} com status {status_cliente}")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cliente com status invalido para operação."}}, 200

        # Verificando o usuario
        usuario_query = f"""
            SELECT
                TOP 1 status
            
            FROM
                USUARIO

            WHERE
                id_usuario = :id_usuario
                AND d_e_l_e_t_ = 0
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": usuario_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Usuario {id_usuario} nao existe")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: usuario não existente."}}, 200

        status_usuario = response[0]
        if status_usuario != 'A':
            logger.error(f"Usuario {id_usuario} com status {status_usuario}")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: usuario com status invalido para operação."}}, 200

        # Verificando relação usuario-cliente
        query = """
            SELECT
                TOP 1 uc.status

            FROM
                USUARIO u

                INNER JOIN USUARIO_CLIENTE uc ON
                    u.id_usuario = uc.id_usuario

                INNER JOIN CLIENTE c ON
                    uc.id_cliente = c.id_cliente

            WHERE
                u.id_usuario = :id_usuario
                AND c.id_cliente = :id_cliente
                AND u.d_e_l_e_t_ = 0
                AND uc.d_e_l_e_t_ = 0
        """

        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Relação usuario:{id_usuario} e cliente:{id_cliente} inexistente.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: relação usuario-cliente inválida."}
                    }, 200 

        status_usuario_cliente = response[0]

        if status == 'P':
            logger.error(f"Status da relação usuario:{id_usuario} e cliente:{id_cliente} não pode ser alterado para 'P'")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: relação não pode ser atualizada para pendente."}
                    }, 200

        if (not status) or (status == status_usuario_cliente):
            logger.info(f"Status da relação usuario:{id_usuario} e cliente:{id_cliente} inalterado")
            return {"status":200,
                    "resposta":{
                        "tipo":"16",
                        "descricao":f"Operação de alteração de status não ocorreu."}
                    }, 200

        query = """
            UPDATE
                USUARIO_CLIENTE

            SET
                status = :status,
                data_aprovacao = CASE
                                     WHEN :status = 'A'
                                         THEN GETDATE()
                                     ELSE
                                         NULL
                                 END

            WHERE
                id_usuario = :id_usuario
                AND id_cliente = :id_cliente
        """

        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "status": status,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        logger.info(f"Status da relação usuario:{id_usuario} e cliente:{id_cliente} alterada para {status}")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Status alterado com sucesso."}
                }, 200