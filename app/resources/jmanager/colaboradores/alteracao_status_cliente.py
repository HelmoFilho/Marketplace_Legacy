#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class AlteracaoStatusCliente(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1])
    def post(self) -> dict:
        """
        Método POST do Alteração de Status do Cliente

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_cliente", "status"]

        correct_types = {"id_cliente": int}

        # Checa chaves inválidas e faltantes e valores nulos
        if (validity := dm.check_validity(request_response = response_data, 
                                not_null = produto_columns, 
                                correct_types = correct_types,
                                comparison_columns = produto_columns)):
            return validity

        # Pegando os dados da resposta
        id_cliente = response_data.get("id_cliente")

        status = str(response_data.get("status")).upper()
        status = status if status in ["A", "I"] else None

        # Verificando a existencia do cliente
        cliente_query = f"""
            SELECT
                TOP 1 id_cliente
            
            FROM
                CLIENTE

            WHERE
                id_cliente = :id_cliente
        """

        params = {
            "id_cliente": id_cliente
        }

        cliente_query = dm.raw_sql_return(cliente_query, params = params, raw = True, first = True)

        if not cliente_query:
            logger.error(f"Cliente {id_cliente} nao existe")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Cliente informado não existe."}}, 409

        if status is not None:

            # Atualização do status
            update_query = f"""
                UPDATE
                    CLIENTE
                
                SET
                    status = :status,
                    data_aprovacao = CASE
                                         WHEN :status = 'A'
                                             THEN GETDATE()
                                         ELSE
                                             data_aprovacao
                                     END
                
                WHERE
                    id_cliente = :id_cliente
            """

            params = {
                "id_cliente": id_cliente,
                "status": status
            }

            dm.raw_sql_execute(update_query, params = params)
            
            logger.info("Status do cliente alterado com sucesso.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Operação de alteração de status ocorreu sem problemas."}
                    }, 200 

        logger.info("Status do cliente inalterado")
        return {"status":200,
                "resposta":{
                    "tipo":"16",
                    "descricao":f"Operação de alteração de status não ocorreu."}
                }, 200 