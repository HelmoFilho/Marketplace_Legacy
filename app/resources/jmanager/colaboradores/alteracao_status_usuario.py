#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class AlteracaoStatusUsuario(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1,2,3,4,5,6])
    def post(self) -> dict:
        """
        Método POST do Alteração de Status do Usuario

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_usuario", "status"]

        no_use_columns = ["observacao"]

        correct_types = {"id_usuario": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                not_null = produto_columns,
                                no_use_columns = no_use_columns,
                                comparison_columns = produto_columns, 
                                correct_types = correct_types)):
            return validity

        # Pegando os dados da resposta
        id_usuario = response_data.get("id_usuario")
        
        status = str(response_data.get("status")).upper()
        status = status if status in ["A", "I"] else None

        observacao = str(response_data.get("observacao")).upper() if response_data.get("observacao") else None
        observacao = observacao if status == 'I' else None

        # Verificando a existencia do usuario
        usuario_query = f"""
            SELECT
                TOP 1 id_usuario
            
            FROM
                USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        usuario_query = dm.raw_sql_return(usuario_query, params = params, raw = True, first = True)

        if not usuario_query:
            logger.error(f"Usuario {id_usuario} nao existe")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Usuario informado não existe."}}, 409

        if status is not None:

            # Atualização do status
            update_query = f"""
                UPDATE
                    USUARIO
                
                SET
                    status = :status,
                    data_aceite = CASE
                                      WHEN :status = 'A'
                                          THEN GETDATE()
                                      ELSE
                                          data_aceite
                                  END,
                    observacao = CASE
                                     WHEN :observacao IS NOT NULL AND :status = 'I'
                                         THEN :observacao
                                     ELSE
                                         observacao
                                 END
                
                WHERE
                    id_usuario = :id_usuario
            """

            params = {
                "id_usuario": id_usuario,
                "status": status,
                "observacao": observacao
            }

            dm.raw_sql_execute(update_query, params = params)

            logger.info("Status do usuario alterada com sucesso.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Operação de alteração de status ocorreu sem problemas."}
                    }, 200 

        logger.info("Status do usuario inalterado")
        return {"status":200,
                "resposta":{
                    "tipo":"16",
                    "descricao":f"Operação de alteração de status não ocorreu."}
                }, 200