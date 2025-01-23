#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure
from app.server import logger

class ValidarCuponsMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Validar Cupons do Marketplace

        Returns:
            [dict]: Status da transação
        """

        data_hora = datetime.now()

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_cliente"]
        unnecessary_keys = ["codigo_cupom", "id_cupom"]
        
        correct_types = {
            "id_cliente": int,
            "id_cupom": int,
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = necessary_keys,
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        id_cliente = int(response_data.get("id_cliente"))

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        id_cupom = int(response_data.get("id_cupom")) if response_data.get("id_cupom") else None
        codigo = str(response_data.get("codigo_cupom")) if response_data.get("codigo_cupom") else None

        if not any([codigo, id_cupom]):
            logger.error(f"Usuario nao enviou nada para ser validado.")
            return {"status":403,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Usuario nao enviou nada para ser validado."}}, 403

        if codigo and not id_cupom:

            query = """
                SELECT
                    TOP 1 id_cupom

                FROM
                    CUPOM

                WHERE
                    codigo_cupom COLLATE Latin1_General_CI_AI = :code

                ORDER BY
                    data_cadastro
            """

            params = {
                "code": codigo.upper()
            }

            query = dm.raw_sql_return(query, params = params, raw = True, first = True)

            if not query:
                logger.error(f"Codigo de cupom:{codigo} nao existe")
                return {"status":200,
                        "resposta":{"tipo":"15","descricao":f"Cupom invalido."},
                        "dados": {
                            "id_cupom": None,
                            "valido": False,
                            "motivo": "Codigo de cupom inválido."
                        }}    

            id_cupom = query[0]

        dict_cupom = {
            "id_cliente": id_cliente,
            "id_cupom": id_cupom,
            "data_hora": data_hora,
            "codigo": True
        }

        answer, response = dj.json_cupom_validado(**dict_cupom)

        if answer and codigo:

            query = """
                SELECT
                    top 1 CASE WHEN oculto = 'N' THEN 0 ELSE 1 END as oculto

                FROM
                    CUPOM

                WHERE
                    id_cupom = :id_cupom
            """

            params = {
                "id_cupom": id_cupom,
            }

            query = dm.raw_sql_return(query, params = params, raw = True, first = True)

            if query:

                if query[0]:

                    query = """
                        SELECT
                            id_cupom

                        FROM
                            CUPOM_CLIENTE_OCULTO

                        WHERE
                            id_cliente = :id_cliente
                            AND id_cupom = :id_cupom
                    """

                    params = {
                        "id_cupom": id_cupom,
                        "id_cliente": id_cliente,
                    }

                    query = dm.raw_sql_return(query, params = params, raw = True, first = True)

                    if not query:

                        insert_data = {
                            "id_cliente": id_cliente,
                            "data_habilitado": data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                            "id_cupom": id_cupom,
                            "status": 'A'
                        }

                        dm.raw_sql_insert("CUPOM_CLIENTE_OCULTO", insert_data)

        if not answer:
            msg = response.get("motivo").split("Cupom ")[1]
            logger.info(f"Cupom:{id_cupom} {msg}")
            return {"status":200,
                    "resposta":{"tipo":"15","descricao":f"Cupom invalido."},
                    "dados": response} 

        return {"status":200,
                "resposta":{"tipo":"1","descricao":f"Cupom valido."},
                "dados": response}    