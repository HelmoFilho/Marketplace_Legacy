#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroCargoJManager(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def get(self):
        """
        Método GET do Registro de Cargos Jmanager

        Returns:
            [dict]: Status da transação
        """

        # Cria o JSON
        cargo_query = f"""
            SELECT
                id as id_cargo,
                upper(descricao) as descricao

            FROM
                JMANAGER_CARGO
        """

        data = dm.raw_sql_return(cargo_query)

        if data:
            logger.info("Dados de cargo enviados para o front-end.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."}, "dados": data}, 200
        
        logger.info("Nao existem dados para serem retornados.")
        return {"status":404,"resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200


    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def post(self):
        """
        Método POST do Registro de Cargos Jmanager

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True)

        necessary_keys = ["cargo"]

        # Checa invalidas, faltantes e Valores nulos
        if (validity := dm.check_validity(request_response = response_data,
                                bool_incorrect = False,
                                not_null = necessary_keys,
                                comparison_columns = necessary_keys)):
            return validity

        # Verificando existência do cargo enviado
        holder_cargo = []
        params = {}

        cargo = str(response_data["cargo"]).upper().split()

        string_cargo = ""

        for index, word in enumerate(cargo):

            params.update({
                f"word_{index}": f"%{word}%"
            })

            string_cargo += f"""AND (
                descricao COLLATE Latin1_General_CI_AI LIKE :word_{index}
            )"""

        cargo_query = f"""
            SELECT
                *
            
            FROM
                JMANAGER_CARGO
            
            WHERE
                1 = 1
                {string_cargo}
        """

        data = dm.raw_sql_return(cargo_query, params = params)
            
        if data:
            logger.error(f"Cargo {str(response_data['cargo'])} já cadastrado na base..")
            return {"status":409,
                    "resposta":{"tipo":"5","descricao":f"Registro já existe."}}, 200
            
        # Comita os dados nas tabelas JMANAGER_CARGO
        insert = {
            "id": dm.id_handler_raw("JMANAGER_CARGO", "id"), 
            "descricao": str(response_data.get("cargo")).upper()
        }

        dm.raw_sql_insert("JMANAGER_CARGO", insert)

        logger.info(f"Novo cargo {str(response_data['cargo'])} adicionado a base.")
        return {"status":201,
                "resposta":{"tipo":"1","descricao":f"Novo cargo adicionado."}}, 201


    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def put(self):
        """
        Método PUT do Registro de Cargos Jmanager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get_info_thread().get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True)        

        necessary_keys = ["id_cargo", "cargo"]

        correct_types = {"id_cargo": int}

        # Checa invalidas, faltantes e Valores nulos
        if (validity := dm.check_validity(request_response = response_data,
                                bool_incorrect = False, correct_types = correct_types,
                                not_null = necessary_keys,
                                comparison_columns = necessary_keys)):
            return validity

        # Verifica as chaves enviadas
        id_cargo = response_data.get("id_cargo")
        new_cargo = str(response_data.get("cargo")).upper()

        query = f"""
            SELECT
                TOP 1 id
            
            FROM
                JMANAGER_CARGO

            WHERE
                id = :id_cargo
        """

        params = {
            "id_cargo": id_cargo
        }

        query = dm.raw_sql_return(query, params = params, raw = True, first = True)

        if not query:
            logger.error("Falha pois registro do cargo nao existe.")
            return {"status":409,
                    "resposta":{"tipo":"5","descricao":f"Registro não existe."}}, 200

        # Comita as mudanças realizadas
        update = {
            "id": id_cargo,
            "descricao": new_cargo
        }

        key_columns = ["id"]
        
        dm.raw_sql_update("JMANAGER_CARGO", update, key_columns)

        logger.info("Registro do cargo foi alterado com sucesso.")
        return {"status":200,
                "resposta":{"tipo":"1","descricao":f"Registro foi alterado."}}, 200

    
    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def delete(self):
        """
        Método DELETE do Registro de Cargos Jmanager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get_info_thread().get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, values_upper = True)

        id_cargo = response_data.get("id_cargo")

        necessary_keys = ["id_cargo"]

        correct_types = {"id_cargo": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data,
                                bool_incorrect = False, correct_types = correct_types,
                                not_null = necessary_keys,
                                comparison_columns = necessary_keys)):
            return validity

        query = """
            SELECT
                TOP 1 id
            
            FROM
                JMANAGER_CARGO
            
            WHERE
                id = :id_cargo
        """

        params = {
            "id_cargo": id_cargo
        }

        query = dm.raw_sql_return(query, params = params, raw = True, first = True)

        if not query:
            logger.error("Falha pois registro do cargo nao existe.")
            return {"status":409,
                    "resposta":{"tipo":"5","descricao":f"Registro não existe."}}, 409

        # Comita as mudanças realizadas
        delete = {
            "id": id_cargo
        }

        dm.raw_sql_delete("JMANAGER_CARGO", delete)

        logger.info("Registro foi deletado com sucesso.")
        return {"status":200,
                "resposta":{"tipo":"1","descricao":f"Registro foi deletado."}}, 200