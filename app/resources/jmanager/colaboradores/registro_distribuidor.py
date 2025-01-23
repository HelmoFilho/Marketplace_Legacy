#=== Importações de módulos externos ===#
from flask_restful import Resource
import os, re

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroDistribuidor(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self) -> dict:
        """
        Método GET do Listar Distribuidor
        
        Returns: [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Criando o JSON
        query = f"""
            SELECT
                id_distribuidor,
                cnpj,
                razao_social,
                nome_fantasia,
                status,
                endereco,
                cep,
                bairro,
                cidade,
                estado,
                numero,
                complemento,
                telefone,
                responsavel,
                telefone_responsavel,
                email,
                inscricao_estadual,
                cor_a,
                cor_b,
                cor_c,
                imagem
            
            FROM
                DISTRIBUIDOR
            
            WHERE
                (
                    (
                        0 NOT IN :distribuidores
                        AND id_distribuidor IN :distribuidores
                    )
                        OR
                    (
                        0 IN :distribuidores
                    )
                )
            
            ORDER BY
                id_distribuidor ASC;
        """

        params = {
            "distribuidores": info.get("id_distribuidor")
        }

        bindparams = ["distribuidores"]

        dict_query = {
            "query": query,
            "params": params,
            "bindparams": bindparams
        }

        data = dm.raw_sql_return(**dict_query)

        image_server = os.environ.get('IMAGE_SERVER_GUARANY_PS')

        for distribuidor in data:

            imagens = distribuidor.get("imagem")

            final_imagens = []
            
            if imagens:

                try:
                    imagens = str(imagens).split(",")

                except: 
                    imagens = [imagens]

                id_distribuidor = distribuidor.get("id_distribuidor")

                final_imagens = [
                    f'{image_server}/imagens/distribuidores/auto/{id_distribuidor}/logos/{imagem}'
                    for imagem in imagens
                ]

            distribuidor["imagem"] = final_imagens

        if data:

            logger.info("Dados enviados para o front-end.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados."}, "dados": data}, 200
        
        logger.info("Nao existem dados para enviar para o front-end.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200


    @logger
    @secure.auth_wrapper(permission_range=[1])
    def post(self):
        """
        Método POST do Registro de Distribuidores

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True)

        necessary_keys = ["cnpj", "razao_social", "nome_fantasia", "endereco", "cep", "bairro",
                            "cidade", "estado", "numero", "telefone", "responsavel",
                            "telefone_responsavel", "email", "inscricao_estadual", "cor_a", "cor_b",
                            "cor_c"]

        no_use_columns = ["complemento", "imagem"]

        correct_types = {
            "numero": int
        }

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                no_use_columns = no_use_columns,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            return validity

        # Check do distribuidor
        email = str(response_data["email"])
        if not re.match(r"^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", email):
            logger.error("Usuario tentou se cadastrar com email invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: email invalido."}}, 200 

        try:
            cnpj = re.sub("[^0-9]", "", response_data["cnpj"])
            if not secure.cnpj_validator(cnpj):
                logger.error("Usuario tentou se cadastrar com cnpj invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cnpj invalido."}}, 200 

        except:
            logger.error("Distribuidor tentou se cadastrar com cnpj invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cnpj invalido."}}, 200 

        distribuidor_query = f"""
            SELECT
                TOP 1 id_distribuidor
            
            FROM
                DISTRIBUIDOR
            
            WHERE
                cnpj = :cnpj
        """

        params = {
            "cnpj": cnpj
        }

        distribuidor_query = dm.raw_sql_return(distribuidor_query, params = params, raw = True, first = True)
        
        if distribuidor_query:
            logger.error("Falha pois distribuidor ja existe.")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Distribuidor já existe."}}, 409

        # Criação dos objetos que serão utilizados para atualizar a tabela
        new_distribuidor = {
            "id_distribuidor": dm.id_handler_raw("DISTRIBUIDOR", "id_distribuidor"),
            "status": "A"
        }

        new_distribuidor.update(response_data)

        # Comita os dados na tabela DISTRIBUIDOR
        dm.raw_sql_insert("DISTRIBUIDOR", new_distribuidor)

        logger.info("Cadastro do distribuidor foi concluido.")
        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Cadastro do distribuidor concluido."},
                "dados": new_distribuidor}, 201


    @logger
    @secure.auth_wrapper(permission_range=[1])
    def put(self) -> dict:
        """
        Método PUT do Registro de Distribuidores

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True, trim_values = True)

        necessary_keys = ["id_distribuidor", "nome_fantasia", "endereco", "cep", "bairro", "cidade", 
                            "estado", "numero", "complemento", "telefone", "responsavel", 
                            "telefone_responsavel", "email", "inscricao_estadual", 
                            "cor_a", "cor_b", "cor_c", "imagem", "status"]

        not_null = ["id_distribuidor"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas, valores incorretos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data, 
                                            bool_missing = False,
                                            comparison_columns = necessary_keys, 
                                            not_null = not_null, 
                                            correct_types = correct_types)):
            return validity

        # Remove o id do distribuidor do commit
        distribuidor = response_data.get("id_distribuidor")
        answer = dm.distribuidor_check(distribuidor)

        try: del response_data["id_distribuidor"]
        except: pass

        if not answer[0]:
            return answer[1]

        # status
        response_data["status"] = str(response_data.get("status")).upper()\
                    if str(response_data.get("status")).upper() in {"I", "A"} else None

        # Comita as mudanças na tabela DISTRIBUIDOR
        update_distribuidor = {
            key: value
            for key, value in response_data.items()
            if value
        }

        if update_distribuidor:
            
            key_columns = ["id_distribuidor"]
            update_distribuidor["id_distribuidor"] = distribuidor

            dm.raw_sql_update("Distribuidor", update_distribuidor, key_columns)

            logger.info("Alteracoes no cadastro do distribuidor foi concluido")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Alterações no cadastro do distribuidor concluidas."}}, 200

        return {"status":400,
                "resposta":{
                    "tipo":"13",
                    "descricao":f"Ação recusada: Sem alterações para realizar."}}, 200