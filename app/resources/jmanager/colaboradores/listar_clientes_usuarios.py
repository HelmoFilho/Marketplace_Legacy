#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarClientesUsuarios(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Listar Clientes Usuarios

        Returns:
            [dict]: Status da transação
        """

        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper=True)

        no_use_columns = ["id_usuario", "id_distribuidor", "data_cadastro", "status", "id_cliente", "busca"]

        correct_types = {"id_usuario": int, "id_distribuidor": int, "data_cadastro": list, "id_cliente": int}

        # Checa as chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                          no_use_columns = no_use_columns, 
                                          correct_types = correct_types)):
            return validity

        # Checando a pagina e o limite atual
        pagina, limite = dm.page_limit_handler(response_data)

        string_data = ""
        string_busca = ""
        string_status = ""
        string_id_usuario = ""
        string_id_cliente = ""

        params = {
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        # Checando o id do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))\
                            if response_data.get("id_distribuidor") else 0

        if id_distribuidor:
            answer, response = dm.distr_usuario_check(id_distribuidor)
            if not answer:
                return response

            distribuidores = [id_distribuidor]

        else:
            global_info.save_info_thread(id_distribuidor = id_distribuidor)
            distribuidores = info.get("id_distribuidor")

        params["distribuidores"] = distribuidores

        # Pegando os dados dos distribuidores
        query = """
            SELECT
                d.id_distribuidor,
                UPPER(d.nome_fantasia) as nome_fantasia,
                UPPER(d.razao_social) as razao_social

            FROM
                DISTRIBUIDOR d
        """

        response = dm.raw_sql_return(query)

        saved_distribuidores = {
            distribuidor["id_distribuidor"]: distribuidor
            for distribuidor in response
        }

        # Checando a data de cadastro
        data_cadastro = response_data.get("data_cadastro")

        if data_cadastro:

            if len(data_cadastro) == 1:
                date_1, date_2 = data_cadastro[0], ""
            
            else:
                date_1, date_2 = data_cadastro[0], data_cadastro[1]

            if date_1 and date_2:
                
                if date_1:
                    try:
                        date_1 = datetime.strptime(date_1, '%Y-%m-%d')
                        date_1 = date_1.strftime('%Y-%m-%d')
                        if date_1 < "1900-01-01":
                            date_1 = "1900-01-01"
                    except:
                        date_1 = "1900-01-01"
                else:
                    date_1 = "1900-01-01"

                if date_2:
                    try:
                        date_2 = datetime.strptime(date_2, '%Y-%m-%d')
                        date_2 = date_2.strftime('%Y-%m-%d')
                        if date_2 > "3000-01-01":
                            date_2 = "3000-01-01"
                    except:
                        date_2 = "3000-01-01"
                else:
                    date_2 = "3000-01-01"

                string_data = "AND c.data_cadastro BETWEEN :date_1 AND :date_2"

                params.update({
                    "date_1": date_1,
                    "date_2": date_2
                })

        # Campo de busca
        busca = response_data.get("busca")

        if busca:

            busca = str(busca)

            params.update({
                f"word": f"%{busca}%"
            })

            string_busca = f"""AND (
                c.razao_social COLLATE Latin1_General_CI_AI LIKE :word
                OR c.nome_fantasia COLLATE Latin1_General_CI_AI LIKE :word
                OR c.cnpj LIKE :word
                OR uc.cpf LIKE :word
                OR uc.email COLLATE Latin1_General_CI_AI LIKE :word
                OR uc.nome COLLATE Latin1_General_CI_AI LIKE :word
            )"""

        # status
        status = response_data.get("status")

        if status:
            status = str(status).upper()
            if status != "I":
                status = 'A'

            params["status"] = status
            string_status = "AND c.status = :status"

        # Id_cliente
        id_cliente = response_data.get("id_cliente")

        if id_cliente:
            params["id_cliente"] = int(id_cliente)
            string_id_cliente = "AND c.id_cliente = :id_cliente"

        # Id_usuario
        id_usuario = response_data.get("id_usuario")

        if id_usuario:
            params["id_usuario"] = int(id_usuario)
            string_id_usuario = "AND uc.id_usuario = :id_usuario"

        # realizando a query
        id_cliente_query = f"""
            SELECT
                c.id_cliente,
                COUNT(*) OVER() as 'count__'
            
            FROM
                (

                    SELECT
                        DISTINCT
                            c.id_cliente

                    FROM
                        CLIENTE c

                        LEFT JOIN
                        (

                            SELECT
                                uc.id_cliente,
                                uc.id_usuario,
                                u.cpf,
                                u.email,
                                u.nome

                            FROM
                                USUARIO u

                                INNER JOIN USUARIO_CLIENTE uc ON
                                    u.id_usuario = uc.id_usuario

                            WHERE
                                u.d_e_l_e_t_ = 0
                                AND uc.d_e_l_e_t_ = 0

                        ) uc ON
                            c.id_cliente = uc.id_cliente

                    WHERE
                        (
                            (
                                0 IN :distribuidores
                            )
                                OR
                            (
                                0 NOT IN :distribuidores
                                AND c.id_cliente IN (
                                                        SELECT
                                                            c.id_cliente

                                                        FROM
                                                            CLIENTE c

                                                            INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                                                                c.id_cliente = cd.id_cliente

                                                            INNER JOIN DISTRIBUIDOR d ON
                                                                cd.id_distribuidor = d.id_distribuidor

                                                        WHERE
                                                            d.id_distribuidor IN :distribuidores
                                                            {string_id_cliente}
                                                            AND cd.status = 'A'
                                                            AND cd.d_e_l_e_t_ = 0
                                                    )
                            )
                        )
                        {string_busca}
                        {string_id_usuario}
                        {string_id_cliente}
                        {string_data}
                        {string_status}

                ) c

            ORDER BY
                c.id_cliente

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY
        """

        dict_query = {
            "query": id_cliente_query,
            "params": params,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:

            logger.info(f"Dados de cliente nao foram encontrados.")
            return {"status":400,
                    "resposta":{
                        "tipo":"6",
                        "descricao":f"Dados não encontrados para estes filtros."}}, 200

        clientes = [cliente[0]  for cliente in response]
        counter = response[0][1]

        client_query = f"""
            SELECT
                c.id_cliente,
                c.status,
                c.cnpj as cnpj,
                UPPER(c.razao_social) as razao_social,
                UPPER(c.nome_fantasia) as nome_fantasia,
                UPPER(c.endereco) as endereco,
                c.endereco_num,
                UPPER(c.endereco_complemento) as endereco_complemento,
                UPPER(c.bairro) as bairro,
                UPPER(c.cep) as cep,
                UPPER(c.cidade) as cidade,
                UPPER(c.estado) as estado,
                UPPER(c.telefone) as telefone,
                c.data_cadastro,
                c.data_aprovacao,
                UPPER(c.status_receita) as status_receita

            FROM
                CLIENTE c
            
            WHERE
                c.id_cliente IN :id_clientes

            ORDER BY
                c.id_cliente
        """

        params = {
            "id_clientes": clientes
        }

        dict_query = {
            "query": client_query,
            "params": params,
        }
        
        client_query = dm.raw_sql_return(**dict_query)

        # Criando o json
        for cliente in client_query:

            # Usuarios
            query = """
                SELECT
                    u.id_usuario,
                    UPPER(u.nome) as nome,
                    u.cpf,
                    u.status,
                    UPPER(u.email) as email,
                    u.telefone,
                    u.data_cadastro,
                    id_u.status_usuario_cliente

                FROM
                    (

                        SELECT
                            DISTINCT
                                u.id_usuario,
                                uc.status as status_usuario_cliente,
                                uc.data_aprovacao as data_usuario_cliente

                        FROM
                            USUARIO u

                            INNER JOIN USUARIO_CLIENTE uc ON
                                u.id_usuario = uc.id_usuario

                            INNER JOIN CLIENTE c ON
                                uc.id_cliente = c.id_cliente

                        WHERE
                            uc.id_cliente = :id_cliente
                            AND uc.d_e_l_e_t_ = 0
                            AND u.d_e_l_e_t_ = 0

                    ) id_u

                    INNER JOIN USUARIO u ON
                        id_u.id_usuario = u.id_usuario

                ORDER BY
                    id_u.data_usuario_cliente,
                    u.id_usuario
            """

            params = {
                "id_cliente": cliente["id_cliente"]
            }

            dict_query = {
                "query": query,
                "params": params
            }

            response = dm.raw_sql_return(**dict_query)

            cliente["usuarios"] = response

            # Distribuidores
            query = """
                SELECT
                    id_distribuidor,
                    status_cliente_distribuidor

                FROM
                    (

                        SELECT
                            DISTINCT
                                d.id_distribuidor,
                                cd.status as status_cliente_distribuidor,
                                cd.data_aprovacao as data_usuario_cliente

                        FROM
                            CLIENTE c

                            INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                                c.id_cliente = cd.id_cliente

                            INNER JOIN DISTRIBUIDOR d ON
                                cd.id_distribuidor = d.id_distribuidor

                        WHERE
                            cd.id_cliente = :id_cliente
                            AND d.id_distribuidor != 0
                            AND cd.d_e_l_e_t_ = 0

                    ) id_d

                ORDER BY
                    data_usuario_cliente,
                    id_distribuidor
            """

            params = {
                "id_cliente": cliente["id_cliente"]
            }

            dict_query = {
                "query": query,
                "params": params
            }

            response = dm.raw_sql_return(**dict_query)

            to_save_distribuidores = []

            for distribuidor in response:
                
                id_distribuidor = distribuidor["id_distribuidor"]

                saved_distribuidor = saved_distribuidores.get(id_distribuidor)
                if saved_distribuidor:

                    saved_distribuidor["status_cliente_distribuidor"] = distribuidor["status_cliente_distribuidor"]
                    to_save_distribuidores.append(saved_distribuidor)

            cliente["distribuidores"] = to_save_distribuidores  
        
        if client_query:

            logger.info(f"Dados de cliente foram enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados encontrados."}, 
                    "informacoes": {
                        "itens": counter,
                        "paginas": (counter // limite) + (counter % limite > 0),
                        "pagina_atual": pagina}, 
                    "dados": client_query}, 200

        logger.info(f"Dados de cliente nao foram encontrados.")
        return {"status":400,
                "resposta":{
                    "tipo":"6",
                    "descricao":f"Dados não encontrados para estes filtros."}}, 200