#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarUsuariosClientes(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Listar Usuarios Clientes

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper=True)

        necessary_keys = ["id_usuario", "id_distribuidor", "data_cadastro", "status", "id_cliente", "nome"]

        correct_types = {"id_usuario": int, "id_distribuidor": int, "data_cadastro": list, "id_cliente": int}

        if not response_data.get("id_distribuidor"):
            response_data.pop("id_distribuidor", None)

        if not response_data.get("id_usuario"):
            response_data.pop("id_usuario", None)

        if not response_data.get("id_cliente"):
            response_data.pop("id_cliente", None)

        if not response_data.get("status"):
            response_data.pop("status", None)

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys, 
                                          bool_missing = False,
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

                string_data = "AND u.data_cadastro BETWEEN :date_1 AND :date_2"

                params.update({
                    "date_1": date_1,
                    "date_2": date_2
                })

        # Campo de busca
        busca = response_data.get("nome")

        if busca:

            busca = str(busca)

            params.update({
                f"word": f"%{busca}%"
            })

            string_busca = f"""AND (
                u.nome COLLATE Latin1_General_CI_AI LIKE :word
            )"""

        # status
        status = response_data.get("status")

        if status:
            status = str(status).upper()
            if status != "I":
                status = 'A'

            params["status"] = status
            string_status = "AND u.status = :status"

        # Id_cliente
        id_cliente = response_data.get("id_cliente")

        if id_cliente:
            params["id_cliente"] = int(id_cliente)
            string_id_cliente = "AND c.id_cliente = :id_cliente"

        # Id_usuario
        id_usuario = response_data.get("id_usuario")

        if id_usuario:
            params["id_usuario"] = int(id_usuario)
            string_id_usuario = "AND u.id_usuario = :id_usuario"

        # Salvando informação de todos os distribuidores
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

        # Realizando a query
        query = f"""
            SELECT
                u.id_usuario,
                UPPER(u.nome) as nome,
                u.cpf,
                u.status,
                UPPER(u.email) as email,
                u.telefone,
                u.data_cadastro,
                u.aceite_termos,
                u.data_aceite,
                id_u.count__

            FROM
                (

                    SELECT
                        u.id_usuario,
                        COUNT(*) OVER() as 'count__'

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    u.id_usuario

                            FROM
                                USUARIO u

                                INNER JOIN USUARIO_CLIENTE uc ON
                                    u.id_usuario = uc.id_usuario

                                INNER JOIN CLIENTE c ON
                                    uc.id_cliente = c.id_cliente

                            WHERE
                                (
                                    (
                                        0 IN :distribuidores
                                    )
                                        OR
                                    (
                                        0 NOT IN :distribuidores
                                        AND u.id_usuario IN (
                                                                SELECT
                                                                    u.id_usuario

                                                                FROM
                                                                    USUARIO u

                                                                    INNER JOIN USUARIO_CLIENTE uc ON
                                                                        u.id_usuario = uc.id_usuario

                                                                    INNER JOIN CLIENTE c ON
                                                                        uc.id_cliente = c.id_cliente

                                                                    INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                                                                        c.id_cliente = cd.id_cliente

                                                                    INNER JOIN DISTRIBUIDOR d ON
                                                                        cd.id_distribuidor = d.id_distribuidor

                                                                WHERE
                                                                    d.id_distribuidor IN :distribuidores
                                                                    AND cd.status = 'A'
                                                                    AND cd.d_e_l_e_t_ = 0
                                                            )
                                    )
                                )
                                {string_id_usuario}
                                {string_id_cliente}
                                {string_busca}
                                {string_data}
                                {string_status}
                                AND u.d_e_l_e_t_ = 0
                                AND uc.d_e_l_e_t_ = 0

                        ) u

                    ORDER BY
                        u.id_usuario

                    OFFSET
                        :offset ROWS

                    FETCH
                        NEXT :limite ROWS ONLY

                ) id_u

                INNER JOIN USUARIO u ON
                    id_u.id_usuario = u.id_usuario

            ORDER BY
                u.id_usuario
        """

        dict_query = {
            "query": query,
            "params": params
        }
        
        user_query = dm.raw_sql_return(**dict_query)

        counter = 0

        # Criando o json
        for usuario in user_query:

            counter = usuario.pop("count__", 0)

            # Clientes
            query = """
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
                    UPPER(c.status_receita) as status_receita,
                    id_c.status_usuario_cliente

                FROM
                    (

                        SELECT
                            DISTINCT
                                c.id_cliente,
                                uc.status as status_usuario_cliente,
                                uc.data_aprovacao as data_usuario_cliente

                        FROM
                            USUARIO u

                            INNER JOIN USUARIO_CLIENTE uc ON
                                u.id_usuario = uc.id_usuario

                            INNER JOIN CLIENTE c ON
                                uc.id_cliente = c.id_cliente

                        WHERE
                            uc.id_usuario = :id_usuario
                            AND uc.d_e_l_e_t_ = 0
                            AND u.d_e_l_e_t_ = 0

                    ) id_c

                    INNER JOIN CLIENTE c ON
                        id_c.id_cliente = c.id_cliente

                ORDER BY
                    id_c.data_usuario_cliente,
                    c.id_cliente
            """

            params = {
                "id_usuario": usuario.get("id_usuario")
            }

            dict_query = {
                "query": query,
                "params": params
            }

            response = dm.raw_sql_return(**dict_query)

            if not response: continue
            usuario["clientes"] = response

            for cliente in response:

                query = """
                    SELECT
                        DISTINCT
                            d.id_distribuidor,
                            cd.status,
                            cd.data_aprovacao

                    FROM
                        CLIENTE c

                        INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                            c.id_cliente = cd.id_cliente

                        INNER JOIN DISTRIBUIDOR d ON
                            cd.id_distribuidor = d.id_distribuidor

                    WHERE
                        cd.id_cliente = :id_cliente
                        AND d.id_distribuidor != 0
                        AND cd.status = 'A'
                        AND cd.d_e_l_e_t_ = 0

                    ORDER BY
                        cd.data_aprovacao
                """

                params = {
                    "id_cliente": cliente.get("id_cliente")
                }

                dict_query = {
                    "query": query,
                    "params": params
                }

                response = dm.raw_sql_return(**dict_query)

                distribuidores = []

                for distribuidor in response:
                    
                    id_distribuidor = distribuidor["id_distribuidor"]

                    saved_distribuidor = saved_distribuidores.get(id_distribuidor)
                    if saved_distribuidor:

                        saved_distribuidor["status_cliente_distribuidor"] = distribuidor["status"]
                        distribuidores.append(saved_distribuidor)

                cliente["distribuidores"] = distribuidores                  
        

        if user_query:

            logger.info(f"Dados de usuario foram enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados encontrados."}, 
                    "informacoes": {
                        "itens": counter,
                        "paginas": (counter // limite) + (counter % limite > 0),
                        "pagina_atual": pagina}, 
                    "dados": user_query}, 200

        logger.error(f"Dados de usuario nao foram encontrados.")
        return {"status":400,
                "resposta":{
                    "tipo":"6",
                    "descricao":f"Dados não encontrados para estes filtros."}}, 200