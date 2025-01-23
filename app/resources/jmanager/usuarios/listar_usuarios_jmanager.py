#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarUsuariosJmanager(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def post(self):
        """
        Método POST do Listar Usuarios Jmanager

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request()
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_distribuidor"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                correct_types = correct_types,
                                not_null = produto_columns,
                                comparison_columns = produto_columns)):
            return validity

        pagina, limite = dm.page_limit_handler(response_data)

        id_perfil = int(info.get('id_perfil'))
        id_distribuidor = int(response_data["id_distribuidor"])

        if id_distribuidor:
            answer, response = dm.distr_usuario_check(id_distribuidor)
            if not answer:
                return response

            id_distribuidor = [id_distribuidor]

        else:
            global_info.save_info_thread(id_distribuidor = info.get("id_distribuidor"))
            id_distribuidor = info.get("id_distribuidor")

        # Pegando os ids
        query = """
            SELECT
                DISTINCT ju.id_usuario,
                COUNT(*) OVER() as count__

            FROM
                JMANAGER_USUARIO ju

                INNER JOIN JMANAGER_USUARIO_DISTRIBUIDOR jud ON
                    ju.id_usuario = jud.id_usuario

                INNER JOIN DISTRIBUIDOR d ON
                    jud.id_distribuidor = d.id_distribuidor

            WHERE
                ju.id_perfil >= :id_perfil
                AND (
                        (
                            0 IN :id_distribuidor
                        )
                            OR
                        (
                            0 NOT IN :id_distribuidor
                            AND d.id_distribuidor in :id_distribuidor
                        )
                    )

            ORDER BY
                ju.id_usuario

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY
        """

        params = {
            "id_perfil": id_perfil,
            "id_distribuidor": id_distribuidor,
            "offset": (pagina - 1) * limite,
            "limite": limite,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:

            logger.info(f"Sem dados de usuario jmanager para devolver.")
            return {"status":404,
                    "resposta":{
                        "tipo":"7",
                        "descricao":f"dados não encontrados para estes filtros."}}, 200

        id_usuarios = [usuario[0]   for usuario in response]
        counter = response[0][-1]

        # Realizando o filtro
        query = """
            SELECT
                ju.id_usuario,
                ju.id_perfil,
                UPPER(jf.descricao) as perfil,
                UPPER(ju.email) as email,
                UPPER(ju.nome_completo) as nome_completo,
                ju.cpf,
                ju.id_cargo,
                UPPER(jc.descricao) as cargo,
                ju.telefone,
                ju.cep,
                UPPER(ju.endereco) as endereco,
                ju.endereco_num,
                UPPER(ju.complemento) as complemento,
                UPPER(ju.bairro) as bairro,
                UPPER(ju.cidade) as cidade,
                UPPER(ju.estado) as estado,
                ju.data_registro,
                UPPER(ju.status) as status

            FROM
                JMANAGER_USUARIO ju

                INNER JOIN JMANAGER_CARGO jc ON
                    ju.id_cargo = jc.id

                INNER JOIN JMANAGER_PERFIL jf ON
                    ju.id_perfil = jf.id

            WHERE
                ju.id_usuario IN :id_usuarios

            ORDER BY
                ju.id_usuario
        """

        params = {
            "id_usuarios": id_usuarios,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        usuario_query = dm.raw_sql_return(**dict_query)

        data = []

        if usuario_query:

            query = """
                SELECT
                    d.id_distribuidor,
                    UPPER(d.nome_fantasia) as nome_fantasia

                FROM
                    DISTRIBUIDOR d
            """

            response = dm.raw_sql_return(query)

            saved_distribuidores = {
                distribuidor["id_distribuidor"]: distribuidor
                for distribuidor in response
            }

            for usuario in usuario_query:

                distribuidor_query = """
                    SELECT
                        DISTINCT
                            d.id_distribuidor,
                            jud.status as status_usuario_distribuidor

                    FROM
                        DISTRIBUIDOR d

                        INNER JOIN JMANAGER_USUARIO_DISTRIBUIDOR jud ON
                            d.id_distribuidor = jud.id_distribuidor

                        INNER JOIN JMANAGER_USUARIO ju ON
                            jud.id_usuario = ju.id_usuario

                    WHERE
                        ju.id_usuario = :id_usuario
                        AND jud.d_e_l_e_t_ = 0
                        AND d.status = 'A'
                        AND jud.status = 'A'
                """

                params = {
                    "id_usuario": usuario.get('id_usuario')
                }

                dict_query = {
                    "query": distribuidor_query,
                    "params": params,
                    "raw": True,
                }

                response = dm.raw_sql_return(**dict_query)

                to_save_distribuidores = []

                for distribuidor in response:
                    
                    id_distribuidor = distribuidor[0]

                    saved_distribuidor = saved_distribuidores.get(id_distribuidor)
                    if saved_distribuidor:

                        saved_distribuidor["status_usuario_distribuidor"] = distribuidor[1]
                        to_save_distribuidores.append(saved_distribuidor)

                if to_save_distribuidores:

                    usuario["distribuidores"] = to_save_distribuidores
                    data.append(usuario)

            # Retorna resposta pro front-end
            if data:

                maximum_all = int(counter)
                maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

                logger.info(f"Dados dos usuarios jmanager foram enviados.")
                return {"status":200,
                        "resposta":{
                            "tipo":"1",
                            "descricao":f"Dados encontrados."}, 
                        "informacoes": {
                            "itens": maximum_all,
                            "paginas": maximum_pages,
                            "pagina_atual": pagina}, 
                        "dados": data}, 200

        logger.info(f"Sem dados de usuario jmanager para devolver.")
        return {"status":404,
                "resposta":{
                    "tipo":"7",
                    "descricao":f"dados não encontrados para estes filtros."}}, 200