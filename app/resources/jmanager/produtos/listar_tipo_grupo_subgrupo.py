#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ListarTipoGrupoSubgrupo(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Tipo Grupo Subgrupo

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys=True)

        if not response_data.get("id_tipo"):
            response_data.pop("id_tipo", None)

        if not response_data.get("id_grupo"):
            response_data.pop("id_grupo", None)

        if not response_data.get("id_subgrupo"):
            response_data.pop("id_subgrupo", None)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor", "id_tipo", "id_grupo", "id_subgrupo", "status", "produto",
                            "busca"]

        correct_types = {
            "id_distribuidor": int,
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "produto": bool
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = ["id_distribuidor"],
                                            bool_missing = False,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando as variáveis enviadas
        pagina, limite = dm.page_limit_handler(response_data)  
        
        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = response_data.get("id_grupo") if response_data.get("id_grupo") else None
        id_subgrupo = response_data.get("id_subgrupo") if response_data.get("id_subgrupo") else None
        
        produto = True if response_data.get("produto") else False

        status = response_data.get("status")

        if status:
            if str(status).upper() in ["I", 'FALSE']:
                status = 'I'

            else:
                status = 'A'

        else:
            status = None

        busca = response_data.get("busca")

        # Tratamento do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor")) if response_data.get("id_distribuidor") else 0

        if id_distribuidor != 0:
            answer, response = dm.distr_usuario_check(id_distribuidor)
            if not answer:
                return response

            distribuidores = [id_distribuidor]

        else:
            global_info.save_info_thread(id_distribuidor = id_distribuidor)
            distribuidores = info.get('id_distribuidor')
    
        
        # Iniciando os parametros
        query = ""
        params = {}

        # Busca de produtos
        if id_subgrupo:

            string_busca = ""

            params = {
                "id_distribuidor": id_distribuidor,
                "distribuidores": distribuidores,
                "id_subgrupo": id_subgrupo,
                "id_grupo": id_grupo,
                "id_tipo": id_tipo,
                "status": status,
                "offset": (pagina - 1) * limite,
                "limite": limite
            }

            if busca:

                busca = str(busca).upper().split()

                string_busca = ""

                for index, word in enumerate(busca):

                    params.update({
                        f"word_{index}": f"%{word}%"
                    })

                    string_busca += f"""AND (
                        p.descricao COLLATE Latin1_General_CI_AI LIKE :word_{index}
                        OR p.descricao_completa COLLATE Latin1_General_CI_AI LIKE :word_{index}
                        OR p.sku LIKE :word_{index}
                        OR p.dun14 LIKE :word_{index}
                        OR p.id_produto LIKE :word_{index}
                        OR m.desc_marca COLLATE Latin1_General_CI_AI LIKE :word_{index}
                    )"""

            query = f"""
                SELECT
                    id_produto,
                    id_distribuidor,
                    COUNT(*) OVER() as 'count__'

                FROM
                    (

                        SELECT
                            DISTINCT
                                p.id_produto,
                                pd.id_distribuidor,
                                pd.ranking,
                                p.descricao_completa

                        FROM
                            PRODUTO p

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                p.id_produto = pd.id_produto

                            INNER JOIN PRODUTO_SUBGRUPO ps ON
                                pd.id_produto = ps.codigo_produto

                            INNER JOIN SUBGRUPO s ON
                                ps.id_subgrupo = s.id_subgrupo
                                AND ps.id_distribuidor = s.id_distribuidor

                            INNER JOIN GRUPO_SUBGRUPO gs ON
                                s.id_subgrupo = gs.id_subgrupo

                            INNER JOIN GRUPO g ON
                                gs.id_grupo = g.id_grupo

                            INNER JOIN TIPO t ON
                                g.tipo_pai = t.id_tipo

                            INNER JOIN MARCA m ON
                                pd.id_marca = m.id_marca

                        WHERE
                            (
                                (
                                    0 IN :distribuidores
                                )
                                    OR
                                (
                                    0 NOT IN :distribuidores
                                    AND pd.id_distribuidor IN :distribuidores
                                )
                            )
                            AND ps.id_subgrupo = :id_subgrupo
                            AND ps.id_distribuidor = :id_distribuidor
                            AND g.id_grupo = CASE WHEN :id_grupo IS NULL THEN g.id_grupo ELSE :id_grupo END
                            AND t.id_tipo = CASE WHEN :id_tipo IS NULL THEN t.id_tipo ELSE :id_tipo END
                            {string_busca}
                            AND p.status = 'A'
                            AND pd.status = 'A'

                    ) p

                ORDER BY
                    CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                    descricao_completa

                OFFSET
                    :offset ROWS

                FETCH
                    NEXT :limite ROWS ONLY
            """

            dict_query = {
                "query": query,
                "params": params,
                "raw": True
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:
                logger.info(f"Dados nao encontrados.")
                return {"status":400,
                        "resposta":{
                            "tipo":"6",
                            "descricao":f"dados não encontrados para estes filtros."}}, 200

            prod_cod_list = [
                {
                    "id_produto": prod_cod[0],
                    "id_distribuidor": prod_cod[1],
                }
                for prod_cod in response
            ]

            counter = response[0][2]

            produtos = dj.json_products(prod_cod_list, "jmanager")

            if produtos:
                logger.info(f"Dados enviados para o front-end.")
                return {"status":200,
                        "resposta":{
                            "tipo":"1",
                            "descricao":f"Dados enviados."},
                        "informacoes": {
                            "itens": counter,
                            "paginas": counter//limite + (counter % limite > 0),
                            "pagina_atual": pagina},
                        "dados":produtos}, 200

            logger.info(f"Dados nao encontrados.")
            return {"status":400,
                    "resposta":{
                        "tipo":"6","descricao":f"dados não encontrados para estes filtros."}}, 200

        # Busca de subgrupos
        elif id_grupo:

            if produto:
            
                query = """
                    SELECT
                        s.id_subgrupo,
                        s.descricao,
                        s.id_distribuidor,
                        UPPER(d.nome_fantasia) as nome_fantasia,
                        s.status,
                        s.data_cadastro,
                        s.ranking,
                        id_s.status as status_grupo_subgrupo

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    s.id_subgrupo,
                                    gs.status

                            FROM
                                PRODUTO p

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                    pd.id_produto = ps.codigo_produto

                                INNER JOIN SUBGRUPO s ON
                                    ps.id_subgrupo = s.id_subgrupo
                                    AND ps.id_distribuidor = s.id_distribuidor

                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                (
                                    (
                                        0 IN :distribuidores
                                    )
                                        OR
                                    (
                                        0 NOT IN :distribuidores
                                        AND pd.id_distribuidor IN :distribuidores
                                    )
                                )
                                AND g.id_grupo = :id_grupo
                                AND t.id_tipo = CASE WHEN :id_tipo IS NULL THEN t.id_tipo ELSE :id_tipo END
                                AND s.id_distribuidor = :id_distribuidor
                                AND s.status = CASE WHEN :status IS NULL THEN s.status ELSE :status END
                                AND p.status = 'A'
                                AND pd.status = 'A'

                        ) id_s

                        INNER JOIN SUBGRUPO s ON
                            id_s.id_subgrupo = s.id_subgrupo

                        INNER JOIN DISTRIBUIDOR d ON
                            s.id_distribuidor = d.id_distribuidor

                    ORDER BY
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                        descricao
                """

            else:

                query = """
                    SELECT
                        s.id_subgrupo,
                        s.descricao,
                        s.id_distribuidor,
                        UPPER(d.nome_fantasia) as nome_fantasia,
                        s.status,
                        s.data_cadastro,
                        s.ranking,
                        id_s.status as status_grupo_subgrupo

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    s.id_subgrupo,
                                    gs.status

                            FROM
                                SUBGRUPO s

                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                s.id_distribuidor = :id_distribuidor
                                AND g.id_grupo = :id_grupo
                                AND t.id_tipo = CASE WHEN :id_tipo IS NULL THEN t.id_tipo ELSE :id_tipo END
                                AND s.status = CASE WHEN :status IS NULL THEN s.status ELSE :status END

                        ) id_s

                        INNER JOIN SUBGRUPO s ON
                            id_s.id_subgrupo = s.id_subgrupo

                        INNER JOIN DISTRIBUIDOR d ON
                            s.id_distribuidor = d.id_distribuidor

                    ORDER BY
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                        descricao
                """

            params = {
                "id_distribuidor": id_distribuidor,
                "id_grupo": id_grupo,
                "id_tipo": id_tipo,
                "status": status,
            }

        # Busca de grupo
        elif id_tipo:

            if produto:
            
                query = """
                    SELECT
                        g.id_grupo,
                        g.descricao,
                        g.id_distribuidor,
                        UPPER(d.nome_fantasia) as nome_fantasia,
                        g.tipo_pai,
                        g.status,
                        g.data_cadastro,
                        g.ranking

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    g.id_grupo

                            FROM
                                PRODUTO p

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                    pd.id_produto = ps.codigo_produto

                                INNER JOIN SUBGRUPO s ON
                                    ps.id_subgrupo = s.id_subgrupo
                                    AND ps.id_distribuidor = s.id_distribuidor

                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                (
                                    (
                                        0 IN :distribuidores
                                    )
                                        OR
                                    (
                                        0 NOT IN :distribuidores
                                        AND pd.id_distribuidor IN :distribuidores
                                    )
                                )
                                AND s.id_distribuidor = :id_distribuidor
                                AND t.id_tipo = :id_tipo
                                AND g.status = CASE WHEN :status IS NULL THEN g.status ELSE :status END
                                AND p.status = 'A'
                                AND pd.status = 'A'

                        ) id_g

                        INNER JOIN GRUPO g ON
                            id_g.id_grupo = g.id_grupo

                        INNER JOIN DISTRIBUIDOR d ON
                            g.id_distribuidor = d.id_distribuidor

                    ORDER BY
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                        descricao
                """

            else:

                query = """
                    SELECT
                        g.id_grupo,
                        g.descricao,
                        g.id_distribuidor,
                        UPPER(d.nome_fantasia) as nome_fantasia,
                        g.tipo_pai,
                        g.status,
                        g.data_cadastro,
                        g.ranking

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    g.id_grupo

                            FROM
                                GRUPO g

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                g.id_distribuidor = :id_distribuidor
                                AND t.id_tipo = :id_tipo
                                AND g.status = CASE WHEN :status IS NULL THEN g.status ELSE :status END

                        ) id_g

                        INNER JOIN GRUPO g ON
                            id_g.id_grupo = g.id_grupo

                        INNER JOIN DISTRIBUIDOR d ON
                            g.id_distribuidor = d.id_distribuidor

                    ORDER BY
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                        descricao
                """

            params = {
                "id_distribuidor": id_distribuidor,
                "id_tipo": id_tipo,
                "status": status,
            }

        # Busca de tipo
        else:
            
            if produto:
            
                query = """
                    SELECT
                        t.id_tipo,
                        t.descricao,
                        t.id_distribuidor,
                        UPPER(d.nome_fantasia) as nome_fantasia,
                        t.status,
                        t.padrao,
                        t.data_cadastro,
                        t.ranking

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    t.id_tipo

                            FROM
                                PRODUTO p

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                    pd.id_produto = ps.codigo_produto

                                INNER JOIN SUBGRUPO s ON
                                    ps.id_subgrupo = s.id_subgrupo
                                    AND ps.id_distribuidor = s.id_distribuidor

                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                (
                                    (
                                        0 IN :distribuidores
                                    )
                                        OR
                                    (
                                        0 NOT IN :distribuidores
                                        AND pd.id_distribuidor IN :distribuidores
                                    )
                                )
                                AND s.id_distribuidor = :id_distribuidor
                                AND t.status = CASE WHEN :status IS NULL THEN t.status ELSE :status END
                                AND p.status = 'A'
                                AND pd.status = 'A'

                        ) id_t

                        INNER JOIN TIPO t ON
                            id_t.id_tipo = t.id_tipo

                        INNER JOIN DISTRIBUIDOR d ON
                            t.id_distribuidor = d.id_distribuidor

                    ORDER BY
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                        descricao
                """

            else:

                query = """
                    SELECT
                        t.id_tipo,
                        t.descricao,
                        t.id_distribuidor,
                        UPPER(d.nome_fantasia) as nome_fantasia,
                        t.status,
                        t.padrao,
                        t.data_cadastro,
                        t.ranking

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    t.id_tipo

                            FROM
                                TIPO t

                            WHERE
                                t.id_distribuidor = :id_distribuidor
                                AND t.status = CASE WHEN :status IS NULL THEN t.status ELSE :status END

                        ) id_t

                        INNER JOIN TIPO t ON
                            id_t.id_tipo = t.id_tipo

                        INNER JOIN DISTRIBUIDOR d ON
                            t.id_distribuidor = d.id_distribuidor

                    ORDER BY
                        CASE WHEN ranking > 0 THEN ranking ELSE 999999 END ASC,
                        descricao
                """

            params = {
                "id_distribuidor": id_distribuidor,
                "status": status,
            }

        # # Criando o JSON
        if produto:
            params["distribuidores"] = distribuidores

        dict_query = {
            "query": query,
            "params": params
        }

        data = dm.raw_sql_return(**dict_query)

        if data:

            logger.info(f"Dados enviados para o front-end.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."},"dados":data}, 200

        logger.info(f"Dados nao encontrados.")
        return {"status":400,
                "resposta":{
                    "tipo":"6","descricao":f"dados não encontrados para estes filtros."}}, 200