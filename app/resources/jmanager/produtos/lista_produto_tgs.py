#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure 

class ListarProdutoTGS(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Produto por Tipo Grupo Subgrupo do Jmanager

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

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor", "id_tipo", "id_grupo", "id_subgrupo", "status", "busca"]

        not_null = ["id_distribuidor", "id_subgrupo"]

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
                                            not_null = not_null,
                                            bool_missing = False,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando as variáveis enviadas
        pagina, limite = dm.page_limit_handler(response_data)

        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = int(response_data.get("id_grupo")) if response_data.get("id_grupo") else None
        id_subgrupo = int(response_data.get("id_subgrupo"))

        status = "I" if str(response_data.get("status")).upper() in ["I", "FALSE"] \
                    else "A" if str(response_data.get("status")).upper() != "NONE" else None

        busca = response_data.get("busca")

        string_busca = ""
        params = {
            "id_subgrupo": id_subgrupo,
            "id_grupo": id_grupo,
            "id_tipo": id_tipo,
            "status": status,
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

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

        params.update({
            "id_distribuidor": id_distribuidor,
            "distribuidores": distribuidores,
        })

        ## Procura por campo de busca
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
                        AND t.status = CASE WHEN :status IS NULL THEN t.status ELSE :status END
                        AND g.status = CASE WHEN :status IS NULL THEN g.status ELSE :status END
                        AND gs.status = CASE WHEN :status IS NULL THEN gs.status ELSE :status END
                        AND s.status = CASE WHEN :status IS NULL THEN s.status ELSE :status END
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