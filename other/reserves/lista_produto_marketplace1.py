#=== Importações de módulos externos ===#
from flask_restful import Resource
from timeit import default_timer as timer

#=== Importações de módulos internos ===#
import functions.data_management as dm
import other.reserves.default_json as dj
import functions.security as secure
from app.config.sqlalchemy_config import db
from app.server import logger

class Test(Resource):

    def get(self):

        id_cliente_token = None
        id_usuario = None

        info = secure.verify_token("user")
        if type(info) is dict:
            id_usuario = info.get("id_usuario")
            id_cliente_token = info.get("id_cliente")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_cliente", "id_tipo", "id_grupo", "id_subgrupo", "busca", "id_produto"]

        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "id_produto": [list, str]
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = ["id_distribuidor"],
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = int(response_data.get("id_cliente")) if response_data.get("id_cliente") else None
        
        ## Busca por tipo-grupo-subgrupo
        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = int(response_data.get("id_grupo")) if response_data.get("id_grupo") else None
        id_subgrupo = int(response_data.get("id_subgrupo")) if response_data.get("id_subgrupo") else None

        ## Busca por campo de busca
        busca = str(response_data.get("busca")) if response_data.get("busca") else None
        
        ## Busca por id_produto
        id_produto = response_data.get("id_produto")  

        # Realizando a query
        hold_id_produto = []

        with db() as session:

            produto_query = []
            ordering = """
                ORDER BY
                    CASE WHEN ranking > 0 THEN 1 ELSE 0 END DESC,
                    ranking ASC,
                    descricao_completa  

                OFFSET
                    :offset ROWS

                FETCH
                    NEXT :limite ROWS ONLY;
            """

            # Criando a tabela temporaria
            query = """
                SET NOCOUNT ON;
                
                -- Verificando se a tabela tempor�ria existe
                IF OBJECT_ID('tempDB..#HOLD_PRODUTO', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE #HOLD_PRODUTO
                END;

                CREATE TABLE #HOLD_PRODUTO
                (
                    id_produto VARCHAR(100),
                    id_distribuidor INT,
                    ranking INT,
                    descricao_completa VARCHAR(MAX)
                );
            """

            dm.raw_sql_execute(query, commit = False, connector = session)

            # Verificar o cliente
            null_search = False

            if id_cliente_token and id_cliente:
                if id_cliente not in id_cliente_token:
                    null_search = True

            # Pegando os distribuidores validos do cliente
            if not id_usuario:
                query = """
                    SELECT
                        DISTINCT id_distribuidor

                    FROM
                        DISTRIBUIDOR

                    WHERE
                        status = 'A'
                """

                params = {}

            else:
                query = """
                    SELECT
                        DISTINCT d.id_distribuidor

                    FROM
                        CLIENTE c

                        INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                            c.id_cliente = cd.id_cliente

                        INNER JOIN DISTRIBUIDOR d ON
                            cd.id_distribuidor = d.id_distribuidor

                    WHERE
                        c.status = 'A'
                        AND c.status_receita = 'A'
                        AND cd.status = 'A'
                        AND cd.d_e_l_e_t_ = 0
                        AND d.status = 'A'
                        AND	c.id_cliente = :id_cliente
                        AND d.id_distribuidor = CASE
                                                    WHEN :id_distribuidor = 0
                                                        THEN d.id_distribuidor
                                                    ELSE
                                                        :id_distribuidor
                                                END
                """

                params = {
                    "id_cliente": id_cliente,
                    "id_distribuidor": id_distribuidor
                }

            distribuidores = dm.raw_sql_return(query, params = params, raw = True, connector = session)

            if distribuidores:

                distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

                # Filtros
                if not null_search:

                    string_busca = ""
                    string_tgs = ""
                    string_id_produto = ""                    

                    params = {}

                    bindparams = []

                    ## Procura por campo de busca
                    if busca:

                        busca = str(busca).upper().split()

                        params = {}
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
                            )"""

                    ## Procura por id_produto
                    elif id_produto:

                        if type(id_produto) not in [list, tuple, set]:
                            id_produto = [id_produto]

                        else:
                            id_produto = list(id_produto)

                        bindparams.append("id_produto")

                        params.update({
                            "id_produto": id_produto
                        })

                        string_id_produto = "AND p.id_produto IN :id_produto"

                    ## Procura por tipo, grupo e/ou subgrupo
                    elif id_tipo or id_grupo or id_subgrupo:

                        # Pegando os subgrupos
                        string_tgs = f"""
                            AND ps.id_distribuidor = :id_distribuidor
                            AND s.id_subgrupo = CASE
                                                    WHEN :id_subgrupo IS NOT NULL
                                                        THEN :id_subgrupo
                                                    ELSE
                                                        s.id_subgrupo
                                                END
                            AND g.id_grupo = CASE
                                                 WHEN :id_grupo IS NOT NULL
                                                     THEN :id_grupo
                                                 ELSE
                                                     g.id_grupo
                                             END
                            AND t.id_tipo = CASE
                                                WHEN :id_tipo IS NOT NULL
                                                    THEN :id_tipo
                                                ELSE
                                                    t.id_tipo
                                            END
                            AND t.status = 'A'
                            AND g.status = 'A'
                            AND gs.status = 'A'
                            AND s.status = 'A'
                            AND ps.status = 'A'
                        """
                        
                        params.update({
                            "id_tipo": id_tipo,
                            "id_grupo": id_grupo,
                            "id_subgrupo": id_subgrupo,
                            "id_distribuidor": id_distribuidor,
                        })

                    # Ordenação
                    ordering = """
                        ORDER BY
                            ordem DESC,
                            pd.ranking ASC,
                            p.descricao_completa  

                        OFFSET
                            :offset ROWS

                        FETCH
                            NEXT :limite ROWS ONLY
                    """

                    params.update({
                        "offset": (pagina - 1) * limite,
                        "limite": limite
                    })

                    # Realizando a query
                    query = f"""
                        SELECT
                            DISTINCT p.id_produto,
                            pd.id_distribuidor,
                            pd.ranking,
                            p.descricao_completa,
                            CASE WHEN pd.ranking > 0 THEN 1 ELSE 0 END as ordem,
                            COUNT(*) OVER() AS 'count__'

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
                            LEN(p.sku) > 0
                            AND p.status = 'A'
                            AND pd.status = 'A'
                            AND pd.id_distribuidor IN {tuple(distribuidores)}
                            {string_busca}
                            {string_tgs}
                            {string_id_produto}

                        {ordering}
                    """

                    timer1 = timer()
                    produto_query = dm.raw_sql_return(query, params = params, raw = True,
                                                        bindparams = bindparams, connector = session)
                    timer2 = timer()


            if produto_query:

                hold_id_produto = [
                    {
                        "id_produto": produto[0],
                        "id_distribuidor": produto[1]
                    }
                    for produto in produto_query
                    if produto
                ]

        # Criando o JSON
        if hold_id_produto:
            timer3 = timer()
            full_data = dj.json_products(hold_id_produto, id_cliente = id_cliente)
            timer4 = timer()

            print("1 - {:.20f}".format(timer2 - timer1))
            print("2 - {:.20f}".format(timer4 - timer3))

        else:
            full_data = []

        if full_data:

            maximum_all = int(produto_query[0][-1])
            maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

            logger.info(f"Dados enviados para o front-end.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."},
                    "informacoes": {
                        "itens": maximum_all,
                        "paginas": maximum_pages,
                        "pagina_atual": pagina},
                    "dados":full_data}, 200

        logger.info(f"Dados nao encontrados.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200