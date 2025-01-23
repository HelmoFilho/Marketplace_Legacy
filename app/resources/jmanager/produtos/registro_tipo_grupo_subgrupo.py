#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroTipoGrupoSubgrupoProduto(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def post(self) -> dict:
        """
        Método POST do Registro do Tipo Grupo Subgrupo Produto

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request()
        response_copy = response_data.copy()

        for key, value in response_copy.items():
            if not value and key != 'id_distribuidor':
                response_data.pop(key, None)
                
        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["tipo", "tipo_pai", "grupo", "grupo_pai", 
                                "subgrupo", "subgrupo_pai", "id_produto"]

        correct_types = {
            "id_distribuidor": int,
            "tipo_pai": int,
            "grupo_pai": int,
            "subgrupo_pai": int,
            "id_produto": [list, str]
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types,
                                            not_null = necessary_keys)):
            
            return validity
        
        # Verificação do id distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        # Pegando as informações recebidas
        tipo = response_data.get("tipo")
        
        tipo_pai = int(response_data.get("tipo_pai")) \
                        if response_data.get("tipo_pai") else None
        grupo = response_data.get("grupo")
        
        grupo_pai = int(response_data.get("grupo_pai"))  \
                        if response_data.get("grupo_pai") else None
        subgrupo = response_data.get("subgrupo")

        subgrupo_pai = int(response_data.get("subgrupo_pai"))  \
                        if response_data.get("subgrupo_pai") else None
        id_produto = response_data.get("id_produto")

        if id_produto:
            if isinstance(id_produto, str):
                id_produto = [id_produto]

            id_produto = list(set(id_produto))

        # Holds usuados para commit

        ## Tipo
        tipo_dict = {}

        ## Grupo
        grupo_dict = {}

        ## Subgrupo
        subgrupo_dict = {}
        grupo_subgrupo_dict = {}

        ## Produto
        produto_hold = []
        
        # Hold dos erros
        falhas_hold = {}
        falhas = 0

        # Checa se o id_distribuidor e pelo menos mais algo foi entregue
        if tipo or grupo or subgrupo or id_produto:

            # Realiza o registro de um novo tipo
            if tipo:

                # Pegando o ranking
                query = """
                    SELECT
                        ISNULL(MAX(ranking), 0) + 1

                    FROM
                        TIPO
                """

                dict_query = {
                    "query": query,
                    "first": True,
                    "raw": True,
                }

                response = dm.raw_sql_return(**dict_query)

                ranking = response[0]

                tipo_dict = {
                    "id_distribuidor": id_distribuidor,
                    "descricao": str(tipo).upper(),
                    "padrao": "N",
                    "status": "A",
                }
                
            # Realiza o registro de um novo grupo
            if grupo:

                # Verificando se o tipo pai foi enviado
                if not tipo_pai:

                    logger.error(f"Tipo pai nao foi fornecido")
                    return {"status": 400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Tipo pai não foi fornecido para o grupo."}
                            }, 200

                # Verificando a existência do tipo_pai
                tipo_pai_query = """
                    SELECT
                        TOP 1 id_tipo

                    FROM
                        TIPO

                    WHERE
                        id_distribuidor = :id_distribuidor
                        AND id_tipo = :tipo_pai
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "tipo_pai": tipo_pai
                }

                tipo_pai_query = dm.raw_sql_return(tipo_pai_query, params = params, 
                                                        raw = True, first = True)

                if not tipo_pai_query:

                    logger.error(f"Tipo pai nao existe.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Tipo pai fornecido não existe."}
                            }, 200

                # Verificando o grupo
                grupo_dict = {
                    "id_distribuidor": id_distribuidor,
                    "descricao": str(grupo).upper(),
                    "tipo_pai": tipo_pai,
                    "status": "A",
                }

            # Realiza o registro de um novo subgrupo
            if subgrupo:

                # Verificando se o grupo pai foi enviado
                if not grupo_pai:

                    logger.error(f"Grupo pai nao foi fornecido.")
                    return {"status": 400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Grupo pai não foi fornecido para o subgrupo."}
                            }, 400

                # Verificando a existência do grupo_pai
                grupo_pai_query = """
                    SELECT
                        TOP 1 id_grupo

                    FROM
                        GRUPO

                    WHERE
                        id_distribuidor = :id_distribuidor
                        AND id_grupo = :grupo_pai
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "grupo_pai": grupo_pai
                }

                grupo_pai_query = dm.raw_sql_return(grupo_pai_query, params = params, 
                                                        raw = True, first = True)

                if not grupo_pai_query:
                    logger.error(f"Grupo pai nao existe.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Grupo pai fornecido não existe."}
                            }, 200
                
                grupo_subgrupo_dict = {
                    "id_distribuidor": id_distribuidor,
                    "descricao": str(subgrupo).upper(),
                    "status": "A",
                    "id_grupo": grupo_pai,
                }

            # Realiza o registro de um produto a um subgrupo
            if id_produto:

                if tipo or grupo or subgrupo:
                    logger.error(f"Cadastro invalido de produto a subgrupo.")
                    return {"status": 400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Junção subgrupo-produto deve ser realizada sozinha."}
                            }, 200
                
                # Verificando se o subgrupo pai foi enviado
                if not subgrupo_pai:

                    logger.error(f"Subgrupo pai nao foi fornecido.")
                    return {"status": 400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Subgrupo pai não foi fornecido para os produtos."}
                            }, 200

                # Verificando a existência do grupo_pai
                subgrupo_pai_query = """
                    SELECT
                        TOP 1 id_subgrupo

                    FROM
                        SUBGRUPO

                    WHERE
                        id_distribuidor = :id_distribuidor
                        AND id_subgrupo = :subgrupo_pai
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "subgrupo_pai": subgrupo_pai
                }

                dict_query = {
                    "query": subgrupo_pai_query,
                    "params": params,
                    "raw": True,
                    "first": True,
                }

                subgrupo_pai_query = dm.raw_sql_return(**dict_query)

                if not subgrupo_pai_query:
                    logger.error(f"Subgrupo pai nao existe.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Subgrupo pai fornecido não existe."}
                            }, 200

                # Vefificando sku por sku
                for produto in id_produto:

                    # Verificar se o sku já esta associado ao subgrupo pai
                    produto_subgrupo_query = """
                        SELECT
                            TOP 1 codigo_produto
                        
                        FROM
                            PRODUTO_SUBGRUPO
                        
                        WHERE
                            id_subgrupo = :subgrupo_pai
                            AND codigo_produto = :id_produto
                            AND id_distribuidor = CASE
                                                      WHEN :id_distribuidor = 0
                                                          THEN id_distribuidor
                                                      ELSE
                                                          :id_distribuidor
                                                  END
                    """

                    params = {
                        "subgrupo_pai": subgrupo_pai,
                        "id_produto": produto,
                        "id_distribuidor": id_distribuidor
                    }

                    dict_query = {
                        "query": produto_subgrupo_query,
                        "params": params,
                        "raw": True,
                        "first": True,
                    }

                    produto_subgrupo_query = dm.raw_sql_return(**dict_query)

                    if produto_subgrupo_query:

                        falhas += 1

                        if falhas_hold.get("produto_subgrupo"):
                            falhas_hold["produto_subgrupo"]['combinacao'].append({
                                "id_subgrupo": subgrupo_pai,
                                "id_distribuidor": id_distribuidor,
                                "id_produto": produto,
                            })
                        
                        else:
                            falhas_hold[f"produto_subgrupo"] = {
                                "descricao": f"Produto já associado a subgrupo informado",
                                "combinacao": [
                                    {
                                        "id_subgrupo": subgrupo_pai,
                                        "id_distribuidor": id_distribuidor,
                                        "id_produto": produto,
                                    }
                                ]
                            }
                        continue

                    # Verificar se produto está registrado para distribuidor informado.
                    produto_distribuidor_query = """
                        -- Trazendo os objetos
                        SELECT
                            p.id_produto as codigo_produto,
                            p.sku,
                            :id_subgrupo as id_subgrupo,
                            :id_distribuidor as id_distribuidor,
                            'A' as status
                        
                        FROM
                            PRODUTO p

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                p.id_produto = pd.id_produto
                        
                        WHERE
                            p.id_produto = :id_produto
                            AND pd.id_distribuidor = CASE
                                                         WHEN :id_distribuidor = 0
                                                             THEN pd.id_distribuidor
                                                         ELSE
                                                             :id_distribuidor
                                                     END                   
                    """

                    params = {
                        "id_produto": produto,
                        "id_distribuidor": id_distribuidor,
                        "id_subgrupo": subgrupo_pai
                    }

                    produto_distribuidor_query = dm.raw_sql_return(produto_distribuidor_query, 
                                                    params = params)

                    if not produto_distribuidor_query:

                        falhas += 1

                        if falhas_hold.get("produto_distribuidor"):
                            falhas_hold["produto_distribuidor"]['combinacao'].append({
                                "id_produto": produto,
                                "id_distribuidor": id_distribuidor
                            })
                        
                        else:
                            falhas_hold[f"produto_distribuidor"] = {
                                "descricao": f"Produto não associado a distribuidor informado",
                                "combinacao": [
                                    {
                                        "id_produto": produto,
                                        "id_distribuidor": id_distribuidor
                                    }
                                ]
                            }
                        continue

                    # Salvando resultado para o insert
                    produto_hold.extend(produto_distribuidor_query)

        else:
            logger.error(f"Dados especificos nao foram fornecidos.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Dados especificos não foram fornecidos"}}, 200

        # Salva os registros na tabela
        alteracoes = []

        if tipo_dict:
            alteracoes.append("Tipo")
            dm.raw_sql_insert("Tipo", tipo_dict)

        if grupo_dict:
            alteracoes.append("Grupo")
            dm.raw_sql_insert("Grupo", grupo_dict)

        if grupo_subgrupo_dict:
            alteracoes.append("Subgrupo")

            query = """
                SET NOCOUNT ON;

                DECLARE @id_subgrupo INT = NULL;

                INSERT INTO
                    SUBGRUPO
                        (
                            id_distribuidor,
                            descricao,
                            status
                        )

                    VALUES
                        (
                            :id_distribuidor,
                            :descricao,
                            :status
                        );

                SET @id_subgrupo = (SELECT SCOPE_IDENTITY())

                INSERT INTO
                    GRUPO_SUBGRUPO
                        (
                            id_distribuidor,
                            id_grupo,
                            id_subgrupo,
                            status
                        )

                    VALUES
                        (
                            :id_distribuidor,
                            :id_grupo,
                            @id_subgrupo,
                            :status
                        )
            """

            dm.raw_sql_execute(query, params = grupo_subgrupo_dict)

        if produto_hold:
            alteracoes.append("Produto")
            dm.raw_sql_insert("Produto_Subgrupo", produto_hold)

            # Altera rankings
            query = """
                SET NOCOUNT ON;

                IF Object_ID('tempDB..#API_RANKING_SUBGRUPO','U') IS NOT NULL
                BEGIN
                    DROP TABLE #API_RANKING_SUBGRUPO;
                END

                IF Object_ID('tempDB..#API_RANKING_GRUPO','U') IS NOT NULL
                BEGIN
                    DROP TABLE #API_RANKING_GRUPO;
                END

                IF Object_ID('tempDB..#API_RANKING_TIPO','U') IS NOT NULL
                BEGIN
                    DROP TABLE #API_RANKING_TIPO;
                END

                -- Subgrupo
                SELECT
                    DISTINCT 
                        RANK() OVER(PARTITION BY ps.id_distribuidor ORDER BY AVG(CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END)) as subgrupo_ranking,
                        ps.id_subgrupo,
                        ps.id_distribuidor

                INTO
                    #API_RANKING_SUBGRUPO

                FROM
                    (

                        SELECT
                            id_produto,
                            id_distribuidor,
                            ranking

                        FROM
                            PRODUTO_DISTRIBUIDOR

                        WHERE
                            status = 'A'

                        UNION

                        SELECT
                            id_produto,
                            0 as id_distribuidor,
                            ranking

                        FROM
                            PRODUTO_DISTRIBUIDOR

                        WHERE
                            status = 'A'

                    ) pd

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        pd.id_produto = ps.codigo_produto
                        AND pd.id_distribuidor = ps.id_distribuidor

                GROUP BY
                    ps.id_distribuidor,
                    ps.id_subgrupo

                ORDER BY
                    ps.id_distribuidor,
                    subgrupo_ranking;


                UPDATE
                    s

                SET
                    s.ranking = tars.subgrupo_ranking

                FROM
                    SUBGRUPO s

                    INNER JOIN #API_RANKING_SUBGRUPO tars ON
                        s.id_subgrupo = tars.id_subgrupo
                        AND s.id_distribuidor = tars.id_distribuidor;

                -- Grupo
                SELECT
                    DISTINCT 
                        RANK() OVER(PARTITION BY g.id_distribuidor ORDER BY AVG(subgrupo_ranking)) as grupo_ranking,
                        g.id_grupo,
                        g.id_distribuidor

                INTO
                    #API_RANKING_GRUPO

                FROM
                    #API_RANKING_SUBGRUPO tars

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        gs.id_subgrupo = tars.id_subgrupo
                        AND gs.id_distribuidor = tars.id_distribuidor

                    INNER JOIN GRUPO g ON
                        gs.id_grupo = g.id_grupo
                        AND gs.id_distribuidor = g.id_distribuidor
                    
                GROUP BY
                    g.id_distribuidor,
                    g.id_grupo

                ORDER BY
                    g.id_distribuidor,
                    grupo_ranking;


                UPDATE
                    g

                SET
                    g.ranking = targ.grupo_ranking

                FROM
                    GRUPO g

                    INNER JOIN #API_RANKING_GRUPO targ ON
                        g.id_grupo = targ.id_grupo
                        AND g.id_distribuidor = targ.id_distribuidor;

                -- Tipo
                SELECT
                    DISTINCT 
                        RANK() OVER(PARTITION BY t.id_distribuidor ORDER BY AVG(grupo_ranking)) as tipo_ranking,
                        t.id_tipo,
                        t.id_distribuidor

                INTO
                    #API_RANKING_TIPO

                FROM
                    #API_RANKING_GRUPO targ

                    INNER JOIN GRUPO g ON
                        targ.id_grupo = g.id_grupo
                        AND targ.id_distribuidor = g.id_distribuidor

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo
                        AND g.id_distribuidor = t.id_distribuidor
                    
                GROUP BY
                    t.id_distribuidor,
                    t.id_tipo

                ORDER BY
                    t.id_distribuidor,
                    tipo_ranking;


                UPDATE
                    t

                SET
                    t.ranking = tart.tipo_ranking

                FROM
                    Tipo t

                    INNER JOIN #API_RANKING_TIPO tart ON
                        t.id_tipo = tart.id_tipo
                        AND t.id_distribuidor = tart.id_distribuidor;


                DROP TABLE #API_RANKING_SUBGRUPO;
                DROP TABLE #API_RANKING_GRUPO;
                DROP TABLE #API_RANKING_TIPO;
            """
            
            dm.raw_sql_execute(query)

        if alteracoes:
            
            # Altera o tipo-grupo-subgrupo do 0
            if id_distribuidor != 0:
                
                query = """
                    SET NOCOUNT ON;

                    DECLARE @id_tipo INT = NULL;

                    -- Tipo
                    SET @id_tipo = (SELECT TOP 1 id_tipo FROM TIPO WHERE padrao = 'S' and id_distribuidor = 0)
                    IF @id_tipo IS NULL
                    BEGIN

                        INSERT INTO 
                            TIPO
                                (
                                    id_distribuidor,
                                    descricao,
                                    status,
                                    padrao
                                )

                            VALUES
                                (
                                    0,
                                    'CATEGORIAS',
                                    'A',
                                    'S'
                                );

                        SET @id_tipo = (SELECT SCOPE_IDENTITY());

                    END


                    -- Grupo
                    INSERT INTO
                        GRUPO
                            (
                                id_distribuidor,
                                tipo_pai,
                                cod_grupo_distribuidor,
                                descricao,
                                status
                            )

                        SELECT
                            DISTINCT
                                0 as id_distribuidor,
                                @id_tipo as tipo_pai,
                                g.cod_grupo_distribuidor,
                                (SELECT TOP 1 descricao FROM GRUPO WHERE cod_grupo_distribuidor = g.cod_grupo_distribuidor),
                                'A' as status

                        FROM
                            GRUPO g

                        WHERE
                            NOT EXISTS (
                                            SELECT
                                                1

                                            FROM
                                                GRUPO

                                            WHERE
                                                id_distribuidor = 0
                                                AND cod_grupo_distribuidor = g.cod_grupo_distribuidor
                                    );

                    -- Subgrupo
                    INSERT INTO
                        SUBGRUPO
                            (
                                id_distribuidor,
                                cod_subgrupo_distribuidor,
                                descricao,
                                status
                            )

                        SELECT
                            DISTINCT
                                0 as id_distribuidor,
                                s.cod_subgrupo_distribuidor,
                                (SELECT TOP 1 descricao FROM SUBGRUPO WHERE cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor),
                                'A' as status

                        FROM
                            SUBGRUPO s

                        WHERE
                            NOT EXISTS (
                                            SELECT
                                                1

                                            FROM
                                                SUBGRUPO

                                            WHERE
                                                id_distribuidor = 0
                                                AND cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor
                                    );


                    -- Grupo Subgrupo
                    INSERT INTO
                        GRUPO_SUBGRUPO
                            (
                                id_distribuidor,
                                id_grupo,
                                id_subgrupo,
                                status
                            )

                        SELECT
                            0 as id_distribuidor,
                            g.id_grupo,
                            s.id_subgrupo,
                            gs.status

                        FROM
                            (

                                SELECT
                                    DISTINCT
                                        g.cod_grupo_distribuidor,
                                        'A' as status,
                                        s.cod_subgrupo_distribuidor

                                FROM
                                    SUBGRUPO s

                                    INNER JOIN GRUPO_SUBGRUPO gs ON
                                        s.id_subgrupo = gs.id_subgrupo
                                        AND s.id_distribuidor = gs.id_distribuidor

                                    INNER JOIN GRUPO g ON
                                        gs.id_grupo = g.id_grupo
                                        AND gs.id_distribuidor = g.id_distribuidor

                                WHERE
                                    s.id_distribuidor != 0	

                            ) gs                           

                            INNER JOIN SUBGRUPO s ON
                                gs.cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor

                            INNER JOIN GRUPO g ON
                                gs.cod_grupo_distribuidor = g.cod_grupo_distribuidor

                        WHERE
                            s.id_distribuidor = 0;


                    -- Produto Subgrupo
                    INSERT INTO
                        PRODUTO_SUBGRUPO
                            (
                                codigo_produto,
                                sku,
                                id_distribuidor,
                                id_subgrupo,
                                status
                            )

                        SELECT
                            ps.codigo_produto,
                            ps.sku,
                            s.id_distribuidor,
                            s.id_subgrupo,
                            ps.status_produto_subgrupo

                        FROM
                            (

                                SELECT
                                    DISTINCT
                                        s.cod_subgrupo_distribuidor,
                                        ps.status as status_produto_subgrupo,
                                        ps.codigo_produto,
                                        ps.sku

                                FROM
                                    SUBGRUPO s

                                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                                        s.id_subgrupo = ps.id_subgrupo
                                        AND s.id_distribuidor = ps.id_distribuidor

                                WHERE
                                    s.id_distribuidor != 0	

                            ) ps

                            INNER JOIN SUBGRUPO s ON
                                ps.cod_subgrupo_distribuidor = s.cod_subgrupo_distribuidor

                        WHERE
                            s.id_distribuidor = 0
                """

                dm.raw_sql_execute(query)

            logger.info(f"{' '.join(alteracoes)} foram registrados com {falhas} falhas.")

        else:
            logger.info("Nenhuma alteração foi realizada")
        
        if falhas > 0:
            return {"status": 200,
                    "resposta": {
                        "tipo": "15",
                        "descricao": f"Houve erros com a transação"},
                    "situacao": {
                        "sucessos": len(id_produto) - falhas,
                        "falhas": falhas,
                        "descricao": falhas_hold
                    }},200

        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Registro realizado com sucesso."}}, 201


    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def put(self) -> dict:
        """
        Método PUT do Registro do Tipo Grupo Subgrupo

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_tipo", "tipo", "id_grupo", "grupo", "id_subgrupo", "subgrupo",
                            "status_tipo", "status_grupo", "status_subgrupo", "status_grupo_subgrupo"]

        correct_types = {
            "id_distribuidor": int,
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys,
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types,
                                            not_null = necessary_keys)):
            
            return validity

        # Verificação do id distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        # Pegando as informações recebidas

        ## Variáveis de tipo
        id_tipo = int(response_data.get("id_tipo")) \
                    if response_data.get("id_tipo") else None
        
        tipo_desc = response_data.get("tipo")
        status_tipo = response_data.get("status_tipo")
        
        ## Variáveis de grupo
        id_grupo = int(response_data.get("id_grupo")) \
                    if response_data.get("id_grupo") else None
        
        grupo_desc = response_data.get("grupo")
        status_grupo = response_data.get("status_grupo")
        
        ## Variáveis de subgrupo
        id_subgrupo = int(response_data.get("id_subgrupo")) \
                        if response_data.get("id_subgrupo") else None
        
        subgrupo_desc = response_data.get("subgrupo")
        status_subgrupo = response_data.get("status_subgrupo")

        status_grupo_subgrupo = response_data.get("status_grupo_subgrupo")

        # Alteração de descrição
        if id_tipo or id_grupo or id_subgrupo:
            
            # Realiza a alteração de um tipo
            if id_tipo:

                # Verifica se nova descricao ou status do tipo foi enviado
                if not tipo_desc and not status_tipo:
                    logger.error(f"Nova descricao ou status do tipo vazio.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Alterações no tipo não foram fornecidas."}
                            }, 400

                # Verifica existência do tipo informado
                tipo_query = f"""
                    SELECT
                        TOP 1 id_tipo,
                        status
                    
                    FROM
                        TIPO

                    WHERE
                        id_tipo = :id_tipo
                        AND id_distribuidor = :id_distribuidor
                """

                params = {
                    'id_tipo': id_tipo,
                    "id_distribuidor": id_distribuidor
                }

                tipo_query = dm.raw_sql_return(tipo_query, params = params, raw = True, first = True)
                    
                if not tipo_query:

                    logger.error(f"Tipo informado nao existe.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Tipo informado não existe"}
                            }, 400

                # Realiza alterações
                status_query = tipo_query[1]

                tipo_update = {
                    "id_tipo": id_tipo,
                    "id_distribuidor": id_distribuidor,
                }

                # Verificação da descrição
                if tipo_desc:
                    tipo_update["descricao"] = str(tipo_desc).upper()

                # Verificação do status
                if status_tipo:

                    status_tipo = "I"   if str(status_tipo).upper() in ["NONE", "I", "N"]   else "A"

                    # Mudança do status das tabelas grupo e grupo_subgrupo
                    if status_query != status_tipo:

                        tipo_update["status"] = status_tipo

                # Realizando mudança no tipo
                dm.raw_sql_update("Tipo", tipo_update, ["id_tipo", "id_distribuidor"])

            # Realiza a alteração de status de um grupo_subgrupo
            if id_grupo and id_subgrupo and status_grupo_subgrupo:

                status_grupo_subgrupo = "I"   if str(status_grupo_subgrupo).upper() in ["NONE", "I", "N"]   else "A"

                grupo_subgrupo_update = {
                    "id_grupo": id_grupo,
                    "id_subgrupo": id_subgrupo,
                    "status": status_grupo_subgrupo
                }

                key_columns = ["id_grupo", "id_subgrupo"]

                dm.raw_sql_update("Grupo_Subgrupo", grupo_subgrupo_update, key_columns)

            # Realiza a alteração de um grupo
            if id_grupo:

                # Verifica se nova descricao ou status do grupo foi enviado
                if not grupo_desc and not status_grupo:
                    logger.error(f"Nova descricao ou status do grupo vazio.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Alterações no grupo não foram fornecidas."}
                            }, 400

                # Verifica existência do tipo informado
                grupo_query = f"""
                    SELECT
                        TOP 1 id_grupo,
                        status
                    
                    FROM
                        GRUPO

                    WHERE
                        id_grupo = :id_grupo
                        AND id_distribuidor = :id_distribuidor
                """

                params = {
                    'id_grupo': id_grupo,
                    "id_distribuidor": id_distribuidor
                }

                grupo_query = dm.raw_sql_return(grupo_query, params = params, raw = True, first = True)
                    
                if not grupo_query:

                    logger.error(f"Grupo informado nao existe.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Grupo informado não existe"}
                            }, 400

                # Realiza alterações
                status_query = grupo_query[1]

                grupo_update = {
                    "id_grupo": id_grupo,
                    "id_distribuidor": id_distribuidor
                }

                # Verificação da descrição
                if grupo_desc:
                    grupo_update["descricao"] = str(grupo_desc).upper()

                # Verificação do status
                if status_grupo:

                    status_grupo = "I"   if str(status_grupo).upper() in ["NONE", "I", "N"]   else "A"

                    # Mudança do status da tabela grupo_subgrupo
                    if status_query != status_grupo:

                        grupo_update["status"] = status_grupo

                # Realizando mudança no grupo
                dm.raw_sql_update("Grupo", grupo_update, ["id_grupo", "id_distribuidor"])

            # Realiza a alteração de um subgrupo
            if id_subgrupo:

                # Verifica se nova descricao ou status do subgrupo foi enviado
                if not subgrupo_desc and not status_subgrupo:
                    logger.error(f"Nova descricao ou status do subgrupo vazio.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Alterações no subgrupo não foram fornecidas."}
                            }, 400

                # Verifica existência do tipo informado
                subgrupo_query = f"""
                    SELECT
                        TOP 1 id_subgrupo,
                        status
                    
                    FROM
                        SUBGRUPO

                    WHERE
                        id_subgrupo = :id_subgrupo
                        AND id_distribuidor = :id_distribuidor
                """

                params = {
                    'id_subgrupo': id_subgrupo,
                    "id_distribuidor": id_distribuidor
                }

                subgrupo_query = dm.raw_sql_return(subgrupo_query, params = params, 
                                                        raw = True, first = True)
                    
                if not subgrupo_query:

                    logger.error(f"Subgrupo informado nao existe.")
                    return {"status":400,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Subgrupo informado não existe"}
                            }, 400

                # Realiza alterações
                status_query = subgrupo_query[1]

                subgrupo_update = {
                    "id_subgrupo": id_subgrupo,
                    "id_distribuidor": id_distribuidor
                }

                # Verificação da descrição
                if subgrupo_desc:
                    subgrupo_update["descricao"] = str(subgrupo_desc).upper()

                # Verificação do status
                if status_subgrupo:

                    status_subgrupo = "I"   if str(status_subgrupo).upper() in ["NONE", "I", "N"]   else "A"

                    # Mudança do status da tabela produto_subgrupo
                    if status_query != status_subgrupo:

                        subgrupo_update["status"] = status_subgrupo
                
                # Realizando mudança no subgrupo
                dm.raw_sql_update("Subgrupo", subgrupo_update, ["id_subgrupo", "id_distribuidor"])

        # Checa se algo foi alterado
        else:
            logger.error(f"Dados especificos nao foram fornecidos.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Dados especificos não foram fornecidos"}}, 400

        logger.info("Atualizacoes foram realizadas.")
        return {"status":200,
                "resposta":{"tipo":"1","descricao":f"Registros alterados com sucesso."}}, 200