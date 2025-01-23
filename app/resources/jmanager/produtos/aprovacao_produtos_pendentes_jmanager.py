#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroProdutosPendentesJmanager(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2,3,4])
    def post(self) -> dict:
        """
        Método POST do Aprovar Produtos Pendentes

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        necessary_keys = ["acao"]

        unnecessary_keys = ["produtos"]

        correct_types = {
            "produtos": [list, dict]
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,  
                                not_null = necessary_keys,
                                no_use_columns = unnecessary_keys,
                                correct_types = correct_types)):
            return validity

        # Pegando as credenciais do usuario
        perfil_cadastro = int(info.get("id_perfil"))
        distribuidor_cadastro = info.get("id_distribuidor")
        id_usuario = int(info.get("id_usuario"))

        email = f"""
            SELECT
                TOP 1 email
            
            FROM
                JMANAGER_USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        email = dm.raw_sql_return(email, params = params, raw = True, first = True)[0]

        # Verificando ação requisitada
        acao = str(response_data.get("acao")).lower()

        if acao not in {"aprovar", "reprovar", "aprovar_massa_xxx"}:
            logger.error("Acao requisitada nao e valida.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Ação escolhida é inválida."}}, 200

        # Pegando os produtos para aprovação/reprovação
        if acao in {"aprovar", "reprovar"}:
            lista = response_data.get("produtos")
        
            if type(lista) is dict:
                lista = [lista]

            lista = list({frozenset(item.items()): item  for item in list(lista)}.values())

        # Armazenamento das falhas e sucessos ocorridos
        falhas = 0
        falhas_holder = {}
        
        if acao == "aprovar":

            for produto in lista:

                id_distribuidor = produto.get("id_distribuidor")
                cod_prod_distr = produto.get("cod_prod_distr")

                try:
                    id_distribuidor = int(id_distribuidor)

                except:
                    falhas += 1

                    if falhas_holder.get("distribuidor_id_invalido"):
                        falhas_holder["distribuidor_id_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["distribuidor_id_invalido"] = {
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }],
                            "motivo": f"Distribuidor com valor não-numérico."
                        }
                    continue

                if id_distribuidor <= 0:
                    falhas += 1

                    if falhas_holder.get("distribuidor_invalido"):
                        falhas_holder["distribuidor_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["distribuidor_invalido"] = {
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }],
                            "motivo": f"Distribuidor inválido para processo de aprovação."
                        }
                    continue

                if perfil_cadastro != 1:
                    if id_distribuidor not in distribuidor_cadastro:
                        falhas += 1

                        if falhas_holder.get("distribuidor"):
                            falhas_holder["distribuidor"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            })
                        
                        else:
                            falhas_holder["distribuidor"] = {
                                "combinacao": [{
                                    "id_distribuidor": id_distribuidor,
                                    "cod_prod_distr": cod_prod_distr,
                                }],
                                "motivo": f"Distribuidor tentando aprovar produto de outro distribuidor."
                            }
                        continue

                produto_query = f"""
                    SELECT
                        TOP 1 id_distribuidor
                    
                    FROM
                        API_PRODUTO

                    WHERE
                        sku IS NOT NULL
                        AND status_aprovado in ('P', 'R')
                        AND id_distribuidor = :id_distribuidor
                        AND cod_prod_distr = :cod_prod_distr
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "cod_prod_distr": cod_prod_distr
                }

                produto_query = dm.raw_sql_return(produto_query, params = params, raw = True, first = True)
                
                if not produto_query:
                    falhas += 1

                    if falhas_holder.get("produto"):
                        falhas_holder["produto"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["produto"] = {
                            "motivo": f"Produto não existe ou já foi aprovado.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }]
                        }
                    continue

                query = f"""
                    SET NOCOUNT ON;

                    DECLARE @id_tipo INT = NULL;
                    DECLARE @id_grupo INT = NULL;
                    DECLARE @id_subgrupo INT = NULL;
                    DECLARE @id_fornecedor INT = NULL;
                    DECLARE @id_marca INT = NULL;
                    DECLARE @id_distribuidor INT = :id_distribuidor;

                    -- Criando tabela temporaria
                    IF Object_ID('tempDB..#API_PRODUTO_TMP','U') IS NOT NULL
                    BEGIN
                        DROP TABLE #API_PRODUTO_TMP;
                    END

                    -- Salvando informações na tabela temporária
                    SELECT 
                        TOP 1 * 
                        INTO #API_PRODUTO_TMP
                                                            
                    FROM 
                        API_PRODUTO
                                                            
                    WHERE 
                        COD_PROD_DISTR = :codigo_produto
                        AND ID_DISTRIBUIDOR = :id_distribuidor
                        AND STATUS_APROVADO IN ('P', 'R')
                        AND sku IS NOT NULL
                                                            
                    ORDER BY 
                        AGRUPAMENTO_VARIANTE, 
                        COD_FRAG_DISTR, 
                        COD_PROD_DISTR;

                    -- Salvando informações do fornecedor
                    SET @id_fornecedor = (SELECT TOP 1 id_fornecedor FROM FORNECEDOR WHERE
                        cod_fornecedor_distribuidor = (SELECT TOP 1 cod_fornecedor FROM #API_PRODUTO_TMP))

                    IF @id_fornecedor IS NULL
                    BEGIN
                        INSERT INTO 
                            FORNECEDOR
                                (
                                    cod_fornecedor_distribuidor,
                                    desc_fornecedor,
                                    status
                                )
                            
                            SELECT  
                                cod_fornecedor,
                                descri_fornecedor,
                                'A'

                            FROM 
                                #API_PRODUTO_TMP A

                        SET @id_fornecedor = (SELECT SCOPE_IDENTITY());
                    END

                    -- Salvando TIPO caso não exista
                    SET @id_tipo = (SELECT TOP 1 id_tipo FROM TIPO WHERE padrao = 'S' and id_distribuidor = @id_distribuidor)
                    IF @id_tipo IS NULL
                    BEGIN

                        INSERT INTO 
                            TIPO
                                (
                                    id_distribuidor,
                                    descricao,
                                    status,
                                    padrao,
                                    ranking
                                )
                                                                
                            SELECT	
                                id_distribuidor,
                                'CATEGORIAS' as descricao,
                                'A' as status,
                                'S' padrao,
                                (SELECT ISNULL(MAX(RANKING), 0) + 1 FROM TIPO) as ranking
                        
                            FROM 
                                #API_PRODUTO_TMP A;

                        SET @id_tipo = (SELECT SCOPE_IDENTITY());
                    END

                    -- Salvando GRUPO caso não exista
                    SET @id_grupo = (SELECT TOP 1 id_grupo FROM GRUPO WHERE id_distribuidor = @id_distribuidor 
                        AND cod_grupo_distribuidor = (SELECT TOP 1 cod_grupo FROM #API_PRODUTO_TMP))

                    IF @id_grupo IS NULL
                    BEGIN
                        INSERT INTO 
                            GRUPO
                                (
                                    id_distribuidor,
                                    tipo_pai,
                                    descricao,
                                    cod_grupo_distribuidor,
                                    status
                                )

                        SELECT  
                            id_distribuidor,
                            @id_tipo as tipo_pai,
                            descr_grupo,
                            cod_grupo,
                            'A'

                        FROM 
                            #API_PRODUTO_TMP A;

                        SET @id_grupo = (SELECT SCOPE_IDENTITY());
                    END

                    -- Salvando SUBGRUPO caso não exista
                    SET @id_subgrupo = (SELECT TOP 1 id_subgrupo FROM SUBGRUPO WHERE id_distribuidor = (SELECT TOP 1 id_distribuidor FROM #API_PRODUTO_TMP)
                        AND cod_subgrupo_distribuidor = (SELECT TOP 1 cod_subgrupo FROM #API_PRODUTO_TMP))

                    IF @id_subgrupo IS NULL
                    BEGIN
                        INSERT INTO 
                            SUBGRUPO
                                (
                                    id_distribuidor,
                                    descricao,
                                    cod_subgrupo_distribuidor,
                                    status
                                )

                        SELECT  
                            id_distribuidor,
                            descr_subgrupo,
                            cod_subgrupo,
                            'A'

                        FROM 
                            #API_PRODUTO_TMP A;
                            
                        SET @id_subgrupo = (SELECT SCOPE_IDENTITY());
                    END

                    -- Salvando GRUPO_SUBGRUPO caso não exista
                    IF NOT EXISTS (SELECT TOP 1 1 FROM GRUPO_SUBGRUPO WHERE id_grupo = @id_grupo AND id_subgrupo = @id_subgrupo)

                        INSERT INTO 
                            GRUPO_SUBGRUPO
                                (
                                    id_distribuidor,
                                    id_grupo,
                                    id_subgrupo,
                                    status
                                )

                            SELECT  
                                ID_DISTRIBUIDOR,
                                @id_grupo id_grupo,
                                @id_subgrupo id_subgrupo,
                                'A' STATUS
                            FROM 
                                #API_PRODUTO_TMP A;
                                                                                        

                    -- Salvando MARCA caso não exista
                    SET @id_marca = (SELECT TOP 1 id_marca FROM MARCA WHERE id_distribuidor = @id_distribuidor
                        AND cod_marca_distribuidor = (SELECT TOP 1 id_marca FROM #API_PRODUTO_TMP))

                    IF @id_marca IS NULL
                    BEGIN

                        INSERT INTO 
                            MARCA
                                (
                                    id_distribuidor,
                                    desc_marca,
                                    cod_marca_distribuidor,
                                    status
                                )

                            SELECT  
                                id_distribuidor,
                                descr_marca desc_marca,
                                cod_marca,
                                'A' STATUS

                            FROM 
                                #API_PRODUTO_TMP A;

                        SET @id_marca = (SELECT SCOPE_IDENTITY());
                    END

                    -- Salvando PRODUTO_DISTRIBUIDOR_ID caso não exista
                    INSERT 
                        INTO PRODUTO_DISTRIBUIDOR_ID
                    SELECT  
                        ID_DISTRIBUIDOR,
                        ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR) AGRUPAMENTO_VARIANTE,
                        GETDATE() DATA_CADASTRO
                    FROM 
                        #API_PRODUTO_TMP A
                    WHERE NOT EXISTS ( 
                                        SELECT 
                                            1 
                                        FROM 
                                            PRODUTO_DISTRIBUIDOR_ID B
                                        WHERE 
                                            B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                                            AND B.id_distribuidor = A.ID_DISTRIBUIDOR );


                    -- Salvando PRODUTO_DISTRIBUIDOR caso não exista
                    INSERT 
                        INTO PRODUTO_DISTRIBUIDOR
                            (
                                id_produto,
                                id_distribuidor,
                                agrupamento_variante,
                                cod_prod_distr,
                                multiplo_venda,
                                ranking,
                                unidade_venda,
                                quant_unid_venda,
                                giro,
                                agrup_familia,
                                id_marca,
                                id_fornecedor,
                                status
                            )

                    SELECT  
                        (
                            SELECT 
                                CONVERT(VARCHAR, ID_PRODUTO) 
                            FROM 
                                PRODUTO_DISTRIBUIDOR_ID B
                            WHERE 
                                B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                                AND B.id_distribuidor = A.ID_DISTRIBUIDOR
                        ) 
                        +'-'+ 
                        (
                            SELECT 
                                CONVERT(VARCHAR, COUNT(*)+1) 
                            FROM 
                                PRODUTO_DISTRIBUIDOR C 
                            WHERE 
                                C.id_distribuidor = A.ID_DISTRIBUIDOR AND C.AGRUPAMENTO_VARIANTE=ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR) 
                        ) 
                        as id_produto,
                        id_distribuidor,
                        agrupamento_variante,
                        cod_prod_distr,
                        multiplo_venda,
                        ranking,
                        unidade_venda,
                        quant_unid_venda,
                        giro,
                        agrup_familia,
                        @id_marca id_marca,
                        @id_fornecedor id_fornecedor,
                        status

                    FROM 
                        #API_PRODUTO_TMP A

                    WHERE NOT EXISTS ( 
                                        SELECT 
                                            1 
                                        FROM 
                                            PRODUTO_DISTRIBUIDOR B
                                        WHERE B.AGRUPAMENTO_VARIANTE = ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                                            AND B.id_distribuidor = A.ID_DISTRIBUIDOR
                                            AND B.COD_PROD_DISTR = A.COD_PROD_DISTR);


                    -- Salvando PRODUTO caso não exista
                    INSERT INTO 
                        PRODUTO
                            (
                                ID_PRODUTO,
                                ID_AGRUPAMENTO,
                                DESCRICAO,
                                DESCRICAO_COMPLETA,
                                SKU,
                                DUN14,
                                STATUS,
                                TIPO_PRODUTO,
                                VARIANTE,
                                UNIDADE_VENDA,
                                QUANT_UNID_VENDA,
                                UNIDADE_EMBALAGEM,
                                QUANTIDADE_EMBALAGEM,
                                VOLUMETRIA,
                                DT_INSERT,
                                DT_UPDATE,
                                DATA_CADASTRO
                            )

                        SELECT  
                            (
                                SELECT 
                                    ID_PRODUTO
                                FROM 
                                    PRODUTO_DISTRIBUIDOR C 
                                WHERE 
                                    C.id_distribuidor = A.ID_DISTRIBUIDOR
                                        AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                        AND C.COD_PROD_DISTR=A.COD_PROD_DISTR)
                            ID_PRODUTO,
                            AGRUPAMENTO_VARIANTE ID_AGRUPAMENTO,
                            DESCR_REDUZIDA_DISTR DESCRICAO,
                            DESCR_COMPLETA_DISTR DESCRICAO_COMPLETA,
                            SKU,
                            DUN14,
                            STATUS,
                            TIPO_PRODUTO,
                            VARIANTE,
                            UNIDADE_VENDA,
                            QUANT_UNID_VENDA,
                            UNIDADE_EMBALAGEM,
                            QUANTIDADE_EMBALAGEM,
                            VOLUMETRIA,
                            DT_INSERT,
                            DT_UPDATE,
                            GETDATE() DATA_CADASTRO
                        FROM 
                            #API_PRODUTO_TMP A
                        WHERE NOT EXISTS ( 
                                            SELECT 
                                                1 
                                            FROM 
                                                PRODUTO B
                                            WHERE 
                                                B.ID_PRODUTO IN (
                                                                    SELECT 
                                                                        ID_PRODUTO
                                                                    FROM 
                                                                        PRODUTO_DISTRIBUIDOR C 
                                                                    WHERE 
                                                                        C.id_distribuidor = A.ID_DISTRIBUIDOR
                                                                            AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                                                            AND C.COD_PROD_DISTR=A.COD_PROD_DISTR) );


                    -- Salvando PRODUTO_SUBGRUPO caso não exista
                    INSERT
                        INTO PRODUTO_SUBGRUPO
                    SELECT  
                        (SELECT 
                            ID_PRODUTO
                        FROM 
                            PRODUTO_DISTRIBUIDOR C 
                        WHERE 
                            C.id_distribuidor = A.ID_DISTRIBUIDOR
                            AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                            AND C.COD_PROD_DISTR=A.COD_PROD_DISTR)
                        CODIGO_PRODUTO,
                        SKU,
                        ID_DISTRIBUIDOR,
                        CONVERT(VARCHAR, ID_DISTRIBUIDOR)+cod_subgrupo id_subgrupo,
                        'A' STATUS
                    FROM 
                        #API_PRODUTO_TMP A
                    WHERE NOT EXISTS ( 
                                        SELECT 
                                            1 
                                        FROM 
                                            PRODUTO_SUBGRUPO ps 
                                        WHERE 
                                            ps.id_subgrupo = @id_subgrupo
                                            AND ps.sku = A.SKU
                                            AND ps.id_distribuidor = A.ID_DISTRIBUIDOR 
                                            AND ps.codigo_produto = (
                                                                        SELECT 
                                                                            ID_PRODUTO
                                                                        FROM 
                                                                            PRODUTO_DISTRIBUIDOR C 
                                                                        WHERE 
                                                                            C.id_distribuidor = A.ID_DISTRIBUIDOR
                                                                            AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                                                            AND C.COD_PROD_DISTR=A.COD_PROD_DISTR))

                    -- Update na tabela API_PRODUTO
                    UPDATE 
                        A
                    SET
                        A.ID_PRODUTO = CASE
                                            WHEN (SELECT TOP 1 id_produto FROM #API_PRODUTO_TMP) IS NOT NULL
                                                THEN A.ID_PRODUTO
                                            ELSE (SELECT ID_PRODUTO
                                                    FROM PRODUTO_DISTRIBUIDOR C 
                                                    WHERE C.id_distribuidor = B.ID_DISTRIBUIDOR
                                                        AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(B.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, B.ID_DISTRIBUIDOR)+'-'+B.COD_PROD_DISTR)
                                                        AND C.COD_PROD_DISTR=B.COD_PROD_DISTR)
                                        END,
                        A.STATUS_APROVADO = 'A',
                        A.USUARIO_APROVADO = :email
                    FROM
                        API_PRODUTO AS A
                        INNER JOIN 
                            #API_PRODUTO_TMP AS B
                                ON A.COD_PROD_DISTR = B.COD_PROD_DISTR
                                AND A.ID_DISTRIBUIDOR=B.ID_DISTRIBUIDOR
                    WHERE 
                        A.COD_PROD_DISTR = :codigo_produto
                        AND A.ID_DISTRIBUIDOR = :id_distribuidor
                        AND A.STATUS_APROVADO IN ('P', 'R');


                    DROP TABLE #API_PRODUTO_TMP
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "codigo_produto": cod_prod_distr,
                    "email": email
                }
        
                dm.raw_sql_execute(query, params = params)

            if lista:

                # Altera o tipo-grupo-subgrupo do 0
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
                                AND s.id_distribuidor = g.id_distribuidor

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

        elif acao == "aprovar_massa_xxx" and perfil_cadastro == 1:

            query = f"""
                SET NOCOUNT ON;

                -- Criando tabela temporaria
                IF Object_ID('tempDB..#API_PRODUTO_TMP','U') IS NOT NULL
                BEGIN
                    DROP TABLE #API_PRODUTO_TMP;
                END

                IF Object_ID('tempDB..#API_UNIQUE_AGRUPAMENTO','U') IS NOT NULL
                BEGIN
                    DROP TABLE #API_UNIQUE_AGRUPAMENTO;
                END

                -- Salvando informações na tabela temporária
                SELECT 
                    * 
                                    
                INTO 
                    #API_PRODUTO_TMP
                                                    
                FROM 
                    API_PRODUTO
                                                    
                WHERE 
                    STATUS_APROVADO IN ('P')
                    AND sku IS NOT NULL
                                                    
                ORDER BY 
                    AGRUPAMENTO_VARIANTE, 
                    COD_FRAG_DISTR, 
                    COD_PROD_DISTR;

                -- Salvando os agrupamentos unicos
                SELECT
                    AGRUPAMENTO_VARIANTE,
                    COD_FRAG_DISTR,
                    COD_PROD_DISTR,
                    ID_DISTRIBUIDOR,
                    ROW_NUMBER() OVER(PARTITION BY ID_DISTRIBUIDOR, ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR) ORDER BY COD_PROD_DISTR) ORDER_FRAG

                INTO
                    #API_UNIQUE_AGRUPAMENTO

                FROM
                    #API_PRODUTO_TMP A;	

                -- Salvando informações do fornecedor
                INSERT INTO 
                    FORNECEDOR
                        (
                            cod_fornecedor_distribuidor,
                            desc_fornecedor,
                            status
                        )
                                                        
                    SELECT
                        cod_fornecedor,
                        descri_fornecedor,
                        status

                    FROM
                        (
                        
                            SELECT  
                                DISTINCT
                                    cod_fornecedor,
                                    descri_fornecedor,
                                    'A' as status,
                                    A.ID_DISTRIBUIDOR
                                                        
                            FROM 
                                (
                                    SELECT
                                        DISTINCT 
                                            COD_FORNECEDOR,
                                            ID_DISTRIBUIDOR,
                                            DESCRI_FORNECEDOR

                                    FROM
                                        #API_PRODUTO_TMP 

                                ) A
                                                        
                            WHERE NOT EXISTS (  SELECT 
                                                    1 
                                                FROM 
                                                    FORNECEDOR B
                                                WHERE 
                                                    B.cod_fornecedor_distribuidor = COD_FORNECEDOR)
                        
                        ) A
                                            
                    ORDER BY
                        A.ID_DISTRIBUIDOR;


                -- Salvando TIPO caso não exista
                INSERT INTO 
                    TIPO
                        (
                            id_distribuidor,
                            descricao,
                            status,
                            padrao	
                        )
                                                        
                    SELECT
                        id_distribuidor,
                        descricao,
                        status,
                        padrao

                    FROM
                        (
                        
                            SELECT	
                                DISTINCT
                                    A.id_distribuidor,
                                    'CATEGORIAS' descricao,
                                    'A' status,
                                    'S' PADRAO
                                                        
                            FROM 
                                (
                                    SELECT
                                        DISTINCT
                                            ID_DISTRIBUIDOR

                                    FROM
                                        #API_PRODUTO_TMP

                                    WHERE
                                        id_distribuidor NOT IN (SELECT id_distribuidor FROM TIPO WHERE padrao = 'S')
                                ) A
                        
                        ) A
                        
                    ORDER BY
                        A.ID_DISTRIBUIDOR;


                -- Salvando GRUPO caso não exista
                INSERT INTO 
                    GRUPO
                        (
                            id_distribuidor,
                            tipo_pai,
                            descricao,
                            cod_grupo_distribuidor,
                            status
                        )

                    SELECT
                        id_distribuidor,
                        tipo_pai,
                        descricao,
                        cod_grupo,
                        status

                    FROM
                        (
                        
                            SELECT  
                                DISTINCT
                                    id_distribuidor,
                                    (

                                        SELECT 
                                            MAX(ID_TIPO)

                                        FROM 
                                            TIPO C 

                                        WHERE 
                                            C.PADRAO='S' AND C.id_distribuidor = A.ID_DISTRIBUIDOR

                                    ) tipo_pai,
                                    descr_grupo descricao,
                                    cod_grupo,
                                    'A' STATUS

                            FROM 
                                #API_PRODUTO_TMP A
                        
                            WHERE
                                NOT EXISTS (SELECT 1 FROM GRUPO WHERE cod_grupo = A.cod_grupo AND id_distribuidor = A.id_distribuidor)
                        
                        ) A
                        
                    ORDER BY
                        A.ID_DISTRIBUIDOR,
                        A.COD_GRUPO;

                -- Salvando SUBGRUPO caso não exista
                INSERT INTO 
                    SUBGRUPO
                        (
                            id_distribuidor,
                            descricao,
                            cod_subgrupo_distribuidor,
                            status
                        )

                    SELECT
                        id_distribuidor,
                        descr_subgrupo,
                        cod_subgrupo,
                        STATUS

                    FROM
                        (
                        
                            SELECT  
                                DISTINCT
                                    id_distribuidor,
                                    descr_subgrupo,
                                    cod_subgrupo,
                                    'A' STATUS

                            FROM 
                                #API_PRODUTO_TMP A
                        
                            WHERE
                                NOT EXISTS (SELECT 1 FROM SUBGRUPO s WHERE s.cod_subgrupo_distribuidor = A.cod_subgrupo AND s.id_distribuidor = A.id_distribuidor)
                        
                        ) A
                        
                    ORDER BY
                        A.ID_DISTRIBUIDOR,
                        A.COD_SUBGRUPO;
                                                        

                -- Salvando GRUPO_SUBGRUPO caso não exista
                INSERT INTO 
                    GRUPO_SUBGRUPO
                        (
                            id_distribuidor,
                            id_grupo,
                            id_subgrupo,
                            status
                        )

                    SELECT
                        ID_DISTRIBUIDOR,
                        id_grupo,
                        id_subgrupo,
                        STATUS

                    FROM
                        (
                        
                            SELECT  
                                DISTINCT
                                    A.ID_DISTRIBUIDOR,
                                    g.id_grupo,
                                    s.id_subgrupo,
                                    'A' STATUS
                            FROM 
                                (
                                    SELECT
                                        DISTINCT 
                                            ID_DISTRIBUIDOR,
                                            COD_GRUPO,
                                            COD_SUBGRUPO

                                    FROM
                                        #API_PRODUTO_TMP
                                ) A

                                INNER JOIN GRUPO g ON
                                    A.id_distribuidor = g.id_distribuidor
                                    AND A.cod_grupo = g.cod_grupo_distribuidor

                                INNER JOIN SUBGRUPO s ON
                                    A.id_distribuidor = s.id_distribuidor
                                    AND A.cod_subgrupo = s.cod_subgrupo_distribuidor

                            WHERE 
                                NOT EXISTS ( 
                                                SELECT 
                                                    1 
                                                FROM 
                                                    GRUPO_SUBGRUPO GB
                                                WHERE 
                                                    GB.id_subgrupo = s.id_subgrupo
                                                    AND GB.id_grupo = g.id_grupo
                                                    AND GB.id_distribuidor = A.ID_DISTRIBUIDOR 
                                        )
                        
                        ) A
                                
                    ORDER BY
                        A.ID_DISTRIBUIDOR,
                        A.id_grupo,
                        A.id_subgrupo;
                                                                                

                -- Salvando MARCA caso não exista
                INSERT INTO 
                    MARCA
                        (
                            id_distribuidor,
                            desc_marca,
                            cod_marca_distribuidor,
                            status
                        )

                    SELECT
                        id_distribuidor,
                        descr_marca,
                        cod_marca,
                        status

                    FROM
                        (
                        
                            SELECT  
                                DISTINCT
                                    id_distribuidor,
                                    descr_marca,
                                    cod_marca,
                                    status

                            FROM 
                                #API_PRODUTO_TMP A

                            WHERE 
                                NOT EXISTS ( 
                                                SELECT 
                                                    1 
                                                FROM 
                                                    MARCA B
                                                WHERE 
                                                    B.cod_marca_distribuidor = A.cod_marca
                                                    AND B.id_distribuidor = A.ID_DISTRIBUIDOR 
                                            )
                        
                        ) A
                                    
                    ORDER BY
                        A.ID_DISTRIBUIDOR,
                        A.COD_MARCA;


                -- Salvando PRODUTO_DISTRIBUIDOR_ID caso não exista
                INSERT 
                    INTO PRODUTO_DISTRIBUIDOR_ID
                SELECT  
                    DISTINCT
                        ID_DISTRIBUIDOR,
                        ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR) AGRUPAMENTO_VARIANTE,
                        GETDATE() DATA_CADASTRO
                FROM 
                    (
                        SELECT
                            DISTINCT 
                                ID_DISTRIBUIDOR,
                                AGRUPAMENTO_VARIANTE,
                                COD_PROD_DISTR

                        FROM
                            #API_PRODUTO_TMP
                    ) A
                WHERE NOT EXISTS ( 
                                    SELECT 
                                        1 
                                    FROM 
                                        PRODUTO_DISTRIBUIDOR_ID B
                                    WHERE 
                                        B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                                        AND B.id_distribuidor = A.ID_DISTRIBUIDOR );


                -- Salvando PRODUTO_DISTRIBUIDOR caso não exista
                INSERT INTO 
                    PRODUTO_DISTRIBUIDOR
                        (
                            id_produto,
                            id_distribuidor,
                            agrupamento_variante,
                            cod_prod_distr,
                            multiplo_venda,
                            ranking,
                            unidade_venda,
                            quant_unid_venda,
                            giro,
                            agrup_familia,
                            id_marca,
                            id_fornecedor,
                            status
                        )

                    SELECT  
                        (
                            SELECT 
                                CONVERT(VARCHAR, ID_PRODUTO) 
                            FROM 
                                PRODUTO_DISTRIBUIDOR_ID B
                            WHERE 
                                B.AGRUPAMENTO_VARIANTE = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                                AND B.id_distribuidor = A.ID_DISTRIBUIDOR) 
                        +'-'+ 
                        (
                            SELECT
                                CONVERT(VARCHAR, (

                                                        (
                                                            SELECT 
                                                                COUNT(*)
                                                            FROM 
                                                                PRODUTO_DISTRIBUIDOR C 
                                                            WHERE 
                                                                C.id_distribuidor = A.ID_DISTRIBUIDOR 
                                                                AND C.AGRUPAMENTO_VARIANTE=ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR) 
                                                        )

                                                        +

                                                        (

                                                            SELECT
                                                                ORDER_FRAG
                                        
                                                            FROM
                                                                #API_UNIQUE_AGRUPAMENTO D

                                                            WHERE
                                                                D.id_distribuidor = A.ID_DISTRIBUIDOR 
                                                                AND D.COD_PROD_DISTR = A.COD_PROD_DISTR
                                                        )
                                                        
                                                )
                                    )

                        ) id_produto,
                        id_distribuidor,
                        agrupamento_variante,
                        cod_prod_distr,
                        multiplo_venda,
                        ranking,
                        unidade_venda,
                        quant_unid_venda,
                        giro,
                        agrup_familia,
                        (

                            SELECT
                                TOP 1 id_marca

                            FROM
                                MARCA

                            WHERE
                                cod_marca_distribuidor = A.cod_marca
                                AND id_distribuidor = A.id_distribuidor
                                
                        ) id_marca,
                        (

                            SELECT
                                TOP 1 id_fornecedor

                            FROM
                                FORNECEDOR

                            WHERE
                                cod_fornecedor_distribuidor = A.cod_fornecedor
                                
                        ) id_fornecedor,
                        status

                    FROM 
                        #API_PRODUTO_TMP A

                    WHERE NOT EXISTS ( 
                                        SELECT 
                                            1 
                                        FROM 
                                            PRODUTO_DISTRIBUIDOR B
                                        WHERE B.AGRUPAMENTO_VARIANTE = ISNULL(AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, ID_DISTRIBUIDOR)+'-'+COD_PROD_DISTR)
                                            AND B.id_distribuidor = A.ID_DISTRIBUIDOR
                                            AND B.COD_PROD_DISTR = A.COD_PROD_DISTR);


                -- Salvando PRODUTO caso não exista
                INSERT INTO 
                    PRODUTO
                        (
                            ID_PRODUTO,
                            ID_AGRUPAMENTO,
                            DESCRICAO,
                            DESCRICAO_COMPLETA,
                            SKU,
                            DUN14,
                            STATUS,
                            TIPO_PRODUTO,
                            VARIANTE,
                            UNIDADE_VENDA,
                            QUANT_UNID_VENDA,
                            UNIDADE_EMBALAGEM,
                            QUANTIDADE_EMBALAGEM,
                            VOLUMETRIA,
                            DT_INSERT,
                            DT_UPDATE,
                            DATA_CADASTRO
                        )
                SELECT 
                    DISTINCT 
                    (
                        SELECT 
                            ID_PRODUTO
                        FROM 
                            PRODUTO_DISTRIBUIDOR C 
                        WHERE 
                            C.id_distribuidor = A.ID_DISTRIBUIDOR
                                AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                AND C.COD_PROD_DISTR=A.COD_PROD_DISTR)
                    ID_PRODUTO,
                    AGRUPAMENTO_VARIANTE ID_AGRUPAMENTO,
                    DESCR_REDUZIDA_DISTR DESCRICAO,
                    DESCR_COMPLETA_DISTR DESCRICAO_COMPLETA,
                    SKU,
                    DUN14,
                    STATUS,
                    TIPO_PRODUTO,
                    VARIANTE,
                    UNIDADE_VENDA,
                    QUANT_UNID_VENDA,
                    UNIDADE_EMBALAGEM,
                    QUANTIDADE_EMBALAGEM,
                    VOLUMETRIA,
                    DT_INSERT,
                    DT_UPDATE,
                    GETDATE() DATA_CADASTRO
                FROM 
                    #API_PRODUTO_TMP A
                WHERE NOT EXISTS ( 
                                    SELECT 
                                        1 
                                    FROM 
                                        PRODUTO B
                                    WHERE 
                                        B.ID_PRODUTO IN (
                                                            SELECT 
                                                                ID_PRODUTO
                                                            FROM 
                                                                PRODUTO_DISTRIBUIDOR C 
                                                            WHERE 
                                                                C.id_distribuidor = A.ID_DISTRIBUIDOR
                                                                    AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                                                    AND C.COD_PROD_DISTR=A.COD_PROD_DISTR) );


                -- Salvando PRODUTO_SUBGRUPO caso não exista
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
                    DISTINCT
                        (SELECT 
                            ID_PRODUTO
                        FROM 
                            PRODUTO_DISTRIBUIDOR C 
                        WHERE 
                            C.id_distribuidor = A.ID_DISTRIBUIDOR
                            AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                            AND C.COD_PROD_DISTR=A.COD_PROD_DISTR)
                        CODIGO_PRODUTO,
                        SKU,
                        A.ID_DISTRIBUIDOR,
                        s.id_subgrupo,
                        'A' STATUS
                FROM 
                    (
                        SELECT
                            DISTINCT 
                                ID_DISTRIBUIDOR,
                                COD_PROD_DISTR,
                                AGRUPAMENTO_VARIANTE,
                                SKU,
                                COD_SUBGRUPO

                        FROM
                            #API_PRODUTO_TMP
                    ) A

                    INNER JOIN SUBGRUPO s ON
                        A.id_distribuidor = s.id_distribuidor
                        AND A.cod_subgrupo = s.cod_subgrupo_distribuidor

                WHERE NOT EXISTS ( 
                                    SELECT 
                                        1 
                                    FROM 
                                        PRODUTO_SUBGRUPO ps 
                                    WHERE 
                                        ps.id_subgrupo = s.id_subgrupo
                                        AND ps.sku = A.SKU
                                        AND ps.id_distribuidor = A.ID_DISTRIBUIDOR 
                                        AND ps.codigo_produto = (
                                                                    SELECT 
                                                                        ID_PRODUTO
                                                                    FROM 
                                                                        PRODUTO_DISTRIBUIDOR C 
                                                                    WHERE 
                                                                        C.id_distribuidor = A.ID_DISTRIBUIDOR
                                                                        AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(A.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, A.ID_DISTRIBUIDOR)+'-'+A.COD_PROD_DISTR)
                                                                        AND C.COD_PROD_DISTR=A.COD_PROD_DISTR))

                -- Update na tabela API_PRODUTO
                UPDATE 
                    A
                SET
                    A.ID_PRODUTO = CASE
                                        WHEN (SELECT TOP 1 id_produto FROM #API_PRODUTO_TMP) IS NOT NULL
                                            THEN A.ID_PRODUTO
                                        ELSE (SELECT ID_PRODUTO
                                                FROM PRODUTO_DISTRIBUIDOR C 
                                                WHERE C.id_distribuidor = B.ID_DISTRIBUIDOR
                                                    AND ISNULL(C.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, C.ID_DISTRIBUIDOR)+'-'+C.COD_PROD_DISTR) = ISNULL(B.AGRUPAMENTO_VARIANTE, CONVERT(VARCHAR, B.ID_DISTRIBUIDOR)+'-'+B.COD_PROD_DISTR)
                                                    AND C.COD_PROD_DISTR=B.COD_PROD_DISTR)
                                    END,
                    A.STATUS_APROVADO = 'A',
                    A.USUARIO_APROVADO = :email
                FROM
                    API_PRODUTO AS A
                    INNER JOIN 
                        #API_PRODUTO_TMP AS B
                            ON A.COD_PROD_DISTR = B.COD_PROD_DISTR
                            AND A.ID_DISTRIBUIDOR=B.ID_DISTRIBUIDOR
                WHERE 
                    A.STATUS_APROVADO IN ('P');


                DROP TABLE #API_PRODUTO_TMP;
                DROP TABLE #API_UNIQUE_AGRUPAMENTO;
            """

            params = {
                "email": email
            }

            dm.raw_sql_execute(query, params = params)

            # Altera o tipo-grupo-subgrupo do 0
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
                            AND s.id_distribuidor = g.id_distribuidor

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

        elif acao == "reprovar":

            update_hold = []

            for produto in lista:

                id_distribuidor = produto.get("id_distribuidor")
                cod_prod_distr = produto.get("cod_prod_distr")
                motivo_reprovado = produto.get("motivo_reprovado")

                try:
                    id_distribuidor = int(id_distribuidor)

                except:
                    falhas += 1

                    if falhas_holder.get("distribuidor_id_invalido"):
                        falhas_holder["distribuidor_id_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["distribuidor_id_invalido"] = {
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }],
                            "motivo": f"Distribuidor com valor não-numérico."
                        }
                    continue

                if id_distribuidor <= 0:
                    falhas += 1

                    if falhas_holder.get("distribuidor_invalido"):
                        falhas_holder["distribuidor_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["distribuidor_invalido"] = {
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }],
                            "motivo": f"Distribuidor inválido para processo de reprovação."
                        }
                    continue

                if perfil_cadastro != 1:
                    if id_distribuidor not in distribuidor_cadastro:
                        falhas += 1

                        if falhas_holder.get("distribuidor"):
                            falhas_holder["distribuidor"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            })
                        
                        else:
                            falhas_holder["distribuidor"] = {
                                "combinacao": [{
                                    "id_distribuidor": id_distribuidor,
                                    "cod_prod_distr": cod_prod_distr,
                                }],
                                "motivo": f"Distribuidor tentando reprovar produto de outro distribuidor."
                            }
                        continue

                produto_query = f"""
                    SELECT
                        TOP 1 id_distribuidor
                    
                    FROM
                        API_PRODUTO

                    WHERE
                        sku IS NOT NULL
                        AND status_aprovado in ('P', 'A')
                        AND id_distribuidor = :id_distribuidor
                        AND cod_prod_distr = :cod_prod_distr
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "cod_prod_distr": cod_prod_distr
                }

                produto_query = dm.raw_sql_return(produto_query, params = params, raw = True, first = True)
                
                if not produto_query:
                    falhas += 1

                    if falhas_holder.get("produto"):
                        falhas_holder["produto"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["produto"] = {
                            "motivo": f"Produto não existe ou já foi aprovado.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }]
                        }
                    continue

                if not str(id_distribuidor).isdecimal():
                    falhas += 1

                    if falhas_holder.get("distribuidor_invalido"):
                        falhas_holder["distribuidor_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["distribuidor_invalido"] = {
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }],
                            "motivo": f"Distribuidor com valor não-numérico."
                        }
                    continue

                if perfil_cadastro != 1:
                    if int(id_distribuidor) not in distribuidor_cadastro:
                        falhas += 1

                        if falhas_holder.get("distribuidor"):
                            falhas_holder["distribuidor"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            })
                        
                        else:
                            falhas_holder["distribuidor"] = {
                                "combinacao": [{
                                    "id_distribuidor": id_distribuidor,
                                    "cod_prod_distr": cod_prod_distr,
                                }],
                                "motivo": f"Distribuidor tentando aprovar produto de outro distribuidor."
                            }
                        continue

                produto_query = f"""
                    SELECT
                        top 1 id_distribuidor

                    FROM
                        API_PRODUTO

                    WHERE
                        cod_prod_distr = :cod_prod_distr
                        AND id_distribuidor = :id_distribuidor
                        AND status_aprovado in ('P', 'A')
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "cod_prod_distr": cod_prod_distr
                }
                
                produto_query = dm.raw_sql_return(produto_query, params = params, raw = True, first = True)

                if not produto_query:
                    falhas += 1

                    if falhas_holder.get("produto"):
                        falhas_holder["produto"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                        })
                    
                    else:
                        falhas_holder["produto"] = {
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                            }],
                            "motivo": f"Produto não existe ou já foi reprovado."
                        }
                    continue

                update_hold.append({
                    "id_distribuidor": id_distribuidor,
                    "cod_prod_distr": cod_prod_distr,
                    "usuario_aprovado": email,
                    "motivo_reprovado": motivo_reprovado,
                    "status_aprovado": "R"
                })
            
            if update_hold:
                
                key_columns = ["id_distribuidor", "cod_prod_distr"]

                dm.raw_sql_update("API_PRODUTO", update_hold, key_columns)

        logger.info(f"O servico de {acao} terminou com {falhas} falhas")
        if falhas > 0:
            logger.error(f"O servico de {acao} terminou com {falhas} falhas", extra = falhas_holder)

        if falhas <= 0:
            return {"status":201,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Produtos {'aprovados' if 'aprovar' in acao else 'reprovados'} com sucesso."}}, 201
        
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Houveram erros com a transação",
                "situacao": {
                    "sucessos": len(produto) - falhas,
                    "falhas": falhas,
                    "descricao": falhas_holder}
                }}, 200