#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class RegistroPreProduto(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Registro de Pré-Produtos

        Returns:
            [dict]: Status da transação
        """
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        response_data = dm.get_request(values_upper=True, trim_values=True, 
                            not_change_values = ["url_imagem"], bool_save_request = False)

        necessary_keys = ["produtos"]

        correct_types = {"produtos": [list, dict]}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response=response_data,
                                          comparison_columns=necessary_keys,
                                          not_null=necessary_keys,
                                          correct_types = correct_types)):

            return validity[0], 200

        produtos = response_data.get("produtos")

        if type(produtos) is dict:
            produtos = [produtos]

        # Registro de Falhas
        total = len(produtos)
        falhas = 0
        falhas_hold = {}

        # Hold dos commits
        accepted_keys = ["id_distribuidor", "agrupamento_variante", "descr_reduzida_distr", 
        "descr_completa_distr", "cod_prod_distr", "cod_frag_distr", "sku", "dun14", "status", 
        "tipo_produto", "cod_marca", "descr_marca", "cod_grupo", "descr_grupo", "cod_subgrupo", 
        "descr_subgrupo", "variante", "multiplo_venda", "unidade_venda", "quant_unid_venda", 
        "unidade_embalagem", "quantidade_embalagem", "ranking", "giro", "agrup_familia", 
        "volumetria", "cod_fornecedor", "descri_fornecedor", "url_imagem"]   

        not_null_insert = ["id_distribuidor", "agrupamento_variante", "descr_reduzida_distr", 
        "descr_completa_distr", "cod_prod_distr", "cod_frag_distr", "sku", "status",
        "tipo_produto", "cod_marca", "descr_marca", "cod_grupo", "descr_grupo", "cod_subgrupo", 
        "descr_subgrupo", "variante", "multiplo_venda", "unidade_venda", "quant_unid_venda", 
        "unidade_embalagem", "quantidade_embalagem", "ranking", "giro", "cod_fornecedor", 
        "descri_fornecedor"]

        update_keys = {"id_distribuidor", "cod_prod_distr", "descr_reduzida_distr", 
        "descr_completa_distr", "sku", "dun14", "status", "tipo_produto", "cod_marca", 
        "descr_marca", "cod_grupo", "descr_grupo", "cod_subgrupo", "descr_subgrupo", 
        "variante", "multiplo_venda", "unidade_venda", "quant_unid_venda", 
        "unidade_embalagem", "quantidade_embalagem", "ranking", "giro", "agrup_familia", 
        "volumetria", "cod_fornecedor","descri_fornecedor", "url_imagem"}

        produtos_insert = []
        produtos_update = []

        saved_produto_distr_update = []

        for produto in produtos:

            if not isinstance(produto, dict):

                falhas += 1

                if falhas_hold.get("produto"):
                    falhas_hold["produto"]['combinacao'][0]["contador"] += 1
                
                else:
                    falhas_hold[f"produto"] = {
                        "descricao": f"Produto não está em formato válido (objeto|dicionário)",
                        "combinacao": [
                            {
                                "contador": 1,
                            }
                        ]
                    }
                continue

            copy_dict = produto.copy()
                
            produto = {
                key: copy_dict.get(key)
                for key in accepted_keys
            }

            produto["status"] = 'I' if produto.get("status") in ["I", 'i', False, None] else 'A'

            # cod_prod_distr
            id_distribuidor = produto.get("id_distribuidor")
            cod_prod_distr = produto.get("cod_prod_distr")
            agrupamento_variante = produto.get("agrupamento_variante")
            cod_frag_distr = produto.get("cod_frag_distr")
            sku = produto.get("sku")

            if cod_frag_distr and agrupamento_variante and cod_prod_distr:

                if str(cod_prod_distr) != f"{agrupamento_variante}{cod_frag_distr}":
                    falhas += 1

                    if falhas_hold.get("codigo_produto"):
                        falhas_hold["codigo_produto"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr
                        })
                    
                    else:
                        falhas_hold[f"codigo_produto"] = {
                            "descricao": f"Chaves 'agrupamento_variante' e 'cod_frag_distr' com valores diferentes de 'cod_prod_distr'",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cod_prod_distr": cod_prod_distr
                                }
                            ]
                        }
                    continue

            try:
                id_distribuidor = int(id_distribuidor)

            except:
                falhas += 1

                if falhas_hold.get("distribuidor_id_invalido"):
                    falhas_hold["distribuidor_id_invalido"]["combinacao"].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_prod_distr": cod_prod_distr
                    })
                
                else:
                    falhas_hold["distribuidor_id_invalido"] = {
                        "motivo": f"Distribuidor com valor não-numérico.",
                        "combinacao": [{
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr
                        }],
                    }
                continue
            
            if id_distribuidor == 0:
                falhas += 1

                if falhas_hold.get("distribuidor_0"):
                    falhas_hold["distribuidor_0"]["combinacao"].append({
                        "id_distribuidor": 0,
                        "cod_prod_distr": cod_prod_distr
                    })
                
                else:
                    falhas_hold["distribuidor_0"] = {
                        "motivo": f"Distribuidor 0 não pode cadastrar produtos",
                        "combinacao": [{
                            "id_distribuidor": 0,
                            "cod_prod_distr": cod_prod_distr
                        }],
                    }
                continue

            # Checando a existencia do distribuidor
            distri_query = f"""
                SELECT
                    TOP 1 id_distribuidor

                FROM
                    DISTRIBUIDOR
                
                WHERE
                    id_distribuidor = :id_distribuidor
                    AND status = 'A'
            """

            params = {
                "id_distribuidor": id_distribuidor
            }

            distri_query = dm.raw_sql_return(distri_query, params = params, raw = True, first = True)

            if not distri_query:

                falhas += 1

                if falhas_hold.get("distribuidor"):
                    falhas_hold["distribuidor"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr
                    })
                
                else:
                    falhas_hold[f"distribuidor"] = {
                        "descricao": f"Distribuidor não existente ou inativo",
                        "combinacao": [
                            {
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr
                            }
                        ]
                    }
                continue


            # Checando se o produto já está cadastrado
            produto_query = f"""
                SELECT 
                    TOP 1 id_produto,
                    status
                    
                FROM 
                    API_PRODUTO
                
                WHERE
                    cod_prod_distr = :cod_prod_distr
                    AND id_distribuidor = :id_distribuidor
            """

            params = {
                "cod_prod_distr": cod_prod_distr,
                "id_distribuidor": id_distribuidor
            }

            dict_query = {
                "query": produto_query,
                "params": params,
                "first": True,
                "raw": True
            }

            produto_query = dm.raw_sql_return(**dict_query)

            if produto_query:
                
                hold = {
                    key: produto.get(key)
                    for key in update_keys
                }

                hold["dt_update"] = data_hora

                if hold not in produtos_update:
                    produtos_update.append(hold)

                if (id_produto := produto_query[0]) and (produto_query[1] != "R"):

                    saved_produto_distr_update.append({
                        "id_produto": id_produto,
                        "giro": hold.get("giro"),
                        "ranking": hold.get("ranking"),
                    })

                continue

            # Verifica se as chaves escolhidas receberam nulo
            null = []
            for key in not_null_insert:
                if str(produto.get(key)) != "0":
                    if str(produto.get(key)).upper() != "FALSE":
                        if not bool(produto.get(key)):
                            null.append(key)

            # Caso algum not_null tenha recebido nulo...
            if null:
                null = list(set(null))

                falhas += 1

                if falhas_hold.get("valores_nulos"):
                    falhas_hold["valores_nulos"]['combinacao'].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_prod_distr": cod_prod_distr,
                        "chaves": null
                    })
                
                else:
                    falhas_hold[f"valores_nulos"] = {
                        "descricao": f"Chaves com valor vazio ou nulo",
                        "combinacao": [
                            {
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr,
                                "chaves": null
                            }
                        ]
                    }
                continue

            hold = {
                key: produto.get(key)
                for key in accepted_keys
            }

            hold.update({
                "dt_insert": data_hora,
                "status_aprovado": 'P'
            })
                        
            if hold not in produtos_insert:
                produtos_insert.append(hold)

        if produtos_insert:

            dm.raw_sql_insert("API_PRODUTO", produtos_insert)

        if produtos_update:

            key_columns = ["cod_prod_distr", "id_distribuidor"]
            dm.raw_sql_update("API_PRODUTO", produtos_update, key_columns)

        if saved_produto_distr_update:

            key_columns = ["id_produto"]
            dm.raw_sql_update("PRODUTO_DISTRIBUIDOR", saved_produto_distr_update, key_columns)

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

        sucessos = total - falhas

        if falhas == 0:
            logger.info(f"Todas as {sucessos} transacoes ocorreram sem problemas")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados para a administração para análise."},
                    "situacao": {
                        "sucessos": sucessos,
                        "falhas": falhas,
                        "descricao": falhas_hold}}, 200

        logger.info(f"Ocorreram {falhas} falhas e {sucessos} sucessos durante a transacao")
        return {"status":200,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Ocorreram erros na transação."},
                "situacao": {
                    "sucessos": sucessos,
                    "falhas": falhas,
                    "descricao": falhas_hold}}, 200