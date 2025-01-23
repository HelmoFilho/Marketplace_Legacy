#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarTipoGrupoSubgrupoMarketplace(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self) -> dict:
        """
        Método POST do Listar Tipo Grupo Subgrupo do Marketplace

        Returns:
            [dict]: Status da transação
        """
        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]

        unnecessary_keys = ["id_cliente"]

        correct_types = {"id_distribuidor": int, "id_cliente": int}

        # Checa chaves inválidas e chaves faltantes
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            return validity

        id_distribuidor = int(response_data.get("id_distribuidor")) \
                            if response_data.get("id_distribuidor") else 0
                            
        id_cliente = int(response_data.get("id_cliente"))\
                        if response_data.get("id_cliente") and id_usuario else None

        if id_usuario:
            
            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

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
                    c.id_cliente = :id_cliente
                    AND d.id_distribuidor = CASE
                                                WHEN :id_distribuidor = 0
                                                    THEN d.id_distribuidor
                                                ELSE
                                                    :id_distribuidor
                                            END
                    AND d.id_distribuidor != 0
                    AND c.status = 'A'
                    AND c.status_receita = 'A'
                    AND cd.status = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND d.status = 'A'
            """

            params = {
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor
            }

        else:
            query = """
                SELECT
                    DISTINCT id_distribuidor

                FROM
                    DISTRIBUIDOR

                WHERE
                    id_distribuidor = CASE
                                          WHEN :id_distribuidor = 0
                                              THEN id_distribuidor
                                          ELSE
                                              :id_distribuidor
                                      END
                    AND id_distribuidor != 0
                    AND status = 'A'
            """

            params = {
                "id_distribuidor": id_distribuidor
            }      

        dict_query = {
            "query": query,
            "params": params,
            "raw": True
        }

        distribuidores = dm.raw_sql_return(**dict_query)
        if not distribuidores:
            logger.error("Sem distribuidores cadastrados válidos para a operação.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Sem distribuidores cadastrados."}}, 200
                        
        distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

        params = {
            "id_distribuidor": id_distribuidor,
            "distribuidores": distribuidores,
        }

        # Criando o json
        if id_usuario:

            params["id_cliente"] = id_cliente

            query = r"""
                SELECT
                    t.id_tipo,
                    t.descricao descricao_tipo,
                    g.id_grupo,
                    g.descricao descricao_grupo,
                    s.id_subgrupo,
                    s.descricao descricao_subgrupo

                FROM
                    (

                        SELECT
                            DISTINCT t.id_tipo,
                            g.id_grupo,
                            s.id_subgrupo

                        FROM
                            (

                                SELECT
                                    DISTINCT
                                        pd.id_produto

                                FROM
                                    PRODUTO p
                                    
                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        p.id_produto = pd.id_produto

                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                        pd.id_produto = tpp.id_produto
                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                    INNER JOIN TABELA_PRECO tp ON
                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                    INNER JOIN FORNECEDOR f ON
                                        pd.id_fornecedor = f.id_fornecedor

                                    INNER JOIN MARCA m ON
                                        pd.id_marca = m.id_marca
                                        AND pd.id_distribuidor = m.id_distribuidor

                                WHERE
                                    pd.id_distribuidor IN :distribuidores
                                    AND (
                                            (
                                                tp.tabela_padrao = 'S'
                                            )
                                                OR
                                            (
                                                tp.id_tabela_preco IN (
                                                                            SELECT
                                                                                id_tabela_preco

                                                                            FROM
                                                                                TABELA_PRECO_CLIENTE

                                                                            WHERE
                                                                                id_distribuidor IN :distribuidores
                                                                                AND id_cliente = :id_cliente
                                                                        )
                                            )
                                        )
                                    AND	tp.dt_inicio <= GETDATE()
                                    AND	tp.dt_fim >= GETDATE()
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND tp.status = 'A'
                                    AND m.status = 'A'
                                    AND f.status = 'A'
                                    
                            ) pd

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
                            ps.id_distribuidor = :id_distribuidor
                            AND t.status = 'A'
                            AND g.status = 'A'
                            AND gs.status = 'A'
                            AND s.status = 'A'
                            AND ps.status = 'A'

                    ) tgs

                    INNER JOIN TIPO t ON
                        tgs.id_tipo = t.id_tipo

                    INNER JOIN GRUPO g ON
                        tgs.id_grupo = g.id_grupo

                    INNER JOIN SUBGRUPO s ON
                        tgs.id_subgrupo = s.id_subgrupo

                ORDER BY
                    CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END,
                    t.descricao,
                    CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END,
                    g.descricao,
                    CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END,
                    s.descricao
            """

        else:
            
            query = r"""
                SELECT
                    t.id_tipo,
                    t.descricao descricao_tipo,
                    g.id_grupo,
                    g.descricao descricao_grupo,
                    s.id_subgrupo,
                    s.descricao descricao_subgrupo

                FROM
                    (

                        SELECT
                            DISTINCT t.id_tipo,
                            g.id_grupo,
                            s.id_subgrupo

                        FROM
                            (

                                SELECT
                                    DISTINCT
                                        pd.id_produto

                                FROM
                                    PRODUTO p
                                    
                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        p.id_produto = pd.id_produto

                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                        pd.id_produto = tpp.id_produto
                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                    INNER JOIN TABELA_PRECO tp ON
                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                    INNER JOIN FORNECEDOR f ON
                                        pd.id_fornecedor = f.id_fornecedor

                                    INNER JOIN MARCA m ON
                                        pd.id_marca = m.id_marca
                                        AND pd.id_distribuidor = m.id_distribuidor

                                WHERE
                                    pd.id_distribuidor IN :distribuidores
                                    AND tp.tabela_padrao = 'S'
                                    AND	tp.dt_inicio <= GETDATE()
                                    AND	tp.dt_fim >= GETDATE()
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND tp.status = 'A'
                                    AND m.status = 'A'
                                    AND f.status = 'A'
                                    
                            ) pd

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
                            ps.id_distribuidor = :id_distribuidor
                            AND t.status = 'A'
                            AND g.status = 'A'
                            AND gs.status = 'A'
                            AND s.status = 'A'
                            AND ps.status = 'A'

                    ) tgs

                    INNER JOIN TIPO t ON
                        tgs.id_tipo = t.id_tipo

                    INNER JOIN GRUPO g ON
                        tgs.id_grupo = g.id_grupo

                    INNER JOIN SUBGRUPO s ON
                        tgs.id_subgrupo = s.id_subgrupo

                ORDER BY
                    CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END,
                    t.descricao,
                    CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END,
                    g.descricao,
                    CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END,
                    s.descricao
            """        

        dict_query = {
            "query": query,
            "params": params,
        }
        
        data = dm.raw_sql_return(**dict_query)

        if data:

            dados = []

            for tgs in data:

                for saved_tgs in dados:
                    if saved_tgs["id_tipo"] == tgs["id_tipo"]:
                        
                        grupos = saved_tgs["grupo"]

                        for grupo in grupos:
                            if grupo["id_grupo"] == tgs["id_grupo"]:

                                subgrupos = grupo["subgrupo"]

                                for subgrupo in subgrupos:
                                    if subgrupo["id_subgrupo"] == tgs["id_subgrupo"]:
                                        break

                                else:
                                    subgrupos.append({
                                        "id_subgrupo": tgs["id_subgrupo"],
                                        "descricao_subgrupo": tgs["descricao_subgrupo"],
                                    })

                                break

                        else:
                            grupos.append({
                                "id_grupo": tgs["id_grupo"],
                                "descricao_grupo": tgs["descricao_grupo"],
                                "subgrupo": [{
                                    "id_subgrupo": tgs["id_subgrupo"],
                                    "descricao_subgrupo": tgs["descricao_subgrupo"],
                                }]
                            })
                            
                        break


                else:
                    dados.append({
                        "id_tipo": tgs["id_tipo"],
                        "descricao_tipo": tgs["descricao_tipo"],
                        "grupo": [{
                            "id_grupo": tgs["id_grupo"],
                            "descricao_grupo": tgs["descricao_grupo"],
                            "subgrupo": [{
                                "id_subgrupo": tgs["id_subgrupo"],
                                "descricao_subgrupo": tgs["descricao_subgrupo"],
                                }]
                            }]
                    })
            
            if dados:
                    
                logger.info(f"Dados de tipo-grupo-subgrupo enviados.")
                return {"status":200,
                        "resposta":{"tipo":"1","descricao":f"Dados enviados."},"dados":dados}, 200

        logger.info(f"Dados de tipo-grupo-subgrupo nao encontrados")
        return {"status":404,
                "resposta":{
                    "tipo":"7",
                    "descricao":f"Sem dados para retornar."}}, 200