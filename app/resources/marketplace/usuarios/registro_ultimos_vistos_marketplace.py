#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroUltimosVistosMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Registro dos ultimos vistos do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["id_produto", "id_cliente"]

        correct_types = {"id_cliente": int}

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity

        id_usuario = global_info.get("id_usuario")

        id_produto = response_data.get("id_produto")
        id_cliente = int(response_data.get("id_cliente"))

        answer, response = dm.cliente_check(id_cliente)

        if not answer:
            return response
        
        # Pegando distribuiodres do cliente
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
                AND d.id_distribuidor != 0
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'
        """

        params = {
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error("Sem distribuidores cadastrados válidos para a operação.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Sem distribuidores cadastrados."}}, 200
        
        distribuidores = [id_distribuidor[0] for id_distribuidor in response]

        # Deletando ultimos vistos sem estoque
        query = """
            DELETE FROM
                uuv

            FROM
                USUARIO_ULTIMOS_VISTOS uuv

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    uuv.id_produto = pd.id_produto
                    AND uuv.id_distribuidor = pd.id_distribuidor

            WHERE
                id_usuario = :id_usuario
                AND id_cliente = :id_cliente
                AND pd.estoque <= 0
        """

        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        # Checando existência do produto
        query = """
            SELECT
                pd.id_distribuidor,
                pd.estoque

            FROM
                PRODUTO p

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto

            WHERE
                p.id_produto = :id_produto
                AND pd.id_distribuidor IN :distribuidores
                AND p.status = 'A'
                AND pd.status = 'A'
        """

        params = {
            "id_produto": id_produto,
            "distribuidores": distribuidores,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
            "first": True
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.error(f"Produto {id_produto} não-existente.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Produto não-existente."}}, 200
        
        id_distribuidor = response[0]
        estoque = response[1]

        # Checando a validade do produto
        if estoque <= 0:
            logger.error(f"Produto {id_produto} sem estoque.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Produto sem estoque."}}, 200

        # query = """
        #     SELECT
        #         TOP 1 1

        #     FROM
        #         PRODUTO p

        #         INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
        #             p.id_produto = pd.id_produto

        #         INNER JOIN TABELA_PRECO_PRODUTO tpp ON
        #             pd.id_produto = tpp.id_produto
        #             AND pd.id_distribuidor = tpp.id_distribuidor

        #         INNER JOIN TABELA_PRECO tp ON
        #             tpp.id_tabela_preco = tp.id_tabela_preco
        #             AND tpp.id_distribuidor = tp.id_distribuidor

        #         INNER JOIN PRODUTO_SUBGRUPO ps ON
        #             pd.id_produto = ps.codigo_produto

        #         INNER JOIN SUBGRUPO s ON
        #             ps.id_subgrupo = s.id_subgrupo
        #             AND ps.id_distribuidor = s.id_distribuidor

        #         INNER JOIN GRUPO_SUBGRUPO gs ON
        #             s.id_subgrupo = gs.id_subgrupo
        #             AND s.id_distribuidor = gs.id_distribuidor

        #         INNER JOIN GRUPO g ON
        #             gs.id_grupo = g.id_grupo

        #         INNER JOIN TIPO t ON
        #             g.tipo_pai = t.id_tipo

        #         INNER JOIN MARCA m ON
        #             pd.id_marca = m.id_marca
        #             AND pd.id_distribuidor = m.id_distribuidor

        #         INNER JOIN FORNECEDOR f ON
        #             pd.id_fornecedor = f.id_fornecedor

        #     WHERE
        #         p.id_produto = :id_produto
        #         AND pd.id_distribuidor = :id_distribuidor
        #         AND (
        #                 (
        #                     tp.tabela_padrao = 'S'
        #                 )
        #                     OR
        #                 (
        #                     tp.id_tabela_preco IN (
        #                                                 SELECT
        #                                                     id_tabela_preco

        #                                                 FROM
        #                                                     TABELA_PRECO_CLIENTE

        #                                                 WHERE
        #                                                     id_distribuidor = :id_distribuidor
        #                                                     AND id_cliente = :id_cliente
        #                                             )
        #                 )
        #             )
        #         AND tp.dt_inicio <= GETDATE()
        #         AND tp.dt_fim >= GETDATE()
        #         AND tp.status = 'A'
        #         AND p.status = 'A'
        #         AND pd.status = 'A'
        #         AND ps.status = 'A'
        #         AND s.status = 'A'
        #         AND gs.status = 'A'
        #         AND g.status = 'A'
        #         AND t.status = 'A'
        #         AND m.status = 'A'
        #         AND f.status = 'A'
        # """

        # params = {
        #     "id_produto": id_produto,
        #     "id_distribuidor": id_distribuidor,
        #     "id_cliente": id_cliente,
        # }

        # dict_query = {
        #     "query": query,
        #     "params": params,
        #     "raw": True,
        #     "first": True,
        # }

        # response = dm.raw_sql_return(**dict_query)
        # if not response:
        #     logger.error(f"Produto {id_produto} não-valido")
        #     return {"status": 400,
        #             "resposta": {
        #                 "tipo": "13", 
        #                 "descricao": "Ação recusada: Produto não-valido."}}, 200

        # Verificando se o produto já não esta salvo como ultimo visto
        query = """
            SELECT
                1

            FROM
                USUARIO_ULTIMOS_VISTOS

            WHERE
                id_usuario = :id_usuario
                AND id_cliente = :id_cliente
                AND id_produto = :id_produto
        """

        params = {
            "id_produto": id_produto,
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if response:
            
            query = """
                UPDATE
                    USUARIO_ULTIMOS_VISTOS

                SET
                    data_cadastro = GETDATE()

                WHERE
                    id_usuario = :id_usuario
                    AND id_cliente = :id_cliente
                    AND id_produto = :id_produto
            """

            params = {
                "id_produto": id_produto,
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            dm.raw_sql_execute(**dict_query)

            logger.info(f"Usuario atualizou Produto {id_produto} nos ultimos vistos.")
            return {"status":201,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Registro do ultimo visto atualizado."}}, 200
        
        # Deletando ultimos vistos mais antigos
        max_ultimos_vistos = int(os.environ.get("MAX_LIMITE_ULTIMOS_VISTOS_PS"))

        query = """
            DELETE FROM
                USUARIO_ULTIMOS_VISTOS

            WHERE
                id_usuario = :id_usuario
                AND id_cliente = :id_cliente
                AND (

                        id_produto IN (
                                            SELECT
                                                id_produto

                                            FROM
                                                (
                
                                                    SELECT
                                                        ROW_NUMBER() OVER(ORDER BY data_cadastro DESC, id_produto) ordem, 
                                                        id_produto

                                                    FROM
                                                        USUARIO_ULTIMOS_VISTOS 

                                                    WHERE
                                                        id_usuario = :id_usuario
                                                        AND id_cliente = :id_cliente
                
                                                ) uuv

                                            WHERE
                                                ordem >= :max_ultimos_vistos
                                      )
                            OR
                        id_produto IN (SELECT id_produto FROM PRODUTO_DISTRIBUIDOR WHERE estoque <= 0)
                    )
        """

        params = {
            "max_ultimos_vistos": max_ultimos_vistos,
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        # Salvando o ultimo visto
        insert_data = {
            "id_produto": id_produto,
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor,
        }

        dm.raw_sql_insert("USUARIO_ULTIMOS_VISTOS", insert_data)
        
        logger.info(f"Usuario adicionou P:{id_produto} nos ultimos vistos.")
        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Cadastro do ultimo visto concluido."}}, 201   