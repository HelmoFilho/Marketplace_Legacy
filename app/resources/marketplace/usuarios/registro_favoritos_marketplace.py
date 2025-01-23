#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroFavoritosMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Registro dos Favoritos do Marketplace

        Returns:
            [dict]: Status da transação
        """
        id_usuario = int(global_info.get('id_usuario'))

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["id_produto", "id_cliente"]

        not_null = ["id_cliente"]

        correct_types = {"id_cliente": int}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys, 
                                not_null = not_null,
                                correct_types = correct_types)):
            
            return validity

        # Verificando o id_cliente enviado
        id_cliente = int(response_data.get("id_cliente"))

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        # Pegando os produtos
        id_produto = response_data.get("id_produto")

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

        # Checando existência do produto
        query = """
            SELECT
                pd.id_distribuidor,
                p.sku

            FROM
                PRODUTO p

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto

            WHERE
                p.id_produto = :id_produto
                AND pd.id_distribuidor IN :distribuidores
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
        sku = response[1]

        # Checando a validade do produto
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

        # Verificando se o produto já não esta salvo como favorito
        query = """
            SELECT
                1

            FROM
                USUARIO_FAVORITO

            WHERE
                id_usuario = :id_usuario
                AND id_produto = :id_produto
        """

        params = {
            "id_produto": id_produto,
            "id_usuario": id_usuario,
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)
        if response:
            logger.error(f"Produto {id_produto} já faz parte dos favoritos do usuario.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Produto já faz parte dos favoritos do usuario."}}, 200
        
        # 0realizando a inserção

        insert_data = {
            "id_usuario": id_usuario,
            "id_produto": id_produto,
            "id_distribuidor": id_distribuidor,
            "sku": sku,
        }

        dm.raw_sql_insert("USUARIO_FAVORITO", insert_data)

        logger.info(f"Produto {id_produto} salvo nos favoritos do usuario")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Favoritos modificados com sucesso"
                }}, 200