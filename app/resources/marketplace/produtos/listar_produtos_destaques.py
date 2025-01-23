#=== Importações de módulos externos ===#
from flask_restful import Resource
import itertools, os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

server = os.environ.get("PSERVER_PS").lower()

if server == "production":
    route = str(os.environ.get("SERVER_GUARANY_PS")).lower()
    image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
    
elif server == "development":
    route = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

else:
    route = f"http://localhost:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

class ListarProdutosDestaquesGrupo(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required = False)
    def post(self) -> dict:
        """
        Método POST do Listar Produtos Destaques dos grupos

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(trim_values = True)

        necessary_keys = ["id_distribuidor", "id_grupo"]
        unnecessary_keys = ["id_cliente"]

        correct_types = {
            "id_distribuidor": int,
            "id_grupo": int,
            "id_cliente": int
        }

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys,
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        id_usuario = global_info.get("id_usuario")
        id_cliente = response_data.get("id_cliente")

        id_distribuidor = int(response_data["id_distribuidor"])
        id_grupo = int(response_data["id_grupo"])

        if id_usuario:

            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

            if id_distribuidor != 0:

                query = """
                    SELECT
                        TOP 1 1

                    FROM
                        CLIENTE c

                        INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                            c.id_cliente = cd.id_cliente

                        INNER JOIN.DISTRIBUIDOR d ON
                            cd.id_distribuidor = d.id_distribuidor

                    WHERE
                        d.id_distribuidor = :id_distribuidor
                        AND c.id_cliente = :id_cliente
                        AND cd.data_aprovacao <= GETDATE()
                        AND c.status = 'A'
                        AND c.status_receita = 'A'
                        AND cd.status = 'A'
                        AND cd.d_e_l_e_t_ = 0
                        AND d.status = 'A'
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "id_cliente": id_cliente,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True,
                }

                response = dm.raw_sql_return(**dict_query)
                if not response:
                    logger.error("Cliente não pode ver produtos deste distribuidor")
                    return {"status": 400,
                            "resposta": {
                                "tipo": "13", 
                                "descricao": "Ação recusada: Cliente sem permissão de ver estes produtos."}}, 200


        query = """
            SELECT
                id_produto

            FROM
                PRODUTO_DESTAQUES

            WHERE
                id_grupo = :id_grupo
                AND id_distribuidor = :id_distribuidor
        """

        params = {
            "id_grupo": id_grupo,
            "id_distribuidor": id_distribuidor
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Sem produtos destaques para grupo {id_grupo}")
            return {"status": 404,
                    "resposta": {
                        "tipo": "7", 
                        "descricao": "Sem dados para retornar."}}, 200

        produtos = list(itertools.chain(*response))

        query = """
            SELECT
                DISTINCT
                    p.id_produto,
                    UPPER(p.descricao_completa) as descricao_produto,
                    p.sku,
                    pd.id_distribuidor,
                    p.tokens_imagem as tokens

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

                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca
                    AND pd.id_distribuidor = m.id_distribuidor

                INNER JOIN FORNECEDOR f ON
                    pd.id_fornecedor = f.id_fornecedor

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
                p.id_produto IN :produtos
                AND g.id_grupo = :id_grupo
                AND ps.id_distribuidor = :id_distribuidor
                AND LEN(p.tokens_imagem) > 0
                AND tp.tabela_padrao = 'S'
                AND tp.dt_inicio <= GETDATE()
                AND tp.dt_fim >= GETDATE()
                AND tp.status = 'A'
                AND ps.status = 'A'
                AND s.status = 'A'
                AND gs.status = 'A'
                AND g.status = 'A'
                AND t.status = 'A'
                AND m.status = 'A'
                AND f.status = 'A'
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "id_grupo": id_grupo,
            "produtos": produtos
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Informações dos produtos destaques do grupo {id_grupo} não encontradas")
            return {"status": 404,
                    "resposta": {
                        "tipo": "7", 
                        "descricao": "Sem dados para retornar."}}, 200
        
        dados = response
        for produto in dados:

            imagens_list = []

            imagens = produto.pop("tokens")
            if imagens:

                imagens = imagens.split(",")
                imagens_list = [
                    f"{image}/imagens/{imagem}".replace("/auto/", f"/150/")
                    for imagem in imagens
                    if imagem
                ]

                if imagens_list:
                    produto["imagens"] = imagens_list

        logger.info(f"Informações dos produtos destaques do grupo {id_grupo} enviadas")
        return {"status": 200,
                "resposta": {
                    "tipo": "1", 
                    "descricao": "Produtos destaques enviados."},
                "dados": dados}, 200