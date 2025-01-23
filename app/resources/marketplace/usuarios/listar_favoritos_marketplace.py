#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ListarFavoritosMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Registro dos Favoritos do Marketplace

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(trim_values = True)

        necessary_keys = ["id_distribuidor", "id_cliente"]
        unnecessary_keys = ["ordenar", "desconto", "estoque", "tipo_oferta", "marca",
                            "grupos", "subgrupos"]

        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "ordenar": int,
            "desconto": bool,
            "estoque": bool,
            "tipo_oferta": int,
            "marca": [list, int],
            "grupos": [list, int],
            "subgrupos": [list, int],
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = necessary_keys,
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_usuario = global_info.get("id_usuario")
        id_cliente = int(response_data["id_cliente"])
        id_distribuidor = response_data.get("id_distribuidor")

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        data = {
            "id_usuario": id_usuario, 
            "pagina": pagina,
            "limite": limite,
            "image_replace": "150"
        }

        dict_favoritos = response_data | data

        favorites = dj.json_favoritos(**dict_favoritos)                    
        
        if favorites:

            query = """
                SELECT
                    DISTINCT m.id_distribuidor,
                    m.id_marca,
                    m.desc_marca

                FROM
                    USUARIO_FAVORITO uf
                    
                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        uf.id_usuario = :id_usuario
                        AND uf.id_produto = pd.id_produto
                        AND uf.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                WHERE
                    uf.id_distribuidor IN (

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
                                                                                WHEN ISNULL(:id_distribuidor, 0) = 0
                                                                                    THEN d.id_distribuidor
                                                                                ELSE
                                                                                    :id_distribuidor
                                                                            END
                                                    AND c.status = 'A'
                                                    AND c.status_receita = 'A'
                                                    AND cd.status = 'A'
                                                    AND cd.d_e_l_e_t_ = 0
                                                    AND d.status = 'A'     

                                          )
            """

            params = {
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            marcas = dm.raw_sql_return(**dict_query)

            if marcas:

                counter = favorites.get("counter")
                data = favorites.get("produtos")

                logger.info(f"Dados dos favoritos enviados para o usuario.")
                return {"status":200,
                        "resposta":{
                            "tipo":"1",
                            "descricao":f"Dados enviados."},
                        "informacoes": {
                            "itens": counter,
                            "paginas": counter//limite + (counter % limite > 0),
                            "pagina_atual": pagina},
                        "dados":{
                            "produtos": data,
                            "marcas": marcas
                        }}, 200

        logger.info(f"Dados de favoritos nao encontrados para o usuario.")
        return {"status":404,
                "resposta":{"tipo": "7", "descricao": "Sem dados para retornar."}}, 200