#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure
import functions.default_json as dj

class ListarUltimosVistosMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Registro dos ultimos vistos do Marketplace

        Returns:
            [dict]: Status da transação
        """

        id_usuario = int(global_info.get('id_usuario'))

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_cliente"]
        unnecessary_keys = ["id_distribuidor", "ordenar", "desconto", "estoque", "tipo_oferta", 
                            "marca", "grupos", "subgrupos"]

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

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = necessary_keys,
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        id_cliente = int(response_data["id_cliente"])

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        limite = int(os.environ.get("MAX_LIMITE_ULTIMOS_VISTOS_PS"))
        data = {
            "id_usuario": id_usuario, 
            "limite": limite,
            "image_replace": "150"
        }

        dict_ultimos_vistos = response_data | data

        data = dj.json_ultimos_vistos(**dict_ultimos_vistos)

        if data: 

            id_distribuidor = response_data.get("id_distribuidor")

            query = """
                SELECT
                    DISTINCT m.id_distribuidor,
                    m.id_marca,
                    m.desc_marca

                FROM
                    USUARIO_ULTIMOS_VISTOS uuv

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        uuv.id_usuario = :id_usuario
                        AND uuv.id_cliente = :id_cliente
                        AND uuv.id_produto = pd.id_produto
                        AND uuv.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto                    

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                WHERE
                    uuv.id_distribuidor IN (

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

                logger.info(f"Dados de ultimos vistos enviados para o front-end.")
                return {"status":200,
                        "resposta":{
                            "tipo":"1",
                            "descricao":f"Dados enviados."},
                        "dados":{
                            "produtos": data,
                            "marcas": marcas
                        }}, 200

        logger.info(f"Dados de ultimos vistos nao encontrados.")
        return {"status":404,
                "resposta":{"tipo": "7", "descricao": "Sem dados para retornar."}}, 200