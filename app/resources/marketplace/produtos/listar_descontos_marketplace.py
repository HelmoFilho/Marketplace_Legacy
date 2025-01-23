#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ListarDescontosMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Descontos do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor", "id_cliente"]
        unnecessary_keys = ["ordenar", "estoque", "multiplo_venda", "tipo_oferta", 
                                "marca", "grupos", "subgrupos"]
        
        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "ordenar": int,
            "estoque": bool,
            "multiplo_venda": int,
            "tipo_oferta": int,
            "marca": [list, int],
            "grupos": [list, int],
            "subgrupos": [list, int]
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
        id_cliente = int(response_data.get("id_cliente")) \
                        if response_data.get("id_cliente") else None

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        data = {
            "id_cliente": id_cliente,
            "id_usuario": id_usuario,
            "pagina": pagina,
            "limite": limite,
            "image_replace": "150"
        }

        dict_desconto = response_data | data

        data = dj.json_desconto(**dict_desconto)

        if data:

            produtos = data.get("produtos")
            counter = data.get("counter")

            logger.info("Produtos com desconto encontrados.")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."},
                    "informacoes": {
                        "itens": counter,
                        "paginas": counter//limite + (counter % limite > 0),
                        "pagina_atual": pagina},
                    "dados": produtos}, 200

        logger.info(f"Produtos com desconto nao encontrados para o usuario.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200