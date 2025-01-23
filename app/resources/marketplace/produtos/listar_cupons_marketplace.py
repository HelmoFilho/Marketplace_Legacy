#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure
from app.server import logger

class ListarCuponsMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Cupons do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor", "id_cliente"]

        unnecessary_keys = ["id_cupom"]
        
        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_cupom": [list, int]
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            no_use_columns = unnecessary_keys,
                                            not_null = necessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = int(response_data.get("id_cliente"))
        id_cupom = response_data.get("id_cupom")

        # Verificando o id_cliente enviado
        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        dict_cupons = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor,
            "id_cupom": id_cupom,
            "pagina": pagina,
            "limite": limite
        }

        data = dj.json_cupons(**dict_cupons)

        if not data:
            logger.info('Nenhum cupom encontrado para o usuario informado.')
            return {"status":404,
                    "resposta":{
                        "tipo": "7",
                        "descricao": f"Sem dados para retornar."}}, 200

        counter = data.get("counter")
        cupons = data.get("cupons")

        maximum_pages = counter//limite + (counter % limite > 0)

        logger.info("Cupons enviados para o usuario.")
        return {"status":200,
                "resposta":{"tipo":"1","descricao":f"Dados enviados."},
                "informacoes": {
                    "itens": counter,
                    "paginas": maximum_pages,
                    "pagina_atual": pagina},
                "dados":cupons}, 200