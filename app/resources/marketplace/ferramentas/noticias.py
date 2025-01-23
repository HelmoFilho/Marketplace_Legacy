#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class Noticias(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self):
        """
        Método POST das Noticias do Marketplace

        Returns:
            [dict]: Status da transação
        """

        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = dm.get_request()

        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_cliente", "id_noticia"]

        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_noticia": int
        }

        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys,
                                            no_use_columns = unnecessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = int(response_data.get("id_cliente"))\
                        if response_data.get("id_cliente") and id_usuario else None

        id_noticia = int(response_data.get("id_noticia"))\
                        if response_data.get("id_noticia") else None

        if id_usuario:   
            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

        dict_noticias = {
            "id_cliente": id_cliente,
            "id_noticia": id_noticia,
            "id_distribuidor": id_distribuidor,
            "image_replace": "500",
            "pagina": pagina,
            "limite": limite,
        }

        noticias_query = dj.json_noticias(**dict_noticias)

        if not noticias_query:
            logger.info("Nenhuma noticia encontrada.")
            return {"status":404,
                    "resposta":{
                        "tipo":"7",
                        "descricao":f"Sem dados para retornar."}}, 200

        noticias = noticias_query.get("data")
        count = noticias_query.get("count")

        logger.info("Noticias encontradas.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Noticias encontradas."},
                "informacoes": {
                    "itens": count,
                    "paginas": (count // limite) + (count % limite > 0),
                    "pagina_atual": pagina
                },
                "dados": noticias}, 200