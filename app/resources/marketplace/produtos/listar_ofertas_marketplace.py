#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class ListarOfertasMarketplace(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self) -> dict:
        """
        Método POST do Listar Ofertas do Marketplace

        Returns:
            [dict]: Status da transação
        """

        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_oferta", "id_tipo", "id_grupo", "id_subgrupo", 
                            "paginar", "busca", "id_orcamento", "id_cliente", "id_produto",
                            "ordenar", "estoque", "multiplo_venda", "tipo_oferta", "marca", 
                            "grupos", "subgrupos", "desconto", "sku"]

        if id_usuario:
            necessary_keys.append("id_cliente")
        
        else:
            unnecessary_keys.append("id_cliente")
        
        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_oferta": [list, int],
            "sku": [list, int],
            "id_produto": [list, str],
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "paginar": bool,
            "id_orcamento": [list, int],
            "ordenar": int,
            "desconto": bool,
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
                                            no_use_columns = unnecessary_keys,
                                            not_null = necessary_keys,
                                            correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        id_distribuidor = int(response_data.get("id_distribuidor"))
        id_cliente = response_data.get("id_cliente") \
                        if response_data.get("id_cliente") else None
                        
        id_tipo = response_data.get("id_tipo")
        id_grupo = response_data.get("id_grupo")
        id_subgrupo = response_data.get("id_subgrupo")

        id_oferta = response_data.get("id_oferta")

        paginar = True if response_data.get("paginar") else False

        # Verificando o id_cliente enviado
        if id_usuario:

            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

        else:
            id_cliente = None
            id_usuario = None

        # Verificando possibilidade de procura por orçamento
        # if id_orcamento:

        #     if not id_usuario or not id_cliente:

        #         logger.error(f"Usuario tentando procura de oferta por orcamento sem credenciais.")
        #         return {"status":403,
        #                 "resposta":{
        #                     "tipo":"13",
        #                     "descricao":f"Ação recusada: Usuario tentando procura de oferta por orcamento sem credenciais."}}, 403

        #     if type(id_orcamento) is int:
        #         id_orcamento = [id_orcamento]

        #     elif type(id_orcamento) is str:
        #         id_orcamento = [int(id_orcamento)]

        # Verificando o paginar
        if response_data.get("paginar"):
            paginar = True

        elif id_oferta:
            paginar = True

        elif not (id_tipo or id_grupo or id_subgrupo):
            paginar = True

        elif ((id_tipo or id_grupo) and not id_subgrupo):
            paginar = True 

        if not paginar:
            pagina = 1
            limite = 999999           

        dict_get_oferta = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor,
            "pagina": pagina,
            "limite": limite,
            "busca": response_data.get("busca"),
            "id_tipo": id_tipo,
            "id_grupo": id_grupo,
            "id_subgrupo": id_subgrupo,
            "id_oferta": id_oferta,
            "id_produto": response_data.get("id_produto"),
            "sku": response_data.get("sku"),
            "ordenar": response_data.get("ordenar"),
            "desconto": response_data.get("desconto"),
            "estoque": response_data.get("estoque"),
            "multiplo_venda": response_data.get("multiplo_venda"),
            "tipo_oferta": response_data.get("tipo_oferta"),
            "marca": response_data.get("marca"),
            "grupos": response_data.get("grupos"),
            "subgrupos": response_data.get("subgrupos")
        }       

        ofertas_query = dj.json_ofertas(**dict_get_oferta)

        # Criando o json
        if not ofertas_query:
            logger.info(f"Dados de oferta nao encontrados.")
            return {"status":404,
                    "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200

        counter = ofertas_query.get("counter")
        ofertas = ofertas_query.get("ofertas")
        
        if ofertas:
            msg = {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."}}
            
            if paginar:
                msg['informacoes'] = {
                    "itens": counter,
                    "paginas": counter//limite + (counter % limite > 0),
                    "pagina_atual": pagina
                }

            msg["dados"] = ofertas

            logger.info("Dados de ofertas enviados.")
            return msg, 200

        logger.info(f"Dados de oferta nao encontrados.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200