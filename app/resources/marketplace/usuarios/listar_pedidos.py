#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.payment_management as pm
import functions.data_management as dm
import functions.security as secure

class ListarPedidos(Resource):
      
    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Listar pedidos

        Returns:
            [dict]: Status da transação
        """
        id_usuario = global_info.get("id_usuario")

        response_data = dm.get_request()

        necessary_columns = ["id_cliente"]
        no_use_columns = ["id_distribuidor", "data_pedido_de", "data_pedido_ate", "etapa", "busca",
                          "id_pedido"]

        correct_types = {
            "id_cliente": int,
            "id_pedido": int,
            "id_distribuidor": [list, int],
            "etapa": int
        }

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          no_use_columns = no_use_columns,
                                          not_null = necessary_columns,
                                          correct_types = correct_types)):
            return validity

        # Verificando o id_cliente utilizado
        id_cliente = int(response_data.get("id_cliente"))
        
        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        pagina, limite = dm.page_limit_handler(response_data)
        global_info.save_info_thread(id_cliente = id_cliente)

        # Pegando os distribuidores dos pedidos do cliente
        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        bindparams = []

        string_id_distribuidor = ""
        string_data_pedido_de = ""
        string_data_pedido_ate = ""
        string_etapa = ""
        string_busca = ""
        string_id_pedido = ""

        ## Filtro do distribuidor
        id_distribuidor = response_data.get("id_distribuidor")

        if id_distribuidor:

            if type(id_distribuidor) not in [list, tuple, set]:
                id_distribuidor = [id_distribuidor]

            id_distribuidor = list(id_distribuidor)
            string_id_distribuidor = "AND pdd.id_distribuidor IN :distribuidores"
            
            bindparams.append("distribuidores")
            params["distribuidores"] = id_distribuidor

        ## Filtro por data de criacao
        data_pedido_de = response_data.get("data_pedido_de")

        if data_pedido_de:
            try:
                pattern = "%Y-%m-%d"
                datetime.datetime.strptime(data_pedido_de, pattern)
            
            except:
                logger.error("Variavel data_pedido_de com valor invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"4",
                            "descricao":"Campos ['data_pedido_de'] com valores de tipos incorretos."}}, 400

            string_data_pedido_de = "AND pdd.data_criacao >= :data_pedido_de"
            params["data_pedido_de"] = data_pedido_de

        data_pedido_ate = response_data.get("data_pedido_ate")

        if data_pedido_ate:
            try:
                pattern = "%Y-%m-%d"
                datetime.datetime.strptime(data_pedido_ate, pattern)
            
            except:
                logger.error("Variavel data_pedido_ate com valor invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"4",
                            "descricao":"Campos ['data_pedido_ate'] com valores de tipos incorretos."}}, 400

            string_data_pedido_ate = "AND pdd.data_criacao <= :data_pedido_ate"
            params["data_pedido_ate"] = data_pedido_ate

        ## Filtro por campo de busca
        busca = response_data.get("busca")

        if busca:

            busca = str(busca).upper().split()

            string_busca = ""

            for index, word in enumerate(busca):

                params.update({
                    f"word_{index}": f"{word}"
                })

                string_busca += f"""AND (
                    pdd.id_pedido = :word_{index}
                )"""

        ## Filtro de etapa
        etapa = response_data.get("etapa")

        if etapa:

            string_etapa = "AND pddetp.id_etapa >= :etapa"
            params["etapa"] = int(etapa)

        ## Filtro de id_pedido
        id_pedido = response_data.get("id_pedido")

        if id_pedido:

            string_id_pedido = "AND pdd.id_pedido = :id_pedido"
            params["id_pedido"] = int(id_pedido)

        # Realizando a query de pedido
        pedido_query = f"""
            SELECT
                p.id_pedido,
                COUNT(*) OVER() count__

            FROM
                (

                    SELECT
                        DISTINCT pdd.id_pedido,
                        pdd.data_criacao

                    FROM
                        PEDIDO pdd

                        INNER JOIN PEDIDO_ETAPA pddetp ON
                            pdd.id_pedido = pddetp.id_pedido
                            AND pdd.id_distribuidor = pddetp.id_distribuidor
                            AND pdd.id_usuario = pddetp.id_usuario
                            AND pdd.id_cliente = pddetp.id_cliente

                    WHERE
                        pdd.id_usuario = :id_usuario
                        AND pdd.id_cliente = :id_cliente
                        AND pdd.data_criacao >= DATEADD(MONTH, -12, GETDATE())
                        {string_id_pedido}
                        {string_id_distribuidor}
                        {string_data_pedido_de}
                        {string_data_pedido_ate}
                        {string_busca}
                        {string_etapa}
                        AND d_e_l_e_t_ = 0

                ) p

            ORDER BY
                p.data_criacao DESC

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY;
        """

        dict_query = {
            "query": pedido_query,
            "params": params,
            "bindparams": bindparams,
            "raw": True,
        }

        pedido_query = dm.raw_sql_return(**dict_query)

        if not pedido_query:
            logger.info(f"Para os filtros informados, nao existem pedidos salvos.")
            return {"status":404,
                    "resposta":{
                        "tipo": "7", 
                        "descricao": "Sem dados para retornar."}}, 200

        count = pedido_query[0][1]

        informacoes = {
            "itens": int(count),
            "paginas": count//limite + (count % limite > 0),
            "pagina_atual": pagina
        }

        list_id_pedido = [pedido[0]   for pedido in pedido_query]

        json_data = []

        for id_pedido in list_id_pedido:
            
            dict_pedido = {
                "id_pedido": id_pedido,
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
            }

            pedido = pm.pegar_pedido(**dict_pedido)
            if pedido:
                json_data.append(pedido)

        logger.info(f"Existem {len(json_data)} pedidos encontrados.")
        return {"status": 200,
                "resposta":{
                    "tipo": "1", 
                    "descricao": "Pedidos encontrados."},
                "informacoes": informacoes,
                "dados": json_data}, 200