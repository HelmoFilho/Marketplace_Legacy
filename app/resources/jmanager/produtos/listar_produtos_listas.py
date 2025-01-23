#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarProdutosListas(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Produtos das listas

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["status", 'objeto', "id_distribuidor", "data_cadastro", "id_lista"]

        correct_types = {"id_distribuidor": int, "id_lista": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, bool_missing = False, 
                                correct_types = correct_types,
                                not_null = ["id_distribuidor", "id_lista"],
                                comparison_columns = produto_columns)):
            return validity

        # Cuida do limite de informações mandadas e da pagina atual
        pagina, limite = dm.page_limit_handler(response_data)

        # Tratamento do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        # Query da lista
        id_lista = int(response_data.get("id_lista"))

        lista_query = f"""
            SELECT
                TOP 1 id_lista
            
            FROM 
                LISTA_PRODUTOS
            
            WHERE
                id_lista = :id_lista
                AND id_distribuidor = CASE
                                          WHEN :id_distribuidor = 0
                                              THEN id_distribuidor
                                          ELSE
                                              :id_distribuidor
                                      END
                AND d_e_l_e_t_ = 0
                AND status = 'A'
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "id_lista": id_lista
        }

        dict_query = {
            "query": lista_query,
            "params": params,
            "first": True,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Lista nao existe para distribuidor informado.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Lista não existe para distribuidor informado."}
                    }, 200

        # Montando a query
        params = {
            "id_lista": id_lista,
            "offset": (pagina - 1) * limite,
            "limite": limite,
            "id_distribuidor": id_distribuidor
        }

        string_date_1 = ""
        string_date_2 = ""
        string_busca = ""

        # Tratamento da data
        data_cadastro = response_data.get("data_cadastro")

        if data_cadastro:

            if len(data_cadastro) <= 0:
                date_1, date_2 = None, None

            elif len(data_cadastro) == 1:
                date_1, date_2 = data_cadastro[0], None
            
            else:
                date_1, date_2 = data_cadastro[0], data_cadastro[1]
                
            if date_1:
                
                try:
                    date_1 = datetime.strptime(date_1, '%Y-%m-%d')
                    date_1 = date_1.strftime('%Y-%m-%d')
                    if date_1 < "1900-01-01":
                        date_1 = "1900-01-01"

                    params["date_1"] = date_1
                    string_date_1 = "AND lp.data_criacao >= :date_1"

                except:
                    pass

            if date_2:

                try:
                    date_2 = datetime.strptime(date_2, '%Y-%m-%d')
                    date_2 = date_2.strftime('%Y-%m-%d')
                    if date_2 > "3000-01-01":
                        date_2 = "3000-01-01"

                    params["date_2"] = date_2
                    string_date_2 = "AND lp.data_criacao <= :date_2"

                except:
                    pass

        # Tratamento do status
        status = str(response_data.get("status")).upper() if response_data.get("status") else "A"

        if status != 'I':
            status = 'A'

        params["status"] = status

        # Tratamento do objeto
        busca = response_data.get('objeto')

        if busca:

            busca = str(busca).upper().split()

            string_busca = ""

            for index, word in enumerate(busca):

                params.update({
                    f"word_{index}": f"%{word}%"
                })

                string_busca += f"""AND (
                    p.descricao_completa COLLATE Latin1_General_CI_AI LIKE :word_{index}
                    OR p.sku LIKE :word_{index}
                    OR p.id_produto LIKE :word_{index}
                )"""

        query = f"""
            SELECT
                p.id_produto,
                p.sku,
                p.descricao_completa as descricao_produto,
                pd.cod_prod_distr,
                pd.agrupamento_variante,
                SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
                :status status,
                COUNT(*) OVER() as count__

            FROM
                (

                    SELECT
                        DISTINCT
                            lpr.codigo_produto as id_produto

                    FROM
                        LISTA_PRODUTOS lp
                                    
                        INNER JOIN LISTA_PRODUTOS_RELACAO lpr ON
                            lp.id_lista = lpr.id_lista

                    WHERE
                        lp.id_lista = :id_lista
                        AND lp.id_distribuidor = CASE
                                                     WHEN :id_distribuidor = 0
                                                         THEN lp.id_distribuidor
                                                     ELSE
                                                         :id_distribuidor
                                                 END
                        {string_date_1}
                        {string_date_2}
                        AND lp.status = :status
                        AND lpr.status = :status
                        AND lp.d_e_l_e_t_ = 0

                ) lp

                INNER JOIN PRODUTO p ON
                    lp.id_produto = p.id_produto

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto

            WHERE
                LEN(p.sku) > 0
                AND pd.id_distribuidor = CASE
                                             WHEN :id_distribuidor = 0
                                                 THEN pd.id_distribuidor
                                             ELSE
                                                 :id_distribuidor
                                         END
                {string_busca}
                AND p.status = 'A'
                AND pd.status = 'A'
        """

        dict_query = {
            "query": query,
            "params": params,
        }

        data = dm.raw_sql_return(**dict_query)

        if data:

            maximum_all = int(data[0].get("count__"))
            maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

            for query in data:
                del query["count__"]

            logger.info(f"Dados do produto foram enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados encontrados."},
                    "informacoes": {
                        "itens": maximum_all,
                        "paginas": maximum_pages,
                        "pagina_atual": pagina}, 
                    "dados": {
                        "id_lista": id_lista,
                        "id_distribuidor": id_distribuidor,
                        "produtos": data
                    }}, 200

        logger.info(f"Dados dos produtos da lista {id_lista} nao foram encontrados.")
        return {"status":400,
                "resposta":{
                    "tipo":"6",
                    "descricao":f"Dados não encontrados para estes filtros."}}, 200