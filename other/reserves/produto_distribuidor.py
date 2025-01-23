#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.file_management as fm
import functions.data_management as dm
import functions.security as secure


class ProdutosDistribuidor(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Produtos do Distribuidor

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get_info_thread().get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys=True, trim_values=True)

        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_distribuidor"]
        
        no_use = ["id_subgrupo", "id_grupo", "id_tipo", "status", 'objeto', "data_cadastro"]

        for i in no_use:
            if not response_data.get(i):
                response_data.pop(i, None)

        correct_types = {
            "id_distribuidor": int,
            "id_subgrupo": int,
            "id_grupo": int,
            "id_tipo": int,
            "data_cadastro": list
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response=response_data,  
                                          comparison_columns = produto_columns,
                                          no_use_columns = no_use, 
                                          not_null = produto_columns,
                                          correct_types = correct_types)):
            return validity

        # Cuida do limite de informações mandadas e da pagina atual
        pagina, limite = dm.page_limit_handler(response_data)

        string_date_1 = ""
        string_date_2 = ""
        string_busca = ""
        string_tipo = ""
        string_grupo = ""
        string_subgrupo = ""

        params = {
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        bindparams = []

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
                    string_date_1 = "AND pd.data_cadastro >= :date_1"

                except:
                    pass

            if date_2:

                try:
                    date_2 = datetime.strptime(date_2, '%Y-%m-%d')
                    date_2 = date_2.strftime('%Y-%m-%d')
                    if date_2 > "3000-01-01":
                        date_2 = "3000-01-01"

                    params["date_2"] = date_2
                    string_date_2 = "AND pd.data_cadastro <= :date_2"

                except:
                    pass

        # Tratamento do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        params["id_distribuidor"] = id_distribuidor

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
                    OR p.dun14 LIKE :word_{index}
                )"""

        # Tratamento de tipo/grupo/subgrupo
        id_tipo = int(response_data.get('id_tipo')) if response_data.get('id_tipo') else None
        id_grupo = int(response_data.get('id_grupo')) if response_data.get('id_grupo') else None
        id_subgrupo = int(response_data.get('id_subgrupo')) if response_data.get('id_subgrupo') else None
        
        if id_tipo or id_grupo or id_subgrupo:

            if id_subgrupo:
                string_tipo = f"AND s.id_subgrupo = :id_subgrupo "
                params["id_subgrupo"] = id_subgrupo

            if id_grupo:
                string_grupo = "AND g.id_grupo = :id_grupo "
                params["id_grupo"] = id_grupo

            if id_tipo:
                string_subgrupo = "AND t.id_tipo = :id_tipo "
                params["id_tipo"] = id_tipo

        # Tratamento de status
        status = str(response_data.get("status")).upper()

        if status in {"I", "False"}:
            status = "I"

        elif status != 'A':
            status = None

        params["status"] = status

        # Realizando as queries nas tabelas
        produto_query = f"""
        -- Vendo o resultado
        SELECT
            pd.id_distribuidor,
            d.nome_fantasia as nome_distribuidor,
            pd.id_produto,
            pd.agrupamento_variante,
            pd.cod_prod_distr,
            SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
            pd.multiplo_venda,
            pd.ranking,
            pd.unidade_venda,
            pd.quant_unid_venda,
            pd.giro,
            pd.agrup_familia,
            pd.status,
            p.descricao_completa as descr_completa_distr,
            p.sku,    
            pe.qtd_estoque as quantidade_estoque,
            p.volumetria,
            p.variante,
            p.unidade_embalagem,
            p.quantidade_embalagem,
            p.dun14,
            p.tipo_produto,
            t.id_tipo as cod_tipo,
            t.descricao as descr_tipo,
            g.id_grupo as cod_grupo,
            g.descricao as descr_grupo,
            s.id_subgrupo as cod_subgrupo,
            s.descricao as descr_subgrupo,
            m.id_marca as cod_marca,
            m.desc_marca as descr_marca,
            f.id_fornecedor as cod_fornecedor,
            f.desc_fornecedor as descri_fornecedor,
            pd.data_cadastro,
            COUNT(*) OVER() AS 'count__'

        FROM
            PRODUTO p
                        
            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                p.id_produto = pd.id_produto

            INNER JOIN MARCA m ON
                pd.id_marca = m.id_marca

            INNER JOIN FORNECEDOR f ON
                pd.id_fornecedor = f.id_fornecedor

            LEFT JOIN PRODUTO_ESTOQUE pe ON
                p.id_produto = pe.id_produto

            INNER JOIN DISTRIBUIDOR d ON
                pd.id_distribuidor = d.id_distribuidor

            LEFT JOIN SUBGRUPO s ON
                pd.id_subgrupo = s.id_subgrupo
                AND pd.id_distribuidor = s.id_distribuidor
                
            LEFT JOIN GRUPO g ON
                pd.id_grupo = g.id_grupo

            LEFT JOIN TIPO t ON
                g.tipo_pai = t.id_tipo

        WHERE
            pd.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN pd.id_distribuidor ELSE :id_distribuidor END 
            {string_busca}
            {string_tipo}
            {string_grupo}
            {string_subgrupo}
            {string_date_1}
            {string_date_2}
            AND pd.status = CASE
                                WHEN :status IS NOT NULL
                                    THEN :status
                                ELSE
                                    pd.status
                            END
            AND p.status = CASE
                            WHEN :status IS NOT NULL
                                THEN :status
                            ELSE
                                p.status
                        END
            AND LEN(p.sku) > 0

        ORDER BY
            d.id_distribuidor ASC,
            CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ASC,
            p.data_cadastro ASC

        OFFSET
            :offset ROWS

        FETCH
            NEXT :limite ROWS ONLY;
        """

        dict_query = {
            "query": produto_query,
            "params": params,
            "bindparams": bindparams
        }

        data = dm.raw_sql_return(**dict_query)

        if data:

            maximum_all = int(data[0].get("count__"))
            maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

            for row in data:
                del row["count__"]
                row["imagem"] = fm.product_image_url(row.get("sku"))

            logger.info(f"Dados do produto foram enviados.")
            return {"status": 200,
                    "resposta": {
                        "tipo": "1",
                        "descricao": f"Dados encontrados."},
                    "informacoes": {
                        "itens": maximum_all,
                        "paginas": maximum_pages,
                        "pagina_atual": pagina},
                    "dados": data}, 200

        logger.info(f"Dados do produto nao foram encontrados.")
        return {"status": 200,
                "resposta": {
                    "tipo": "6",
                    "descricao": f"Dados não encontrados para estes filtros."}}, 200


    @logger
    @secure.auth_wrapper(permission_range=[1])
    def put(self) -> dict:
        """
        Método PUT do Editar Produtos do Distribuidor

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get_info_thread().get("token_info")

        usuario = int(info.get("id_usuario"))

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys=True, trim_values=True)

        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["id_distribuidor", "id_produto", "cod_prod_distr", 
            "descr_completa_distr", "dun14", "status", "tipo_produto", "cod_marca",
            "variante", "multiplo_venda", "unidade_venda", "quant_unid_venda", 
            "unidade_embalagem", "quantidade_embalagem", "ranking", "giro",
            "agrup_familia", "volumetria", "cod_fornecedor"]
        
        not_null = ["id_distribuidor", "id_produto", "cod_prod_distr"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response=response_data,
                                        not_null = not_null,
                                        correct_types = correct_types,
                                        comparison_columns=produto_columns)):
            return validity
        
        # Verificando informações de entrada
        id_produto = str(response_data.get("id_produto"))
        codigo_produto = str(response_data.get("cod_prod_distr"))

        # Verificando o id do distribuidor
        if int(info.get("id_perfil")) != 1:
            if int(response_data["id_distribuidor"]) not in info.get("id_distribuidor"):
                logger.error(f"Usuario {usuario} tentando editar produto do distribuidor {id_distribuidor}")
                return {"status": 403,
                        "resposta": {
                            "tipo": "13",
                            "descricao": f"Ação recusada: Produto de distribuidor diferente dos permitido."}}, 200

            else:
                response_data["id_distribuidor"] = [int(response_data["id_distribuidor"])]
        
        else:
            response_data["id_distribuidor"] = [response_data.get("id_distribuidor")]

        id_distribuidor = list(response_data.get("id_distribuidor"))
        
        answer = dm.distribuidor_check(int(id_distribuidor))
        if not answer[0]:
            return answer[1]
                
        # Verificando a existência do produto para o distribuidor
        existence_query = f"""
            SELECT
                TOP 1 id_produto
            
            FROM
                PRODUTO_DISTRIBUIDOR
            
            WHERE
                id_distribuidor = :id_distribuidor
                AND id_produto = :id_produto
                AND cod_prod_distr = :codigo_produto
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "id_produto": id_produto,
            "codigo_produto": codigo_produto
        }

        existence_query = dm.raw_sql_return(existence_query, params = params, raw = True, first = True)
        
        if not existence_query:
            logger.error(f"Produto especificado nao existe")
            return {"status": 409,
                    "resposta": {
                        "tipo": "5",
                        "descricao": f"Produto do distribuidor não existe."}}, 200
        
        changes = {
            key:  response_data.get(key)  if bool(response_data.get(key))  else None
            for key in produto_columns
        }

        # Variaveis
        descr_completa_distr = changes.get("descr_completa_distr")
        tipo_produto = changes.get("tipo_produto")
        dun14 = changes.get("dun14")
        status = str(changes.get("status")).upper() \
                    if str(changes.get("status")).upper() in {"I", "A"} else None
        cod_marca = changes.get("cod_marca")
        variante = changes.get("variante")
        multiplo_venda = changes.get("multiplo_venda")
        unidade_venda = changes.get("unidade_venda")
        quant_unid_venda = changes.get("quant_unid_venda")
        unidade_embalagem = changes.get("unidade_embalagem")
        quantidade_embalagem = changes.get("quantidade_embalagem")
        ranking = changes.get("ranking")
        giro = changes.get("giro")
        agrup_familia = changes.get("agrup_familia")
        volumetria = changes.get("volumetria")
        cod_fornecedor = changes.get("cod_fornecedor")

        # Update na tabela produto
        params_p = {
            "id_produto": id_produto,
            "cod_prod_distr": codigo_produto,
            "descr_completa_distr": descr_completa_distr,
            "tipo_produto": tipo_produto,
            "dun14": dun14,
            "variante": variante,
            "unidade_venda": unidade_venda,
            "quant_unid_venda": quant_unid_venda,
            "unidade_embalagem": unidade_embalagem,
            "quantidade_embalagem": quantidade_embalagem,
            "volumetria": volumetria
        }

        for key, value in params_p.items():

            if not value:
                del params_p[key]

        key_columns = ["id_produto"]

        dm.raw_sql_update("PRODUTO", params_p, key_columns)

        # Update na tabela produto_distribuidor
        params_pd = {
            "id_distribuidor": id_distribuidor,
            "id_produto": id_produto,
            "cod_prod_distr": codigo_produto,
            "status": status,
            "id_marca": cod_marca,
            "multiplo_venda": multiplo_venda,
            "unidade_venda": unidade_venda,
            "quant_unid_venda": quant_unid_venda,
            "ranking": ranking,
            "giro": giro,
            "agrup_familia": agrup_familia,
            "cod_fornecedor": cod_fornecedor
        }

        key_columns = ["id_distribuidor", "id_produto", "cod_prod_distr"]

        for key, value in params_pd.items():

            if not value:
                del params_p[key]

        dm.raw_sql_update("PRODUTO_DISTRIBUIDOR", params_pd, key_columns)

        logger.info("Todas as mudancas foram realizadas com sucesso")
        return {"status":200,
                "resposta":{"tipo":"1", "descricao": "Todas as mudanças foram feitas."}}, 200