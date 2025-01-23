#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarProdutosPendentesJmanager(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar_Produtos_Pendentes

        Returns:
            [dict]: Status da transação
        """        
        info = global_info.get("token_info")

        distribuidor_token = info.get("id_distribuidor")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["aprovado", 'objeto', "id_distribuidor", "data_cadastro"]

        correct_types = {"id_distribuidor": int, "data_cadastro": list}

        if not response_data.get("id_distribuidor"):
            response_data.pop("id_distribuidor", None)

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                correct_types = correct_types, 
                                bool_missing = False,
                                comparison_columns = produto_columns)):
            return validity

        # Cuida do limite de informações mandadas e da pagina atual
        pagina, limite = dm.page_limit_handler(response_data)

        # Tratamento do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor")) if response_data.get("id_distribuidor") else 0

        if id_distribuidor != 0:
            answer, response = dm.distr_usuario_check(id_distribuidor)
            if not answer:
                return response

            id_distribuidor = [id_distribuidor]

        else:
            global_info.save_info_thread(id_distribuidor = id_distribuidor)
            id_distribuidor = distribuidor_token

        ## Filtros dos dados
        string_data = ""
        string_objeto = ""

        params = {
            "offset": (pagina - 1) * limite,
            "limite": limite,
            "distribuidores": id_distribuidor
        }

        # Tratamento da data
        data_cadastro = response_data.get("data_cadastro")

        if data_cadastro:

            if len(data_cadastro) == 1:
                date_1, date_2 = data_cadastro[0], ""
            
            else:
                date_1, date_2 = data_cadastro[0], data_cadastro[1]

            if date_1 and date_2:
                
                if date_1:
                    try:
                        date_1 = datetime.strptime(date_1, '%Y-%m-%d')
                        date_1 = date_1.strftime('%Y-%m-%d')
                        if date_1 < "1900-01-01":
                            date_1 = "1900-01-01"
                    except:
                        date_1 = "1900-01-01"
                else:
                    date_1 = "1900-01-01"

                if date_2:
                    try:
                        date_2 = datetime.strptime(date_2, '%Y-%m-%d')
                        date_2 = date_2.strftime('%Y-%m-%d')
                        if date_2 > "3000-01-01":
                            date_2 = "3000-01-01"
                    except:
                        date_2 = "3000-01-01"
                else:
                    date_2 = "3000-01-01"

                string_data = "AND ap.dt_insert BETWEEN :date_1 AND :date_2"

                params.update({
                    "date_1": date_1,
                    "date_2": date_2
                })

        # Tratamento do aprovado 
        aprovado = str(response_data.get("aprovado")).upper()
        if aprovado not in {"A", "R", "P"}:
            aprovado = "P"

        params["aprovado"] = aprovado

        # Tratamento do objeto
        objeto = response_data.get('objeto')  

        if objeto:

            objeto = str(objeto).upper().split()

            string_objeto = ""

            for index, word in enumerate(objeto):

                params.update({
                    f"word_{index}": f"%{word}%"
                })

                string_objeto += f"""
                AND (
                        ap.descr_completa_distr COLLATE Latin1_General_CI_AI LIKE :word_{index}
                        OR ap.descr_reduzida_distr COLLATE Latin1_General_CI_AI LIKE :word_{index}
                        OR ap.sku LIKE :word_{index}
                    )
                """     

        # Realizando as queries nas tabelas
        produto_query = f"""
            SET NOCOUNT ON;

            -- Verificando se a tabela temporária existe
            IF Object_ID('tempDB..#HOLD_PRODUTO','U') IS NOT NULL 
            BEGIN
                DROP TABLE #HOLD_PRODUTO
            END;

            -- Declarando variáveis locais
            DECLARE @count_pendente INT;
            DECLARE @count_aprovado INT;
            DECLARE @count_reprovado INT;

            -- Realizando a query
            SELECT
                DISTINCT ap.cod_prod_distr,
                ap.id_distribuidor,
                ap.status_aprovado

            INTO
                #HOLD_PRODUTO

            FROM
                API_PRODUTO ap

                INNER JOIN DISTRIBUIDOR d ON
                    ap.id_distribuidor = d.id_distribuidor

            WHERE
                (
                    (
                        0 IN :distribuidores
                    )
                        OR
                    (
                        0 NOT IN :distribuidores
                        AND ap.id_distribuidor IN :distribuidores
                    )
                )
                AND LEN(ap.sku) > 0
                {string_objeto}
                {string_data};

            -- Pegando os valores do contadores
            SET @count_aprovado  = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'A');
            SET @count_pendente  = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'P');
            SET @count_reprovado = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'R');

            -- Vendo o resultado
            SELECT
                ap.id_distribuidor,
                ap.id_produto,
                ap.agrupamento_variante,
                ap.descr_reduzida_distr,
                ap.descr_completa_distr,
                ap.cod_prod_distr,
                ap.cod_frag_distr,
                ap.sku,
                ap.dun14,
                ap.status,
                ap.tipo_produto,
                ap.cod_marca,
                ap.descr_marca,
                ap.cod_grupo,
                ap.descr_grupo,
                ap.cod_subgrupo,
                ap.descr_subgrupo,
                ap.variante,
                ap.multiplo_venda,
                ap.unidade_venda,
                ap.quant_unid_venda,
                ap.unidade_embalagem,
                ap.quantidade_embalagem,
                ap.ranking,
                ap.giro,
                ap.agrup_familia,
                ap.volumetria,
                ap.cod_fornecedor,
                ap.descri_fornecedor,
                ap.dt_insert,
                ap.dt_update,
                ap.status_aprovado,
                ap.usuario_aprovado,
                ap.url_imagem,
                ap.motivo_reprovado,
                d.nome_fantasia,
                COUNT(*) OVER() as "count__",
                @count_aprovado as "count_aprovado",
                @count_pendente as "count_pendente",
                @count_reprovado as "count_reprovado"

            FROM
                #HOLD_PRODUTO as hr

                INNER JOIN API_PRODUTO ap ON
                    hr.cod_prod_distr = ap.cod_prod_distr
                    AND hr.id_distribuidor = ap.id_distribuidor

                INNER JOIN DISTRIBUIDOR d ON
                    ap.id_distribuidor = d.id_distribuidor

            WHERE
                ap.status_aprovado = :aprovado

            ORDER BY
                ap.dt_insert ASC

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY;

            -- Deleta a tabela temporaria
            DROP TABLE #HOLD_PRODUTO;
        """

        dict_query = {
            "query": produto_query,
            "params": params,
        }

        data = dm.raw_sql_return(**dict_query)

        if data:
            
            maximum_aprovados = data[0].get("count_aprovado")
            maximum_pendentes = data[0].get("count_pendente")
            maximum_reprovados = data[0].get("count_reprovado")

            maximum_all = data[0].get("count__")
            maximum_pages = maximum_all//limite + (maximum_all % limite > 0)

            for info in data:
                del info["count_aprovado"]
                del info["count_pendente"]
                del info["count_reprovado"]
                del info["count__"]

            logger.info(f"Dados do produto foram enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados encontrados."},
                    "informacoes": {
                        "itens": maximum_all,
                        "paginas": maximum_pages,
                        "pagina_atual": pagina, 
                        "aprovados": maximum_aprovados,
                        "reprovados": maximum_reprovados,
                        "pendentes": maximum_pendentes}, 
                    "dados": data}, 200

        logger.error(f"Dados do produto nao foram encontrados.")
        return {"status":400,
                "resposta":{
                    "tipo":"6",
                    "descricao":f"Dados não encontrados para estes filtros."}}, 200