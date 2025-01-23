#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger


class ListarProdutosPendentesJmanager(Resource):

    @logger
    def post(self) -> dict:
        """
        Método POST do Listar_Produtos_Pendentes

        Returns:
            [dict]: Status da transação
        """
        logger.info("Metodo POST do Endpoint Listar Produtos Pendentes foi requisitado.")
        
        # Checa token
        info = secure.verify_token("jmanager")
        if type(info) is tuple:
            return info

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["aprovado", 'objeto', "id_distribuidor", "data_cadastro"]

        correct_types = {"id_distribuidor": int, "data_cadastro": list}

        if not response_data.get("id_distribuidor"):
            response_data.pop("id_distribuidor", None)

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                bool_missing = False, 
                                correct_types = correct_types, 
                                comparison_columns = produto_columns)):
            return validity

        # Cuida do limite de informações mandadas e da pagina atual
        pagina, limite = dm.page_limit_handler(response_data)

        # Tratamento da data
        data_cadastro = response_data.get("data_cadastro")

        if not data_cadastro:
            data_cadastro = []
        
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

        data_cadastro = [date_1, date_2]

        # Tratamento do distribuidor
        if int(info.get("id_perfil")) != 1:
            if response_data.get("id_distribuidor"):
                if int(response_data["id_distribuidor"]) not in info.get("id_distribuidor"):
                    response_data["id_distribuidor"] = info.get("id_distribuidor")

                else:
                    response_data["id_distribuidor"] = [int(response_data["id_distribuidor"])]

            else:
                response_data["id_distribuidor"] = info.get("id_distribuidor")
        
        else:
            if response_data.get("id_distribuidor"):
                response_data["id_distribuidor"] = [response_data.get("id_distribuidor")]

            else:
                response_data["id_distribuidor"] = [0]

        distribuidor = list(response_data.get("id_distribuidor"))
        
        answer = dm.distribuidor_check(distribuidor)
        if not answer[0]:
            return answer[1]

        # Tratamento do aprovado        
        if not response_data.get("aprovado"):
            response_data["aprovado"] = "P"

        # Tratamento do objeto
        objeto = str(response_data.get('objeto')) if response_data.get('objeto') else None        

        # Realizando as queries nas tabelas
        produto_query = f"""
            -- USE [B2BTESTE2]

            SET NOCOUNT ON;

            -- Verificando se a tabela temporária existe
            IF Object_ID('tempDB..#HOLD_PRODUTO','U') IS NOT NULL 
            BEGIN
                DROP TABLE #HOLD_PRODUTO
            END;

            IF OBJECT_ID('tempDB..#HOLD_STRING', 'U') IS NOT NULL
            BEGIN
                DROP TABLE #HOLD_STRING
            END;

            -- Criando a tabela temporaria de palavras
            CREATE TABLE #HOLD_STRING
            (
                id INT IDENTITY(1,1),
                string VARCHAR(100)
            );

            -- Declarando variáveis locais
            DECLARE @count_pendente INT;
            DECLARE @count_aprovado INT;
            DECLARE @count_reprovado INT;
            DECLARE @count_total INT;
                        
            -- Declarando variáveis de controle
            DECLARE @status VARCHAR(1) = :status;
            DECLARE @id_distribuidor INT = :id_distribuidor;
            DECLARE @offset INT = :offset;
            DECLARE @limite INT = :limite;
            DECLARE @date_1 VARCHAR(10) = :date_1;
            DECLARE @date_2 VARCHAR(10) = :date_2;
            DECLARE @objeto VARCHAR(MAX) = :objeto;

            -- Realizando a query
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
                d.nome_fantasia
                            
            INTO 
                #HOLD_PRODUTO

            FROM
                API_PRODUTO ap

                INNER JOIN DISTRIBUIDOR d ON
                    ap.id_distribuidor = d.id_distribuidor

            WHERE
                ap.sku IS NOT NULL 
                AND ap.sku != ''
                AND LEN(ap.sku) > 0
                AND ap.dt_insert BETWEEN @date_1 AND @date_2
                AND (
                        (0 IN :id_distribuidor)
                            OR
                        (0 NOT IN :id_distribuidor AND ap.id_distribuidor IN :id_distribuidor)
                    );

            -- Filtrando pelo objeto
            IF @objeto IS NOT NULL
            BEGIN

                -- Removendo espacos em branco adicionais e salvando as palavras individualmente
                INSERT INTO
                    #HOLD_STRING
                        (
                            string
                        )

                    SELECT
                        VALUE

                    FROM
                        STRING_SPLIT(TRIM(@objeto), ' ')

                    WHERE
                        VALUE != ' ';

                -- Setando os contadores
                DECLARE @max_count INT = (SELECT MAX(id) FROM #HOLD_STRING) + 1
                DECLARE @actual_count INT = 1
                DECLARE @word VARCHAR(100)

                WHILE @actual_count < @max_count
                BEGIN

                    -- Pegando palavra atual
                    SET @word = '%' + (SELECT TOP 1 string FROM #HOLD_STRING WHERE id = @actual_count) + '%'

                    -- Fazendo query na palavra atual
                    DELETE FROM
                        #HOLD_PRODUTO

                    WHERE
                        id_produto NOT IN (
                                                SELECT
                                                    thp.id_produto

                                                FROM
                                                    #HOLD_PRODUTO thp

                                                    INNER JOIN API_PRODUTO ap ON
                                                        thp.id_produto = ap.id_produto

                                                WHERE
                                                    ap.descr_completa_distr COLLATE Latin1_General_CI_AI LIKE @word
                                                    OR ap.sku LIKE @word
                                            )

                    -- Verificando se ainda existem registros
                    IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
                    BEGIN
                        SET @actual_count = @actual_count + 1
                    END

                    ELSE
                    BEGIN
                        SET @actual_count = @max_count
                    END

                END

            END

            -- Pegando os valores do contadores
            SET @count_aprovado  = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'A');
            SET @count_pendente  = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'P');
            SET @count_reprovado = (SELECT COUNT(*) FROM #HOLD_PRODUTO WHERE status_aprovado = 'R');
            SET @count_total = @count_aprovado + @count_pendente + @count_reprovado;

            -- Vendo o resultado
            SELECT
                hr.*,
                @count_total as "count__",
                @count_aprovado as "count_aprovado",
                @count_pendente as "count_pendente",
                @count_reprovado as "count_reprovado"

            FROM
                #HOLD_PRODUTO as hr

            WHERE
                hr.status_aprovado = @status

            ORDER BY
                hr.dt_insert ASC

            OFFSET
                @offset ROWS

            FETCH
                NEXT @limite ROWS ONLY;

            -- Deleta a tabela temporaria
            DROP TABLE #HOLD_STRING;
            DROP TABLE #HOLD_PRODUTO;
        """

        params = {
            "id_distribuidor": distribuidor,
            "status": response_data.get("aprovado"),
            "date_1": data_cadastro[0],
            "date_2": data_cadastro[1],
            "offset": (pagina - 1) * limite,
            "limite": limite,
            "objeto": objeto
        }

        bindparams = ["id_distribuidor"]

        data = dm.raw_sql_return(produto_query, params = params, bindparams = bindparams)

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