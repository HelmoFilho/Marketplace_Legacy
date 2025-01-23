#=== Importações de módulos externos ===#
from flask_restful import Resource
import numpy as np

#=== Importações de módulos internos ===#
import functions.file_management as fm
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class ListarOfertasMarketplace(Resource):

    @logger
    def post(self) -> dict:
        """
        Método POST do Listar Ofertas do Marketplace

        Returns:
            [dict]: Status da transação
        """
        logger.info("Metodo POST do Endpoint Listar Ofertas do Marketplace foi requisitado.")

        id_cliente_token = None
        id_usuario = None

        info = secure.verify_token("user")
        if type(info) is dict:
            id_cliente_token = info.get("id_cliente")
            id_usuario = int(info.get('id_usuario'))

        # Pega os dados do front-end
        response_data = dm.get_request(norm_keys = True, trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]
        unnecessary_keys = ["id_oferta", "id_tipo", "id_grupo", "id_subgrupo", 
                            "paginar", "busca", "id_orcamento", "id_cliente", "id_produto"]

        if id_cliente_token:
            necessary_keys.append("id_cliente")
        
        else:
            unnecessary_keys.append("id_cliente")
        
        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_oferta": [list, int],
            "id_produto": [list, str],
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "paginar": bool,
            "id_orcamento": [list, int]
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

        busca = response_data.get("busca") if response_data.get("busca") else None

        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = int(response_data.get("id_grupo")) if response_data.get("id_grupo") else None
        id_subgrupo = int(response_data.get("id_subgrupo")) if response_data.get("id_subgrupo") else None

        id_oferta = response_data.get("id_oferta") if response_data.get("id_oferta") else None

        id_produto = response_data.get("id_produto") if response_data.get("id_produto") else None

        id_orcamento = response_data.get("id_orcamento") if response_data.get("id_orcamento") else None

        paginar = True if response_data.get("paginar") else False

        # Criando a query de desconto antecipadamente
        desconto_query = """
            -- USE [B2BTESTE2]

            SET NOCOUNT ON;

            -- Declaracao de variaveis globais
            --## Variaveis entradas
            DECLARE @id_produto_input VARCHAR(MAX) = :id_produto;

            --## Variaveis de apoio
            DECLARE @actual_produto INT;
            DECLARE @max_produto INT;
            DECLARE @id_produto VARCHAR(100);
            DECLARE @id_distribuidor INT;
            DECLARE @desconto DECIMAL(8,3);
            DECLARE @data_inicio DATETIME;
            DECLARE @data_final DATETIME;

            --## Variaveis tabelas
            DECLARE @id_produto_hold TABLE (
                id INT IDENTITY(1,1),
                id_produto VARCHAR(100),
                id_distribuidor INT
            );

            DECLARE @desconto_hold TABLE (
                id_produto VARCHAR(100),
                desconto DECIMAL(8,3),
                data_inicio DATETIME,
                data_final DATETIME
            );

            -- Pegando os id_produto
            INSERT INTO
                @id_produto_hold
                    (
                        id_produto,
                        id_distribuidor
                    )

                SELECT
                    p.id_produto,
                    pd.id_distribuidor

                FROM
                    STRING_SPLIT(@id_produto_input, ',') as NDA

                    INNER JOIN PRODUTO p ON
                        NDA.value = p.id_produto

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto

                WHERE
                    p.status = 'A'
                    AND pd.status = 'A';

            -- Adicionando os descontos do produto
            SELECT
                @max_produto = MAX(id),
                @actual_produto = MIN(id)

            FROM
                @id_produto_hold;

            WHILE @actual_produto <= @max_produto
            BEGIN

                SELECT
                    @id_produto = id_produto,
                    @id_distribuidor = id_distribuidor

                FROM
                    @id_produto_hold

                WHERE
                    id = @actual_produto;

                -- Salvando o desconto
                SET @desconto = NULL;
                SET @data_inicio = NULL;
                SET @data_final = NULL;

                SELECT
                    TOP 1 @desconto = od.desconto,
                    @data_inicio = o.data_inicio,
                    @data_final = o.data_final
                                            
                FROM
                    OFERTA o
                                            
                    INNER JOIN OFERTA_DESCONTO od ON
                        o.id_oferta = od.id_oferta
                                                
                    INNER JOIN OFERTA_PRODUTO op ON
                        od.id_oferta = op.id_oferta
                                                
                WHERE
                    o.status = 'A'
                    AND od.status = 'A'
                    AND op.status = 'A'
                    AND o.data_cadastro <= GETDATE()
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND o.tipo_oferta = 1
                    AND op.id_produto = @id_produto
                    AND o.id_distribuidor = @id_distribuidor

                ORDER BY
                    od.desconto DESC;

                -- Verificando o desconto
                IF @desconto IS NOT NULL
                BEGIN

                    IF @desconto <= 0 OR @desconto IS NULL
                    BEGIN
                        SET @desconto = 0;
                    END
                                        
                    ELSE IF @desconto >= 100
                    BEGIN
                        SET @desconto = 100;
                    END

                END

                -- Salvando a oferta
                INSERT INTO
                    @desconto_hold
                        (
                            id_produto,
                            desconto,
                            data_inicio,
                            data_final
                        )

                    VALUES
                        (
                            @id_produto,
                            @desconto,
                            @data_inicio,
                            @data_final
                        );

                -- Incrementando o contador
                SET @actual_produto = @actual_produto + 1;

            END

            -- Mostrando o resultado
            SELECT
                id_produto,
                desconto,
                data_inicio,
                data_final

            FROM
                @desconto_hold;                     
        """

        # Verificando o id_cliente enviado
        if id_cliente and id_usuario:

            if id_cliente_token:

                if int(id_cliente) not in id_cliente_token:
                    id_cliente = None

                else:
                    id_cliente = int(id_cliente)

            else:
                id_cliente = None

            if not id_cliente:
                logger.error(f"Usuario {id_usuario} tentando realizar acao por cliente nao atrelado ao mesmo.")
                return {"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario tentando realizar ação por cliente não atrelado ao mesmo."}}, 403

        else:
            id_cliente = None
            id_usuario = None

        # Verificando possibilidade de procura por orçamento
        if id_orcamento:

            if not id_usuario or not id_cliente:

                logger.error(f"Usuario tentando procura de oferta por orcamento sem credenciais.")
                return {"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario tentando procura de oferta por orcamento sem credenciais."}}, 403

            if type(id_orcamento) is int:
                id_orcamento = [id_orcamento]

            elif type(id_orcamento) is str:
                id_orcamento = [int(id_orcamento)]

            id_orcamento = ",".join([str(i) for i in id_orcamento])

        # Verificando o paginar
        if response_data.get("paginar"):
            paginar = True

        elif id_oferta:
            paginar = True

        elif not (id_tipo or id_grupo or id_subgrupo):
            paginar = True

        elif ((id_tipo or id_grupo) and not id_subgrupo):
            paginar = True 

        # Tratando id_oferta
        if id_oferta:
            if type(id_oferta) is int:
                id_oferta = [id_oferta]

            if type(id_oferta) is str:
                id_oferta = [int(id_oferta)]

            id_oferta = ",".join([str(i)  for i in id_oferta])

        # Tratando id_produto
        if id_produto:
            if type(id_produto) is str:
                id_produto = [id_produto]

            id_produto = ",".join([str(i)  for i in id_produto])

        # Fazendo as queries
        counter = 0

        ofertas_query = """
            -- USE [B2BTESTE2]

            SET NOCOUNT ON;

            -- Verificando se a tabela temporaria existe
            IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
            BEGIN
                DROP TABLE #HOLD_OFERTA
            END;

            IF OBJECT_ID('tempDB..#HOLD_PRE_OFERTA', 'U') IS NOT NULL
            BEGIN
                DROP TABLE #HOLD_PRE_OFERTA
            END;

            IF OBJECT_ID('tempDB..#HOLD_PRODUTO', 'U') IS NOT NULL
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

            -- Criando a tabela temporaria de oferta
            CREATE TABLE #HOLD_OFERTA
            (
                id_oferta INT,
                tipo_oferta INT,
                id_distribuidor INT,
                ordem INT,
                descricao_oferta VARCHAR(1000) 
            );

            -- Variaveis globais
            ---- Variaveis de entrada
            DECLARE @id_distribuidor INT = :id_distribuidor;

            DECLARE @id_cliente INT = :id_cliente;
            DECLARE @id_usuario INT = :id_usuario;

            DECLARE @id_tipo INT = :id_tipo;
            DECLARE @id_grupo INT = :id_grupo;
            DECLARE @id_subgrupo INT = :id_subgrupo;

            DECLARE @busca VARCHAR(1000) = :busca;

            DECLARE @id_oferta VARCHAR(MAX) = :id_oferta;

            DECLARE @id_produto VARCHAR(MAX) = :id_produto;

            DECLARE @id_orcamento VARCHAR(MAX) = :id_orcamento;

            DECLARE @paginar INT = :paginar;

            DECLARE @offset INT = :offset;
            DECLARE @limite INT = :limite;

            ---- Variaveis de apoio
            DECLARE @max_count INT;
            DECLARE @actual_count INT = 1;
            DECLARE @word VARCHAR(1000);

            ---- Variaveis tabelas
            DECLARE @id_distribuidor_hold TABLE (
                id_distribuidor INT
            );

            -- Pegando distribuidores válidos para o cliente
            INSERT INTO
                @id_distribuidor_hold

                SELECT
                    DISTINCT d.id_distribuidor

                FROM
                    CLIENTE c

                    INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                        c.id_cliente = cd.id_cliente

                    INNER JOIN DISTRIBUIDOR d ON
                        cd.id_distribuidor = d.id_distribuidor

                WHERE
                    c.id_cliente = CASE
                                       WHEN @id_cliente IS NULL
                                           THEN c.id_cliente
                                       ELSE
                                           @id_cliente
                                   END
                    AND d.id_distribuidor = CASE
                                                WHEN @id_distribuidor = 0
                                                    THEN d.id_distribuidor
                                                ELSE
                                                    @id_distribuidor
                                            END
                    AND c.status = 'A'
                    AND c.status_receita = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND cd.status = 'A'
                    AND d.status = 'A';

            -- Salvando os produtos validos
            SELECT
                p.id_produto,
                p.sku,
                pd.id_distribuidor

            INTO
                #HOLD_PRODUTO

            FROM
                PRODUTO p
                                    
                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto

            WHERE
                LEN(p.sku) > 0
                AND p.id_produto IS NOT NULL
                AND p.status = 'A'
                AND pd.status = 'A'
                AND pd.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

            -- Filtrando por...

            -- ## tipo/grupo/subgrupo
            IF (@id_tipo IS NOT NULL) OR (@id_grupo IS NOT NULL) OR (@id_subgrupo IS NOT NULL)
            BEGIN
                DELETE FROM
                    #HOLD_PRODUTO

                WHERE
                    id_produto NOT IN (
                                            SELECT
                                                p.id_produto

                                            FROM
                                                PRODUTO p
                                        
                                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                                    p.id_produto = ps.codigo_produto
                                        
                                                INNER JOIN SUBGRUPO s ON
                                                    ps.id_distribuidor = s.id_distribuidor 
                                                    AND ps.id_subgrupo = s.id_subgrupo 
                                        
                                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                                    s.id_distribuidor = gs.id_distribuidor 
                                                    AND ps.id_subgrupo = gs.id_subgrupo 
                                        
                                                INNER JOIN GRUPO g ON
                                                    gs.id_grupo = g.id_grupo 
                                        
                                                INNER JOIN TIPO t ON
                                                    g.tipo_pai = t.id_tipo
                                                
                                            WHERE
                                                ps.status = 'A'
                                                AND s.status = 'A'
                                                AND gs.status = 'A'
                                                AND g.status = 'A'
                                                AND t.status = 'A'
                                                AND t.id_tipo = CASE
                                                                    WHEN @id_tipo is NULL
                                                                        THEN t.id_tipo
                                                                    ELSE
                                                                        @id_tipo
                                                                END
                                                AND g.id_grupo = CASE
                                                                     WHEN @id_grupo IS NULL
                                                                         THEN g.id_grupo
                                                                     ELSE
                                                                         @id_grupo
                                                                 END
                                                AND s.id_subgrupo = CASE
                                                                        WHEN @id_subgrupo IS NULL
                                                                            THEN s.id_subgrupo
                                                                        ELSE
                                                                            @id_subgrupo
                                                                    END
                                                AND ps.id_distribuidor = CASE
                                                                             WHEN @id_distribuidor = 0
                                                                                 THEN ps.id_distribuidor
                                                                             ELSE
                                                                                 @id_distribuidor
                                                                         END

                                    )
            END

            -- ## Marca ou descricao de produto
            ELSE IF LEN(@busca) > 0
            BEGIN

                -- Removendo espaco em branco adicionais e salvando as palavras individualmente
                INSERT INTO
                    #HOLD_STRING
                        (
                            string
                        )

                    SELECT
                        VALUE

                    FROM
                        STRING_SPLIT(TRIM(@busca), ' ')

                    WHERE
                        LEN(VALUE) > 0;

                -- Realizando o filtro
                SET @max_count = (SELECT MAX(id) FROM #HOLD_STRING) + 1;

                WHILE @actual_count < @max_count
                BEGIN

                    -- Pegando palavra atual
                    SET @word = '%' + (SELECT TOP 1 string FROM #HOLD_STRING WHERE id = @actual_count) + '%';

                    -- Fazendo query na palavra atual
                    DELETE FROM
                        #HOLD_PRODUTO

                    WHERE
                        id_produto NOT IN (
                                                SELECT
                                                thp.id_produto

                                                FROM
                                                    #HOLD_PRODUTO thp

                                                    INNER JOIN PRODUTO p ON
                                                        thp.id_produto = p.id_produto

                                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                                        p.id_produto = pd.id_produto

                                                    INNER JOIN MARCA m ON
                                                        pd.id_marca = m.id_marca
                                                        AND pd.id_distribuidor = m.id_distribuidor

                                                WHERE
                                                    p.descricao_completa COLLATE Latin1_General_CI_AI LIKE @word
                                                    OR m.desc_marca COLLATE Latin1_General_CI_AI LIKE @word     
                                            );                        

                    -- Verificando se ainda existem registros
                    IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
                    BEGIN
                        SET @actual_count = @actual_count + 1;
                    END

                    ELSE
                    BEGIN
                        SET @actual_count = @max_count;
                    END

                END

            END

            -- ## Orcamento
            ELSE IF @id_orcamento IS NOT NULL AND (@id_usuario IS NOT NULL OR @id_cliente IS NOT NULL)
            BEGIN

                DELETE FROM
                    #HOLD_PRODUTO

                WHERE
                    id_produto NOT IN (

                                            SELECT
                                                thp.id_produto

                                            FROM
                                                #HOLD_PRODUTO thp

                                                INNER JOIN ORCAMENTO_ITEM oi ON
                                                    thp.sku = oi.sku
                                                    AND thp.id_distribuidor = oi.id_distribuidor

                                                INNER JOIN ORCAMENTO o ON
                                                    oi.id_orcamento = o.id_orcamento

                                            WHERE
                                                o.id_cliente = CASE
                                                                   WHEN @id_cliente IS NOT NULL
                                                                       THEN @id_cliente
                                                                   ELSE
                                                                       o.id_cliente
                                                               END
                                                AND o.id_usuario = CASE
                                                                       WHEN @id_usuario IS NOT NULL
                                                                           THEN @id_usuario
                                                                       ELSE
                                                                           o.id_usuario
                                                                   END
                                                AND o.id_orcamento = CASE
                                                                         WHEN @id_orcamento = 0
                                                                             THEN o.id_orcamento
                                                                         ELSE
                                                                             @id_orcamento
                                                                     END
                                    );

            END

            -- ## ID de produto
            ELSE IF @id_produto IS NOT NULL AND LEN(@id_produto) > 0
            BEGIN

                DELETE FROM
                    #HOLD_PRODUTO

                WHERE
                    id_produto NOT IN (
                                            SELECT
                                                VALUE

                                            FROM
                                                STRING_SPLIT(TRIM(@id_produto), ',')

                                            WHERE
                                                LEN(VALUE) > 0
                                        )

            END

            -- Verificando se produtos foram encontrados
            IF EXISTS(SELECT 1 FROM #HOLD_PRODUTO)
            BEGIN

                -- Salvando pre-ofertas
                SELECT
                    DISTINCT o.id_oferta,
                    o.tipo_oferta,
                    o.id_distribuidor,
                    o.ordem,
                    o.descricao_oferta

                INTO
                    #HOLD_PRE_OFERTA

                FROM
                    OFERTA o

                    INNER JOIN OFERTA_PRODUTO op ON
                        o.id_oferta = op.id_oferta

                    INNER JOIN #HOLD_PRODUTO thp ON
                        op.id_produto = thp.id_produto
                        AND o.id_distribuidor = thp.id_distribuidor
                    
                WHERE
                    op.id_produto IS NOT NULL
                    AND o.tipo_oferta IN (2,3)
                    AND op.status = 'A'
                    AND o.status = 'A'
                    AND o.data_cadastro <= GETDATE()
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND o.id_distribuidor IN (SELECT id_distribuidor FROM @id_distribuidor_hold);

                IF EXISTS(SELECT 1 FROM #HOLD_PRE_OFERTA)
                BEGIN

                    -- Salvando ofertas bonificadas
                    INSERT INTO
                        #HOLD_OFERTA

                        SELECT
                            DISTINCT thpo.id_oferta,
                            thpo.tipo_oferta,
                            thpo.id_distribuidor,
                            thpo.ordem,
                            thpo.descricao_oferta

                        FROM
                            #HOLD_PRE_OFERTA thpo

                            INNER JOIN OFERTA_BONIFICADO ob ON
                                thpo.id_oferta = ob.id_oferta

                        WHERE
                            ob.status = 'A'
                            AND ob.id_produto IS NOT NULL
                            AND thpo.tipo_oferta = 2;

                    -- Salvando ofertas escalonada
                    INSERT INTO
                        #HOLD_OFERTA
                
                        SELECT
                            DISTINCT thpo.id_oferta,
                            thpo.tipo_oferta,
                            thpo.id_distribuidor,
                            thpo.ordem,
                            thpo.descricao_oferta

                        FROM
                            #HOLD_PRE_OFERTA thpo

                            INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                                thpo.id_oferta = oef.id_oferta

                        WHERE
                            oef.status = 'A'
                            AND thpo.tipo_oferta = 3;

                    -- Definindo a Paginacao
                    IF @id_subgrupo IS NOT NULL AND @paginar = 0
                    BEGIN
                        SET @offset = 0
                        SET @limite = (SELECT COUNT(*) FROM #HOLD_OFERTA)

                        IF @limite <= 0
                        BEGIN
                            SET @limite = 1
                        END
                    END

                END

            END

            -- Filtrando por id_oferta
            IF LEN(@id_oferta) > 0
            BEGIN
                
                DELETE FROM
                    #HOLD_OFERTA

                WHERE
                    id_oferta NOT IN (
                                        SELECT
                                            VALUE

                                        FROM
                                            STRING_SPLIT(@id_oferta, ',')

                                        WHERE
                                            LEN(VALUE) > 0
                                     )

            END

            -- Mostrando os resultados
            SELECT
                id_oferta,
                tipo_oferta,
                -- id_distribuidor,
                -- ordem,
                -- descricao_oferta,
                COUNT(*) OVER() as COUNT__

            FROM
                #HOLD_OFERTA

            ORDER BY
                ordem ASC,
                descricao_oferta ASC

            OFFSET
                @offset ROWS

            FETCH
                NEXT @limite ROWS ONLY;

            -- Deleta tabelas tempor�rias
            DROP TABLE #HOLD_OFERTA;
            DROP TABLE #HOLD_STRING;

            IF OBJECT_ID('tempDB..#HOLD_PRE_OFERTA', 'U') IS NOT NULL
            BEGIN
                DROP TABLE #HOLD_PRE_OFERTA
            END;

            IF OBJECT_ID('tempDB..#HOLD_PRODUTO', 'U') IS NOT NULL
            BEGIN
                DROP TABLE #HOLD_PRODUTO
            END;
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "offset": (pagina - 1) * limite,
            "limite": limite,
            "paginar": paginar,
            "id_tipo": id_tipo,
            "id_grupo": id_grupo,
            "id_subgrupo": id_subgrupo,
            "busca": busca,
            "id_oferta": id_oferta,
            "id_produto": id_produto,
            "id_orcamento": id_orcamento,
            "id_cliente": id_cliente,
            "id_usuario": id_usuario
        }

        ofertas_query = dm.raw_sql_return(ofertas_query, params = params, raw = True)

        # Criando o json
        oferta_hold = []

        for oferta in ofertas_query:

            counter = oferta[2]

            if oferta[1] == 2:

                # Bonificados
                bonificados_query = """
                    SELECT
                        TOP 1 id_oferta AS id_campanha,
                        id_distribuidor AS id_distribuidor,
                        status,
                        'W' AS tipo_campanha,
                        descricao_oferta AS descricao,
                        data_inicio,
                        data_final,
                        CASE WHEN ordem = 1 THEN 1 ELSE 0 END AS destaque,
                        limite_ativacao_oferta AS quantidade_limite,
                        limite_ativacao_oferta AS quantidade_pontos,
                        0 AS quantidade_produto, -- somar os produtos ativadores
                        CASE WHEN operador = 'Q' THEN necessario_para_ativar ELSE 0 END AS quantidade_ativar,
                        limite_ativacao_cliente AS maxima_ativacao,
                        0 AS quantidade_bonificado,
                        CASE WHEN operador = 'V' THEN necessario_para_ativar ELSE 0 END AS valor_ativar,
                        0 AS automatica,
                        NULL unidade_venda,
                        produto_agrupado as "unificada"
                    
                    FROM
                        OFERTA

                    WHERE
                        id_oferta = :id_oferta

                    ORDER BY
                        ordem ASC,
                        descricao_oferta ASC
                """

                params = {
                    "id_oferta": oferta[0]
                }

                bonificados_query = dm.raw_sql_return(bonificados_query, params = params, first = True)

                bonificados_query["produto_ativador"] = []
                bonificados_query["produto_bonificado"] = []

                # Pegando os ativadores da oferta bonificada
                ativador_query = f"""
                    SELECT
                        o.id_oferta AS id_campanha,
                        o.id_distribuidor,
                        p.id_produto,
                        p.sku,
                        m.desc_marca AS descricao_marca,
                        p.descricao_completa AS descricao_produto,
                        NULL imagem,
                        p.status,
                        tpp.preco_tabela,
                        0 AS quantidade,
                        pe.qtd_estoque AS estoque,
                        CASE WHEN o.operador = 'Q' THEN necessario_para_ativar ELSE 0 END AS quantidade_minima,
                        CASE WHEN o.operador = 'V' THEN necessario_para_ativar ELSE 0 END AS valor_minimo,
                        CASE WHEN o.operador = 'Q' THEN op.quantidade_min_ativacao ELSE 0 END AS quantidade_min_ativacao,
                        CASE WHEN o.operador = 'V' THEN op.valor_min_ativacao ELSE 0 END AS valor_min_ativacao,
                        p.unidade_embalagem,
                        p.quantidade_embalagem,
                        p.unidade_venda,
                        p.quant_unid_venda,
                        pd.multiplo_venda

                    FROM
                        OFERTA o

                        INNER JOIN OFERTA_PRODUTO op ON
                            o.id_oferta = op.id_oferta
                        
                        INNER JOIN PRODUTO p ON
                            op.id_produto = p.id_produto

                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto
                            AND o.id_distribuidor = pd.id_distribuidor
                            
                        INNER JOIN PRODUTO_ESTOQUE pe ON
                            pd.id_produto = pe.id_produto
                            AND pd.id_distribuidor = pe.id_distribuidor 
                            
                        INNER JOIN MARCA m ON
                            pd.id_marca = m.id_marca
                            AND pe.id_distribuidor = m.id_distribuidor

                        INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                            pe.id_produto = tpp.id_produto
                            AND m.id_distribuidor = tpp.id_distribuidor

                        INNER JOIN TABELA_PRECO tp ON
                            tpp.id_tabela_preco = tp.id_tabela_preco
                            AND tpp.id_distribuidor = tp.id_distribuidor

                    WHERE
                        o.id_oferta = :id_oferta
                        AND pd.id_distribuidor = :id_distribuidor
                        AND p.id_produto IS NOT NULL
                        AND o.tipo_oferta = 2
                        AND op.status = 'A'
                        AND p.status = 'A'
                        AND pd.status = 'A'
                        AND tp.status = 'A'
                        AND tp.tabela_padrao = 'S'
                        AND tp.dt_inicio <= GETDATE()
                        AND tp.dt_fim >= GETDATE()

                    ORDER BY
                        pd.ranking ASC
                """

                params = {
                    "id_distribuidor": bonificados_query.get("id_distribuidor"),
                    "id_oferta": bonificados_query.get("id_campanha")
                }

                ativador_query = dm.raw_sql_return(ativador_query, params = params)

                if not ativador_query: continue

                id_produto_hold = []

                for ativador in ativador_query:
                    id_produto_hold.append(ativador.get("id_produto"))
                    ativador["imagem"] = fm.product_image_url(ativador.get("sku"), "150")
                    if not id_cliente:
                        ativador["preco_tabela"] = None
                        ativador["estoque"] = None

                if id_produto_hold:

                    str_id_produto = ",".join(id_produto_hold)

                    params = {
                        "id_produto": str_id_produto
                    }

                    desconto_hold = dm.raw_sql_return(desconto_query, params = params)

                    for produto in desconto_hold:
                        for ativador in ativador_query:
                            if produto.get("id_produto") == ativador.get("id_produto"):
                                if id_cliente is None:
                                    ativador["desconto"] = None

                                elif produto.get("desconto") is None:
                                    ativador["desconto"] = {}

                                else:
                                    ativador["desconto"] = produto.copy()
                                    ativador["desconto"].pop("id_produto")
                                    desconto = 1 - (ativador["desconto"].get('desconto')/100)
                                    ativador["desconto"]["preco_desconto"] = round(ativador["preco_tabela"] * desconto, 3)

                                break

                # Pegando os bonificados da oferta bonificada
                bonificado_query = f"""
                    SELECT
                        o.id_oferta AS id_campanha,
                        o.id_distribuidor,
                        p.id_produto,
                        p.sku,
                        m.desc_marca AS descricao_marca,
                        p.descricao_completa AS descricao_produto,
                        NULL imagem,
                        p.status,
                        tpp.preco_tabela,
                        0 AS quantidade,
                        ob.quantidade_bonificada,
                        pe.qtd_estoque AS estoque,
                        p.unidade_embalagem,
                        p.quantidade_embalagem,
                        p.unidade_venda,
                        p.quant_unid_venda,
                        pd.multiplo_venda

                    FROM
                        OFERTA o

                        INNER JOIN OFERTA_BONIFICADO ob ON
                            o.id_oferta = ob.id_oferta
                        
                        INNER JOIN PRODUTO p ON
                            ob.id_produto = p.id_produto

                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto
                            AND o.id_distribuidor = pd.id_distribuidor
                            
                        INNER JOIN PRODUTO_ESTOQUE pe ON
                            pd.id_produto = pe.id_produto
                            AND pd.id_distribuidor = pe.id_distribuidor 
                            
                        INNER JOIN MARCA m ON
                            pd.id_marca = m.id_marca
                            AND pe.id_distribuidor = m.id_distribuidor

                        INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                            pe.id_produto = tpp.id_produto
                            AND m.id_distribuidor = tpp.id_distribuidor

                        INNER JOIN TABELA_PRECO tp ON
                            tpp.id_tabela_preco = tp.id_tabela_preco
                            AND tpp.id_distribuidor = tp.id_distribuidor

                    WHERE
                        o.id_oferta = :id_oferta
                        AND pd.id_distribuidor = :id_distribuidor
                        AND p.id_produto IS NOT NULL
                        AND o.tipo_oferta = 2
                        AND ob.status = 'A'
                        AND p.status = 'A'
                        AND pd.status = 'A'
                        AND tp.status = 'A'
                        AND tp.tabela_padrao = 'S'
                        AND tp.dt_inicio <= GETDATE()
                        AND tp.dt_fim >= GETDATE()

                    ORDER BY
                        pd.ranking ASC
                """

                params = {
                    "id_distribuidor": bonificados_query.get("id_distribuidor"),
                    "id_oferta": bonificados_query.get("id_campanha")
                }

                bonificado_query = dm.raw_sql_return(bonificado_query, params = params)

                if not bonificado_query: continue

                id_produto_hold = []

                for bonificado in bonificado_query:
                    id_produto_hold.append(bonificado.get("id_produto"))
                    bonificado["imagem"] = fm.product_image_url(bonificado.get("sku"), "150")
                    if not id_cliente:
                        bonificado["preco_tabela"] = None
                        bonificado["estoque"] = None

                if id_produto_hold:

                    str_id_produto = ",".join(id_produto_hold)

                    params = {
                        "id_produto": str_id_produto
                    }

                    desconto_hold = dm.raw_sql_return(desconto_query, params = params)

                    for produto in desconto_hold:
                        for bonificado in bonificado_query:
                            if produto.get("id_produto") == bonificado.get("id_produto"):
                                if id_cliente is None:
                                    bonificado["desconto"] = None

                                elif produto.get("desconto") is None:
                                    bonificado["desconto"] = {}

                                else:
                                    bonificado["desconto"] = produto.copy()
                                    bonificado["desconto"].pop("id_produto")
                                    desconto = 1 - (bonificado["desconto"].get('desconto')/100)
                                    bonificado["desconto"]["preco_desconto"] = round(bonificado["preco_tabela"] * desconto, 3)

                                break

                # Modificações no cabeçalho da oferta
                bonificados_query["produto_ativador"] = list(ativador_query)
                bonificados_query["produto_bonificado"] = list(bonificado_query)
                bonificados_query["unidade_venda"] = ativador_query[0].get("unidade_venda")
                bonificados_query["quantidade_produto"] = len(ativador_query)
                bonificados_query["automatica"] = False 

                oferta_hold.append(bonificados_query)

                continue

            # Escalonados
            elif oferta[1] == 3:

                escalonado_query = f"""
                    SELECT
                        TOP 1 id_oferta AS id_escalonado,
                        id_distribuidor AS id_distribuidor,
                        descricao_oferta as descricao,
                        status,
                        0 total_desconto,
                        produto_agrupado as "unificada"
                    
                    FROM
                        OFERTA

                    WHERE
                        id_oferta = :id_oferta
                """

                params = {
                    "id_oferta": oferta[0]
                }

                escalonado_query = dm.raw_sql_return(escalonado_query, params = params, first = True)

                info_esc_query = f"""
                    SELECT
                        TOP 1 id_oferta AS id_escalonado,
                        id_distribuidor AS id_distribuidor,
                        data_inicio,
                        data_final,
                        limite_ativacao_oferta AS limite,
                        status AS status_escalonado

                    FROM
                        OFERTA

                    WHERE
                        id_oferta = :id_oferta
                """

                escalonado_query["escalonado"] = dm.raw_sql_return(info_esc_query, params = params)

                faixa_query = f"""
                    SELECT
                        o.id_oferta AS id_escalonado,
                        o.id_distribuidor,
                        oef.sequencia,
                        oef.faixa,
                        oef.desconto,
                        NULL unidade_venda

                    FROM
                        OFERTA o

                        INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                            oef.id_oferta = o.id_oferta

                    WHERE
                        o.id_oferta = :id_oferta
                """

                escalonado_query["faixa_escalonado"] = dm.raw_sql_return(faixa_query, params = params)

                # Pegando os Produtos da oferta escalonada
                produtos_query = f"""
                    SELECT
                        DISTINCT o.id_oferta AS id_escalonado,
                        o.id_distribuidor,
                        p.id_produto,
                        p.sku,
                        m.desc_marca AS descricao_marca,
                        p.descricao_completa AS descricao_produto,
                        NULL imagem,
                        p.status,
                        tpp.preco_tabela,
                        0 AS quantidade,
                        pe.qtd_estoque AS estoque,
                        p.unidade_embalagem,
                        p.quantidade_embalagem,
                        p.unidade_venda,
                        p.quant_unid_venda,
                        pd.ranking,
                        pd.multiplo_venda

                    FROM
                        OFERTA o

                        INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                            o.id_oferta = oef.id_oferta
                            
                        INNER JOIN OFERTA_PRODUTO op ON
                            oef.id_oferta = op.id_oferta
                        
                        INNER JOIN PRODUTO p ON
                            op.id_produto = p.id_produto

                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto
                            AND o.id_distribuidor = pd.id_distribuidor
                            
                        INNER JOIN PRODUTO_ESTOQUE pe ON
                            pd.id_produto = pe.id_produto
                            AND pd.id_distribuidor = pe.id_distribuidor 
                            
                        INNER JOIN MARCA m ON
                            pd.id_marca = m.id_marca
                            AND pe.id_distribuidor = m.id_distribuidor

                        INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                            pe.id_produto = tpp.id_produto
                            AND m.id_distribuidor = tpp.id_distribuidor

                        INNER JOIN TABELA_PRECO tp ON
                            tpp.id_tabela_preco = tp.id_tabela_preco
                            AND tpp.id_distribuidor = tp.id_distribuidor

                    WHERE
                        o.id_oferta = :id_oferta
                        AND pd.id_distribuidor = :id_distribuidor
                        AND p.id_produto IS NOT NULL
                        AND o.tipo_oferta = 3
                        AND oef.status = 'A'
                        AND op.status = 'A'
                        AND p.status = 'A'
                        AND pd.status = 'A'
                        AND tp.status = 'A'
                        AND tp.tabela_padrao = 'S'
                        AND tp.dt_inicio <= GETDATE()
                        AND tp.dt_fim >= GETDATE()

                    ORDER BY
                        pd.ranking ASC
                """

                params = {
                    "id_distribuidor": escalonado_query.get("id_distribuidor"),
                    "id_oferta": escalonado_query.get("id_escalonado")
                }

                escalonado_query["produto_escalonado"] = dm.raw_sql_return(produtos_query, params = params)

                id_produto_hold = []

                for escalonado in escalonado_query["produto_escalonado"]:
                    id_produto_hold.append(escalonado.get("id_produto"))
                    escalonado["imagem"] = fm.product_image_url(escalonado.get("sku"), "150")
                    if not id_cliente:
                        escalonado["preco_tabela"] = None
                        escalonado["estoque"] = None
                    try: del escalonado["ranking"]
                    except: pass

                if id_produto_hold and id_cliente:

                    str_id_produto = ",".join(id_produto_hold)

                    params = {
                        "id_produto": str_id_produto
                    }

                    desconto_hold = dm.raw_sql_return(desconto_query, params = params)

                    for produto in desconto_hold:
                        for escalonado in escalonado_query["produto_escalonado"]:
                            if produto.get("id_produto") == escalonado.get("id_produto"):
                                if id_cliente is None:
                                    escalonado["desconto"] = None

                                elif produto.get("desconto") is None:
                                    escalonado["desconto"] = {}

                                else:
                                    escalonado["desconto"] = produto.copy()
                                    escalonado["desconto"].pop("id_produto")
                                    desconto = 1 - (escalonado["desconto"].get('desconto')/100)
                                    escalonado["desconto"]["preco_desconto"] = round(escalonado["preco_tabela"] * desconto, 3)

                                break

                if escalonado_query.get("produto_escalonado"):
                    oferta_hold.append(escalonado_query)

        if oferta_hold:
            msg = {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."}}
            
            if paginar:
                msg['informacoes'] = {
                    "itens": counter,
                    "paginas": counter//limite + (counter % limite > 0),
                    "pagina_atual": pagina
                }

            msg["dados"] = oferta_hold

            logger.info("Dados de ofertas enviados para o marketplace.")
            return msg, 200

        logger.error(f"Dados nao encontrados.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200