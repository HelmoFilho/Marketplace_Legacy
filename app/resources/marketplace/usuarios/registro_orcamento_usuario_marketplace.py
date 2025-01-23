#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime
from flask import request
import json

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class RegistroOrcamentoUsuarioMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self, id_cliente = None):
        """
        Método GET do Orcamento do Usuario

        Returns:
            [dict]: Status da transação
        """
        
        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = {
            "id_cliente": request.args.get('id_cliente'),
            "id_distribuidor": request.args.get('id_distribuidor')
        }

        if response_data.get("id_cliente") is None:
            response_data["id_cliente"] = id_cliente

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_columns = ["id_cliente"]

        no_use_columns = ["id_distribuidor"]

        correct_types = {
            "id_cliente": int,
            "id_distribuidor": int
        }

        # Checa chaves inválidas e faltantes, valores incorretos e nulos
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          not_null = necessary_columns,
                                          no_use_columns = no_use_columns,
                                          correct_types = correct_types)):
            return validity
        
        # Verificando o id_cliente enviado
        id_cliente = int(response_data.get("id_cliente"))

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        # Verificando o distribuidor enviado
        id_distribuidor = response_data.get("id_distribuidor")

        if id_distribuidor:
            id_distribuidor = int(id_distribuidor)

            answer = dm.distribuidor_check(id_distribuidor)
            if not answer[0]:
                return answer[1]

        dict_orcamento = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor,
            "image_replace": '150'
        }

        answer, response = dj.json_orcamento(**dict_orcamento)
        
        if not answer:
            logger.info(f"Dados nao encontrados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Sem dados para retornar."},
                    "dados": {
                        "id_usuario": id_usuario,
                        "id_cliente": id_cliente,
                        "ofertas": [],
                        "itens": []
                    }}, 200

        carrinho_query = response.get("orcamentos")
        id_ofertas = response.get("ofertas")
        id_cupons = response.get("cupons")
        
        # Pegando ofertas do carrinho
        ofertas = []

        if id_ofertas:

            dict_ofertas = {
                "pagina": 1,
                "limite": 999999,
                "id_oferta": list(id_ofertas),
                "id_cliente": id_cliente,
                "id_distribuidor": 0,
            }

            ofertas_return = dj.json_ofertas(**dict_ofertas)

            if ofertas_return:
                ofertas = ofertas_return.get("ofertas")

        # Pegando cupons do carrinho
        cupons = []

        if id_cupons:
            cupons_valid = [cupom.get("id_cupom") for cupom in id_cupons]

            if cupons_valid:

                dict_cupons = {
                    "pagina": 1,
                    "limite": 999999,
                    "id_cupom": list(cupons_valid),
                    "id_cliente": id_cliente,
                    "id_distribuidor": 0,
                }

                cupons = dj.json_cupons(**dict_cupons).get("cupons")

                for orcamento in carrinho_query:
                    for cupom in id_cupons:
                        if orcamento.get("id_distribuidor") == cupom.get("id_distribuidor"):
                            if orcamento.get("id_orcamento") == cupom.get("id_orcamento"):
                                orcamento["id_cupom"] = cupom.get("id_cupom")              

        logger.info(f"Dados enviados para o front-end.")
        return {"status":200,
                "resposta":{"tipo":"1","descricao":f"Dados enviados."},
                "dados": {
                    "id_usuario": id_usuario,
                    "id_cliente": id_cliente,
                    "ofertas": ofertas,
                    "orcamentos": carrinho_query,
                    "cupons": cupons
                }}, 200


    @logger
    @secure.auth_wrapper()
    def post(self, id_cliente = None):
        """
        Método POST do Orcamento do Usuario

        Returns:
            [dict]: Status da transação
        """
        
        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper=True, bool_save_request=False)

        if response_data.get("id_cliente") is None:
            response_data["id_cliente"] = id_cliente

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_columns = ["id_cliente"]

        no_use_columns = ["orcamentos"]

        correct_types = {
            "id_cliente": int,
            "orcamentos": list
        }

        # Checa chaves inválidas e faltantes, valores incorretos e nulos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          not_null = necessary_columns,
                                          no_use_columns = no_use_columns,
                                          correct_types = correct_types)):
            return validity

        # Verificando os dados de entrada
        orcamentos = response_data.get("orcamentos") if response_data.get("orcamentos") else []
        id_cliente = int(response_data.get("id_cliente"))
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        answer, response = dm.cliente_check(id_cliente)
        if not answer:
            return response

        query = """
            SELECT
                DISTINCT d.id_distribuidor

            FROM
                CLIENTE c

                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                    c.id_cliente = cd.id_cliente

                INNER JOIN DISTRIBUIDOR d ON
                    cd.id_distribuidor = d.id_distribuidor

            WHERE
                c.id_cliente = :id_cliente
                AND d.id_distribuidor != 0
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'
        """

        params = {
            "id_cliente": id_cliente
        }

        distribuidores = dm.raw_sql_return(query, params = params, raw = True)
        if not distribuidores:
            logger.error("Sem distribuidores cadastrados válidos para a operação.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Sem distribuidores cadastrados."}}, 200

        distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

        # Hold de falhas
        sucessos = len(orcamentos)
        falhas = 0
        falhas_hold = {}

        # Hold dos inserts
        insert_holder = []
        
        cupons = []
        cupons_validos = []
        ofertas = []
        distribuidores_orcamento = []
        orcamentos_id = {}
        orcamentos_check = {}

        # Verificação dos produtos enviados
        for orcamento in orcamentos:

            if isinstance(orcamento, dict):

                id_distribuidor = str(orcamento.get("id_distribuidor"))
                itens = orcamento.get("itens")
                id_cupom = orcamento.get("id_cupom")

                if isinstance(itens, list) and id_distribuidor.isdecimal():

                    id_distribuidor = int(id_distribuidor)

                    for item in itens:

                        if id_distribuidor not in distribuidores:
                            continue

                        id_produto = item.get("id_produto")
                        id_distribuidor = int(id_distribuidor)
                        quantidade = item.get("quantidade")
                        id_escalonado = item.get("id_escalonado")
                        id_campanha = item.get("id_campanha")
                        bonificado = item.get("bonificado")
                        preco_aplicado = item.get("preco_aplicado")
                        desconto = item.get("desconto")

                        if id_distribuidor not in distribuidores_orcamento:
                            distribuidores_orcamento.append(id_distribuidor)

                        produto = {
                            "id_produto": id_produto,
                            "quantidade": quantidade,
                            "id_distribuidor": id_distribuidor,
                            "preco_aplicado": preco_aplicado,
                            "desconto": None,
                            "id_campanha": None,
                            "id_escalonado": None,
                            "bonificado": None
                        }

                        if id_escalonado:
                            produto["id_escalonado"] = id_escalonado
                            produto["desconto"] = desconto

                            if id_escalonado not in ofertas:
                                ofertas.append(id_escalonado)                                

                        if id_campanha:
                            produto["id_campanha"] = id_campanha
                            produto["bonificado"] = bool(bonificado)
                            if id_campanha not in ofertas:
                                ofertas.append(id_campanha)

                        if id_cupom:

                            cupom_obj = {
                                "id_distribuidor": id_distribuidor,
                                "id_cupom": id_cupom,
                            }

                            if cupom_obj not in cupons:
                                cupons.append(cupom_obj)

                        if orcamentos_check.get(id_distribuidor):
                            orcamentos_check[id_distribuidor].append(produto)
                        else:
                            orcamentos_check[id_distribuidor] = [produto]

        # Verificação dos orcamentos existentes
        if distribuidores_orcamento:

            # Criando os orcamentos dos distribuidores caso necessário
            query = """
                INSERT INTO
                    orcamento
                        (
                            id_usuario,
                            id_distribuidor,
                            id_cliente,
                            status,
                            data_criacao,
                            d_e_l_e_t_,
                            id_cupom,
                            data_update
                        )

                    SELECT
                        :id_usuario,
                        d.id_distribuidor,
                        :id_cliente,
                        'A',
                        GETDATE(),
                        0,
                        NULL,
                        NULL
                            
                    FROM
                        DISTRIBUIDOR d

                    WHERE
                        d.id_distribuidor NOT IN (
                                                    SELECT
                                                        o.id_distribuidor

                                                    FROM
                                                        ORCAMENTO o

                                                    WHERE
                                                        o.id_usuario = :id_usuario
                                                        AND o.id_cliente = :id_cliente
                                                        AND o.d_e_l_e_t_ = 0
                                                        AND o.status = 'A'
                                                 )
                        AND d.id_distribuidor IN :distribuidores
                        AND d.id_distribuidor != 0
                        AND d.status = 'A'
            """

            params = {
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "distribuidores": distribuidores_orcamento,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            dm.raw_sql_execute(**dict_query)

            # Pegando os orcamentos existentes
            query = """
                SELECT
                    id_orcamento,
                    id_distribuidor,
                    data_criacao

                FROM
                    ORCAMENTO

                WHERE
                    id_usuario = :id_usuario
                    AND id_cliente = :id_cliente
                    AND id_distribuidor IN :distribuidores
                    AND d_e_l_e_t_ = 0
                    AND status = 'A'
            """

            params = {
                "id_cliente": id_cliente,
                "id_usuario": id_usuario,
                "distribuidores": distribuidores,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            response = dm.raw_sql_return(**dict_query)
            if not response:
                logger.error("Query de criação de orcamentos falhou. Por favor, verifica-la.")
                return {"status": 200,
                        "resposta": {
                            "tipo": "18", 
                            "descricao": "Erro na criação de orçamento. Por favor,  tente mais tarde."}}, 200

            orcamentos_id = {
                orcamento.get("id_distribuidor"): {
                    "id_orcamento": orcamento.get("id_orcamento"),
                    "id_distribuidor": orcamento.get("id_distribuidor")
                }
                for orcamento in response
            }

        # Checando os produtos
        if orcamentos_check:

            # Pegando os cupons salvos e os validando
            for cupom in cupons:
                
                dict_cupom = {
                    "id_cliente": id_cliente,
                    "id_cupom": cupom.get("id_cupom")
                }

                answer, response = dj.json_cupom_validado(**dict_cupom)
                if answer:
                    cupons_validos.append(cupom)

            # Checagem do carrinho

            ## Pegando as ofertas válidas
            query = """
                IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
                BEGIN
                    DROP TABLE #HOLD_OFERTA
                END;

                CREATE TABLE #HOLD_OFERTA (
                    id_oferta INT,
                    id_distribuidor INT,
                    tipo_oferta INT
                )               
            """

            dict_query = {
                "query": query,
                "commit": False
            }

            dm.raw_sql_execute(**dict_query)

            if ofertas:

                query = """
                    -- Salvando ofertas validas
                    INSERT INTO
                        #HOLD_OFERTA
                            (
                                id_oferta,
                                id_distribuidor,
                                tipo_oferta
                            )

                        SELECT
                            DISTINCT vom.id_oferta,
                            vom.id_distribuidor,
                            vom.tipo_oferta

                        FROM
                            (

                                SELECT
                                    o.id_oferta

                                FROM
                                    OFERTA o

                                WHERE
                                    o.id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE)

                                UNION

                                SELECT
                                    oc.id_oferta

                                FROM
                                    OFERTA_CLIENTE oc

                                WHERE
                                    oc.id_cliente = :id_cliente
                                    AND oc.status = 'A'
                                    AND oc.d_e_l_e_t_ = 0

                            ) offers

                            INNER JOIN VIEW_OFERTAS_MARKETPLACE vom ON
                                offers.id_oferta = vom.id_oferta

                        WHERE
                            vom.id_oferta IN :ofertas
                            AND vom.id_distribuidor IN :distribuidores
                            AND vom.tipo_oferta IN (2,3)
                """

                params = {
                    "id_cliente": id_cliente,
                    "ofertas": ofertas,
                    "distribuidores": distribuidores,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "commit": False
                }

                dm.raw_sql_execute(**dict_query)

            for id_distribuidor, produtos in orcamentos_check.items():

                # Checando os produtos a serem colocados no carrinho
                for index, produto in enumerate(produtos, start = 1):

                    id_produto = produto.get("id_produto")
                    bonificado = bool(produto.get("bonificado"))

                    if bonificado:
                        tipo_venda = 'B'

                    else:
                        tipo_venda = 'V'

                    # Checando a existencia do mesmo
                    query = """
                        SELECT
                            TOP 1 1

                        FROM
                            PRODUTO p

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                p.id_produto = pd.id_produto

                        WHERE
                            p.id_produto = :id_produto
                            AND pd.id_distribuidor = :id_distribuidor
                            AND LEN(p.sku) > 0
                            AND p.status = 'A'
                            AND pd.status = 'A'
                    """

                    params = {
                        "id_produto": id_produto,
                        "id_distribuidor": id_distribuidor,
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "raw": True,
                        "first": True,
                    }

                    response = dm.raw_sql_return(**dict_query)
                    if not response:  

                        if falhas_hold.get("produto_inexistente"):
                            falhas_hold["produto_inexistente"]['combinacao'].append({
                                'id_produto': id_produto,
                                'id_distribuidor': id_distribuidor
                            })
                            
                        else:
                            falhas_hold["produto_inexistente"] = {
                                'motivo': 'Produto não-existente para distribuidor informado.',
                                "combinacao": [{
                                    'id_produto': id_produto,
                                    'id_distribuidor': id_distribuidor
                                }]
                            }   
                            
                        continue                 

                    # Checando a quantidade enviada
                    try:
                        quantidade = int(produto.get("quantidade"))
                        if quantidade <= 0:
                            raise

                    except:
                        if falhas_hold.get("quantidade_invalida"):
                            falhas_hold["quantidade_invalida"]['combinacao'].append({
                                'id_produto': id_produto,
                                'id_distribuidor': id_distribuidor
                            })
                            
                        else:
                            falhas_hold["quantidade_invalida"] = {
                                'motivo': 'Quantidade não enviada ou menor ou igual a zero',
                                "combinacao": [{
                                    'id_produto': id_produto,
                                    'id_distribuidor': id_distribuidor
                                }]
                            }   
                            
                        continue

                    # Checando o estoque do produto
                    query = """
                        SELECT
                            TOP 1 1

                        FROM
                            PRODUTO_ESTOQUE

                        WHERE
                            id_produto = :id_produto
                            AND id_distribuidor = :id_distribuidor
                            AND qtd_estoque > 0
                    """

                    params = {
                        "id_produto": id_produto,
                        "id_distribuidor": id_distribuidor,
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "raw": True,
                        "first": True,
                    }

                    response = dm.raw_sql_return(**dict_query)
                    if not response:  

                        if falhas_hold.get("estoque_inexistente"):
                            falhas_hold["estoque_inexistente"]['combinacao'].append({
                                'id_produto': id_produto,
                                'id_distribuidor': id_distribuidor
                            })
                            
                        else:
                            falhas_hold["estoque_inexistente"] = {
                                'motivo': 'Produto sem estoque cadastrado',
                                "combinacao": [{
                                    'id_produto': id_produto,
                                    'id_distribuidor': id_distribuidor
                                }]
                            }   
                            
                        continue     

                    # Pegando o preco_tabela do produto
                    query = """
                        SELECT
                            TOP 1 MIN(tpp.preco_tabela) preco_tabela

                        FROM
                            TABELA_PRECO tp

                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                tp.id_tabela_preco = tpp.id_tabela_preco
                                AND tp.id_distribuidor = tpp.id_distribuidor

                        WHERE
                            tpp.id_produto = :id_produto
                            AND tp.id_distribuidor = :id_distribuidor
                            AND (
                                    (
                                        tp.tabela_padrao = 'S'
                                    )
                                        OR
                                    (
                                        tp.id_tabela_preco IN (
                                                                    SELECT
                                                                        id_tabela_preco

                                                                    FROM
                                                                        TABELA_PRECO_CLIENTE

                                                                    WHERE
                                                                        id_cliente = :id_cliente
                                                                        AND id_distribuidor = :id_distribuidor
                                                                        AND status = 'A'
                                                                )
                                    )
                                )
                            AND tp.dt_inicio <= GETDATE()
                            AND tp.dt_fim >= GETDATE()
                            AND tp.status = 'A'
                            

                        GROUP BY
                            tpp.id_produto,
                            tpp.id_distribuidor
                    """

                    params = {
                        "id_produto": id_produto,
                        "id_distribuidor": id_distribuidor,
                        "id_cliente": id_cliente,
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "raw": True,
                        "first": True,
                    }

                    response = dm.raw_sql_return(**dict_query)
                    if not response:  

                        if falhas_hold.get("preco_inexistente"):
                            falhas_hold["preco_inexistente"]['combinacao'].append({
                                'id_produto': id_produto,
                                'id_distribuidor': id_distribuidor
                            })
                            
                        else:
                            falhas_hold["preco_inexistente"] = {
                                'motivo': 'Produto sem preço associado',
                                "combinacao": [{
                                    'id_produto': id_produto,
                                    'id_distribuidor': id_distribuidor
                                }]
                            }   
                            
                        continue     

                    preco_tabela = response[0]

                    # Verificando se o produto possui desconto
                    query = """
                        SELECT
                            ISNULL(MAX(od.desconto), 0)

                        FROM
                            (

                                SELECT
                                    o.id_oferta

                                FROM
                                    OFERTA o

                                WHERE
                                    id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)
                                    AND o.tipo_oferta = 1

                                UNION

                                SELECT
                                    o.id_oferta

                                FROM
                                    OFERTA o

                                    INNER JOIN OFERTA_CLIENTE oc ON
                                        o.id_oferta = oc.id_oferta

                                WHERE
                                    oc.id_cliente = :id_cliente
                                    AND o.tipo_oferta = 1
                                    AND oc.status = 'A'
                                    AND oc.d_e_l_e_t_ = 0

                            ) offers

                            INNER JOIN OFERTA o ON
                                offers.id_oferta = o.id_oferta

                            INNER JOIN OFERTA_PRODUTO op ON
                                o.id_oferta = op.id_oferta

                            INNER JOIN OFERTA_DESCONTO od ON
                                o.id_oferta = od.id_oferta

                        WHERE
                            o.tipo_oferta = 1
                            AND op.id_produto = :id_produto
                            AND o.id_distribuidor = :id_distribuidor
                            AND o.limite_ativacao_oferta > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND o.status = 'A'
                            AND op.status = 'A'
                            AND od.status = 'A'
                    """

                    params = {
                        "id_produto": id_produto,
                        "id_distribuidor": id_distribuidor,
                        "id_cliente": id_cliente,
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "raw": True,
                        "first": True,
                    }

                    response = dm.raw_sql_return(**dict_query)
                    
                    desconto_interno = response[0]

                    if desconto_interno is None or desconto_interno <= 0: desconto_interno = 0
                    elif desconto_interno > 100: desconto_interno = 100

                    descontos_escalonado = []
                    desconto_escalonado = 0

                    # Verificando a oferta do produto
                    id_campanha = produto.get("id_campanha")
                    id_escalonado = produto.get("id_escalonado")

                    id_oferta = None
                    tipo_oferta = None

                    if id_campanha or id_escalonado:

                        if id_campanha:
                            id_oferta = id_campanha
                            tipo_oferta = 2

                        elif id_escalonado:
                            id_oferta = id_escalonado
                            tipo_oferta = 3

                        # Checando a existência da oferta
                        query = """
                            SELECT
                                TOP 1 1

                            FROM
                                #HOLD_OFERTA

                            WHERE
                                id_oferta = :id_oferta
                                AND id_distribuidor = :id_distribuidor
                                AND tipo_oferta = :tipo_oferta
                        """

                        params = {
                            "id_oferta": id_oferta,
                            "id_distribuidor": id_distribuidor,
                            "tipo_oferta": tipo_oferta,
                        }

                        dict_query = {
                            "query": query,
                            "params": params,
                            "raw": True,
                            "first": True,
                        }

                        response = dm.raw_sql_return(**dict_query)
                        if not response:

                            if falhas_hold.get("oferta_inexistente"):
                                falhas_hold["oferta_inexistente"]['combinacao'].append({
                                    'id_produto': id_produto,
                                    'id_distribuidor': id_distribuidor,
                                    "id_oferta": id_oferta,
                                })
                                
                            else:
                                falhas_hold["oferta_inexistente"] = {
                                    'motivo': 'Oferta inválido',
                                    "combinacao": [{
                                        'id_produto': id_produto,
                                        'id_distribuidor': id_distribuidor,
                                        "id_oferta": id_oferta,
                                    }]
                                }

                            continue

                        # Checando se o produto faz parte da oferta
                        if id_campanha and bonificado:

                            query = """
                                SELECT
                                    TOP 1 ISNULL(quantidade_bonificada, 0)

                                FROM
                                    OFERTA_BONIFICADO

                                WHERE
                                    id_produto = :id_produto
                                    AND id_oferta = :id_oferta
                                    AND status = 'A'
                            """

                            params = {
                                "id_produto": id_produto,
                                "id_oferta": id_oferta,
                            }

                            dict_query = {
                                "query": query,
                                "params": params,
                                "raw": True,
                                "first": True,
                            }

                            response = dm.raw_sql_return(**dict_query)
                            if not response:

                                if falhas_hold.get("produto_oferta_inexistente"):
                                    falhas_hold["produto_oferta_inexistente"]['combinacao'].append({
                                        'id_produto': id_produto,
                                        'id_distribuidor': id_distribuidor,
                                        "id_oferta": id_oferta,
                                    })
                                    
                                else:
                                    falhas_hold["produto_oferta_inexistente"] = {
                                        'motivo': 'Produto inexistente para oferta informada',
                                        "combinacao": [{
                                            'id_produto': id_produto,
                                            'id_distribuidor': id_distribuidor,
                                            "id_oferta": id_oferta,
                                        }]
                                    }

                                continue

                            quantidade_bonificada = response[0]

                            if quantidade != quantidade_bonificada:

                                if falhas_hold.get("oferta_campanha_quantidade"):
                                    falhas_hold["oferta_campanha_quantidade"]['combinacao'].append({
                                        'id_produto': id_produto,
                                        'id_distribuidor': id_distribuidor,
                                        "id_oferta": id_oferta,
                                    })
                                    
                                else:
                                    falhas_hold["oferta_campanha_quantidade"] = {
                                        'motivo': 'Produto bonificado da campanha com valor diferente do oferecido',
                                        "combinacao": [{
                                            'id_produto': id_produto,
                                            'id_distribuidor': id_distribuidor,
                                            "id_oferta": id_oferta,
                                        }]
                                    }

                                continue
                    
                        else:

                            query = """
                                SELECT
                                    TOP 1 1

                                FROM
                                    OFERTA_PRODUTO

                                WHERE
                                    id_produto = :id_produto
                                    AND id_oferta = :id_oferta
                                    AND status = 'A'
                            """

                            params = {
                                "id_produto": id_produto,
                                "id_oferta": id_oferta,
                            }

                            dict_query = {
                                "query": query,
                                "params": params,
                                "raw": True,
                                "first": True,
                            }

                            response = dm.raw_sql_return(**dict_query)
                            if not response:

                                if falhas_hold.get("produto_oferta_inexistente"):
                                    falhas_hold["produto_oferta_inexistente"]['combinacao'].append({
                                        'id_produto': id_produto,
                                        'id_distribuidor': id_distribuidor,
                                        "id_oferta": id_oferta,
                                    })
                                    
                                else:
                                    falhas_hold["produto_oferta_inexistente"] = {
                                        'motivo': 'Produto inexistente para oferta informada',
                                        "combinacao": [{
                                            'id_produto': id_produto,
                                            'id_distribuidor': id_distribuidor,
                                            "id_oferta": id_oferta,
                                        }]
                                    }

                                continue

                        # Pegando os descontos de escalonamento
                        if id_escalonado:

                            query = """
                                SELECT 
                                    desconto 
                                    
                                FROM 
                                    OFERTA_ESCALONADO_FAIXA 
                                    
                                WHERE 
                                    id_oferta = :id_oferta 
                                    AND status = 'A'
                            """

                            params = {
                                "id_oferta": id_oferta,
                            }

                            dict_query = {
                                "query": query,
                                "params": params,
                                "raw": True,
                            }

                            response = dm.raw_sql_return(**dict_query)
                            if response:

                                descontos_escalonado = set([desc[0]   for desc in response])

                    # Checando os descontos
                    desconto = produto.get("desconto")
                    tipo_desconto = None

                    if desconto:
                        if desconto in descontos_escalonado:
                            desconto_escalonado = desconto

                    if desconto_interno or desconto_escalonado:

                        if desconto_interno >= desconto_escalonado:
                            desconto = desconto_interno
                            tipo_desconto = 1

                        else:
                            desconto = desconto_escalonado
                            tipo_desconto = 2

                    # Salvando o produto
                    id_orcamento = orcamentos_id.get(id_distribuidor)["id_orcamento"]

                    insert_holder.append({
                        "ordem": index,
                        "id_orcamento": id_orcamento,
                        "id_cliente": id_cliente,
                        "id_produto": id_produto,
                        "id_distribuidor": id_distribuidor,
                        "quantidade": quantidade,
                        "tipo": tipo_venda,
                        "preco_tabela": preco_tabela,
                        "preco_venda": produto.get("preco_aplicado"),
                        "id_oferta": id_oferta,
                        "desconto_tipo": None if (not desconto and not desconto_escalonado) else tipo_desconto,
                        "desconto": desconto if desconto else None,
                        "desconto_escalonado": desconto_escalonado if desconto_escalonado else None,
                        "status": 'A',
                        "d_e_l_e_t_": 0,
                    })

            query = """
                IF Object_ID('tempDB..#HOLD_OFERTA','U') IS NOT NULL 
                BEGIN
                    DROP TABLE #HOLD_OFERTA
                END;
            """

            dict_query = {
                "query": query,
                "commit": False
            }

            dm.raw_sql_execute(**dict_query)
            
        # Deletando os dados antigos
        query = """
            DELETE FROM
                ORCAMENTO_ITEM

            WHERE
                id_orcamento IN (
                                    SELECT
                                        o.id_orcamento

                                    FROM
                                        ORCAMENTO o

                                    WHERE
                                        o.id_usuario = :id_usuario
                                        AND o.id_cliente = :id_cliente
                                        AND o.status = 'A'
                                        AND o.d_e_l_e_t_ = 0
                                )
        """

        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        dict_query = {
            "query": query,
            "params": params
        }

        dm.raw_sql_execute(**dict_query)

        # Realizando o insert
        if insert_holder:

            for prod_orcamento in insert_holder:
                prod_orcamento["data_criacao"] = data_hora
            
            dm.raw_sql_insert("ORCAMENTO_ITEM", insert_holder)

            if cupons_validos:

                update_cupons = []

                for row in orcamentos_id.values():
                    row["id_cupom"] = None

                    for cupom in cupons_validos:
                        if str(cupom.get("id_distribuidor")) == str(row.get("id_distribuidor")):
                            row["id_cupom"] = cupom.get("id_cupom")
                            update_cupons.append(row)
                            break

                key_columns = ["id_distribuidor", "id_orcamento"]

                dm.raw_sql_update("ORCAMENTO", update_cupons, key_columns)

        # Atualizando os cupons
        if not cupons_validos:

            query = """
                UPDATE
                    ORCAMENTO

                SET
                    id_cupom = NULL

                WHERE
                    id_orcamento IN (
                                        SELECT
                                            o.id_orcamento

                                        FROM
                                            ORCAMENTO o

                                        WHERE
                                            o.id_usuario = :id_usuario
                                            AND o.id_cliente = :id_cliente
                                            AND o.id_cupom IS NOT NULL
                                            AND o.status = 'A'
                                            AND o.d_e_l_e_t_ = 0
                                    )
            """

            params = {
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
            }

            dict_query = {
                "query": query,
                "params": params
            }

            dm.raw_sql_execute(**dict_query)

        if falhas > 0:

            logger.info(f"Houveram {falhas} falhas e {sucessos} sucessos na operacao do carrinho do usuario.")
            logger.error(f"Houveram {falhas} falhas e {sucessos} sucessos na operacao do carrinho do usuario.", extra = falhas_hold)
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Houveram erros com a transação",
                    "situacao": {
                        "sucessos": sucessos - falhas,
                        "falhas": falhas,
                        "descricao": falhas_hold}
                    }}, 200

        if not insert_holder:
            
            logger.info(f"Carrinho do usuario deletado")
            return {"status":200,
                    "resposta":{
                        "tipo": "17", 
                        "descricao": "Carrinho Deletado."
                    }}, 200

        # Pegando os orcamentos
        orcamento_query = """
            SELECT
                DISTINCT o.id_orcamento,
                o.id_distribuidor

            FROM
                ORCAMENTO o

                INNER JOIN ORCAMENTO_ITEM oi ON
                    o.id_orcamento = oi.id_orcamento
                    AND o.id_distribuidor = oi.id_distribuidor

            WHERE
                o.id_usuario = :id_usuario
                AND o.id_cliente = :id_cliente
                AND o.status = 'A'
                AND o.d_e_l_e_t_ = 0
                AND oi.status = 'A'
                AND oi.d_e_l_e_t_ = 0
        """

        params = {
            "id_cliente": id_cliente,
            "id_usuario": id_usuario
        }

        orcamento_query = dm.raw_sql_return(orcamento_query, params = params)

        if not orcamento_query:
            logger.info(f"Não foram realizadas modificacoes do carrinho do usuario")
            return {"status":200,
                    "resposta":{
                        "tipo":"16",
                        "descricao":f"Não houveram modificações na transação."
                    }}, 200

        logger.info(f"Carrinho do usuario foi salvo")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Carrinho modificado com sucesso"},
                "dados": {
                    "id_cliente": id_cliente,
                    "orcamentos": orcamento_query
                }}, 200