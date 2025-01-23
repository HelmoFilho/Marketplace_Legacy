#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class RegistroProdutoPrecoItem(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Registro de Preço dos produtos

        Returns:
            [dict]: Status da transação
        """        
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        response_data = dm.get_request(values_upper = True, trim_values = True, bool_save_request = False)

        necessary_keys = ["tabela_preco"]

        correct_types = {"tabela_preco": [list, dict]}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity[0], 200

        # Verificando os dados enviados
        tabelas_preco = response_data.get("tabela_preco")
        
        if type(tabelas_preco) is dict:
            tabelas_preco = [tabelas_preco]

        # Registro de falhas
        falhas = 0
        falhas_hold = {}
        total = len(tabelas_preco)

        accepted_keys = ["id_distribuidor", "cod_tabela", "descri_tabela", "data_inicio",
                            "data_fim", "tabela_padrao", "status", "tabela_preco_item"]
        
        item_accepted_keys = ["agrupamento_variante", "cod_prod_distr", "cod_frag_distr", 
                                "status", "preco_tabela"]

        insert_tabela_cabeca = []
        insert_tabela_itens = []

        update_tabela_cabeca = []
        update_tabela_itens = []

        for tabela_preco in tabelas_preco:

            if not isinstance(tabela_preco, dict):

                falhas += 1

                if falhas_hold.get("tabela_preco"):
                    falhas_hold["tabela_preco"]['combinacao'][0]["contador"] += 1
                
                else:
                    falhas_hold[f"tabela_preco"] = {
                        "descricao": f"cabeçalho da tabela de preco não está em formato válido (objeto|dicionário)",
                        "combinacao": [
                            {
                                "contador": 1,
                            }
                        ]
                    }
                continue

            to_insert_tabela_cabeca = []
            to_insert_tabela_itens = []

            to_update_tabela_cabeca = []
            to_update_tabela_itens = []

            break_flag = False

            # Verificação dos dados enviados
            copy_tabela = tabela_preco.copy()
            
            tabela_preco = {
                key: copy_tabela.get(key)
                for key in accepted_keys
            }

            # Verificação de valores nulos
            null = []
            for key in accepted_keys:
                if str(tabela_preco.get(key)) != "0":
                    if str(tabela_preco.get(key)).upper() != "FALSE":
                        if not bool(tabela_preco.get(key)):
                            null.append(key)
            
            if null:
                null = list(set(null))

                falhas += 1

                if falhas_hold.get("valores_nulos_tabela"):
                    falhas_hold["valores_nulos_tabela"]['combinacao'].append({
                        "id_distribuidor": tabela_preco.get("id_distribuidor"),
                        "cod_tabela": tabela_preco.get("cod_tabela"),
                        "chaves": null
                    })
                
                else:
                    falhas_hold[f"valores_nulos_tabela"] = {
                        "descricao": f"Chaves com valor vazio ou nulo",
                        "combinacao": [
                            {
                                "id_distribuidor": tabela_preco.get("id_distribuidor"),
                                "cod_tabela": tabela_preco.get("cod_tabela"),
                                "chaves": null
                            }
                        ]
                    }
                continue

            # id_distribuidor
            id_distribuidor = tabela_preco.get("id_distribuidor")
            cod_tabela = str(tabela_preco.get("cod_tabela"))

            try:
                id_distribuidor = int(id_distribuidor)

            except:
                falhas += 1

                if falhas_hold.get("distribuidor_id_invalido"):
                    falhas_hold["distribuidor_id_invalido"]["combinacao"].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela
                    })
                
                else:
                    falhas_hold["distribuidor_id_invalido"] = {
                        "motivo": f"Distribuidor com valor não-numérico.",
                        "combinacao": [{
                            "id_distribuidor": id_distribuidor,
                            "cod_tabela": cod_tabela
                        }],
                    }
                continue
            
            if id_distribuidor == 0:
                falhas += 1

                if falhas_hold.get("distribuidor_0"):
                    falhas_hold["distribuidor_0"]["combinacao"].append({
                        "id_distribuidor": 0,
                        "cod_tabela": cod_tabela
                    })
                
                else:
                    falhas_hold["distribuidor_0"] = {
                        "motivo": f"Distribuidor 0 não pode cadastrar produtos",
                        "combinacao": [{
                            "id_distribuidor": 0,
                            "cod_tabela": cod_tabela
                        }],
                    }
                continue

            # Checando a existencia do distribuidor
            distri_query = f"""
                SELECT
                    TOP 1 id_distribuidor

                FROM
                    DISTRIBUIDOR
                
                WHERE
                    id_distribuidor = :id_distribuidor
                    AND status = 'A'
            """

            params = {
                "id_distribuidor": id_distribuidor
            }

            distri_query = dm.raw_sql_return(distri_query, params = params, raw = True, first = True)

            if not distri_query:

                falhas += 1

                if falhas_hold.get("distribuidor"):
                    falhas_hold["distribuidor"]['combinacao'].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela
                    })
                
                else:
                    falhas_hold[f"distribuidor"] = {
                        "descricao": f"Distribuidor não existente ou inativo",
                        "combinacao": [
                            {
                                "id_distribuidor": id_distribuidor,
                                "cod_tabela": cod_tabela
                            }
                        ]
                    }
                continue
                
            # Status
            status = str(tabela_preco.get("status")).upper() if tabela_preco.get("status") else None
            tabela_preco["status"] = 'I' if status in ["I", None] else 'A'

            # tabela padrao
            padrao = str(tabela_preco.get("tabela_padrao")).upper()

            # data_inicio
            data_inicio = str(tabela_preco.get("data_inicio"))
            
            try:
                data_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
                data_inicio = data_inicio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            except:
                falhas += 1

                if falhas_hold.get("data_inicio"):
                    falhas_hold["data_inicio"]["combinacao"].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela,
                        "data_inicio": data_inicio
                    })

                falhas_hold[f"falha {falhas}"] = {
                    "motivo": f"Chave ['data_inicio'] com valor inválido ou nulo.",
                    "combinacao": [{
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela,
                        "data_inicio": data_inicio
                    }]
                }
                continue

            # data_fim
            data_fim = str(tabela_preco.get("data_fim"))
            
            try:
                data_fim = datetime.strptime(data_fim, "%Y-%m-%d")
                data_fim = data_fim.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            except:
                falhas += 1

                if falhas_hold.get("data_fim"):
                    falhas_hold["data_fim"]["combinacao"].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela,
                        "data_fim": data_fim
                    })

                falhas_hold[f"falha {falhas}"] = {
                    "motivo": f"Chave ['data_fim'] com valor inválido ou nulo.",
                    "combinacao": [{
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela,
                        "data_fim": data_fim
                    }]
                }
                continue

            # tabela_preco_item
            tabela_preco_item = tabela_preco.get("tabela_preco_item")

            if not isinstance(tabela_preco_item, (list, dict)):

                falhas += 1

                if falhas_hold.get("tabela_preco_item"):
                    falhas_hold["tabela_preco_item"]["combinacao"].append({
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela
                    })

                falhas_hold[f"falha {falhas}"] = {
                    "motivo": f"Chave ['tabela_preco_item'] com valor inválido ou nulo.",
                    "combinacao": [{
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela
                    }]
                }
                continue

            if type(tabela_preco_item) is dict:
                tabela_preco_item = [tabela_preco_item]

            # Verificando se o cabeçalho já está cadastrado
            query = """
                SELECT 
                    cod_tabela

                FROM 
                    API_TABELA_PRECO

                WHERE
                    id_distribuidor = :id_distribuidor
                    AND cod_tabela = :cod_tabela 
            """

            params = {
                "id_distribuidor": id_distribuidor,
                "cod_tabela": cod_tabela
            }

            query = dm.raw_sql_return(query, params = params)

            if query:
                to_update_tabela_cabeca.append({
                    "descri_tabela": str(tabela_preco.get("descri_tabela")).upper(),
                    "tabela_padrao": padrao,
                    "status": tabela_preco.get("status"),
                    "dt_update": data_hora,
                    "dt_inicio": data_inicio,
                    "dt_fim": data_fim,
                    "id_distribuidor": id_distribuidor,
                    "cod_tabela": cod_tabela
                })

            else:
                to_insert_tabela_cabeca.append({
                    "descri_tabela": str(tabela_preco.get("descri_tabela")).upper(),
                    "tabela_padrao": padrao,
                    "status": tabela_preco.get("status"),
                    "dt_insert": data_hora,
                    "dt_inicio": data_inicio,
                    "dt_fim": data_fim,
                    "id_distribuidor": id_distribuidor,
                    "cod_tabela": cod_tabela
                })

            total += len(tabela_preco_item)

            for item in tabela_preco_item:

                if item:

                    if not isinstance(tabela_preco, dict):

                        falhas += 1

                        if falhas_hold.get("tabela_preco"):
                            falhas_hold["tabela_preco"]['combinacao'][0]["contador"] += 1
                            falhas_hold["tabela_preco"]['combinacao'].append({
                                "cod_tabela": cod_tabela,
                                "id_distribuidor": id_distribuidor
                            })
                        
                        else:
                            falhas_hold[f"tabela_preco"] = {
                                "descricao": f"cabeçalho da tabela de preco não está em formato válido (objeto|dicionário)",
                                "combinacao": [
                                    {
                                        "contador": 1,
                                    },
                                    {
                                        "cod_tabela": cod_tabela,
                                        "id_distribuidor": id_distribuidor
                                    }
                                ]
                            }
                        break_flag = True
                        break

                    # Verificação dos dados enviados
                    copy_item = item.copy()

                    item = {
                        key: copy_item.get(key)
                        for key in item_accepted_keys
                    }

                    cod_prod_distr = item.get("cod_prod_distr")

                    # Verificação de valores nulos
                    null = []
                    for key in item_accepted_keys:
                        if str(item.get(key)) != "0":
                            if str(item.get(key)).upper() != "FALSE":
                                if not bool(item.get(key)):
                                    null.append(key)
                    
                    if null:
                        null = list(set(null))

                        falhas += 1

                        if falhas_hold.get("valores_nulos_item"):
                            falhas_hold["valores_nulos_item"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_tabela": cod_tabela,
                                "cod_prod_distr": cod_prod_distr,
                                "chaves": null
                            })
                        
                        else:
                            falhas_hold[f"valores_nulos_item"] = {
                                "descricao": f"Chaves com valor vazio ou nulo",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cod_tabela": cod_tabela,
                                        "cod_prod_distr": cod_prod_distr,
                                        "chaves": null
                                    }
                                ]
                            }
                        break_flag = True
                        break

                    # cod_prod_distr
                    agrupamento_variante = str(item.get("agrupamento_variante"))
                    cod_frag_distr = str(item.get("cod_frag_distr"))

                    if cod_prod_distr != f"{agrupamento_variante}{cod_frag_distr}":
                        falhas += 1

                        if falhas_hold.get("cod_prod_distr"):
                            falhas_hold["cod_prod_distr"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_tabela": cod_tabela,
                                "cod_prod_distr": cod_prod_distr
                            })

                        falhas_hold["cod_prod_distr"] = {
                            "motivo": f"Chaves 'agrupamento_variante' e 'cod_frag_distr' com valores diferentes de 'cod_prod_distr'",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_tabela": cod_tabela,
                                "cod_prod_distr": cod_prod_distr
                            }]
                        }
                        break_flag = True
                        break

                    # Status
                    status = str(tabela_preco.get("status")).upper() if tabela_preco.get("status") else None
                    item["status"] = 'I' if status in ["I", None] else 'A'

                    # preco_tabela
                    try:
                        preco_tabela = float(item.get("preco_tabela"))

                    except:
                        falhas += 1

                        if falhas_hold.get("preco_invalido_item"):
                            falhas_hold["preco_invalido_item"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_tabela": cod_tabela,
                                "cod_prod_distr": cod_prod_distr,
                                "preco_tabela": item.get("preco_tabela"),
                            })
                        
                        else:
                            falhas_hold[f"preco_invalido_item"] = {
                                "descricao": f"Item com preço inválido",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cod_tabela": cod_tabela,
                                        "cod_prod_distr": cod_prod_distr,
                                        "preco_tabela": item.get("preco_tabela"),
                                    }
                                ]
                            }
                        break_flag = True
                        break

                    # Verificando existencia do produto na tabela
                    query = """
                        SELECT 
                            COD_PROD_DISTR

                        FROM 
                            API_TABELA_PRECO_ITEM

                        WHERE
                            ID_DISTRIBUIDOR = :id_distribuidor
                            AND COD_TABELA = :cod_tabela 
                            AND COD_PROD_DISTR = :cod_prod_distr
                    """

                    params = {
                        "id_distribuidor": id_distribuidor,
                        "cod_tabela": cod_tabela,
                        "cod_prod_distr": cod_prod_distr
                    }

                    query = dm.raw_sql_return(query, params = params)

                    if query:
                        to_update_tabela_itens.append({
                            "id_distribuidor": id_distribuidor,
                            "cod_tabela": cod_tabela,
                            "cod_prod_distr": cod_prod_distr,
                            "dt_update": data_hora,
                            "status": item.get("status"),
                            "preco_tabela": preco_tabela,
                        })

                    else:
                        to_insert_tabela_itens.append({
                            "id_distribuidor": id_distribuidor,
                            "cod_tabela": cod_tabela,
                            "agrupamento_variante": agrupamento_variante,
                            "cod_prod_distr": cod_prod_distr,
                            "cod_frag_distr": cod_frag_distr,
                            "status": item.get("status"),
                            "dt_insert": data_hora,
                            "preco_tabela": preco_tabela,
                        })

            if break_flag:
                continue

            if to_insert_tabela_cabeca:
                insert_tabela_cabeca.extend(to_insert_tabela_cabeca)

            if to_insert_tabela_itens:
                insert_tabela_itens.extend(to_insert_tabela_itens)

            if to_update_tabela_cabeca:
                update_tabela_cabeca.extend(to_update_tabela_cabeca)

            if to_update_tabela_itens:
                update_tabela_itens.extend(to_update_tabela_itens)

        # Realizando os commits
        if insert_tabela_cabeca:
            dm.raw_sql_insert("API_TABELA_PRECO", insert_tabela_cabeca)

        if insert_tabela_itens:
            dm.raw_sql_insert("API_TABELA_PRECO_ITEM", insert_tabela_itens)

        if update_tabela_cabeca:
            key_columns = ["id_distribuidor", "cod_tabela"]
            dm.raw_sql_update("API_TABELA_PRECO", update_tabela_cabeca, key_columns)

        if update_tabela_itens:
            key_columns = ["id_distribuidor", "cod_prod_distr", "cod_tabela"]
            dm.raw_sql_update("API_TABELA_PRECO_ITEM", update_tabela_itens, key_columns)

        if any([insert_tabela_cabeca, insert_tabela_itens, update_tabela_cabeca, update_tabela_itens]):

            query = f"""

                -- Salvando tabela preco
                INSERT 
                    INTO TABELA_PRECO
                    
                    SELECT  
                        ID_DISTRIBUIDOR,
                        COD_TABELA ID_TABELA_PRECO,
                        DESCRI_TABELA NOME_TABELA_PRECO,
                        TABELA_PADRAO,
                        STATUS,
                        DT_INICIO,
                        DT_FIM,
                        GETDATE() DT_CADASTRO,
                        GETDATE() DT_UPDATE
                    
                    FROM 
                        API_TABELA_PRECO A
                    
                    WHERE 
                        NOT EXISTS (
                            SELECT 
                                1 
                            
                            FROM 
                                TABELA_PRECO B
                                        
                            WHERE 
                                B.ID_TABELA_PRECO = A.COD_TABELA
                                AND B.id_distribuidor = A.ID_DISTRIBUIDOR
                        );


                -- Atualizar tabela_preco integrada pelo distribuidor
                UPDATE 
                    A
                
                SET 		
                    A.NOME_TABELA_PRECO = B.DESCRI_TABELA,
                    A.TABELA_PADRAO = B.TABELA_PADRAO,
                    A.STATUS = B.STATUS,
                    A.DT_INICIO = B.DT_INICIO,
                    A.DT_FIM = B.DT_FIM,
                    A.DT_UPDATE = GETDATE()
                
                FROM
                    TABELA_PRECO AS A
                    
                    INNER JOIN API_TABELA_PRECO AS B ON 
                        A.ID_TABELA_PRECO = B.COD_TABELA
                        AND A.id_distribuidor = B.ID_DISTRIBUIDOR 
                
                WHERE 
                    1 = 1;

                --Importar tabela_preco_produto caso nao tenha
                INSERT INTO 
                    TABELA_PRECO_PRODUTO
                    
                    SELECT  
                        A.ID_DISTRIBUIDOR,
                        A.COD_TABELA ID_TABELA_PRECO,
                        C.ID_PRODUTO,
                        A.PRECO_TABELA,
                        GETDATE() DT_CADASTRO,
                        GETDATE() DT_UPDATE
                    
                    FROM 
                        API_TABELA_PRECO_ITEM A
                    
                        INNER JOIN PRODUTO_DISTRIBUIDOR C ON 
                            C.AGRUPAMENTO_VARIANTE = A.AGRUPAMENTO_VARIANTE
                            AND C.COD_PROD_DISTR=A.COD_PROD_DISTR
                            AND C.ID_DISTRIBUIDOR=A.ID_DISTRIBUIDOR
                    
                    WHERE 
                        NOT EXISTS (
                            SELECT 
                                1 
                                
                            FROM 
                                TABELA_PRECO_PRODUTO B
                                        
                            WHERE 
                                B.ID_TABELA_PRECO = A.COD_TABELA
                                AND B.id_distribuidor = A.ID_DISTRIBUIDOR
                                AND B.ID_PRODUTO = C.ID_PRODUTO
                        );

                --Atualizar tabela_preco_produto integrada pelo distribuidor
                UPDATE 
                    A
                
                SET 		
                    A.PRECO_TABELA = B.PRECO_TABELA,
                    A.DT_UPDATE = GETDATE() 
                
                FROM
                    TABELA_PRECO_PRODUTO AS A
                    INNER JOIN (
                        SELECT	
                            A.ID_DISTRIBUIDOR, 
                            A.COD_TABELA ID_TABELA_PRECO, 
                            C.ID_PRODUTO, 
                            A.PRECO_TABELA
                                    
                        FROM 
                            API_TABELA_PRECO_ITEM A
                            INNER JOIN PRODUTO_DISTRIBUIDOR C ON 
                                C.AGRUPAMENTO_VARIANTE = A.AGRUPAMENTO_VARIANTE
                                AND C.COD_PROD_DISTR = A.COD_PROD_DISTR
                                AND C.ID_DISTRIBUIDOR = A.ID_DISTRIBUIDOR
                                    
                        WHERE 
                            1 = 1

                    ) AS B ON 
                        A.ID_TABELA_PRECO = B.ID_TABELA_PRECO
                        AND A.id_distribuidor = B.ID_DISTRIBUIDOR 
                        AND A.ID_PRODUTO = B.ID_PRODUTO 
                
                WHERE 
                    A.PRECO_TABELA <> B.PRECO_TABELA;
            """

            dm.raw_sql_execute(query)

        sucessos = total - falhas
        
        if falhas == 0:
            logger.info("Todas as transacoes ocorreram sem problemas")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados para a administração para análise."},
                    "situacao": {
                        "sucessos": sucessos,
                        "falhas": falhas,
                        "descricao": falhas_hold}}, 200
        
        logger.info(f"Ocorreram {falhas} falhas e {sucessos} sucessos durante a transacao")
        return {"status":200,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Ocorreram erros na transação."},
                "situacao": {
                    "sucessos": sucessos,
                    "falhas": falhas,
                    "descricao": falhas_hold}}, 200