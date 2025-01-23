#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime, re

#=== Importações de módulos internos ===#
import functions.payment_management as pm
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class IntegracaoProtheusPedidos(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Recebimento de dados de pedido da integração do protheus do Marketplace

        Returns:
            [dict]: Status da transação
        """

        dict_request = {
            "values_upper": True,
            "bool_save_request": False,
            "trim_values": True
        }

        response_data = dm.get_request(**dict_request)

        necessary_keys = ["pedidos"]
        correct_types = {"pedidos": list}

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity[0], 200  

        pedidos = list(response_data.get("pedidos"))

        # Registro de falhas
        falhas = 0
        falhas_hold = {}
        total = len(pedidos)

        accepted_keys = ["pedido", "id_etapa", "id_sub_etapa", "data_entrega", "id_distribuidor", 
                            "cotacoes", "boletos"]

        update_pedido = []

        insert_pedido_financeiro = []
        update_pedido_financeiro = []

        insert_pedido_produto = []
        update_pedido_produto = []

        insert_pedido_jsl = []
        update_pedido_jsl = []

        # Pegando os distribuidores validos
        query = """
            SELECT
                id_distribuidor

            FROM
                DISTRIBUIDOR

            WHERE
                status = 'A'
                AND id_distribuidor != 0
        """

        dict_query = {
            "query": query,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            return {"status":200,
                    "resposta":{
                        "tipo":"15",
                        "descricao":f"Api sem distribuidores para realizar atualização."}}, 200

        distribuidores = [distribuidor[0] for distribuidor in response]

        # Pegando os status da etapa de pedido
        dict_status = {
            1: {
                1: {
                    "id_etapa": 1,
                    "id_subetapa": 2
                },
                2: {
                    "id_etapa": 2,
                    "id_subetapa": 1
                },
                3: {
                    "id_etapa": 2,
                    "id_subetapa": 1
                }
            },
            2: {
                1: {
                    "id_etapa": 3,
                    "id_subetapa": 1
                },
                2: {
                    "id_etapa": 3,
                    "id_subetapa": 1
                },
                3: {
                    "id_etapa": 3,
                    "id_subetapa": 1
                },
            },
            3: {
                1: {
                    "id_etapa": 3,
                    "id_subetapa": 3
                },
                2: {
                    "id_etapa": 3,
                    "id_subetapa": 3
                },
                3: {
                    "id_etapa": 3,
                    "id_subetapa": 3
                },
            },
            4: {
                1: {
                    "id_etapa": 4,
                    "id_subetapa": 1
                },
                2: {
                    "id_etapa": 4,
                    "id_subetapa": 3
                },
                3: {
                    "id_etapa": 4,
                    "id_subetapa": 3
                },
            }
        }
        
        data_hora = datetime.datetime.now()

        for pedido in pedidos:

            if pedido and isinstance(pedido, dict):
                
                data = {
                    key: pedido.get(key)
                    for key in accepted_keys
                }

                hold_cotacoes_insert = []
                hold_cotacoes_update = []
                hold_produtos_insert = []
                hold_produtos_update = []
                hold_boleto_insert = []
                hold_boleto_update = []

                id_distribuidor = data.get("id_distribuidor")
                id_pedido = data.get("pedido")

                # Checando o distribuidor
                try:
                    id_distribuidor = int(id_distribuidor)

                except:
                    falhas += 1

                    if falhas_hold.get("distribuidor_id_invalido"):
                        falhas_hold["distribuidor_id_invalido"]["combinacao"].append({
                            "id_distribuidor": data.get("id_distribuidor"),
                            "id_pedido": id_pedido,
                        })
                    
                    else:
                        falhas_hold["distribuidor_id_invalido"] = {
                            "motivo": f"Distribuidor com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": data.get("id_distribuidor"),
                                "id_pedido": id_pedido,
                            }],
                        }
                    continue

                if id_distribuidor not in distribuidores:

                    falhas += 1

                    if falhas_hold.get("distribuidor"):
                        falhas_hold["distribuidor"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "id_pedido": id_pedido,
                        })
                    
                    else:
                        falhas_hold[f"distribuidor"] = {
                            "descricao": f"Distribuidor não existente ou inativo",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "id_pedido": id_pedido,
                                }
                            ]
                        }
                    continue

                # Checando o pedido
                try:
                    id_pedido = int(id_pedido)

                except:
                    falhas += 1

                    if falhas_hold.get("id_pedido_invalido"):
                        falhas_hold["id_pedido_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_pedido": data.get("id_pedido"),
                        })
                    
                    else:
                        falhas_hold["id_pedido_invalido"] = {
                            "motivo": f"Pedido com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": data.get("id_pedido"),
                            }],
                        }
                    continue

                query = """
                    SELECT
                        TOP 1 id_pedido,
                        data_finalizacao,
                        forma_pagamento

                    FROM
                        PEDIDO

                    WHERE
                        id_pedido = :id_pedido
                        AND id_distribuidor = :id_distribuidor
                """

                params = {
                    "id_pedido": id_pedido,
                    "id_distribuidor": id_distribuidor
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True
                }

                pedido_query = dm.raw_sql_return(**dict_query)

                if not pedido_query:

                    falhas += 1

                    if falhas_hold.get("id_pedido"):
                        falhas_hold["id_pedido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_pedido": id_pedido,
                        })
                    
                    else:
                        falhas_hold["id_pedido"] = {
                            "motivo": f"Pedido inexistente.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": id_pedido,
                            }],
                        }
                    continue

                data_finalizacao = pedido_query.get("data_finalizacao")
                id_formapgto = pedido_query.get("forma_pagamento")

                dict_pedido = {}

                # Checando a etapa e subetapa
                id_etapa = data.get("id_etapa")
                id_subetapa = data.get("id_sub_etapa")

                try:
                    id_etapa = int(id_etapa)

                except:
                    falhas += 1

                    if falhas_hold.get("id_etapa_invalido"):
                        falhas_hold["id_etapa_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_pedido": id_pedido,
                            "id_etapa": data.get("id_etapa")
                        })
                    
                    else:
                        falhas_hold["id_etapa_invalido"] = {
                            "motivo": f"Etapa com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": id_pedido,
                                "id_etapa": data.get("id_etapa")
                            }],
                        }
                    continue

                if id_etapa not in {0,1,2,3,4,5}:

                    falhas += 1

                    if falhas_hold.get("id_etapa"):
                        falhas_hold["id_etapa"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_pedido": id_pedido,
                            "id_etapa": id_etapa
                        })
                    
                    else:
                        falhas_hold["id_etapa"] = {
                            "motivo": f"Etapa com valor não fora do escopo.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": id_pedido,
                                "id_etapa": id_etapa
                            }],
                        }
                    continue

                dict_etapa = {}

                if id_etapa == 5:
                    
                    dict_etapa = {
                        "id_subetapa": 9
                    }

                elif id_etapa in {1,2,3,4}:

                    try:
                        id_subetapa = int(id_subetapa)

                    except:
                        falhas += 1

                        if falhas_hold.get("id_subetapa_invalido"):
                            falhas_hold["id_subetapa_invalido"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": id_pedido,
                                "id_subetapa": data.get("id_subetapa")
                            })
                        
                        else:
                            falhas_hold["id_subetapa_invalido"] = {
                                "motivo": f"Subetapa com valor não-numérico.",
                                "combinacao": [{
                                    "id_distribuidor": id_distribuidor,
                                    "id_pedido": id_pedido,
                                    "id_subetapa": data.get("id_subetapa")
                                }],
                            }
                        continue

                    if id_subetapa <= 0: id_subetapa = 1
                    elif id_subetapa >= 3: id_subetapa = 3

                    dict_etapa = dict_status[id_etapa][id_subetapa]
                    
                dict_etapa.update({
                    "id_pedido": id_pedido,
                    "id_formapgto": id_formapgto
                })

                data_entrega = dict_pedido.get("data_entrega")

                if not data_entrega:

                    data_entrega = None

                else:

                    try:
                        datetime.datetime.strptime(data_entrega, "%Y-%m-%d")

                    except:
                        falhas += 1

                        if falhas_hold.get("data_entrega_invalida"):
                            falhas_hold["data_entrega_invalida"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": id_pedido,
                                "data_entrega": data_entrega,
                            })
                        
                        else:
                            falhas_hold["data_entrega_invalida"] = {
                                "motivo": f"Data de entrega inválida.",
                                "combinacao": [{
                                    "id_distribuidor": id_distribuidor,
                                    "id_pedido": id_pedido,
                                    "data_entrega": data_entrega,
                                }],
                            }
                        
                        continue

                if not data_finalizacao:

                    if str(id_etapa) == '4' and str(id_subetapa) in {'2','3'}:
                        
                        if not data_entrega:

                            falhas += 1

                            if falhas_hold.get("data_entrega"):
                                falhas_hold["data_entrega"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_pedido": id_pedido,
                                    "data_entrega": data_entrega,
                                })
                            
                            else:
                                falhas_hold["data_entrega"] = {
                                    "motivo": f"Data de pagamento não enviada mas pedido entregue.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_pedido": id_pedido,
                                        "data_entrega": data_entrega,
                                    }],
                                }
                            
                            continue

                        dict_pedido["data_entrega"] = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        dict_pedido["data_finalizacao"] = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                    if str(id_etapa) == '5':
                        dict_pedido["data_finalizacao"] = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                # Checando as cotações
                cotacoes = data.get("cotacoes")
                break_pedido = False

                if not (cotacoes and isinstance(cotacoes, list)):

                    falhas += 1

                    if falhas_hold.get("cotacoes"):
                        falhas_hold["cotacoes"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_pedido": id_pedido,
                            "cotacoes": data.get("cotacoes")
                        })
                    
                    else:
                        falhas_hold["cotacoes"] = {
                            "motivo": f"Cotacoes não enviadas ou com tipo inválido.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_pedido": id_pedido,
                                "cotacoes": data.get("cotacoes")
                            }],
                        }
                    continue

                for cotacao in cotacoes:

                    if cotacao and isinstance(cotacao, dict) and not break_pedido:
                    
                        dict_cotacao = {}

                        # Checando o codigo de cotacao e de pedido
                        cod_cotacao = cotacao.get("cotacao")
                        cod_pedido = cotacao.get('cod_pedido')

                        query = """
                            SELECT
                                tipo

                            FROM
                                PEDIDO_JSL

                            WHERE
                                id_pedido = :id_pedido
                                AND cod_pedido_jsl = :cod_pedido
                                AND cotacao = :cotacao
                        """

                        params = {
                            "id_pedido": id_pedido,
                            "cod_pedido": cod_pedido,
                            "cotacao": cod_cotacao,
                        }

                        dict_query = {
                            "query": query,
                            "params": params,
                            "raw": True,
                            "first": True,
                        }

                        check = dm.raw_sql_return(**dict_query)

                        # Checando a nota fiscal
                        nota_fiscal = cotacao.get("nota_fiscal")
                        nf_serie = cotacao.get("nf_serie")

                        if nota_fiscal and nf_serie:

                            nota_fiscal = re.sub("[^0-9]", "", nota_fiscal)
                            nf_serie = re.sub("[^0-9]", "", nf_serie)
                            
                            if not str(nota_fiscal).isdecimal():

                                falhas += 1

                                if falhas_hold.get("nota_fiscal"):
                                    falhas_hold["nota_fiscal"]["combinacao"].append({
                                        "id_distribuidor": id_distribuidor,
                                        "id_pedido": id_pedido,
                                        "cotacao": cod_cotacao,
                                        "nota_fiscal": nota_fiscal
                                    })
                                
                                else:
                                    falhas_hold["nota_fiscal"] = {
                                        "motivo": f"Nota fiscal inválida.",
                                        "combinacao": [{
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "cotacao": cod_cotacao,
                                            "nota_fiscal": nota_fiscal
                                        }],
                                    }

                                break_pedido = True
                                continue

                            if not str(nf_serie).isdecimal():

                                falhas += 1

                                if falhas_hold.get("nf_serie"):
                                    falhas_hold["nf_serie"]["combinacao"].append({
                                        "id_distribuidor": id_distribuidor,
                                        "id_pedido": id_pedido,
                                        "cotacao": cod_cotacao,
                                        "nf_serie": nf_serie
                                    })
                                
                                else:
                                    falhas_hold["nf_serie"] = {
                                        "motivo": f"Serie da nota fiscal inválida.",
                                        "combinacao": [{
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "cotacao": cod_cotacao,
                                            "nf_serie": nf_serie
                                        }],
                                    }

                                break_pedido = True
                                continue

                            dict_cotacao.update({
                                "nf": nota_fiscal,
                                "nf_serie": nf_serie,
                                "cod_nf": cotacao.get("cod_nf")
                            })

                        # Checando os produtos da cotacao
                        produtos = cotacao.get("produtos")
                        tipo = ""
                        break_pedido = False

                        if not (produtos and isinstance(produtos, list)):

                            falhas += 1

                            if falhas_hold.get("produtos"):
                                falhas_hold["produtos"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_pedido": id_pedido,
                                    "cotacao": cod_cotacao,
                                    "produtos": data.get("produtos")
                                })
                            
                            else:
                                falhas_hold["produtos"] = {
                                    "motivo": f"produtos não enviadas ou com tipo inválido.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_pedido": id_pedido,
                                        "cotacao": cod_cotacao,
                                        "produtos": data.get("produtos")
                                    }],
                                }

                            break_pedido = True
                            continue

                        for produto in produtos:

                            if produto and isinstance(produto, dict) and not break_pedido:

                                cod_prod_distr = produto.get("cod_prod_distr")
                                quantidade_faturada = produto.get("qtd_fat")
                                tipo_venda = str(produto.get("tipo_venda")).upper()[0]
                                preco_venda = produto.get("preco_unit")

                                tipo = tipo_venda
                                    
                                try:
                                    quantidade_faturada = int(quantidade_faturada)

                                except:
                                    falhas += 1

                                    if falhas_hold.get("quantidade_faturada_invalida"):
                                        falhas_hold["quantidade_faturada_invalida"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "cotacao": cod_cotacao,
                                            "cod_prod_distr": cod_prod_distr,
                                            "tipo_venda": produto.get("tipo_venda"),
                                            "qtd_fat": produto.get("qtd_fat")
                                        })
                                    
                                    else:
                                        falhas_hold["quantidade_faturada_invalida"] = {
                                            "motivo": f"Quantidade faturada do produto inválida ou inexistente.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "cotacao": cod_cotacao,
                                                "cod_prod_distr": cod_prod_distr,
                                                "tipo_venda": produto.get("tipo_venda"),
                                                "qtd_fat": produto.get("qtd_fat")
                                            }],
                                        }

                                    break_pedido = True
                                    continue

                                try:
                                    preco_venda = float(preco_venda)

                                except:
                                    falhas += 1

                                    if falhas_hold.get("preco_unitario_invalido"):
                                        falhas_hold["preco_unitario_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "cotacao": cod_cotacao,
                                            "cod_prod_distr": cod_prod_distr,
                                            "tipo_venda": produto.get("tipo_venda"),
                                            "preco_unit": produto.get("preco_unit")
                                        })
                                    
                                    else:
                                        falhas_hold["preco_unitario_invalido"] = {
                                            "motivo": f"Preco unitário do produto inválido ou inexistente.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "cotacao": cod_cotacao,
                                                "cod_prod_distr": cod_prod_distr,
                                                "tipo_venda": produto.get("tipo_venda"),
                                                "preco_unit": produto.get("preco_unit")
                                            }],
                                        }

                                    break_pedido = True
                                    continue

                                query = """
                                    SELECT
                                        TOP 1 id_produto,
                                        preco_produto

                                    FROM
                                        PEDIDO_ITEM

                                    WHERE
                                        id_pedido = :id_pedido
                                        AND cod_prod_distr = :cod_prod_distr
                                        AND tipo_venda = :tipo_venda
                                """

                                params = {
                                    "cod_prod_distr": cod_prod_distr,
                                    "id_pedido": id_pedido,
                                    "tipo_venda": tipo_venda
                                }

                                dict_query = {
                                    "query": query,
                                    "params": params,
                                    "raw": True,
                                    "first": True,
                                }

                                response = dm.raw_sql_return(**dict_query)

                                if not response:

                                    falhas += 1

                                    if falhas_hold.get("produto_inexistente"):
                                        falhas_hold["produto_inexistente"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "cotacao": cod_cotacao,
                                            "cod_prod_distr": cod_prod_distr
                                        })
                                    
                                    else:
                                        falhas_hold["produto_inexistente"] = {
                                            "motivo": f"Produto não associado ao pedido.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "cotacao": cod_cotacao,
                                                "cod_prod_distr": cod_prod_distr
                                            }],
                                        }

                                    break_pedido = True
                                    continue

                                id_produto = response[0]
                                preco_tabela = response[1]

                                if tipo_venda not in {"B", "V"}:

                                    falhas += 1

                                    if falhas_hold.get("tipo_venda_invalido"):
                                        falhas_hold["tipo_venda_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "cotacao": cod_cotacao,
                                            "cod_prod_distr": cod_prod_distr,
                                            "tipo_venda": produto.get("tipo_venda")
                                        })
                                    
                                    else:
                                        falhas_hold["tipo_venda_invalido"] = {
                                            "motivo": f"Tipo de venda inválido, deve ser 'V' ou 'B'.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "cotacao": cod_cotacao,
                                                "cod_prod_distr": cod_prod_distr,
                                                "tipo_venda": produto.get("tipo_venda")
                                            }],
                                        }

                                    break_pedido = True
                                    continue

                                # Checando se o produto já foi salvo
                                query = """
                                    SELECT
                                        1

                                    FROM
                                        PEDIDO_ITEM_RETORNO

                                    WHERE
                                        id_pedido = :id_pedido
                                        AND cod_prod_distr = :cod_prod_distr
                                        AND tipo_venda = :tipo_venda
                                """

                                params = {
                                    "id_pedido": id_pedido,
                                    "cod_prod_distr": cod_prod_distr,
                                    "tipo_venda": tipo_venda
                                }

                                dict_query = {
                                    "query": query,
                                    "params": params,
                                    "raw": True,
                                    "first": True,
                                }

                                response = dm.raw_sql_return(**dict_query)

                                if not response:
                                    hold_produtos_insert.append({
                                        "id_pedido": id_pedido,
                                        "id_distribuidor": id_distribuidor,
                                        "id_produto": id_produto,
                                        "cod_prod_distr": cod_prod_distr,
                                        "quantidade_faturada": quantidade_faturada,
                                        "preco_venda": preco_venda,
                                        "preco_tabela": preco_tabela,
                                        "tipo_venda": tipo_venda,
                                        "d_e_l_e_t_": 0
                                    })

                                else:
                                    hold_produtos_update.append({
                                        "id_pedido": id_pedido,
                                        "cod_prod_distr": cod_prod_distr,
                                        "tipo_venda": tipo_venda,
                                        "quantidade_faturada": quantidade_faturada,
                                        "preco_venda": preco_venda,
                                    })

                        if break_pedido:
                            break

                        if check:
                            
                            if dict_cotacao:

                                dict_cotacao.update({
                                    "id_pedido": id_pedido,
                                    "cotacao": cod_cotacao,
                                    "cod_pedido_jsl": cod_pedido,
                                    "tipo": tipo
                                })

                                hold_cotacoes_update.append(dict_cotacao)

                        else:
                            dict_cotacao.update({
                                "id_pedido": id_pedido,
                                "cotacao": cod_cotacao,
                                "cod_pedido_jsl": cod_pedido,
                                "tipo": tipo
                            })

                            hold_cotacoes_insert.append(dict_cotacao)

                # Checando os boletos
                if id_formapgto == 1:
                    boletos = data.get("boletos")

                    if boletos:

                        if not isinstance(boletos, list):

                            falhas += 1

                            if falhas_hold.get("boletos"):
                                falhas_hold["boletos"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_pedido": id_pedido,
                                    "boletos": data.get("boletos")
                                })
                            
                            else:
                                falhas_hold["boletos"] = {
                                    "motivo": f"Boletos com tipo inválido.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_pedido": id_pedido,
                                        "boletos": data.get("boletos")
                                    }],
                                }
                            continue

                        for boleto in boletos:

                            if boleto and isinstance(boleto, dict) and not break_pedido:

                                # Parcela
                                try:
                                    parcela = int(boleto.get("parcela"))

                                except:
                                    falhas += 1

                                    if falhas_hold.get("parcela_invalida"):
                                        falhas_hold["parcela_invalida"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": boleto.get("parcela"),
                                            "boleto": boleto,
                                        })
                                    
                                    else:
                                        falhas_hold["parcela_invalida"] = {
                                            "motivo": f"Boleto com parcela inválida.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": boleto.get("parcela"),
                                                "boleto": boleto,
                                            }],
                                        }
                                    
                                    break_pedido = True
                                    continue

                                # Codigo barra
                                codigo_barra = boleto.get("cod_bar")

                                if not codigo_barra:

                                    falhas += 1

                                    if falhas_hold.get("cod_barra_invalido"):
                                        falhas_hold["cod_barra_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": parcela,
                                            "cod_bar": boleto.get("cod_bar")
                                        })
                                    
                                    else:
                                        falhas_hold["cod_barra_invalido"] = {
                                            "motivo": f"Codigo barra não enviado.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "cod_bar": boleto.get("cod_bar")
                                            }],
                                        }

                                    break_pedido = True
                                    continue

                                # Valor boleto
                                try:
                                    valor_boleto = float(boleto.get("valor_boleto"))

                                except:
                                    falhas += 1

                                    if falhas_hold.get("valor_boleto_invalido"):
                                        falhas_hold["valor_boleto_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": parcela,
                                            "valor_boleto": produto.get("valor_boleto"),
                                        })
                                    
                                    else:
                                        falhas_hold["valor_boleto_invalido"] = {
                                            "motivo": f"Valor do boleto inválido.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "valor_boleto": produto.get("valor_boleto"),
                                            }],
                                        }
                                    
                                    break_pedido = True
                                    continue

                                # Valor restante
                                try:
                                    valor_restante = float(boleto.get("valor_restante"))

                                except:
                                    falhas += 1

                                    if falhas_hold.get("valor_restante_invalido"):
                                        falhas_hold["valor_restante_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": parcela,
                                            "valor_restante": produto.get("valor_restante"),
                                        })
                                    
                                    else:
                                        falhas_hold["valor_restante_invalido"] = {
                                            "motivo": f"Valor restante do boleto inválido.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "valor_restante": produto.get("valor_restante"),
                                            }],
                                        }
                                    
                                    break_pedido = True
                                    continue

                                # Dias atraso
                                try:
                                    dias_atraso = int(boleto.get("dias_atraso"))

                                except:
                                    falhas += 1

                                    if falhas_hold.get("dias_atraso_invalido"):
                                        falhas_hold["dias_atraso_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": parcela,
                                            "dias_atraso": produto.get("dias_atraso"),
                                        })
                                    
                                    else:
                                        falhas_hold["dias_atraso_invalido"] = {
                                            "motivo": f"Dias de atraso do boleto inválido.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "dias_atraso": produto.get("dias_atraso"),
                                            }],
                                        }
                                    
                                    break_pedido = True
                                    continue

                                # Titulo
                                titulo = boleto.get("titulo")

                                if not titulo:

                                    falhas += 1

                                    if falhas_hold.get("titulo_invalido"):
                                        falhas_hold["titulo_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": parcela,
                                            "titulo": boleto.get("titulo")
                                        })
                                    
                                    else:
                                        falhas_hold["titulo_invalido"] = {
                                            "motivo": f"Titulo não enviado.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "titulo": boleto.get("titulo")
                                            }],
                                        }

                                    break_pedido = True
                                    continue

                                # Data pagamento
                                data_pagamento = boleto.get("data_pagamento")

                                if not data_pagamento:
                                    data_pagamento = None

                                else:
                                    try:
                                        datetime.datetime.strptime(data_pagamento, "%Y-%m-%d")

                                    except:
                                        falhas += 1

                                        if falhas_hold.get("data_pagamento_invalido"):
                                            falhas_hold["data_pagamento_invalido"]["combinacao"].append({
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "data_pagamento": data_pagamento,
                                            })
                                        
                                        else:
                                            falhas_hold["data_pagamento_invalido"] = {
                                                "motivo": f"Data de pagamento inválida.",
                                                "combinacao": [{
                                                    "id_distribuidor": id_distribuidor,
                                                    "id_pedido": id_pedido,
                                                    "parcela": parcela,
                                                    "data_pagamento": data_pagamento,
                                                }],
                                            }
                                        
                                        break_pedido = True
                                        continue

                                # Data baixa
                                data_baixa = boleto.get("data_baixa")

                                if not data_baixa:
                                    data_baixa = None

                                else:
                                    try:
                                        datetime.datetime.strptime(data_baixa, "%Y-%m-%d")

                                    except:
                                        falhas += 1

                                        if falhas_hold.get("data_baixa_invalido"):
                                            falhas_hold["data_baixa_invalido"]["combinacao"].append({
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "data_baixa": data_baixa,
                                            })
                                        
                                        else:
                                            falhas_hold["data_baixa_invalido"] = {
                                                "motivo": f"Data de baixa inválida.",
                                                "combinacao": [{
                                                    "id_distribuidor": id_distribuidor,
                                                    "id_pedido": id_pedido,
                                                    "parcela": parcela,
                                                    "data_baixa": data_baixa,
                                                }],
                                            }
                                        
                                        break_pedido = True
                                        continue

                                # Data vencimento
                                data_vencimento = boleto.get("data_vencimento")

                                try:
                                    datetime.datetime.strptime(data_vencimento, "%Y-%m-%d")

                                except:
                                    falhas += 1

                                    if falhas_hold.get("data_vencimento_invalido"):
                                        falhas_hold["data_vencimento_invalido"]["combinacao"].append({
                                            "id_distribuidor": id_distribuidor,
                                            "id_pedido": id_pedido,
                                            "parcela": parcela,
                                            "data_vencimento": data_vencimento,
                                        })
                                    
                                    else:
                                        falhas_hold["data_vencimento_invalido"] = {
                                            "motivo": f"Data de vencimento inválida ou não-enviada.",
                                            "combinacao": [{
                                                "id_distribuidor": id_distribuidor,
                                                "id_pedido": id_pedido,
                                                "parcela": parcela,
                                                "data_vencimento": data_vencimento,
                                            }],
                                        }
                                    
                                    break_pedido = True
                                    continue

                                # Status Pagamento
                                if id_etapa == 5:
                                    status_pagamento = 'CANCELADO'

                                elif valor_restante == 0:
                                    status_pagamento = 'PAGO'

                                elif dias_atraso > 0:
                                    status_pagamento = 'EM ATRASO' 

                                else:
                                    status_pagamento = 'EM ABERTO'

                                # Checando se a parcela já existe
                                query = """
                                    SELECT
                                        TOP 1 1

                                    FROM
                                        PEDIDO_FINANCEIRO

                                    WHERE
                                        id_pedido = :id_pedido
                                        AND parcela = :parcela
                                """

                                params = {
                                    "id_pedido": id_pedido,
                                    "parcela": parcela
                                }

                                dict_query = {
                                    "query": query,
                                    "params": params,
                                    "raw": True,
                                    "first": True,
                                }

                                response = dm.raw_sql_return(**dict_query)

                                if not response:
                                    hold_boleto_insert.append({
                                        "id_pedido": id_pedido,
                                        "id_distribuidor": id_distribuidor,
                                        "parcela": parcela,
                                        "titulo": titulo,
                                        "codigo_barra": codigo_barra,
                                        "valor_boleto": valor_boleto,
                                        "valor_restante": valor_restante,
                                        "status_pagamento": status_pagamento,
                                        "dias_atraso": dias_atraso,
                                        "data_vencimento": data_vencimento,
                                        "data_pagamento": data_pagamento,
                                        "data_baixa": data_baixa,
                                    })

                                else:
                                    hold_boleto_update.append({
                                        "id_pedido": id_pedido,
                                        "parcela": parcela,
                                        "valor_restante": valor_restante,
                                        "status_pagamento": status_pagamento,
                                        "dias_atraso": dias_atraso,
                                        "data_vencimento": data_vencimento,
                                        "data_pagamento": data_pagamento,
                                        "data_baixa": data_baixa,
                                    })

                if break_pedido:
                    break

                pm.atualizar_etapa_pedido(**dict_etapa)

                if dict_pedido:

                    dict_pedido.update({
                        "id_pedido": id_pedido,
                        "id_distribuidor": id_distribuidor
                    })

                    if dict_pedido not in update_pedido:
                        update_pedido.append(dict_pedido)

                if hold_cotacoes_insert:
                    insert_pedido_jsl.extend(hold_cotacoes_insert)

                if hold_cotacoes_update:
                    update_pedido_jsl.extend(hold_cotacoes_update)

                if hold_produtos_insert:
                    insert_pedido_produto.extend(hold_produtos_insert)

                if hold_produtos_update:
                    update_pedido_produto.extend(hold_produtos_update)

                if hold_boleto_insert:
                    insert_pedido_financeiro.extend(hold_boleto_insert)

                if hold_boleto_update:
                    update_pedido_financeiro.extend(hold_boleto_update)

            else:
                falhas += 1

                if falhas_hold.get("objeto_invalido"):
                    falhas_hold["objeto_invalido"]["combinacao"].append({
                        "obj": pedido,
                    })
                
                else:
                    falhas_hold["objeto_invalido"] = {
                        "motivo": f"Objeto de verificação inválido.",
                        "combinacao": [{
                            "obj": pedido,
                        }],
                    }
                continue

        if update_pedido:
            key_columns = ["id_pedido", "id_distribuidor"]
            dm.raw_sql_update("PEDIDO", update_pedido, key_columns)

        if insert_pedido_produto:
            dm.raw_sql_insert("PEDIDO_ITEM_RETORNO", insert_pedido_produto)

        if update_pedido_produto:
            key_columns = ["id_pedido", "cod_prod_distr", "tipo_venda"]
            dm.raw_sql_update("PEDIDO_ITEM_RETORNO", update_pedido_produto, key_columns)

        if insert_pedido_jsl:
            dm.raw_sql_insert("PEDIDO_JSL", insert_pedido_jsl)

        if update_pedido_jsl:
            key_columns = ["id_pedido", "cotacao", "cod_pedido", "tipo"]
            dm.raw_sql_update("PEDIDO_JSL", update_pedido_jsl, key_columns)

        if insert_pedido_financeiro:
            dm.raw_sql_insert("PEDIDO_FINANCEIRO", insert_pedido_financeiro)

        if update_pedido_financeiro:
            key_columns = ["id_pedido", "parcela"]
            dm.raw_sql_update("PEDIDO_FINANCEIRO", update_pedido_financeiro, key_columns)

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