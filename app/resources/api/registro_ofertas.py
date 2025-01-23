#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime, re

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger 

class RegistroOfertas(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Registro de ofertas do marketplace

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(values_upper=True, bool_save_request = False)

        necessary_keys = ["ofertas"]
        correct_types = {"ofertas": list}

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity[0], 200  

        ofertas = list(response_data.get("ofertas"))

        # Registro de falhas
        falhas = 0
        falhas_hold = {}
        total = len(ofertas)

        accepted_keys = ["id_oferta", "descricao_oferta", "id_distribuidor", "tipo_oferta", "ordem",
                            "operador", "data_inicio", "data_fim", "necessario_para_ativar",
                            "limite_ativacao_cliente", "limite_ativacao_oferta", "produto_agrupado",
                            "status", "produtos", "escalonamento", "clientes"]

        not_null = ["id_oferta", "descricao_oferta", "id_distribuidor", "tipo_oferta", "ordem",
                        "operador", "data_inicio", "data_fim", "necessario_para_ativar",
                        "limite_ativacao_cliente", "limite_ativacao_oferta", "produto_agrupado",
                        "status", "produtos"]

        insert_oferta = []
        update_oferta = []

        insert_produto_oferta = []
        update_produto_oferta = []

        insert_escalonado_oferta = []
        update_escalonado_oferta = []

        insert_bonificado_oferta = []
        update_bonificado_oferta = []

        insert_cliente_oferta = []

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
                        "descricao":f"Api sem distribuidores para realizar cadastro."}}, 200

        distribuidores = [distribuidor[0] for distribuidor in response]

        data_hora = datetime.datetime.now()

        def check_null(check_dict: dict, not_null: list):

            null = []
            for key in not_null:
                if str(check_dict.get(key)) != "0":
                    if str(check_dict.get(key)).upper() != "FALSE":
                        if not bool(check_dict.get(key)):
                            null.append(key)

            return null

        for oferta in ofertas:

            if oferta and isinstance(oferta, dict):

                data = {
                    key: oferta.get(key)
                    for key in accepted_keys
                }

                break_tag = False

                to_insert_oferta = {}
                to_update_oferta = {}

                to_insert_produto_oferta = []
                to_update_produto_oferta = []

                to_insert_escalonado_oferta = []
                to_update_escalonado_oferta = []

                to_insert_bonificado_oferta = []
                to_update_bonificado_oferta = []

                to_insert_cliente_oferta = []

                id_distribuidor = data.get("id_distribuidor")
                id_oferta_api = data.get("id_oferta")

                # Checando dados nulos de oferta
                if (null := check_null(data, not_null)):
                    null = list(set(null))

                    falhas += 1

                    if falhas_hold.get("cadastro_usuario"):
                        falhas_hold["cadastro_usuario"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "chaves": null
                        })
                    
                    else:
                        falhas_hold[f"cadastro_usuario"] = {
                            "descricao": f"Chaves com valor vazio ou nulo para realizar cadastro do oferta na base.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                    "chaves": null
                                }
                            ]
                        }
                    continue

                # Checando o distribuidor
                try:
                    id_distribuidor = int(id_distribuidor)

                except:
                    falhas += 1

                    if falhas_hold.get("distribuidor_id_invalido"):
                        falhas_hold["distribuidor_id_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                        })
                    
                    else:
                        falhas_hold["distribuidor_id_invalido"] = {
                            "motivo": f"Distribuidor com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                            }],
                        }
                    continue

                if id_distribuidor not in distribuidores:

                    falhas += 1

                    if falhas_hold.get("distribuidor"):
                        falhas_hold["distribuidor"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                        })
                    
                    else:
                        falhas_hold[f"distribuidor"] = {
                            "descricao": f"Distribuidor não existente ou inativo",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                }
                            ]
                        }
                    continue

                # Checando o tipo_oferta
                try:
                    tipo_oferta = int(data.get("tipo_oferta"))

                except:
                    falhas += 1

                    if falhas_hold.get("tipo_oferta"):
                        falhas_hold["tipo_oferta"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "tipo_oferta": data.get("id_oferta"),
                        })
                    
                    else:
                        falhas_hold["tipo_oferta"] = {
                            "motivo": f"Tipo oferta com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "tipo_oferta": data.get("id_oferta"),
                            }],
                        }
                    continue

                if tipo_oferta not in {1,2,3}:

                    falhas += 1

                    if falhas_hold.get("tipo_oferta_invalido"):
                        falhas_hold["tipo_oferta_invalido"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "tipo_oferta": tipo_oferta
                        })
                    
                    else:
                        falhas_hold[f"tipo_oferta_invalido"] = {
                            "descricao": f"Tipo oferta entre os não registráveis.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                    "tipo_oferta": tipo_oferta
                                }
                            ]
                        }
                    continue

                # Checando o operador
                operador = str(data.get("operador")).upper()

                if operador not in {"Q", "V"}:
                    falhas += 1

                    if falhas_hold.get("operador"):
                        falhas_hold["operador"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "operador": data.get("operador"),
                        })
                    
                    else:
                        falhas_hold["operador"] = {
                            "motivo": f"Tipo oferta com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "operador": data.get("operador"),
                            }],
                        }
                    continue

                # Checando a data_inicio
                data_inicio = str(data.get("data_inicio"))
                data_inicio = " ".join(re.sub("T|Z", " ", data_inicio).split())

                try:
                    data_inicio = datetime.datetime.strptime(data_inicio, "%Y-%m-%d %H:%M:%S.%f")

                except:
                    falhas += 1

                    if falhas_hold.get("data_inicio"):
                        falhas_hold["data_inicio"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "data_inicio": data.get("data_inicio"),
                        })
                    
                    else:
                        falhas_hold["data_inicio"] = {
                            "motivo": f"Data inicio é inválida.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "data_inicio": data.get("data_inicio"),
                            }],
                        }
                    continue

                # Checando a data_fim
                data_fim = str(data.get("data_fim"))
                data_fim = " ".join(re.sub("T|Z", " ", data_fim).split())

                try:
                    data_fim = datetime.datetime.strptime(data_fim, "%Y-%m-%d %H:%M:%S.%f")

                except:
                    falhas += 1

                    if falhas_hold.get("data_fim"):
                        falhas_hold["data_fim"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "data_fim": data.get("data_fim"),
                        })
                    
                    else:
                        falhas_hold["data_fim"] = {
                            "motivo": f"Data fim é inválida.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "data_fim": data.get("data_fim"),
                            }],
                        }
                    continue

                # Checando ordem
                try:
                    ordem = int(data.get("ordem"))

                except:
                    falhas += 1

                    if falhas_hold.get("ordem"):
                        falhas_hold["ordem"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "ordem": data.get("ordem")
                        })
                    
                    else:
                        falhas_hold["ordem"] = {
                            "motivo": f"Ordem com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "ordem": data.get("ordem")
                            }],
                        }
                    continue

                # Checando necessario_para_ativar
                try:
                    necessario_para_ativar = float(data.get("necessario_para_ativar"))

                except:
                    falhas += 1

                    if falhas_hold.get("necessario_para_ativar"):
                        falhas_hold["necessario_para_ativar"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "necessario_para_ativar": data.get("necessario_para_ativar")
                        })
                    
                    else:
                        falhas_hold["necessario_para_ativar"] = {
                            "motivo": f"necessario_para_ativar com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "necessario_para_ativar": data.get("necessario_para_ativar")
                            }],
                        }
                    continue

                # Checando limite_ativacao_cliente
                try:
                    limite_ativacao_cliente = int(data.get("limite_ativacao_cliente"))

                except:
                    falhas += 1

                    if falhas_hold.get("limite_ativacao_cliente"):
                        falhas_hold["limite_ativacao_cliente"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "limite_ativacao_cliente": data.get("limite_ativacao_cliente")
                        })
                    
                    else:
                        falhas_hold["limite_ativacao_cliente"] = {
                            "motivo": f"limite_ativacao_cliente com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "limite_ativacao_cliente": data.get("limite_ativacao_cliente")
                            }],
                        }
                    continue

                # Checando limite_ativacao_oferta
                try:
                    limite_ativacao_oferta = int(data.get("limite_ativacao_oferta"))

                except:
                    falhas += 1

                    if falhas_hold.get("limite_ativacao_oferta"):
                        falhas_hold["limite_ativacao_oferta"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "limite_ativacao_oferta": data.get("limite_ativacao_oferta")
                        })
                    
                    else:
                        falhas_hold["limite_ativacao_oferta"] = {
                            "motivo": f"limite_ativacao_oferta com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "limite_ativacao_oferta": data.get("limite_ativacao_oferta")
                            }],
                        }
                    continue

                # Checando produto_agrupado
                try:
                    produto_agrupado = int(data.get("produto_agrupado"))

                except:
                    falhas += 1

                    if falhas_hold.get("produto_agrupado"):
                        falhas_hold["produto_agrupado"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                            "produto_agrupado": data.get("produto_agrupado")
                        })
                    
                    else:
                        falhas_hold["produto_agrupado"] = {
                            "motivo": f"produto_agrupado com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                                "produto_agrupado": data.get("produto_agrupado")
                            }],
                        }
                    continue

                # status
                status = str(data.get("status"))
                if status not in {"A", "I"}:
                    status = 'I'

                dict_oferta = {
                    "id_oferta_api": id_oferta_api,
                    "descricao_oferta": data.get("descricao_oferta"),
                    "id_distribuidor": id_distribuidor,
                    "tipo_oferta": tipo_oferta,
                    "ordem": ordem,
                    "operador": operador,
                    "data_inicio": data_inicio.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "data_final":  data_fim.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "necessario_para_ativar": necessario_para_ativar,
                    "limite_ativacao_cliente": limite_ativacao_cliente,
                    "limite_ativacao_oferta": limite_ativacao_oferta,
                    "produto_agrupado": produto_agrupado,
                    "status": status,
                }

                # Checando a oferta
                query = """
                    SELECT
                        TOP 1 id_oferta

                    FROM
                        API_OFERTA

                    WHERE
                        id_oferta_api = :id_oferta_api
                        AND id_distribuidor = :id_distribuidor
                """

                params = {
                    "id_oferta_api": id_oferta_api,
                    "id_distribuidor": id_distribuidor,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True
                }

                response = dm.raw_sql_return(**dict_query)

                if not response:

                    # Inserindo a oferta
                    dict_oferta["data_cadastro"] = data_hora.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    dict_oferta["usuario_cadastro"] = "API PROTHEUS"
                    to_insert_oferta = dict_oferta

                else:

                    # Atualizando a oferta
                    dict_oferta["data_atualizacao"] = data_hora.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    dict_oferta["usuario_atualizacao"] = data_hora.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    to_update_oferta = dict_oferta

                # Salvando os produtos da oferta
                if not (data.get("produtos") and isinstance(data.get("produtos"), list)):
                    
                    falhas += 1

                    if falhas_hold.get("produtos"):
                        falhas_hold["produtos"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                        })
                    
                    else:
                        falhas_hold["produtos"] = {
                            "motivo": f"Produtos com valor inválido.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                            }],
                        }
                    continue

                for produto in data.get("produtos"):

                    if produto and isinstance(produto, dict):

                        # Codigo do produto
                        cod_prod_distr = produto.get("cod_prod_distr")

                        if not cod_prod_distr:

                            falhas += 1

                            if falhas_hold.get("codigo_produto"):
                                falhas_hold["codigo_produto"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                    "cod_prod_distr": produto.get("cod_prod_distr")
                                })
                            
                            else:
                                falhas_hold["codigo_produto"] = {
                                    "motivo": f"Produto sem identificação ou identificação invalida.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "cod_prod_distr": produto.get("cod_prod_distr")
                                    }],
                                }
                            break_tag = True
                            break

                        # Status produto
                        status_produto = produto.get("status")

                        if status_produto not in {"A", "I"}:
                            status_produto = 'I'

                        dict_produto = {
                            "id_oferta_api": id_oferta_api,
                            "id_produto_api": cod_prod_distr,
                            "id_distribuidor": id_distribuidor,
                            "status": status_produto,
                        }

                        bonificado = bool(produto.get("bonificado"))

                        if tipo_oferta == 2 and bonificado:
                            
                            # Quantidade bonificada
                            quantidade_bonificada = produto.get("quantidade_bonificada")

                            try:
                                quantidade_bonificada = int(quantidade_bonificada)

                            except:

                                falhas += 1

                                if falhas_hold.get("quantidade_bonificada"):
                                    falhas_hold["quantidade_bonificada"]["combinacao"].append({
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "quantidade_bonificada": produto.get("quantidade_bonificada")
                                    })
                                
                                else:
                                    falhas_hold["quantidade_bonificada"] = {
                                        "motivo": f"quantidade_bonificada com valor não-numérico.",
                                        "combinacao": [{
                                            "id_distribuidor": id_distribuidor,
                                            "id_oferta": id_oferta_api,
                                            "quantidade_bonificada": produto.get("quantidade_bonificada")
                                        }],
                                    }
                                break_tag = True
                                break

                            if quantidade_bonificada <= 0:

                                falhas += 1

                                if falhas_hold.get("quantidade_bonificada_invalida"):
                                    falhas_hold["quantidade_bonificada_invalida"]["combinacao"].append({
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "quantidade_bonificada": produto.get("quantidade_bonificada")
                                    })
                                
                                else:
                                    falhas_hold["quantidade_bonificada_invalida"] = {
                                        "motivo": f"quantidade_bonificada com valor inválido.",
                                        "combinacao": [{
                                            "id_distribuidor": id_distribuidor,
                                            "id_oferta": id_oferta_api,
                                            "quantidade_bonificada": produto.get("quantidade_bonificada")
                                        }],
                                    }
                                break_tag = True
                                break

                            dict_produto["quantidade_bonificada"] = quantidade_bonificada

                            query = """
                                SELECT
                                    TOP 1 1

                                FROM
                                    API_OFERTA_BONIFICADO

                                WHERE
                                    id_oferta_api = :id_oferta_api
                                    AND id_produto_api = :id_produto_api
                                    AND id_distribuidor = :id_distribuidor
                            """

                            params = {
                                "id_oferta_api": id_oferta_api,
                                "id_produto_api": cod_prod_distr,
                                "id_distribuidor": id_distribuidor
                            }

                            dict_query = {
                                "query": query,
                                "params": params,
                                "first": True,
                                "raw": True
                            }

                            response = dm.raw_sql_return(**dict_query)

                            if response:
                                to_update_bonificado_oferta.append(dict_produto)

                            else:
                                dict_produto["id_produto"] = ""
                                to_insert_bonificado_oferta.append(dict_produto)

                        else:

                            # Quantidade minima de ativação
                            quant_min_ativ = produto.get("quantidade_min_ativacao")

                            try:
                                quant_min_ativ = int(quant_min_ativ)

                            except:

                                falhas += 1

                                if falhas_hold.get("quantidade_min_ativacao"):
                                    falhas_hold["quantidade_min_ativacao"]["combinacao"].append({
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "quantidade_min_ativacao": produto.get("quantidade_min_ativacao")
                                    })
                                
                                else:
                                    falhas_hold["quantidade_min_ativacao"] = {
                                        "motivo": f"quantidade_min_ativacao com valor não-numérico.",
                                        "combinacao": [{
                                            "id_distribuidor": id_distribuidor,
                                            "id_oferta": id_oferta_api,
                                            "quantidade_min_ativacao": produto.get("quantidade_min_ativacao")
                                        }],
                                    }
                                break_tag = True
                                break

                            # Valor minima de ativação
                            valor_min_ativ = produto.get("valor_min_ativacao")

                            try:
                                valor_min_ativ = float(valor_min_ativ)

                            except:

                                falhas += 1

                                if falhas_hold.get("valor_min_ativacao"):
                                    falhas_hold["valor_min_ativacao"]["combinacao"].append({
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "valor_min_ativacao": produto.get("valor_min_ativacao")
                                    })
                                
                                else:
                                    falhas_hold["valor_min_ativacao"] = {
                                        "motivo": f"valor_min_ativacao com valor não-numérico.",
                                        "combinacao": [{
                                            "id_distribuidor": id_distribuidor,
                                            "id_oferta": id_oferta_api,
                                            "valor_min_ativacao": produto.get("valor_min_ativacao")
                                        }],
                                    }
                                break_tag = True
                                break

                            dict_produto["quantidade_min_ativacao"] = quant_min_ativ
                            dict_produto["valor_min_ativacao"] = valor_min_ativ

                            query = """
                                SELECT
                                    TOP 1 1

                                FROM
                                    API_OFERTA_PRODUTO

                                WHERE
                                    id_oferta_api = :id_oferta_api
                                    AND id_produto_api = :id_produto_api
                                    AND id_distribuidor = :id_distribuidor
                            """

                            params = {
                                "id_oferta_api": id_oferta_api,
                                "id_produto_api": cod_prod_distr,
                                "id_distribuidor": id_distribuidor
                            }

                            dict_query = {
                                "query": query,
                                "params": params,
                                "first": True,
                                "raw": True
                            }

                            response = dm.raw_sql_return(**dict_query)

                            if response:
                                to_update_produto_oferta.append(dict_produto)

                            else:
                                to_insert_produto_oferta.append(dict_produto)
            
                if tipo_oferta == 3 and not break_tag:

                    if not (data.get("escalonamento") and isinstance(data.get("escalonamento"), list)):
                    
                        falhas += 1

                        if falhas_hold.get("escalonamento"):
                            falhas_hold["escalonamento"]["combinacao"].append({
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                            })
                        
                        else:
                            falhas_hold["escalonamento"] = {
                                "motivo": f"escalonamento com valor inválido.",
                                "combinacao": [{
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                }],
                            }
                        continue

                    for faixa_esc in data["escalonamento"]:

                        # Sequencia
                        try:
                            sequencia = int(faixa_esc.get("sequencia"))

                        except:
                            falhas += 1

                            if falhas_hold.get("sequencia"):
                                falhas_hold["sequencia"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                    "sequencia": faixa_esc.get("sequencia")
                                })
                            
                            else:
                                falhas_hold["sequencia"] = {
                                    "motivo": f"sequencia com valor não-numérico.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "sequencia": faixa_esc.get("sequencia")
                                    }],
                                }
                            break_tag = True
                            break

                        # Faixa
                        try:
                            faixa = int(faixa_esc.get("faixa"))

                        except:
                            falhas += 1

                            if falhas_hold.get("faixa"):
                                falhas_hold["faixa"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                    "faixa": faixa_esc.get("faixa")
                                })
                            
                            else:
                                falhas_hold["faixa"] = {
                                    "motivo": f"faixa com valor não-numérico.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "faixa": faixa_esc.get("faixa")
                                    }],
                                }
                            break_tag = True
                            break

                        # Desconto
                        try:
                            desconto = float(faixa_esc.get("desconto"))

                        except:
                            falhas += 1

                            if falhas_hold.get("desconto"):
                                falhas_hold["desconto"]["combinacao"].append({
                                    "id_distribuidor": id_distribuidor,
                                    "id_oferta": id_oferta_api,
                                    "desconto": faixa_esc.get("desconto")
                                })
                            
                            else:
                                falhas_hold["desconto"] = {
                                    "motivo": f"desconto com valor não-numérico.",
                                    "combinacao": [{
                                        "id_distribuidor": id_distribuidor,
                                        "id_oferta": id_oferta_api,
                                        "desconto": faixa_esc.get("desconto")
                                    }],
                                }
                            break_tag = True
                            break

                        status_esc = str(faixa_esc.get("status"))
                        if status_esc not in {"A", "I"}:
                            status_esc = 'I'

                        dict_esc = {
                            "id_oferta_api": id_oferta_api,
                            "id_distribuidor": id_distribuidor,
                            "sequencia": sequencia,
                            "faixa": faixa,
                            "desconto": desconto,
                            "status": status,
                        }

                        query = """
                            SELECT
                                TOP 1 1

                            FROM
                                API_OFERTA_ESCALONADO_FAIXA

                            WHERE
                                id_oferta_api = :id_oferta_api
                                AND sequencia = :sequencia
                                AND id_distribuidor = :id_distribuidor
                        """

                        params = {
                            "id_oferta_api": id_oferta_api,
                            "id_distribuidor": id_distribuidor,
                            "sequencia": sequencia,
                        }

                        dict_query = {
                            "query": query,
                            "params": params,
                            "first": True,
                            "raw": True
                        }

                        response = dm.raw_sql_return(**dict_query)

                        if response:
                            to_update_escalonado_oferta.append(dict_esc)

                        else:
                            to_insert_escalonado_oferta.append(dict_esc)

                # Salvando clientes da oferta
                if (data.get("clientes") and isinstance(data.get("clientes"), list)) and not break_tag:

                    clientes = data.get("clientes")

                    query = """
                        SELECT
                            chave_integracao as cod_cliente

                        FROM
                            CLIENTE c

                            INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                                c.id_cliente = cd.id_cliente

                            INNER JOIN DISTRIBUIDOR d ON
                                cd.id_distribuidor = d.id_distribuidor

                        WHERE
                            chave_integracao IN :clientes
                            AND d.id_distribuidor = :id_distribuidor
                            AND chave_integracao NOT IN (
                                                            SELECT
                                                                cod_cliente

                                                            FROM
                                                                API_OFERTA_CLIENTE

                                                            WHERE
                                                                id_oferta_api = :id_oferta_api
                                                        )
                            AND cd.d_e_l_e_t_ = 0
                            AND cd.status = 'A'
                    """

                    params = {
                        "clientes": clientes,
                        "id_distribuidor": id_distribuidor,
                        "id_oferta_api": id_oferta_api
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                    }

                    response = dm.raw_sql_return(**dict_query)

                    if response:

                        to_insert_cliente_oferta.extend([
                            {
                                "cod_cliente": cliente.get("cod_cliente"),
                                "id_oferta_api": id_oferta_api,
                                "status": "A",
                                "data_cadastro": data_hora.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            }

                            for cliente in response
                        ])
                        
                if break_tag:
                    continue

                check_1 = to_insert_produto_oferta or to_update_produto_oferta
                check_2 = to_insert_escalonado_oferta or to_update_escalonado_oferta

                if tipo_oferta == 2 and not (check_1 and check_2):

                    falhas += 1

                    if falhas_hold.get("oferta_campanha"):
                        falhas_hold["oferta_campanha"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                        })
                    
                    else:
                        falhas_hold["oferta_campanha"] = {
                            "motivo": f"oferta campanha sem produtos ativadores ou bonificados.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                            }],
                        }
                    continue

                if tipo_oferta == 3 and not check_1:

                    falhas += 1

                    if falhas_hold.get("oferta_escalonada"):
                        falhas_hold["oferta_escalonada"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "id_oferta": id_oferta_api,
                        })
                    
                    else:
                        falhas_hold["oferta_escalonada"] = {
                            "motivo": f"oferta escalonada sem produtos.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "id_oferta": id_oferta_api,
                            }],
                        }
                    continue

                if to_insert_oferta:
                    insert_oferta.append(to_insert_oferta)

                if to_update_oferta:
                    update_oferta.append(to_update_oferta)

                if to_insert_produto_oferta:
                    insert_produto_oferta.extend(to_insert_produto_oferta)

                if to_update_produto_oferta:
                    update_produto_oferta.extend(to_update_produto_oferta)

                if to_insert_escalonado_oferta:
                    insert_escalonado_oferta.extend(to_insert_escalonado_oferta)

                if to_update_escalonado_oferta:
                    update_escalonado_oferta.extend(to_update_escalonado_oferta)

                if to_insert_bonificado_oferta:
                    insert_bonificado_oferta.extend(to_insert_bonificado_oferta)

                if to_update_bonificado_oferta:
                    update_bonificado_oferta.extend(to_update_bonificado_oferta)

                if to_insert_cliente_oferta:
                    insert_cliente_oferta.extend(to_insert_cliente_oferta)

            else:
                falhas += 1

                if falhas_hold.get("objeto_invalido"):
                    falhas_hold["objeto_invalido"]["combinacao"].append({
                        "obj": oferta,
                    })
                
                else:
                    falhas_hold["objeto_invalido"] = {
                        "motivo": f"Objeto de cadastro inválido.",
                        "combinacao": [{
                            "obj": oferta,
                        }],
                    }
                continue

        if insert_oferta:
            dm.raw_sql_insert("API_OFERTA", insert_oferta)
        
        if update_oferta:
            key_columns = ["id_oferta_api", "id_distribuidor"]
            dm.raw_sql_update("API_OFERTA", update_oferta, key_columns)

        if insert_produto_oferta:
            dm.raw_sql_insert("API_OFERTA_PRODUTO", insert_produto_oferta)

        if update_produto_oferta:
            key_columns = ["id_oferta_api", "id_distribuidor", "id_produto_api"]
            dm.raw_sql_update("API_OFERTA_PRODUTO", update_produto_oferta, key_columns)

        if insert_escalonado_oferta:
            dm.raw_sql_insert("API_OFERTA_ESCALONADO_FAIXA", insert_escalonado_oferta)

        if update_escalonado_oferta:
            key_columns = ["id_oferta_api", "id_distribuidor", "sequencia"]
            dm.raw_sql_update("API_OFERTA_ESCALONADO_FAIXA", update_escalonado_oferta, key_columns)

        if insert_bonificado_oferta:
            dm.raw_sql_insert("API_OFERTA_BONIFICADO", insert_bonificado_oferta)

        if update_bonificado_oferta:
            key_columns = ["id_oferta_api", "id_distribuidor", "id_produto_api"]
            dm.raw_sql_update("API_OFERTA_BONIFICADO", update_bonificado_oferta, key_columns)

        if insert_cliente_oferta:
            dm.raw_sql_insert("API_OFERTA_CLIENTE", insert_cliente_oferta)

        if any([update_oferta, insert_produto_oferta, update_produto_oferta, insert_escalonado_oferta,
                update_escalonado_oferta, insert_bonificado_oferta, update_bonificado_oferta,
                insert_cliente_oferta]):

            query = """
                SET NOCOUNT ON;

                --Salvando o cabecalho da oferta
                INSERT INTO
                    OFERTA
                        (
                            id_oferta_api,
                            descricao_oferta,
                            id_distribuidor,
                            tipo_oferta,
                            ordem,
                            operador,
                            data_inicio,
                            data_final,
                            necessario_para_ativar,
                            limite_ativacao_cliente,
                            limite_ativacao_oferta,
                            produto_agrupado,
                            status,
                            data_cadastro,
                            usuario_cadastro
                        )

                    SELECT
                        id_oferta_api,
                        descricao_oferta,
                        id_distribuidor,
                        tipo_oferta,
                        ordem,
                        operador,
                        data_inicio,
                        data_final,
                        necessario_para_ativar,
                        limite_ativacao_cliente,
                        limite_ativacao_oferta,
                        produto_agrupado,
                        status,
                        GETDATE() as data_cadastro,
                        usuario_cadastro

                    FROM
                        API_OFERTA ao

                    WHERE
                        NOT EXISTS (
                                        SELECT
                                            1

                                        FROM
                                            OFERTA o

                                        WHERE
                                            o.id_oferta_api = ao.id_oferta_api
                                            AND o.id_distribuidor = ao.id_distribuidor
                                )

                -- Salvando os id_oferta nas tabelas de api

                -- # Api oferta
                UPDATE
                    ao

                SET
                    id_oferta = o.id_oferta

                FROM
                    API_OFERTA ao

                    INNER JOIN OFERTA o ON
                        ao.id_oferta_api = o.id_oferta_api

                WHERE
                    ao.id_oferta IS NULL

                -- # Api oferta Bonificado
                UPDATE
                    aob

                SET
                    id_oferta = o.id_oferta,
                    id_produto = p.id_produto

                FROM
                    API_OFERTA_BONIFICADO aob

                    INNER JOIN OFERTA o ON
                        aob.id_oferta_api = o.id_oferta_api

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        aob.id_produto_api = pd.cod_prod_distr
                        AND aob.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                WHERE
                    aob.id_oferta IS NULL

                -- # Api oferta Cliente
                UPDATE
                    aoc

                SET
                    id_oferta = o.id_oferta,
                    id_cliente = c.id_cliente

                FROM
                    API_OFERTA_CLIENTE aoc

                    INNER JOIN OFERTA o ON
                        aoc.id_oferta_api = o.id_oferta_api

                    INNER JOIN CLIENTE c ON
                        aoc.cod_cliente = c.chave_integracao

                WHERE
                    aoc.id_oferta IS NULL

                -- # Api oferta Produto
                UPDATE
                    aop

                SET
                    id_oferta = o.id_oferta,
                    id_produto = p.id_produto

                FROM
                    API_OFERTA_PRODUTO aop

                    INNER JOIN OFERTA o ON
                        aop.id_oferta_api = o.id_oferta_api

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        aop.id_produto_api = pd.cod_prod_distr
                        AND aop.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                WHERE
                    aop.id_oferta IS NULL

                -- # Api oferta Escalonado Faixa
                UPDATE
                    aoef

                SET
                    id_oferta = o.id_oferta

                FROM
                    API_OFERTA_ESCALONADO_FAIXA aoef

                    INNER JOIN OFERTA o ON
                        aoef.id_oferta_api = o.id_oferta_api

                WHERE
                    aoef.id_oferta IS NULL

                -- Salvando as ofertas do cliente
                INSERT INTO
                    OFERTA_CLIENTE
                        (
                            id_oferta,
                            id_cliente,
                            data_cadastro,
                            status,
                            d_e_l_e_t_
                        )

                    SELECT
                        aoe.id_oferta,
                        aoe.id_cliente,
                        GETDATE() as data_cadastro,
                        aoe.status,
                        0 d_e_l_e_t_

                    FROM
                        API_OFERTA_CLIENTE aoe

                    WHERE
                        NOT EXISTS (
                                        SELECT
                                            1

                                        FROM
                                            OFERTA_CLIENTE

                                        WHERE
                                            id_cliente = aoe.id_cliente
                                            AND id_oferta = aoe.id_oferta
                                )
                        AND aoe.id_cliente IS NOT NULL

                -- Salvando os bonificados da oferta
                INSERT INTO
                    OFERTA_BONIFICADO
                        (
                            id_oferta,
                            id_produto,
                            quantidade_bonificada,
                            status
                        )

                    SELECT
                        aob.id_oferta,
                        aob.id_produto,
                        aob.quantidade_bonificada,
                        aob.status

                    FROM
                        API_OFERTA_BONIFICADO aob

                    WHERE
                        aob.id_produto IS NOT NULL
                        AND NOT EXISTS (
                                            SELECT
                                                1

                                            FROM
                                                OFERTA_BONIFICADO

                                            WHERE
                                                id_oferta = aob.id_oferta
                                                AND id_produto = aob.id_produto
                                    )

                -- Salvando os produtos da oferta
                INSERT INTO
                    OFERTA_PRODUTO
                        (
                            id_oferta,
                            id_produto,
                            quantidade_min_ativacao,
                            valor_min_ativacao,
                            status
                        )

                    SELECT
                        aop.id_oferta,
                        aop.id_produto,
                        aop.quantidade_min_ativacao,
                        aop.valor_min_ativacao,
                        aop.status

                    FROM
                        API_OFERTA_PRODUTO aop

                    WHERE
                        aop.id_produto IS NOT NULL
                        AND NOT EXISTS (
                                            SELECT
                                                1

                                            FROM
                                                OFERTA_PRODUTO

                                            WHERE
                                                id_produto = aop.id_produto
                                                AND id_oferta = aop.id_oferta
                                    )

                -- Salvando as faixas de escalonamentos de oferta
                INSERT INTO
                    OFERTA_ESCALONADO_FAIXA
                        (
                            id_oferta,
                            sequencia,
                            faixa,
                            desconto,
                            status
                        )

                    SELECT
                        aoef.id_oferta,
                        aoef.sequencia,
                        aoef.faixa,
                        aoef.desconto,
                        aoef.status

                    FROM
                        API_OFERTA_ESCALONADO_FAIXA aoef

                    WHERE
                        NOT EXISTS (
                                        SELECT
                                            1

                                        FROM
                                            OFERTA_ESCALONADO_FAIXA

                                        WHERE
                                            id_oferta = aoef.id_oferta
                                            AND sequencia = aoef.sequencia
                                )
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