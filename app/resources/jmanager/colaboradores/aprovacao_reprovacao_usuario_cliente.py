#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class AprovacaoReprovacaoUsuarioCliente(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1,2])
    def post(self) -> dict:
        """
        Método POST do Aprovação Reprovação Usuario-Cliente para o distribuidor

        Returns:
            [dict]: Status da transação
        """

        info = global_info.get("token_info")

        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Pega os dados do front-end
        response_data = dm.get_request(norm_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        produto_columns = ["objeto"]

        correct_types = {"objeto": list}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                not_null = produto_columns, 
                                correct_types = correct_types,
                                comparison_columns = produto_columns)):
            return validity

        clientes = response_data.get("objeto")
        clientes = list({frozenset(item.items()): item  for item in clientes}.values())

        distri_token = info.get("id_distribuidor")
        perfil_token = int(info.get("id_perfil"))

        # Registro de falhas
        falhas = 0
        falhas_hold = dict()

        # Registro de commits
        cliente_distribuidor_update_hold = list()
        cliente_distribuidor_insert_hold = list()
        usuario_cliente_update_hold = list()

        # Verificando cada registro
        for cliente in clientes:

            if cliente:

                id_cliente = str(cliente.get("id_cliente"))
                id_distribuidor = str(cliente.get("id_distribuidor"))
                status = str(cliente.get("status")).upper()

                if id_distribuidor == "0":
                    continue

                # Se o tipo das informações não estiverem corretas
                if not id_cliente.isdecimal() or not id_distribuidor.isdecimal(): 

                    falhas += 1

                    if falhas_hold.get("nao_numerico"):
                        falhas_hold["nao_numerico"]['combinacao'].append({
                            "id_cliente": id_cliente,
                            "id_distribuidor": id_distribuidor
                        })
                        
                    else:
                        falhas_hold["nao_numerico"] = {
                            'motivo': 'id do cliente ou do distribuidor não-numérico.',
                            "combinacao": [{
                                "id_cliente": id_cliente,
                                "id_distribuidor": id_distribuidor
                            }]
                        }
                    
                    continue

                # Se o distribuidor pedido for diferente do token
                if perfil_token != 1:
                    if int(id_distribuidor) not in distri_token: 

                        falhas += 1

                        if falhas_hold.get("distribuidor_falso"):
                            falhas_hold["distribuidor_falso"]['combinacao'].append({
                                "id_cliente": id_cliente,
                                "id_distribuidor": id_distribuidor
                            })
                            
                        else:
                            falhas_hold["distribuidor_falso"] = {
                                'motivo': 'distribuidor tentando manipular dados de outro distribuidor.',
                                "combinacao": [{
                                    "id_cliente": id_cliente,
                                    "id_distribuidor": id_distribuidor
                                }]
                            }
                        
                        continue

                # Verificando se o distribuidor existe
                query = """
                    SELECT
                        1

                    FROM
                        DISTRIBUIDOR

                    WHERE
                        id_distribuidor = :id_distribuidor
                """

                params = {
                    "id_distribuidor": id_distribuidor
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True
                }

                answer = dm.raw_sql_return(**dict_query)

                if not answer:

                    falhas += 1

                    if falhas_hold.get("distribuidor"):
                        falhas_hold["distribuidor"]['combinacao'].append({
                            "id_cliente": id_cliente,
                            "id_distribuidor": id_distribuidor
                        })
                        
                    else:
                        falhas_hold["distribuidor"] = {
                            'motivo': 'Distribuidor não existe.',
                            "combinacao": [{
                                "id_cliente": id_cliente,
                                "id_distribuidor": id_distribuidor
                            }]
                        }
                    
                    continue

                # Verificando se o cliente existe
                cliente_query = f"""
                    SELECT
                        TOP 1 id_cliente
                    
                    FROM
                        CLIENTE
                    
                    WHERE
                        id_cliente = :id_cliente
                """

                params = {
                    "id_cliente": id_cliente
                }

                cliente_query = dm.raw_sql_return(cliente_query, params = params, raw = True, first = True)

                if not cliente_query:

                    falhas += 1

                    if falhas_hold.get("cliente"):
                        falhas_hold["cliente"]['combinacao'].append({
                            "id_cliente": id_cliente,
                            "id_distribuidor": id_distribuidor
                        })
                        
                    else:
                        falhas_hold["cliente"] = {
                            'motivo': 'Cliente não existe.',
                            "combinacao": [{
                                "id_cliente": id_cliente,
                                "id_distribuidor": id_distribuidor
                            }]
                        }
                    
                    continue
            
                # Criando/Atualizando o registro do cliente-distribuidor
                cliente_distribuidor_query = f"""
                    SELECT
                        status
                    
                    FROM
                        CLIENTE_DISTRIBUIDOR

                    WHERE
                        id_cliente = :id_cliente
                        AND id_distribuidor = :id_distribuidor
                        AND d_e_l_e_t_ = 0
                """

                params = {
                    "id_cliente": id_cliente,
                    "id_distribuidor": id_distribuidor
                }

                cliente_distribuidor_query = dm.raw_sql_return(cliente_distribuidor_query, 
                                        params = params, raw = True, first = True)

                if cliente_distribuidor_query:

                    hold = {
                        "id_cliente": id_cliente,
                        "id_distribuidor": id_distribuidor,
                        "status": status,
                        "d_e_l_e_t_": 0
                    }

                    if hold not in cliente_distribuidor_update_hold:
                        cliente_distribuidor_update_hold.append(hold)
                
                else:
                    hold = {
                        "id_cliente": id_cliente,
                        "id_distribuidor": id_distribuidor,
                        "data_cadastro": data_hora,
                        "data_aprovacao": data_hora if status == "A" else None,
                        "status": status,
                        "d_e_l_e_t_": 0
                    }

                    if hold not in cliente_distribuidor_insert_hold:
                        cliente_distribuidor_insert_hold.append(hold)
                
                # Aprovando a relação cliente-usuario
                if status == "A":
                    
                    hold = {
                        "id_cliente": id_cliente,
                        "data_aprovacao": data_hora,
                        "status": "A",
                        "d_e_l_e_t_": 0
                    }

                    if hold not in usuario_cliente_update_hold:
                        usuario_cliente_update_hold.append(hold)


        # Realizando os commits
        check_1 = bool(cliente_distribuidor_insert_hold)
        check_2 = bool(cliente_distribuidor_update_hold)
        check_3 = bool(usuario_cliente_update_hold)

        if cliente_distribuidor_insert_hold:

            dm.raw_sql_insert("CLIENTE_DISTRIBUIDOR", cliente_distribuidor_insert_hold)
            
        if cliente_distribuidor_update_hold:

            key_columns = ["id_cliente", "id_distribuidor"]
            dm.raw_sql_update("CLIENTE_DISTRIBUIDOR", cliente_distribuidor_update_hold, key_columns)
        
        if usuario_cliente_update_hold:

            key_columns = ["id_cliente"]
            dm.raw_sql_update("USUARIO_CLIENTE", usuario_cliente_update_hold, key_columns)          

        # Criando a resposta
        total = len(clientes)

        if falhas <= 0:

            if check_1 or check_2 or check_3:
                logger.info("Todas as transacoes ocorreram sem nenhum problema.")
                return {"status":200,
                        "resposta":{
                            "tipo":"1",
                            "descricao":f"Operação ocorreu sem problemas."}
                        }, 200

            else:
                logger.info("Nao houve nenhuma alteracao.")
                return {"status":200,
                        "resposta":{
                            "tipo":"16",
                            "descricao":f"Não houveram modificações na transação."}
                        }, 200
        
        logger.info(f"Houveram {falhas} erros e {total - falhas} sucessos com a transacao.")
        return {"status":200,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Houve erros com a transação."},
                "situacao":{
                    "sucessos": total - falhas,
                    "falhas": falhas,
                    "situacao": falhas_hold
                }}, 200 