#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime, os, re

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class IntegracaoProtheusClientes(Resource):

    @logger
    def post(self):
        """
        Método POST do Recebimento de dados de clientes da integração do protheus do Marketplace

        Returns:
            [dict]: Status da transação
        """

        response_data = dm.get_request(values_upper=True, not_change_values=["token"], bool_save_request = False)

        necessary_keys = ["clientes"]
        correct_types = {"clientes": list}

        # Checa chaves enviadas
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity[0], 200  

        clientes = list(response_data.get("clientes"))

        # Registro de falhas
        falhas = 0
        falhas_hold = {}
        total = len(clientes)

        accepted_keys = ["filial", "cnpj", "cpf", "razao_social", "nome_fantasia", "nome", "email", "senha", 
                            "telefone", "chave", "status", "aprovado", "data_nascimento","id_maxipago",
                            "telefone_cliente", "endereco", "endereco_num", "endereco_complemento", "bairro",
                            "cep", "cidade", "estado"]

        user_keys = ["cpf", "nome", "email", "senha", "telefone"]
        client_keys = ["cnpj", "razao_social", "nome_fantasia", "chave", "status", "aprovado", "telefone_cliente",
                        "endereco", "endereco_num", "bairro", "cep", "cidade", "estado"]

        update_cliente = []
        insert_usuario_maxipago = []
        insert_usuario_cliente = []
        insert_cliente_distribuidor = []

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

        for cliente in clientes:
            
            if cliente and isinstance(cliente, dict):

                data = {
                    key: cliente.get(key)
                    for key in accepted_keys
                }

                id_distribuidor = data.get("filial")
                cpf = re.sub("[^0-9]", "", str(data.get("cpf")))
                cnpj = re.sub("[^0-9]", "", str(data.get("cnpj")))

                # Checando o distribuidor
                try:
                    id_distribuidor = int(id_distribuidor)

                except:
                    falhas += 1

                    if falhas_hold.get("distribuidor_id_invalido"):
                        falhas_hold["distribuidor_id_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                        })
                    
                    else:
                        falhas_hold["distribuidor_id_invalido"] = {
                            "motivo": f"Distribuidor com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                            }],
                        }
                    continue

                if id_distribuidor not in distribuidores:

                    falhas += 1

                    if falhas_hold.get("distribuidor"):
                        falhas_hold["distribuidor"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                        })
                    
                    else:
                        falhas_hold[f"distribuidor"] = {
                            "descricao": f"Distribuidor não existente ou inativo",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                }
                            ]
                        }
                    continue

                # Checando o cpf
                query = """
                    SELECT
                        TOP 1 id_usuario

                    FROM
                        USUARIO

                    WHERE
                        cpf = :cpf
                """

                params = {
                    "cpf": cpf
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True
                }

                response = dm.raw_sql_return(**dict_query)

                if not response:

                    # Checando dados nulos de usuario
                    if (null := check_null(data, user_keys)):
                        null = list(set(null))

                        falhas += 1

                        if falhas_hold.get("cadastro_usuario"):
                            falhas_hold["cadastro_usuario"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                                "chaves": null
                            })
                        
                        else:
                            falhas_hold[f"cadastro_usuario"] = {
                                "descricao": f"Chaves com valor vazio ou nulo para realizar cadastro do usuario na base.",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                        "chaves": null
                                    }
                                ]
                            }
                        continue

                    # Checando o cpf
                    if not secure.cpf_validator(cpf):

                        falhas += 1

                        if falhas_hold.get("cpf"):
                            falhas_hold["cpf"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                            })
                        
                        else:
                            falhas_hold[f"cpf"] = {
                                "descricao": f"CPF inválido.",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                    }
                                ]
                            }
                        continue

                    #Checando data de nascimento
                    try:
                        data_nascimento = str(cliente["data_nascimento"])
                        pattern = "%d%m%Y"
                        data_nascimento = datetime.datetime.strptime(data_nascimento, pattern)
                        data_nascimento = data_nascimento.strftime("%Y-%m-%d")
                    
                    except:
                        data_nascimento = None

                    # Checando o telefone
                    telefone = re.sub("[^0-9]", "", str(data.get("telefone")))

                    if not telefone:

                        falhas += 1

                        if falhas_hold.get("telefone_envio"):
                            falhas_hold["telefone_envio"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                                "telefone": telefone
                            })
                        
                        else:
                            falhas_hold[f"telefone_envio"] = {
                                "descricao": f"Telefone do usuario não enviado.",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                        "telefone": telefone
                                    }
                                ]
                            }
                        continue

                    if telefone[0] == "0":
                        telefone = telefone[1:]

                    if len(telefone) in {8,9}:
                        
                        if len(telefone) == 8:

                            if re.match("^[6-9][0-9]{7}$"):
                                telefone = f"9{telefone}"

                        telefone = f"85{telefone}"

                    if not re.match("^[1-9]{2}([2-5]|9[6-9])[0-9]{7}$", telefone):

                        falhas += 1

                        if falhas_hold.get("telefone"):
                            falhas_hold["telefone"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                                "telefone": telefone
                            })
                        
                        else:
                            falhas_hold[f"telefone"] = {
                                "descricao": f"Telefone invalido.",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                        "telefone": telefone
                                    }
                                ]
                            }
                        continue

                    # Checando a senha
                    senha = str(data.get("senha"))

                    if len(senha) != 32:
                        
                        falhas += 1

                        if falhas_hold.get("telefone"):
                            falhas_hold["telefone"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                            })
                        
                        else:
                            falhas_hold[f"telefone"] = {
                                "descricao": f"Senha inválida. Esperado padrão MD5.",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                    }
                                ]
                            }
                        continue

                    # Checando o email
                    email = str(data.get("email"))

                    if not re.match(r"^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", email):

                        falhas += 1

                        if falhas_hold.get("email"):
                            falhas_hold["email"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                                "email": email
                            })
                        
                        else:
                            falhas_hold[f"email"] = {
                                "descricao": f"Email inválido.",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                        "email": email
                                    }
                                ]
                            }
                        continue
                    
                    # Checando o nome
                    nome = str(data.get("nome"))

                    if not nome:

                        falhas += 1

                        if falhas_hold.get("nome"):
                            falhas_hold["nome"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cpf": cpf,
                                "cnpj": cnpj,
                                "nome": nome
                            })
                        
                        else:
                            falhas_hold[f"nome"] = {
                                "descricao": f"Nome inválido. Esperado nome e sobrenome (2 ou mais letras em cada)",
                                "combinacao": [
                                    {
                                        "id_distribuidor": id_distribuidor,
                                        "cpf": cpf,
                                        "cnpj": cnpj,
                                        "nome": nome
                                    }
                                ]
                            }
                        continue

                    # Salvando novo usuario
                    new_user = {
                        "nome": nome,
                        "cpf": cpf,
                        "email": email,
                        "senha": senha,
                        "data_nascimento": data_nascimento,
                        "telefone": telefone,
                        "status": "P",
                        "d_e_l_e_t_": 0,
                        "alterar_senha": "N",
                        "data_cadastro": data_hora.strftime("%Y-%m-%d")
                    }

                    dm.raw_sql_insert("USUARIO", new_user)

                    query = """
                        SELECT
                            TOP 1 id_usuario

                        FROM
                            USUARIO

                        WHERE
                            cpf = :cpf
                    """

                    params = {
                        "cpf": cpf
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "first": True,
                        "raw": True
                    }

                    response = dm.raw_sql_return(**dict_query)

                id_usuario = response[0]

                # Checando o id_maxipago
                id_maxipago = data.get("id_maxipago")

                if id_maxipago:

                    query = """
                        SELECT
                            TOP 1 id_maxipago

                        FROM
                            USUARIO_DISTRIBUIDOR_MAXIPAGO

                        WHERE
                            id_usuario = :id_usuario
                            AND id_distribuidor = :id_distribuidor
                            AND LEN(id_maxipago) > 0
                    """

                    params = {
                        "id_usuario": id_usuario,
                        "id_distribuidor": id_distribuidor
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "first": True,
                        "raw": True
                    }

                    response = dm.raw_sql_return(**dict_query)

                    if not response:

                        insert_usuario_maxipago.append({
                            "id_usuario": id_usuario,
                            "id_distribuidor": id_distribuidor,
                            "id_maxipago": str(id_maxipago)
                        })

                # Checando dados nulos de cliente
                if (null := check_null(data, client_keys)):
                    null = list(set(null))

                    falhas += 1

                    if falhas_hold.get("cliente"):
                        falhas_hold["cliente"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                            "chaves": null
                        })
                    
                    else:
                        falhas_hold[f"cliente"] = {
                            "descricao": f"Chaves com valor vazio ou nulo para realizar cadastro/atualzacao do cliente na base.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                    "chaves": null
                                }
                            ]
                        }
                    continue

                # Checando o cnpj
                if not secure.cnpj_validator(cnpj):

                    falhas += 1

                    if falhas_hold.get("cnpj"):
                        falhas_hold["cnpj"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                        })
                    
                    else:
                        falhas_hold[f"cnpj"] = {
                            "descricao": f"CNPJ inválido.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                }
                            ]
                        }
                    continue

                # Checando a razao social
                razao_social = str(data.get("razao_social"))

                # Checando o nome fantasia
                nome_fantasia = str(data.get("nome_fantasia"))

                # Checando a chave
                chave = str(data.get("chave"))

                # Checando o telefone do cliente
                telefone_cliente = re.sub("[^0-9]", "", str(data.get("telefone_cliente")))

                if not telefone_cliente:

                    falhas += 1

                    if falhas_hold.get("telefone_cliente_envio"):
                        falhas_hold["telefone_cliente_envio"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                            "telefone_cliente": data.get("telefone_cliente")
                        })
                    
                    else:
                        falhas_hold[f"telefone_cliente_envio"] = {
                            "descricao": f"Telefone do cliente não enviado.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                    "telefone_cliente": data.get("telefone_cliente")
                                }
                            ]
                        }
                    continue

                if telefone_cliente[0] == "0":
                    telefone_cliente = telefone_cliente[1:]

                if len(telefone_cliente) in {8,9}:
                        
                    if len(telefone_cliente) == 8:

                        if re.match("^[6-9][0-9]{7}$"):
                            telefone_cliente = f"9{telefone_cliente}"

                    telefone_cliente = f"85{telefone_cliente}"

                if not re.match("^[1-9]{2}([2-5]|9[6-9])[0-9]{7}$", telefone_cliente):

                    falhas += 1

                    if falhas_hold.get("telefone_cliente"):
                        falhas_hold["telefone_cliente"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                            "telefone_cliente": data.get("telefone_cliente")
                        })
                    
                    else:
                        falhas_hold[f"telefone_cliente"] = {
                            "descricao": f"Telefone do cliente é invalido.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                    "telefone_cliente": data.get("telefone_cliente")
                                }
                            ]
                        }
                    continue

                # Checando o endereco
                endereco = str(data.get("endereco"))

                # Checando o endereco_num
                endereco_num = data.get("endereco_num")

                # Checando o complemento
                endereco_complemento = str(data.get("endereco_complemento")) if data.get("endereco_complemento") else None

                # Checando o bairro
                bairro = str(data.get("bairro"))

                # Checando o cep
                cep = re.sub("[^0-9]", "", str(data.get("cep")))

                if len(cep) != 8:

                    falhas += 1

                    if falhas_hold.get("cep"):
                        falhas_hold["cep"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                        })
                    
                    else:
                        falhas_hold[f"cep"] = {
                            "descricao": f"CEP inválido. Esperado 8 digitos numéricos",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                }
                            ]
                        }
                    continue

                # Checando o cidade
                cidade = str(data.get("cidade"))

                # Checando o endereco
                estado = str(data.get("estado"))

                if len(estado) != 2:

                    falhas += 1

                    if falhas_hold.get("estado"):
                        falhas_hold["estado"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cpf": cpf,
                            "cnpj": cnpj,
                            "estado": estado
                        })
                    
                    else:
                        falhas_hold[f"estado"] = {
                            "descricao": f"Estado inválido. Esperado sigla do estado.",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cpf": cpf,
                                    "cnpj": cnpj,
                                    "estado": estado
                                }
                            ]
                        }
                    continue

                # Checando status
                status = str(data.get("aprovado"))
                if status != 'A':
                    status = "I"

                # Checando status
                status_receita = str(data.get("status"))
                if status_receita is None:
                    status_receita = "I"

                # Checando a condição do cnpj quanto ao cadastro
                client_obj = {
                    "razao_social": razao_social,
                    "nome_fantasia": nome_fantasia,
                    "chave_integracao": chave,
                    "cnpj": cnpj,
                    "endereco": endereco,
                    "endereco_num": endereco_num,
                    "endereco_complemento": endereco_complemento,
                    "bairro": bairro,
                    "cep": cep,
                    "cidade": cidade,
                    "estado": estado,
                    "telefone": telefone_cliente,
                    "status": status,
                    "status_receita": status_receita,
                }

                query = """
                    SELECT
                        TOP 1 id_cliente,
                        data_aprovacao

                    FROM
                        CLIENTE

                    WHERE
                        cnpj = :cnpj
                """

                params = {
                    "cnpj": cnpj
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True
                }

                response = dm.raw_sql_return(**dict_query)

                if not response:

                    # Cadastra o cliente
                    client_obj["data_cadastro"] = data_hora.strftime("%Y-%m-%d")
                    client_obj["data_aprovacao"] = data_hora.strftime("%Y-%m-%d") if status == "A" else None

                    dm.raw_sql_insert("CLIENTE", client_obj)

                    query = """
                        SELECT
                            TOP 1 id_cliente

                        FROM
                            CLIENTE

                        WHERE
                            cnpj = :cnpj
                    """

                    params = {
                        "cnpj": cnpj
                    }

                    dict_query = {
                        "query": query,
                        "params": params,
                        "first": True,
                        "raw": True
                    }

                    response = dm.raw_sql_return(**dict_query)

                    id_cliente = response[0]

                else:
                    
                    # Atualiza o cliente
                    id_cliente, data_aprovacao_response = response

                    if status == "A" and not data_aprovacao_response:
                        client_obj["data_aprovacao"] = data_hora.strftime("%Y-%m-%d")

                    client_obj["id_cliente"] = id_cliente

                    update_cliente.append(client_obj)

                # Relação usuario-cliente
                query = """
                    SELECT
                        TOP 1 status

                    FROM
                        USUARIO_CLIENTE

                    WHERE
                        id_cliente = :id_cliente
                        AND id_usuario = :id_usuario
                """

                params = {
                    "id_usuario": id_usuario,
                    "id_cliente": id_cliente,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True
                }

                response = dm.raw_sql_return(**dict_query)

                if not response:

                    usuario_cliente_obj = {
                        "id_usuario": id_usuario,
                        "id_cliente": id_cliente,
                        "status": "P",
                        "d_e_l_e_t_": 0,
                    }

                    if usuario_cliente_obj not in insert_usuario_cliente:
                        insert_usuario_cliente.append(usuario_cliente_obj)

                # Relação cliente-distribuidor
                query = """
                    SELECT
                        TOP 1 status

                    FROM
                        CLIENTE_DISTRIBUIDOR

                    WHERE
                        id_cliente = :id_cliente
                        AND id_distribuidor = :id_distribuidor
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "id_cliente": id_cliente,
                }

                dict_query = {
                    "query": query,
                    "params": params,
                    "first": True,
                    "raw": True
                }

                response = dm.raw_sql_return(**dict_query)

                if not response:

                    cliente_distribuidor_obj = {
                        "id_distribuidor": id_distribuidor,
                        "id_cliente": id_cliente,
                        "status": "A",
                        "d_e_l_e_t_": 0,
                        "data_cadastro": data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        "data_aprovacao": data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    }

                    if cliente_distribuidor_obj not in insert_cliente_distribuidor:
                        insert_cliente_distribuidor.append(cliente_distribuidor_obj)

            else:
                falhas += 1

                if falhas_hold.get("objeto_invalido"):
                    falhas_hold["objeto_invalido"]["combinacao"].append({
                        "obj": cliente,
                    })
                
                else:
                    falhas_hold["objeto_invalido"] = {
                        "motivo": f"Objeto de cadastro inválido.",
                        "combinacao": [{
                            "obj": cliente,
                        }],
                    }
                continue

        if update_cliente:
            key_columns = ["id_cliente"]
            dm.raw_sql_update("CLIENTE", update_cliente, key_columns)

        if insert_usuario_maxipago:
            dm.raw_sql_insert("USUARIO_DISTRIBUIDOR_MAXIPAGO", insert_usuario_maxipago)

        if insert_usuario_cliente:
            dm.raw_sql_insert("USUARIO_CLIENTE", insert_usuario_cliente)

        if insert_cliente_distribuidor:
            dm.raw_sql_insert("CLIENTE_DISTRIBUIDOR", insert_cliente_distribuidor)
            
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