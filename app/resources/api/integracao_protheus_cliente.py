#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime, re

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class IntegracaoProtheusClientes(Resource):

    @logger
    @secure.auth_wrapper()
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

        accepted_keys = ["id_distribuidor", "cnpj", "cpf", "razao_social", "nome_fantasia", "nome", "email", "senha", 
                            "telefone", "chave", "aprovado", "data_nascimento","id_maxipago", "telefone_cliente",
                            "endereco", "endereco_num", "endereco_complemento", "bairro", "cep", "cidade", "estado",
                            "status_usuario", "status_cliente", "status_receita"]

        update_cliente = []
        update_usuario = []
        insert_usuario_maxipago = []
        insert_usuario_cliente = []
        update_usuario_cliente = []
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

        for cliente in clientes:
            
            if cliente and isinstance(cliente, dict):

                data = {
                    key: cliente.get(key)
                    for key in accepted_keys
                }

                id_distribuidor = data.get("id_distribuidor")
                cpf = re.sub("[^0-9]", "", str(data.get("cpf")))
                cnpj = re.sub("[^0-9]", "", str(data.get("cnpj")))

                if len(cpf) != 11:
                    continue

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

                # Checando status do usuario
                status_usuario = data.get("status_usuario")
                if len(str(status_usuario)) != 1 :
                    status_usuario = "I"

                if status_usuario not in {"A", "I"}:
                    status_usuario = 'I'

                if response:

                    id_usuario = response[0]

                    dict_usuario = {
                        "id_usuario": id_usuario,
                        "status": status_usuario,
                        "status_envio": 'E',
                    }

                    if dict_usuario not in update_usuario:
                        update_usuario.append(dict_usuario)

                else:

                    #Checando data de nascimento
                    try:
                        data_nascimento = str(cliente.get("data_nascimento"))
                        data_nascimento = datetime.datetime.strptime(data_nascimento, "%Y-%m-%d")
                        data_nascimento = data_nascimento.strftime("%Y-%m-%d")
                    
                    except:
                        data_nascimento = None

                    # Checando o telefone
                    telefone = re.sub("[^0-9]", "", str(data.get("telefone")))

                    if telefone:

                        if telefone[0] == "0":
                            telefone = telefone[1:]

                        if len(telefone) in {8,9}:
                            
                            if len(telefone) == 8:

                                if re.match("^[6-9][0-9]{7}$", telefone):
                                    telefone = f"9{telefone}"

                            telefone = f"85{telefone}"

                    # Checando a senha
                    senha = data.get("senha")

                    # Checando o email
                    email = data.get("email")

                    # Checando o nome
                    nome = data.get("nome")

                    # Salvando novo usuario
                    new_user = {
                        "nome": nome,
                        "cpf": cpf,
                        "email": email,
                        "senha": senha,
                        "data_nascimento": data_nascimento,
                        "telefone": telefone,
                        "status": status_usuario,
                        "d_e_l_e_t_": 0,
                        "alterar_senha": "N",
                        "data_cadastro": data_hora.strftime("%Y-%m-%d"),
                        "status_envio": 'E',
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

                # Checando a razao social
                razao_social = data.get("razao_social")

                # Checando o nome fantasia
                nome_fantasia = data.get("nome_fantasia")

                # Checando a chave
                chave = data.get("chave")

                # Checando o telefone do cliente
                telefone_cliente = re.sub("[^0-9]", "", str(data.get("telefone_cliente")))

                if telefone_cliente:

                    if telefone_cliente[0] == "0":
                        telefone_cliente = telefone_cliente[1:]

                    if len(telefone_cliente) in {8,9}:
                            
                        if len(telefone_cliente) == 8:

                            if re.match("^[6-9][0-9]{7}$", telefone_cliente):
                                telefone_cliente = f"9{telefone_cliente}"

                        telefone_cliente = f"85{telefone_cliente}"

                # Checando o endereco
                endereco = data.get("endereco")

                # Checando o endereco_num
                endereco_num = data.get("endereco_num")

                # Checando o complemento
                endereco_complemento = str(data.get("endereco_complemento")) if data.get("endereco_complemento") else None

                # Checando o bairro
                bairro = data.get("bairro")

                # Checando o cep
                cep = re.sub("[^0-9]", "", str(data.get("cep")))

                # Checando o cidade
                cidade = data.get("cidade")

                # Checando o endereco
                estado = str(data.get("estado")) if len(str(data.get("estado"))) == 2 else None

                # Checando status do cliente
                status_cliente = data.get("status_cliente")
                if len(str(status_cliente)) != 1 :
                    status_cliente = "I"

                if status_cliente not in {"A", "I"}:
                    status_cliente = 'I'

                # Checando status da receita
                status_receita = data.get("status_receita")
                if len(str(status_receita)) != 1 :
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
                    "status": status_cliente,
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
                    client_obj.update({
                        "data_cadastro": data_hora.strftime("%Y-%m-%d"),
                        "data_aprovacao": data_hora.strftime("%Y-%m-%d") if status_cliente == "A" else None,
                        "status_envio": 'E',
                    })

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

                    if status_cliente == "A" and not data_aprovacao_response:
                        client_obj["data_aprovacao"] = data_hora.strftime("%Y-%m-%d")

                    client_obj["id_cliente"] = id_cliente

                    client_obj.update({
                        "id_cliente": id_cliente,
                        "status_envio": 'E',
                    })

                    update_cliente.append(client_obj)

                # Relação usuario-cliente
                usuario_cliente_obj = {
                    "id_usuario": id_usuario,
                    "id_cliente": id_cliente,
                    "data_aprovacao": None,
                    "status": status_usuario,
                    "d_e_l_e_t_": 0,
                }

                query = """
                    SELECT
                        TOP 1 status,
                        data_aprovacao

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

                    if status_usuario == 'A':
                        usuario_cliente_obj["data_aprovacao"] = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                    if usuario_cliente_obj not in insert_usuario_cliente:
                        insert_usuario_cliente.append(usuario_cliente_obj)

                else:

                    old_status, old_data = response

                    if status_usuario != old_status:

                        if status_usuario == 'A':
                            usuario_cliente_obj["data_aprovacao"] = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                        if usuario_cliente_obj not in update_usuario_cliente:
                            update_usuario_cliente.append(usuario_cliente_obj)

                    if old_data is None and old_status == 'A':
                        
                        usuario_cliente_obj["data_aprovacao"] = data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                        if usuario_cliente_obj not in update_usuario_cliente:
                            update_usuario_cliente.append(usuario_cliente_obj)

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

        if update_usuario:
            key_columns = ["id_usuario"]
            dm.raw_sql_update("USUARIO", update_usuario, key_columns)

        if insert_usuario_maxipago:
            dm.raw_sql_insert("USUARIO_DISTRIBUIDOR_MAXIPAGO", insert_usuario_maxipago)

        if insert_usuario_cliente:
            dm.raw_sql_insert("USUARIO_CLIENTE", insert_usuario_cliente)

        if update_usuario_cliente:
            key_columns = ["id_usuario", "id_cliente"]
            dm.raw_sql_update("USUARIO_CLIENTE", update_usuario_cliente, key_columns)

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