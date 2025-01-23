#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime
import re, requests, io, os
from flask import request
from PIL import Image

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.jsl_management as jm
import functions.security as secure
import functions.message_send as ms

class CadastroUsuario(Resource):

    @logger
    def post(self) -> dict:
        """
        Método POST do Cadastro de Usuarios

        Returns:
            [dict]: Status da transação
        """

        token_social = request.headers.get("auth-token-social")
        plataforma = request.headers.get("social-platform")

        if plataforma:
            plataforma = str(plataforma).lower()

        social_email = None
        social_id = None
        social_picture = None

        if token_social:

            answer = secure.social_validate(plataforma, token_social)

            if answer[0]:
                social_email = answer[1].get("social_email")
                social_id = answer[1].get("social_id")
                social_picture = answer[1].get("social_picture")

                if not social_email:
                    logger.error(f"Cadastro do id_{plataforma}:{social_id} falhou pois nao existe email associado a conta.")
                    return {"status":403,
                            "resposta":{
                                "tipo":"13",
                                "descricao":f"Ação recusada: Verificação social falhou devido a falta de email associado a conta."}}, 403

            else:
                return answer[1]
        
        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True, trim_values = True)

        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        necessary_columns = ["nome", "cpf", "cnpj", "telefone", "email", "senha", "data_nascimento",
                                "token_aparelho"]

        if social_email:
            response_data["email"] = str(social_email).upper()
            necessary_columns.remove("senha")
            response_data.pop("senha", None)

        # Checa chaves inválidas e faltantes, valores incorretos e nulos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_columns, 
                                          not_null = necessary_columns)):
            return validity

        # Checagem dos dados enviados
        email = str(response_data["email"])
        if not re.match(r"^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", email):
            logger.error("Usuario tentou se cadastrar com email invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: email invalido."}}, 200 

        nome = str(response_data["nome"])
        if len(nome.split()) < 2:
            logger.error("Usuario tentou se cadastrar sem fornecer nome e sobrenome")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Forneca nome e sobrenome."}}, 200 

        try:
            data_nascimento = str(response_data["data_nascimento"])
            pattern = "%Y-%m-%d"
            datetime.strptime(data_nascimento, pattern)
        
        except:
            logger.error("Usuario tentou se cadastrar com data de nascimento invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: data de nascimento invalida."}}, 200 

        try:
            cpf = re.sub("[^0-9]", "", response_data["cpf"])
            if not secure.cpf_validator(cpf):
                logger.error("Usuario tentou se cadastrar com cpf invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cpf invalido."}}, 200 

        except:
            logger.error("Usuario tentou se cadastrar com cpf invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cpf invalido."}}, 200 

        try:
            cnpj = re.sub("[^0-9]", "", response_data["cnpj"])
            if not secure.cnpj_validator(cnpj):
                logger.error("Usuario tentou se cadastrar com cnpj invalido")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cnpj invalido."}}, 200 

        except:
            logger.error("Usuario tentou se cadastrar com cnpj invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cnpj invalido."}}, 200 

        telefone = re.sub("[^0-9]", "", response_data["telefone"])

        if not re.match("^[1-9]{2}([2-5]|9[6-9])[0-9]{7}$", telefone):
            logger.error("Usuario tentou se cadastrar com telefone invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Telefone invalido."}}, 200 

        # Checagem da sessão
        token_aparelho = response_data["token_aparelho"]

        sessao_query = """
            SELECT
                TOP 1 id_sessao

            FROM
                SESSAO

            WHERE
                d_e_l_e_t_ = 0
                AND status = 'A'
                AND token_aparelho = :token_aparelho
        """
        
        params = {
            "token_aparelho": token_aparelho
        }

        dict_query = {
            "query": sessao_query,
            "params": params, 
            "first": True, 
            "raw": True
        }

        sessao_query = dm.raw_sql_return(**dict_query)

        if not sessao_query:
            logger.info(f"Sessao ativa nao existente para dispositivo:{token_aparelho} informado.")
            return {"status": 400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Sessão inexistente."}}, 400

        id_sessao = sessao_query[0]

        # Verifica se id da plataforma do usuario do login social já existe
        if social_id:

            query = f"""
                SELECT
                    id_usuario

                FROM
                    USUARIO

                WHERE
                    id_{plataforma} = :social_id
            """

            params = {
                "social_id": social_id
            }

            dict_query = {
                "query": query,
                "params": params,
                
                "raw": True
            }

            response = dm.raw_sql_return(**dict_query)

            if response:
                logger.error(f"Usuario com social-id:{social_id} do {plataforma} ja registrado")
                return {"status":409,
                        "resposta":{
                            "tipo":"5",
                            "descricao":f"Usuário com cadastro do {plataforma} já cadastrado."}}, 200                


        # Verifica se o usuario já existe
        user_query = """
            SELECT
                id_usuario

            FROM
                USUARIO

            WHERE
                email LIKE :email
        """

        params = {
            'email': email,
        }

        dict_query = {
            "query": user_query,
            "params": params,
            "first": True,
            "raw": True
        }

        user_query = dm.raw_sql_return(**dict_query)

        if user_query:

            logger.error(f"Usuario com email:{email} ja registrado")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Usuário com email já cadastrado."}}, 200

        user_query = """
            SELECT
                id_usuario

            FROM
                USUARIO

            WHERE
                cpf = :cpf
        """

        params = {
            'cpf': cpf,
        }

        dict_query = {
            "query": user_query,
            "params": params,
            "first": True,
            "raw": True
        }

        user_query = dm.raw_sql_return(**dict_query)

        if user_query:

            logger.error(f"Usuario com cpf:{cpf} ja registrado")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Usuário com cpf já cadastrado."}}, 200

        # Checando se o cnpj já existe
        client_query = """
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

        client_query = dm.raw_sql_return(client_query, params = params, first = True, raw = True)
        
        # Verificar senha
        if not social_id:
            senha = response_data.get("senha")

        else:
            password, pass_hash = secure.new_password_generator()
            senha = str(pass_hash).upper()

        # Inserts

        ## Novo usuario
        new_user = {
            "nome": nome,
            "cpf": cpf,
            "status": "P",
            "email": email,
            "telefone": telefone,
            "senha": senha,
            "alterar_senha": "N",
            "data_cadastro": data_hora,
            "data_nascimento": response_data["data_nascimento"],
            "imagem": None,
            "d_e_l_e_t_": 0
        }

        if social_id and plataforma:
            new_user[f"id_{plataforma}"] = social_id

        dm.raw_sql_insert("USUARIO", new_user)

        ## Novo Cliente
        new_client = {}

        if not client_query:
            new_client = {
                "cnpj": cnpj,
                "data_cadastro": data_hora,
                "status": "P",
                "status_receita": "P"
            }

            dm.raw_sql_insert("CLIENTE", new_client)

        ## Junção Cliente-Usuario
        cliente_usuario_query = """
            SELECT
                (

                    SELECT
                        TOP 1 id_usuario

                    FROM
                        USUARIO

                    WHERE
                        email = :email
                        AND cpf = :cpf

                ) as id_usuario,
                (

                    SELECT
                        TOP 1 id_cliente

                    FROM
                        CLIENTE

                    WHERE
                        cnpj = :cnpj

                ) as id_cliente
        """

        params = {
            "email": email,
            "cpf": cpf,
            "cnpj": cnpj
        }

        dict_query = {
            "query": cliente_usuario_query,
            "params": params,
            "first": True,
        }

        query = dm.raw_sql_return(**dict_query)

        id_usuario = query.get("id_usuario")
        id_cliente = query.get("id_cliente")

        global_info.save_info_thread(**{
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        })

        new_user_client = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "status": "P",
            "d_e_l_e_t_": 0
        }

        dm.raw_sql_insert("USUARIO_CLIENTE", new_user_client)

        # Linkando o usuario a sessão atual
        usuario_sessao_query = """
            SELECT
                TOP 1 id_sessao

            FROM
                USUARIO_SESSAO

            WHERE
                id_usuario = :id_usuario
                AND id_sessao = :id_sessao
        """

        params = {
            "id_usuario": id_usuario,
            "id_sessao": id_sessao
        }

        dict_query = {
            "query": usuario_sessao_query,
            "params": params,
            "raw": True,
            "first": True,
        }

        usuario_sessao_query = dm.raw_sql_return(**dict_query)

        new_user_session = {
            "id_sessao": id_sessao,
            "id_usuario": id_usuario,
            "data_insert": data_hora
        }

        dm.raw_sql_insert("USUARIO_SESSAO", new_user_session)

        # Enviando dados do cliente para o protheus
        dict_user_client = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
        }

        answer = jm.send_client_jsl(**dict_user_client)

        if answer:
            dict_update_user = {
                "id_usuario": id_usuario,
                "status_envio": 'E',
            }

            dict_update_client = {
                "id_cliente": id_cliente,
                "status_envio": 'E',
            }

        else:
            dict_update_user = {
                "id_usuario": id_usuario,
                "status_envio": 'F',
            }

            dict_update_client = {
                "id_cliente": id_cliente,
                "status_envio": 'F',
            }

        key_columns = ["id_usuario"]
        dm.core_sql_update("USUARIO", dict_update_user, key_columns)

        key_columns = ["id_cliente"]
        dm.core_sql_update("CLIENTE", dict_update_client, key_columns)

        # Salvando a imagem do usuario
        if social_email and social_picture:
            response = requests.get(social_picture)

            if response.status_code == 200:
                image_bytes = io.BytesIO(response.content)

                image_folder = os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS")
                used_folder = os.environ.get("IMAGE_USED_PATH_PS")

                try:
                    Image.open(image_bytes).verify()
                    imagem = Image.open(image_bytes)
                    image_format = imagem.format

                    caminhos = ["imagens", used_folder, "fotos", "usuarios"]

                    path_url = image_folder

                    for path in caminhos:
                        
                        path_url += f"/{path}"

                        if not os.path.exists(path_url):
                            os.mkdir(path_url)

                    update_data = {
                        "id_usuario": id_usuario,
                        "imagem": f"{id_usuario}.{str(image_format).lower()}"
                    }

                    key_columns = ["id_usuario"]

                    dm.raw_sql_update("USUARIO", update_data, key_columns)

                    imagem.save(f"{path_url}/{id_usuario}.{str(image_format).lower()}")

                except:
                    pass

        # Enviado email e sms para o usuario
        
        ## email
        url = "app\\templates\\bem_vindo_marketplace.html"
        html = ""

        with open(url, 'r', encoding = "utf-8") as f:
            html = str(f.read())

        to_change = {
            "nome": nome.title(),
            "email": email.upper(),
        } 

        for key, value in to_change.items():
            html = html.replace(f"@{key.upper()}@", value)

        email_send, _ = ms.send_email(response_data.get("email"), "Suporte DAG", html) 

        ## sms
        sms_message = f"""Seu cadastro com o email {email.upper()} no UnoMarket foi efetuado com sucesso.
            Por favor, aguarde a verificação das suas informações cadastrais e do CNPJ informado, 
            se tudo ocorrer sem problemas seu acesso será permitido."""

        if re.match("^[1-9]{2}9[6-9][0-9]{7}$", telefone):
            sms_send, _ = ms.send_sms(telefone, sms_message)
        else:
            logger.info("Sms nao enviado devido ao numero cadastrado nao ser um telefone celular.")
            sms_send = False

        if email_send and sms_send:
            logger.info("Nova senha enviada para o usuario por sms e email.")

        else:

            if email_send:
                logger.info("Nova senha enviada para o usuario por email.")

            if email_send:
                logger.info("Nova senha enviada para o usuario por sms.")
                
        msg = f"Cadastro do usuario {id_usuario}"
        
        if not client_query:
            msg += f" e do cliente {id_cliente} foram concluidos"
        
        else:
            msg += " foi concluido"

        logger.info(msg)
        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Cadastro do usuário concluido."}}, 201