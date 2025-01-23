#=== Importações de módulos externos ===#
from google.auth.transport import requests as google_transport
import jwt, hashlib, string, secrets, os, re, base64, itertools
from google.oauth2 import id_token
from Crypto.PublicKey import RSA
from functools import wraps
from itertools import cycle
from flask import request
import numpy as np

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.message_send as sm

def cpf_validator(cpf: str = None) -> bool:
    """
    Valida um cpf

    Args:
        cpf (str, optional): cpf que será validado. Defaults to None.

    Returns:
        bool: caso o cpf seja valido ou não
    """

    try:

        if not cpf:
            return False

        cpf = re.sub("[^0-9]", "", str(cpf)) 

        if len(cpf) != 11 or len(cpf) == cpf.count(cpf[0]):
            return False

        dv = cpf[-2:]

        somatorio = np.sum(int(a) * b  for a, b in zip(cpf[:9], range(10, 1, -1)))
        primeiro_dv = 11 - (somatorio % 11)
        if primeiro_dv >= 10: primeiro_dv = 0

        somatorio = np.sum(int(a) * b for a, b in zip(cpf[:10], range(11, 1, -1)))
        segundo_dv = 11 - (somatorio % 11)
        if segundo_dv >= 10: segundo_dv = 0

        if dv != f"{primeiro_dv}{segundo_dv}":
            return False

        return True

    except:
        return False


def cnpj_validator(cnpj: str = None) -> bool:
    """
    Valida um cnpj

    Args:
        cnpj (str, optional): cnpj que será validado. Defaults to None.

    Returns:
        bool: caso o cnpj seja valido ou não
    """

    try:
        if not cnpj:
            return False

        cnpj = re.sub("[^0-9]", "", str(cnpj))

        if len(cnpj) != 14 or len(cnpj) == cnpj.count(cnpj[0]):
            return False

        dv = cnpj[-2:]
        pesos = range(2,10)

        somatorio = np.sum(int(a) * b  for a, b in zip(cnpj[::-1][2:], cycle(pesos)))
        primeiro_dv = 11 - (somatorio % 11)
        if primeiro_dv >= 10: primeiro_dv = 0

        somatorio = np.sum(int(a) * b  for a, b in zip(cnpj[::-1][1:], cycle(pesos)))
        segundo_dv = 11 - (somatorio % 11)
        if segundo_dv >= 10: segundo_dv = 0

        if dv != f"{primeiro_dv}{segundo_dv}":
            return False

        return True

    except:
        return False


def credit_card_number_validator(numero_cartao: str = None) -> bool:
    """
    Valida o numero de cartão de crédito com o algoritmo de luhn

    Args:
        numero_cartao (str): Numero que deve ser validado. Defaults to None.

    Returns:
        bool: caso o numero de cartao seja valido ou não
    """
    try:

        if not numero_cartao:
            return False

        numero_cartao = re.sub("[^0-9]", "", str(numero_cartao)) 

        if len(numero_cartao) not in range(13, 17):
            return False

        check_number = numero_cartao[:-1]
        check_digit = numero_cartao[-1:]

        new_vals = []

        for index, val in enumerate(check_number[::-1]):
            
            val = int(val)

            if index % 2 == 0:
                new_val = val * 2
                if new_val > 9:
                    new_val -= 9
                val = new_val

            new_vals.append(val)

        somatorio = np.sum(new_vals)
        resto = somatorio % 10

        if resto != 0:

            if int(check_digit) != (10 - resto):
                return False

        return True

    except:
        return False


def credit_card_brand_validator(numero_cartao: str = None) -> str:
    """
    Checa qual é a bandeira do cartão de crédito

    Args:
        numero_cartao (str, optional): Numero que deve ser validado. Defaults to None.

    Returns:
        str: string com a bandeira do cartão. None caso não seja encontrada a bandeira.
    """

    if not numero_cartao:
        return None

    numero_cartao = re.sub("[^0-9]", "", str(numero_cartao)) 

    bandeiras = {
        "Amex": "^3[47][0-9]{13}$",
        "Aura": "^((?!504175))^((?!5067))(^50[0-9])",
        "Banese Card": "^636117",
        "Cabal": "^(60420[1-9]|6042[1-9][0-9]|6043[0-9]{2}|604400)",
        "Diners": "^3(?:0[0-5]|[68][0-9])[0-9]{11}$",
        "Discover": "^6(?:011|5[0-9]{2})[0-9]{12}",
        "Elo": "^4011(78|79)|^43(1274|8935)|^45(1416|7393|763(1|2))|^50(4175|6699|67[0-6][0-9]|677[0-8]|9[0-8][0-9]{2}|99[0-8][0-9]|999[0-9])|^627780|^63(6297|6368|6369)|^65(0(0(3([1-3]|[5-9])|4([0-9])|5[0-1])|4(0[5-9]|[1-3][0-9]|8[5-9]|9[0-9])|5([0-2][0-9]|3[0-8]|4[1-9]|[5-8][0-9]|9[0-8])|7(0[0-9]|1[0-8]|2[0-7])|9(0[1-9]|[1-6][0-9]|7[0-8]))|16(5[2-9]|[6-7][0-9])|50(0[0-9]|1[0-9]|2[1-9]|[3-4][0-9]|5[0-8]))",
        "Fort Brasil": "^628167",
        "GrandCard": "^605032",
        "Hipercard": "^606282|^3841(?:[0|4|6]{1})0",
        "JCB": "^(?:2131|1800|35\d{3})\d{11}",
        "Mastercard": "^((5(([1-2]|[4-5])[0-9]{8}|0((1|6)([0-9]{7}))|3(0(4((0|[2-9])[0-9]{5})|([0-3]|[5-9])[0-9]{6})|[1-9][0-9]{7})))|((508116)\\d{4,10})|((502121)\\d{4,10})|((589916)\\d{4,10})|(2[0-9]{15})|(67[0-9]{14})|(506387)\\d{4,10})",
        "Personal Card": "^636085",
        "Sorocred": "^627892|^636414",
        "Valecard": "^606444|^606458|^606482",
        "Visa": "^4[0-9]{15}$",
    }

    for flag, regex in bandeiras.items():
        if re.match(regex, numero_cartao):
            return flag

    return None


def password_compare(password: str, table_password: str) -> bool:
    """
    Compara a senha encriptada com a senha no banco de dados

    Args:
        password (str): Senha que foi digitada pelo usuario
        table_password (str): Senha que está salva na tabela

    Returns:
        bool: resposta da comparação das variáveis
    """    
    # P.S: tinha mais coisa aqui, mas foi deletado
    return str(password).upper() == str(table_password).upper()


def new_password_generator(password_length: int = 8) -> tuple:
    """
    Cria uma Senha aleatoria de n caracteres (4 numeros e 4 letras)

        - Caso for impar a maioria será numeros
        - Caso for par, ambos numeros e letras terão mesma quantidade        

    Args:
        password (str): Senha que foi digitada pelo usuario

    Returns:
        tuple: Tupla contendo a senha e o hash da mesma
    """

    assert isinstance(password_length, int), "password_length with invalid type"
    assert password_length > 0, "password_length must be greater than zero"

    # Variáveis de controle
    number_value = int(np.ceil(password_length/2))
    char_value = password_length - number_value

    letters = 0
    numbers = 0
    new_password = ""

    # Enquanto a nova senha não for formada
    while(len(new_password) < password_length):

        if numbers < number_value and letters < char_value:
            
            if secrets.randbelow(10) % 2:
                new_password += str(secrets.choice(string.digits))
                numbers += 1

            else:
                new_password += str(secrets.choice(string.ascii_uppercase))
                letters += 1

        elif numbers < number_value:
            new_password += str(secrets.choice(string.digits))
            numbers += 1

        elif letters < char_value:
            new_password += str(secrets.choice(string.ascii_uppercase))
            letters += 1

    return new_password, hashlib.md5(bytes(new_password, encoding = "utf-8")).hexdigest()


def decode_token(token: str, public_key: str = None, 
                    verify_signature: bool = True) -> tuple:
    """
    Decoda um token no formato JWT

    Args:
        token (str): Token a ser decodado
        public_key (str, optional): Chave de decode. Defaults to None.
        verify_signature (bool, optional): Se será necessário utilizar uma chave para decodar o token. Defaults to True.

    Returns:
        tuple: Tupla contendo o resultado da operação e os dados do payload do token
    """

    try:
        dict_jwt = {
            "jwt": token,
            "key": public_key,
            "algorithms": ["RS256"], 
            "options": {
                "verify_signature": verify_signature
            }
        }

        info = jwt.decode(**dict_jwt)

    except jwt.exceptions.ExpiredSignatureError:
        logger.error("Token de sessao expirou.")

        msg = {"status":401,
               "resposta":{
                   "tipo":"11",
                   "descricao":f"Token de sessao expirou."}}, 401

        return False, msg
    
    except Exception as e:
        logger.exception(e)

        msg = {"status":401,
               "resposta":{
                   "tipo":"11",
                   "descricao":f"Token inválido."}}, 401

        return False, msg 

    return True, info


def encode_token(payload: dict, private_key: str) -> str:
    """
    Encoda o token do cliente

    Args:
        payload (dict): Dados que serão utilizados para criar o token.
        private_key (str): Chave privada de encode do token

    Returns:
        str: Token do usuario
    """

    dict_jwt = {
        "payload": payload,
        "key": private_key,
        "algorithm": "RS256",
    }

    return jwt.encode(**dict_jwt)
    

def delete_token(token: str, agent: str) -> bool:
    """
    Deleta o token de uma tabela

    Args:
        token (str): Token a ser deletado
        table (str): Tabela de onde o toke deve ser deletado

    Returns:
        bool: Se a transação deu certo ou não
    """
    agent = str(agent).lower()

    # Jmanager
    if agent == "jmanager":

        query = """
            UPDATE
                JMANAGER_LOGIN

            SET
                manter_logado = NULL,
                data_login = NULL,
                token_sessao = NULL

            WHERE
                token_sessao = :token
        """

        params = {
            "token": token
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

    # Marketplace
    elif agent == "marketplace":

        query = """
            UPDATE
                LOGIN

            SET
                d_e_l_e_t_ = 1,
                data_logout = GETDATE()

            WHERE
                token = :token
        """

        params = {
            "token": token
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)


def verify_token(agent: str, permission_range: list = None, bool_alterar_senha: bool = True, **kwargs) -> tuple:
    """
    Verifica a integridade do token e de algumas especificações

    Args:
        agent (str): De quem pertence o token
        permission_range (list, optional): Niveis de permissão minimos. Defaults to [].
        bool_alterar_senha (bool, optional): Caso alterar senha seja preciso ser verificado. Defaults to True.
        
    kwargs:
        token_envio (str, optional): Caso um token especifico queira ser passado pelo processo de validação. Defaults to None.

    Returns:
        tuple: Tupla contendo um booleano resultado da validação e a resposta da validação
    """

    if permission_range is None:
        permission_range = []

    token_envio = kwargs.get("token_envio")
    agent = str(agent).lower()

    if not token_envio:
        token = request.headers.get("token")

    else:
        token = token_envio

    if not token:
        logger.error("Token nao enviado.")
        msg = {"status":401,
               "resposta":{
                   "tipo": "13", 
                   "descricao": "Ação recusada: Token não enviado."}}, 401

        return False, msg

    token = str(token)

    # Verificar se o token possui 3 partes
    if len(token.split(".")) != 3:
        logger.error("Chave de criacao do token esta com estrutura invalida.")
        msg = {"status":401,
               "resposta":{
                   "tipo":"11",
                   "descricao":f"Token inválido."}}, 401

        return False, msg

    # Verificações específicas
    
    ## Uno Market
    if agent == "marketplace":

        # Verificar se o token é valido
        public_key = open(file = f"other\\keys\\marketplace.pub", mode = "r").read()

        dict_decode = {
            "token": token,
            "public_key": public_key,
        }

        answer, response = decode_token(**dict_decode)

        if not answer:
            return False, response

        token_payload = response

        id_usuario = token_payload.get("id_usuario")

        # Verifica se o alterar_senha está ativo
        if token_payload.get("alterar_senha") and bool_alterar_senha:

            logger.error("Validacao normal foi requisitada, mas o token é somente para alterar_senha.")
            return False, ({"status":401,
                            "resposta":{
                                "tipo":"11",
                                "descricao":f"Token inválido."}}, 401)

        # Verificar se o token existe na tabela
        login_query = f"""
            SELECT
                TOP 1 1
            
            FROM
                LOGIN
            
            WHERE
                id_usuario = :id_usuario
                AND token = :token
                AND data_logout IS NULL
                AND d_e_l_e_t_ = 0
        """

        params = {
            "token": token,
            "id_usuario": id_usuario,
        }

        dict_query = {
            "query": login_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            
            logger.error("Token nao existe na database.")
            msg = {"status":401,
                   "resposta":{
                       "tipo":"11",
                       "descricao":f"Token inválido."}}, 401

            return False, msg

        # Verifica se o usuario é valido
        user_query = f"""
            SELECT
                TOP 1 email
            
            FROM
                USUARIO
            
            WHERE
                id_usuario = :id_usuario
                AND status = 'A'
                AND d_e_l_e_t_ = 0
        """

        params = {
            "id_usuario": id_usuario,
        }

        dict_query = {
            "query": user_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error("Usuario não é válido.")
            return False, ({"status":401,
                            "resposta":{
                                "tipo":"11",
                                "descricao":f"Token inválido."}}, 401)

        email = response[0]

        # Pegando os clientes atuais do usuario
        query = """
            SELECT
                c.id_cliente

            FROM
                USUARIO u

                INNER JOIN USUARIO_CLIENTE uc ON
                    u.id_usuario = uc.id_usuario

                INNER JOIN CLIENTE c ON
                    uc.id_cliente = c.id_cliente

                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                    c.id_cliente = cd.id_cliente

                INNER JOIN.DISTRIBUIDOR d ON
                    cd.id_distribuidor = d.id_distribuidor

            WHERE
                u.id_usuario = :id_usuario
                AND d.id_distribuidor != 0
                AND uc.data_aprovacao <= GETDATE()
                AND cd.data_aprovacao <= GETDATE()
                AND uc.d_e_l_e_t_ = 0
                AND cd.d_e_l_e_t_ = 0
                AND u.status = 'A'
                AND uc.status = 'A'
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND d.status = 'A'
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:

            logger.error(f"Usuario {id_usuario} sem cliente valido atrelado ao mesmo no momento da validação do token.")
            msg = {"status":401,
                   "resposta":{
                        "tipo":"11",
                        "descricao":f"Token inválido."}}, 401

            return False, msg

        token_payload.update({
            "id_cliente": list(set(itertools.chain(*response))),
            "email": email
        })

        save_data = {
            "id_usuario": id_usuario,
            "email": email,
            "token_info": token_payload,
            "token": token,
        }

        global_info.save_info_thread(**save_data)

        return True, token_payload

    ## JsManager
    elif agent == "jmanager":

        # Verificar se o token é valido
        public_key = open(file = f"other\\keys\\jmanager.pub", mode = "r").read()

        dict_decode = {
            "token": token,
            "public_key": public_key,
        }

        answer, response = decode_token(**dict_decode)

        if not answer:
            return False, response

        token_payload = response

        id_usuario = token_payload.get("id_usuario")
        id_perfil = token_payload.get("id_perfil")
        distribuidores = token_payload.get("id_distribuidor")

        # Verifica se o alterar_senha está ativo
        if token_payload.get("alterar_senha") and bool_alterar_senha:

            logger.error("Validacao normal foi requisitada, mas o token é somente para alterar_senha.")
            return False, ({"status":401,
                            "resposta":{
                                "tipo":"11",
                                "descricao":f"Token inválido."}}, 401)

        # Verificar se o token existe na tabela
        login_query = f"""
            SELECT
                TOP 1 id_usuario
            
            FROM
                JMANAGER_LOGIN
            
            WHERE
                id_usuario = :id_usuario
                AND token_sessao = :token
        """

        params = {
            "id_usuario": id_usuario,
            "token": token
        }

        dict_query = {
            "query": login_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)
            
        if not response:
            
            logger.error("Token nao existe na database.")
            msg = {"status":401,
                   "resposta":{
                       "tipo":"11",
                       "descricao":f"Token inválido."}}, 401

            return False, msg

        # Verifica se o usuario é valido
        user_query = f"""
            SELECT
                TOP 1 email
            
            FROM
                JMANAGER_USUARIO
            
            WHERE
                id_usuario = :id_usuario
                AND id_perfil = :id_perfil
                AND status = 'A'
        """

        params = {
            "id_usuario": id_usuario,
            "id_perfil": id_perfil,
        }

        dict_query = {
            "query": user_query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error("Usuario não é válido.")
            return False, ({"status":401,
                            "resposta":{
                                "tipo":"11",
                                "descricao":f"Token inválido."}}, 401)

        email = response[0]

        # Verificando os distribuidores
        if str(distribuidores).isdecimal():
            distribuidores = [int(distribuidores)]
        
        elif not isinstance(distribuidores, (list, tuple, set)):

            logger.error(f"Usuario Jmanager {id_usuario} com distribuidores invalidos no token.")
            return False, ({"status":401,
                            "resposta":{
                                "tipo":"11",
                                "descricao":f"Token inválido."}}, 401)

        query = f"""
            SELECT
                DISTINCT d.id_distribuidor 
            
            FROM
                JMANAGER_USUARIO ju

                INNER JOIN JMANAGER_USUARIO_DISTRIBUIDOR jud ON
                    ju.id_usuario = jud.id_usuario

                INNER JOIN DISTRIBUIDOR d ON
                    jud.id_distribuidor = d.id_distribuidor
            
            WHERE
                ju.id_usuario = :id_usuario
                AND d.id_distribuidor IN :distribuidores
                AND jud.d_e_l_e_t_ = 0
                AND d.status = 'A'
                AND ju.status = 'A'
                AND jud.status = 'A'
        """

        params = {
            "id_usuario": id_usuario,
            "distribuidores": distribuidores
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        distribuidores_query = list(itertools.chain(*response))

        if len(distribuidores_query) != len(distribuidores):
            logger.error(f"Token do usuario Jmanager {id_usuario} sem todos os distribuidores ativos associados ao mesmo.")
            return False, ({"status":401,
                            "resposta":{
                                "tipo":"11",
                                "descricao":f"Token inválido."}}, 401)

        distribuidores = distribuidores_query

        if permission_range:

            permission_range = set(filter(lambda x: x >= 1, list(permission_range)))
            if id_perfil not in permission_range:
                logger.error("Usuario sem permissao para acessar o servico.")
                return False, ({"status":403,
                                "resposta":{
                                    "tipo":"12",
                                    "descricao":f"Usuario sem permissão para realizar ação."}}, 403)

        if (id_perfil == 1) and (0 not in distribuidores):
            logger.error(f"Usuario root do Jmanager id:{id_usuario} sem distribuidor 0 entre seus distribuidores.")
            return False, ({"status":403,
                            "resposta":{
                                "tipo":"12",
                                "descricao":f"Usuario sem permissão para realizar ação."}}, 403)

        if (id_perfil != 1) and (0 in distribuidores):
            logger.error(f"Usuario normal do Jmanager id:{id_usuario} com distribuidor 0 entre seus distribuidores.")
            return False, ({"status":403,
                            "resposta":{
                                "tipo":"12",
                                "descricao":f"Usuario sem permissão para realizar ação."}}, 403)

        token_payload.update({
            "id_distribuidor": distribuidores,
            "email": email
        })

        save_data = {
            "id_usuario": id_usuario,
            "id_perfil": id_perfil,
            "email": email,
            "token_info": token_payload,
            "token": token,
        }

        global_info.save_info_thread(**save_data)

        return True, token_payload

    ## Api de Registro
    elif agent == "api-registro":

        # Verificar se o token é valido
        public_key = open(file = f"other\\keys\\api-registro.pub", mode = "r").read()

        dict_decode = {
            "token": token,
            "public_key": public_key,
        }

        answer, response = decode_token(**dict_decode)

        if not answer:
            return False, response

        token_payload = response

        return True, token_payload

    else:
        raise KeyError("Invalid agents to decode the token.")


def auth_wrapper(permission_range: list = None, bool_alterar_senha: bool = True, 
                    bool_auth_required: bool = True, forced_user: str = None):
    """
    Middleware para realizar a autenticação dos usuarios

    Args:
        permission_range (list): Permissões permitidas para usuarios jmanager,
        bool_alterar_senha (bool): Caso a verificação de alterar senha tenha que ser ignorada
        bool_auth_required (bool): Caso a rota necessite de autenticação para entrar.
        forced_user (str): Caso seja necessário forcar uma autenticação especifica em uma rota.

    Returns:
        [No defined type]: Retorna seja lá o que o func retornar
    """
    if permission_range is None:
        permission_range = []

    def decorador(func: callable):
        """
        Decorador em si

        Args:
            func (callable): função qualquer que receberá a autenticação
        """
        
        @wraps(func)
        def wrapper(self, *args, **kwargs):

            data = global_info.get_info_thread()

            if forced_user:
                route_type_used = forced_user
            else:
                route_type_used = data.get("api") 

            token = request.headers.get("token")

            if not token and bool_auth_required:

                logger.error("Token nao enviado.")
                response = {"status":401,
                            "resposta":{
                                "tipo": "13", 
                                "descricao": "Ação recusada: Token não enviado."}}, 401 

            if token or bool_auth_required:

                dict_verify_token = {
                    "agent": route_type_used,
                    "token_envio": token,
                    "permission_range": permission_range,
                    "bool_alterar_senha": bool_alterar_senha,
                }

                answer, response = verify_token(**dict_verify_token)

                if not answer:
                    delete_token(token, route_type_used)
                    return response          

            response = func(self, *args, **kwargs)
            return response

        return wrapper
    
    return decorador


def social_validate(plataforma: str, token: str) -> tuple:
    """
    Realiza a validação do acesso social do usuario

    Args:
        plataforma (str): De qual plataforma o usuario está logando

            - google
            - facebook
            - apple

        token (str): Token de identificação do usuario da plataforma

    kwargs:
        connector: Conector do sql caso necessário

    Returns:
        tuple: Tupla contendo o resultado da operação e os dados de resposta
    """

    plataforma = str(plataforma).lower()
    social_id = None
    social_email = None

    if plataforma not in ["google", "facebook", "apple"]:
        return False, ({"status":401,
                        "resposta":{
                            "tipo":"11",
                            "descricao":f"'social-platform' não foi enviada."}}, 401)

    try:
        # Verificando o token
        if plataforma == "google":

            client_id = os.environ.get("GOOGLE_CLIENT_ID_PS")

            response = id_token.verify_oauth2_token(token, google_transport.Request(), client_id)
            
            social_id = response.get("sub")
            social_email = response.get("email")
            social_picture = response.get("picture")

            if social_picture:
                social_picture = str(social_picture).split("=")[0]
                social_picture = f"{social_picture}=s150-c"

            verified_email = response.get("email_verified")

            if not verified_email:
                raise Exception(f"Google - Email {social_email} is not verified by google.")

        elif plataforma == "facebook":

            url = "https://graph.facebook.com/me"

            params = {
                "access_token": token,
                "fields": "id,email,picture.type(large)"
            }

            dict_request = {
                "url": url,
                "params": params,
                "timeout": 5
            }

            answer, response = sm.request_api(**dict_request)
            if not answer:
                return response

            if response.status_code != 200:
                
                data = response.json()["error"]
                error_type = data["type"]
                error_message = data["message"]
                raise Exception(f"Facebook - Error: {error_type} \nmessage: {error_message}")
            
            data = response.json()

            social_id = data["id"]
            social_email = data["email"]
            if data.get("picture"):
                if data["picture"].get("url"):
                    social_picture = data["picture"]["url"]

                if data["picture"].get("data"):
                    if data["picture"]["data"].get("url"):
                        social_picture = data["picture"]["data"]["url"]

        elif plataforma == "apple":

            # Passo 1 - Pegar o header do token
            token_header = jwt.get_unverified_header(token)

            # Passo 2 - Pegar chaves publicas
            url = "https://appleid.apple.com/auth/keys"

            dict_request = {
                "url": url,
                "method": "get",
                "timeout": 5,
            }

            answer, response = sm.request_api(**dict_request)
            if not answer:
                return response
            
            if response.status_code != 200:

                logger.error(f"Erro vindo da apple => status: {response.status_code} content: {response.text}")
                return {"status":401,
                        "resposta":{
                            "tipo":"11",
                            "descricao":f"Validação da apple indisponivel temporariamente"}}, 401

            # Passo 3 - Verificar se a chave publica do token existe
            keys = response.json()["keys"]

            check_key = token_header.get("kid")
            full_key = {}

            for public_key in keys:

                if public_key["kid"] == check_key:

                    full_key = public_key                    
                    break

            else:
                logger.error("Token da apple com chave publica no header não existente entre as pegues.")

                msg = {"status":401,
                       "resposta":{
                           "tipo":"11",
                           "descricao":f"Token de apple inválido."}}, 401

                return False, msg

            # Passo 4 - Transformar "n" e "e" na chave publica
            n = full_key["n"]
            e = full_key["e"]

            if len(n) % 4 != 0:
                n += "=" * (4 - len(n) % 4)

            binary_string_n = bytes(n, encoding='ascii')
            modulus = int.from_bytes(base64.urlsafe_b64decode(binary_string_n))

            binary_string_e = bytes(e, encoding='ascii')
            hexa_exponent = base64.urlsafe_b64decode(binary_string_e)
            binary_exponent = "".join([f"{hexa:08b}" for hexa in hexa_exponent])
            exponent = int(binary_exponent, 2)

            public_key = RSA.construct((modulus, exponent)).export_key()

            # Passo 5 - Decodar tokem com chave publica
            try:
                audience = os.environ.get("APPLE_CLIENT_ID_PS")

                dict_jwt = {
                    "jwt": token,
                    "algorithms": ["RS256"],
                    "key": public_key,
                    "audience": audience,
                    "options": {
                        "verify_signature": True
                    }
                }

                token_payload = jwt.decode(**dict_jwt)

            except jwt.exceptions.ExpiredSignatureError:
                logger.error("Token de sessao da apple expirado.")

                msg = {"status":401,
                       "resposta":{
                           "tipo":"11",
                           "descricao":f"Token de apple expirado."}}, 401

                return False, msg
            
            except Exception as e:
                logger.exception(e)

                msg = {"status":401,
                       "resposta":{
                           "tipo":"11",
                           "descricao":f"Token da apple inválido."}}, 401

                return False, msg 

            # Passo 6 - Verificando o "nonce"
            true_nonce = os.environ.get("APPLE_NONCE_VALUE_PS")

            if not true_nonce == token_payload["nonce"]:
                logger.error("Token da apple com valor de 'nonce' diferente do salvo.")

                msg = {"status":401,
                       "resposta":{
                           "tipo":"11",
                           "descricao":f"Token de apple inválido."}}, 401

                return False, msg

            # Passo 7 - Verificando o iss
            if not token_payload["iss"] == "https://appleid.apple.com":

                logger.error("Token da apple com iss diferente do valor recomendado.")

                msg = {"status":401,
                       "resposta":{
                           "tipo":"11",
                           "descricao":f"Token de apple inválido."}}, 401

                return False, msg

            # Passo 8 - Verificando se o email é verificado
            if not token_payload.get("email_verified"):

                logger.error("Token da apple com email não verificado.")

                msg = {"status":401,
                       "resposta":{
                           "tipo":"11",
                           "descricao":f"Token de apple inválido."}}, 401

                return False, msg

            social_id = token_payload["sub"]
            social_email = token_payload["email"]
            social_picture = None

        data = {
            "social_id": social_id,
            "social_email": social_email,
            "social_picture": social_picture
        }

        return True, data

    except Exception as e:
        logger.exception(e)
        msg = {"status":401,
               "resposta":{
                   "tipo":"11",
                   "descricao":f"Validação do social do {plataforma} falhou."}}, 401

        return False, msg


def login_social(plataforma: str, token: str, **kwargs) -> tuple:
    """
    Pega as informações para realizar o login do usuario por uma conta social

    Args:
        plataforma (str): De qual plataforma o usuario está logando
        token (str): Token de identificação do usuario da plataforma

    Returns:
        tuple: Tupla contendo o resultado da operação e os dados de resposta
    """

    plataforma = str(plataforma).lower()

    answer = social_validate(plataforma, token)

    if not answer[0]:
        return answer

    social_id = answer[1].get("social_id")
    social_email = answer[1].get("social_email")

    try:

        if not social_id:
            msg = {"status":401,
                   "resposta":{
                       "tipo":"11",
                       "descricao":f"Login social sem id cadastrado para a plataforma {plataforma}."}}, 401

            return False, msg

        if not social_email:
            msg = {"status":401,
                   "resposta":{
                       "tipo":"11",
                       "descricao":f"Login social sem email cadastrado para a plataforma {plataforma}."}}, 401

            return False, msg

        # Verificando os dados no banco para validação
        to_return = {
            "social_id": social_id
        }

        user_id_query = f"""
            SELECT
                TOP 1 email

            FROM
                USUARIO

            WHERE
                id_{plataforma} = :social_id
        """

        params = {
            "social_id": social_id
        }

        dict_query = {
            "query": user_id_query,
            "params": params,
            "first": True, 
            "raw": True,
        }

        user_id_query = dm.raw_sql_return(**dict_query)

        if user_id_query:
            
            to_return["social_email"] = user_id_query[0]

            return True, to_return

        # Verificação por email
        user_query = """
            SELECT
                TOP 1 id_usuario, 
                email

            FROM
                USUARIO

            WHERE
                email COLLATE Latin1_General_CI_AI LIKE :email
        """

        params = {
            "email": social_email
        }

        dict_query = {
            "query": user_query,
            "params": params,
            "first": True, 
            "raw": True,
        }

        user_query = dm.raw_sql_return(**dict_query)

        if user_query:

            id_usuario, email = user_query

            update_data = {
                "id_usuario": id_usuario,
                f"id_{plataforma}": social_id
            }

            key_columns = ["id_usuario"]

            dm.raw_sql_update("USUARIO", update_data, key_columns)

            to_return["social_email"] = email
            return True, to_return
    
        logger.info("Usuario com cadastro/associação por login social permitido.")
        msg = {"status":401,
               "resposta":{
                   "tipo":"19",
                   "descricao":f"Usuario com cadastro social permitido."}}, 401

        return False, msg

    except Exception as e:
        logger.exception(e)
        msg = {"status":401,
                "resposta":{
                    "tipo":"11",
                    "descricao":f"Login social não cadastrado."},
                "social_email":social_email}, 401

        return False, msg