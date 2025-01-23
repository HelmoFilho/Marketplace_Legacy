#=== Importações de módulos externos ===#
from firebase_admin import messaging, initialize_app, credentials
import requests, json, smtplib, ssl, os, datetime, itertools, re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm

cred = credentials.Certificate("other\\json\\credenciais_firebase.json")
default_app = initialize_app(cred)

server = os.environ.get("PSERVER_PS").lower()

if server == "production":
    route = str(os.environ.get("SERVER_GUARANY_PS")).lower()
    image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
    
elif server == "development":
    route = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

else:
    route = f"http://localhost:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"
    
    
def request_api(url: str, headers: dict = None, body: dict = None, params: dict = None,
                timeout: float = None, **kwargs) -> tuple:
    """
    Realiza uma request para a url requerida

    Args:
        url (str): Rota que sera feita a request
        headers (dict, optional): Headers adicionais da request. Defaults to None.
        body (dict, optional): Body da request. Defaults to None.
        params (dict, optional): Parâmetros de url da request. Defaults to None.
        timeout (float, optional): Tempo de espera de resposta do servidor. Defaults to None.

    kwargs:
        method (str): Qual o metodo utilizado para ser feita a request
            
            - GET

            - POST

            - PUT

            - DELETE

        Caso não seja fornecidor será padronizado para POST

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    if not isinstance(headers, dict):
        headers = {}

    if not isinstance(body, dict):
        body = {}

    if not isinstance(params, dict):
        params = {}

    if not (isinstance(timeout, (float, int))):

        if timeout is not None:
            timeout = 5

    method = kwargs.get("method")

    match method:

        case "get" | "put" | "delete" | "options":
            pass

        case _:
            method = "post"

    try:
        if not headers.get("Content-Type"):
            headers["Content-Type"] = "application/json"

        dict_request = {
            "method": method,
            "url": url, 
            "timeout": timeout
        }

        if params:
            dict_request["params"] = params

        if body:
            dict_request["json"] = body

        if headers:
            dict_request["headers"] = headers

        request_response = requests.request(**dict_request)
        return True, request_response

    except requests.exceptions.Timeout as e:
        msg = "request não respondeu dentro do tempo de timeout."

        logger.error(msg)
        logger.exception(e)
        
        msg = {"status":400,
               "resposta":{
                   "tipo":"15",
                   "descricao":f"Houve erro com a transação: Tempo de comunicação estourado"}}, 200

        return False, msg

    except Exception as e:
        msg = "Houve um erro na comunicacao com a request."

        logger.error(msg)
        logger.exception(e)

        msg = {"status":400,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Houve erro com a transação: {msg}."}}, 200

        return False, None


def send_sms(destination_number: str = None, message: str = None) -> bool:
    """
    Envia mensagem sms para um numero brasileiro com uma mensagem especifica

    Args:
        destination_number (str): Numero de destino. Defaults to None.
        message (str): mensagem que sera mostrada no aparelho do individuo. Defaults to None.

    Returns:
        bool: Situação da transação
    """

    msg = ""

    if destination_number and message:

        to_send = str(destination_number)

        # Verifica se o telefone é de celular
        if re.match("^(55)?[1-9]{2}9[6-9][0-9]{7}$", to_send):

            # Adiciona o 55 na frente do numero se o mesmo não tiver
            if (to_send[:2] != "55") and len(to_send) == 11:  to_send = "55" + to_send

            # Pega a url do serviço de envio de sms
            url = os.environ.get("SMS_API_PS")

            # Cria o payload com as informações necessarias
            payload = json.dumps({
                "destination": to_send,
                "messageText": message
            })

            # Cria o header do request
            headers = {
                'username': f'{os.environ.get("SMS_MAIL_PS")}',
                'authenticationToken': f'{os.environ.get("SMS_TOKEN_PS")}',
                'charset': 'utf-8',
                'Content-Type': 'application/json'
            }

            try:
                dict_request = {
                    
                }
                requests.post()
                requests.request("POST", url, headers=headers, data=payload)
                logger.info("Mensagem sms enviada com sucesso.")
                return True, None

            except:
                msg = "Mensagem SMS nao enviada devido a erro com o request direto."
                logger.error(msg)
        
        else:
            msg = "Mensagem SMS nao enviada devido a string do numero nao ser inteiramente numerica."
            logger.error(msg)

    else:
        msg = "Mensagem SMS nao enviada devido a numero e/ou mensagem nao serem enviados."
        logger.error(msg)        

    return False, msg


def send_email(reciever: list[str]|str, subject: str, html: str = None, text: str = None,
                hide_to: bool = False) -> bool:
    """
    Envia mensagem sms para um numero brasileiro com uma mensagem especifica

    Args:
        reciever (list[str]|str): emails dos individuos que receberão o email.
        subject (str): De quem o email é enviado.
        html (str, optional): Html que será mostrado, Caso enviado o texto não sera mostrado. Defaults to None.
        text (str, optional): Texto que será mostrado, só será mostrado caso o html não ser enviado. Defaults to None.
        hide_to (bool, optional): Caso o 'enviado para' deva ser mostrado no email. Defaults to False

    Returns:
        bool: Situação da transação
    """

    if not isinstance(reciever, (list, set, tuple)):
        reciever = [reciever]

    reciever = list(reciever)

    # Criação do corpo do email
    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = os.environ.get("MAIL_USERNAME_PS")

    if not hide_to:
        message["To"] = ",".join([str(email) for email in reciever])

    if html:
        body = MIMEText(html, "html")
        
    else:
        MIMEText(text, "plain")
    
    message.attach(body)

    # Servidor e de qual port é realizado o envio do email
    server = os.environ.get("MAIL_SERVER_PS")
    port = int(os.environ.get("MAIL_PORT_PS"))
    chryptography = os.environ.get("MAIL_CRYPTOGRAPHY_PS")
    username = os.environ.get("MAIL_USERNAME_PS")
    password = os.environ.get("MAIL_PASSWORD_PS")

    try:
        if str(chryptography).upper() == "SSL" or chryptography is None:
            
            context = ssl.create_default_context()
            
            with smtplib.SMTP_SSL(server, port, context = context) as server:
                
                server.login(username, password)
                response = server.sendmail(username, reciever, message.as_string())

            erros_emails = list(response.keys())

            if len(erros_emails) >= len(reciever):
                logger.info("Mensagem de EMAIL nao enviada devido a algum erro com a string de email.")
                return False, None

            dict_response = {
                "sucessos": len(reciever) - len(erros_emails),
                "falhas": len(erros_emails),
                "emails_falhos": erros_emails,
            }

            logger.info("Mensagem de email enviada com sucesso.")
            return True, dict_response

        elif str(chryptography).upper() in {"TSL", "STARTTLS"}:
            
            with smtplib.SMTP(server, port) as server:
                
                server.starttls()
                server.login(username, password)
                response = server.sendmail(username, reciever, message.as_string())

            erros_emails = list(response.keys())

            if len(erros_emails) >= len(reciever):
                logger.info("Mensagem de EMAIL nao enviada devido a algum erro com a string de email.")
                return False, None

            dict_response = {
                "sucessos": len(reciever) - len(erros_emails),
                "falhas": len(erros_emails),
                "emails_falhos": erros_emails,
            }

            logger.info("Mensagem de email enviada com sucesso.")
            return True, dict_response

        raise KeyError("Variavel MAIL_CRYPTOGRAPHY_PS nao foi declarada ou com valor invalido no env")

    except Exception as e:
        logger.error("Mensagem de EMAIL nao enviada devido a algum erro com o envio.")
        logger.exception(e)
        return False, "Mensagem nao enviada devido a algum erro com o envio."


def google_notification(tokens: list[str], titulo: str, corpo: str, filtros: dict = None, imagem: str = None) -> dict:
    """
    Envia uma notificacao do google firebase

    Args:
        tokens (list[str]): Tokens do firebase dos aparelhos para os quais serão enviados as notificações.
        titulo (str): Titulo da notificacao.
        corpo (str): Corpo da notificação.
        filtros (dict, optional): Dados adicionais da notificação. Defaults to None.
        imagem (str, optional): URL da imagem que aparecerá na notificação. Defaults to None.

    Returns:
        dict: informações do envio da notificação.
    """

    assert isinstance(tokens, (list, str)), "Tokens with invalid type."

    if isinstance(tokens, str):
        tokens = [tokens]

    if not filtros:
        filtros = {}

    else:
        assert isinstance(filtros, dict), "filters with invalid type."

    total = len(tokens)
    tokens_per_time = 500
    paginas = (total // tokens_per_time) + (total % tokens_per_time > 0)

    user_error = []

    erros = 0

    for pagina in range(paginas):

        registration_tokens = tokens[pagina * tokens_per_time: (pagina + 1) * tokens_per_time]
        
        message = messaging.MulticastMessage(
            notification=messaging.Notification(
                title= titulo,
                body= corpo,
                image= imagem
            ),
            data = filtros,
            tokens = registration_tokens,
        )

        response = messaging.send_multicast(message)
        count_error = len(registration_tokens) - response.success_count

        if count_error > 0:
            for index, notification_obj in enumerate(response.responses):
                if not notification_obj.success:
                    user_error.append(registration_tokens[index])

        erros += count_error

    return {
        "envios": total,
        "sucessos": total - erros,
        "erros": erros,
        "tokens_falhas": user_error,
    }


def status_notification(id_notificacao: int, status: str, tokens_falhos: list[str] = None,
                data_envio: datetime = None, enviado_por: str = 'padrao'):
    """
    Altera o status da notificacao

    Args:
        id_notificacao (int): Id da notificação que será alterado o status.
        status (str): Status para qual deve ser alterado e deve ser
            
            * E
            * F

        tokens_falhos (list[str]): Tokens que falharam no envio de status 'E'. No status 'F' não enviar. Defaults to None.
        data_envio (datetime): Objeto datetime da data de envio.
        enviado_por (str): Quem está enviando a notificação.
    """

    status = str(status).upper()

    assert status in ["E", "F"], 'Invalid status'

    if tokens_falhos:
        assert isinstance(tokens_falhos, list), "tokens_falhos must be a list."

    if data_envio:
        assert isinstance(data_envio, datetime.datetime), "data_envio must be a datetime object."
    
    else:
        data_envio = datetime.datetime.now()

    data_envio_string = data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    query = """
        SELECT
            TOP 1 rapp.id_rota

        FROM
            NOTIFICACAO ntfc

            INNER JOIN ROTAS_APP rapp ON
                ntfc.id_rota = rapp.id_rota

        WHERE
            ntfc.id_notificacao = :id_notificacao
    """

    params = {
        "id_notificacao": id_notificacao
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True,
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.error(f"Notificacao {id_notificacao} não existente para alteração de status.")
        return

    id_rota = response[0]

    if status == 'E':

        # Notificacao
        query = """
            UPDATE
                NOTIFICACAO

            SET
                status = 'E',
                data_envio = :data_envio,
                enviado_por = UPPER(:enviado_por)

            WHERE
                id_notificacao = :id_notificacao
        """

        params = {
            "id_notificacao": id_notificacao,
            "data_envio": data_envio_string,
            "enviado_por": enviado_por
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        # Atualizando os tokens invalidos
        if tokens_falhos:

            total = len(tokens_falhos)
            tokens_per_time = 500
            users_per_time = 1000

            paginas = (total // users_per_time) + (total % users_per_time > 0)

            query = """
                UPDATE
                    NOTIFICACAO_USUARIO

                SET
                    status = 'F',
                    data_envio = :data_envio,
                    lida = NULL

                WHERE
                    id_notificacao = :id_notificacao
                    AND token IN :tokens
            """

            for pagina in range(paginas):

                users_notification_failed = tokens_falhos[pagina * tokens_per_time: (pagina + 1) * tokens_per_time]

                params = {
                    "id_notificacao": id_notificacao,
                    "data_envio": data_envio_string,
                    "tokens": users_notification_failed
                }

                dict_query = {
                    "query": query,
                    "params": params,
                }

                dm.raw_sql_execute(**dict_query)

        query = """
            UPDATE
                NOTIFICACAO_USUARIO

            SET
                status = 'F',
                data_envio = :data_envio

            WHERE
                id_notificacao = :id_notificacao
                AND data_envio IS NULL
                AND ISNULL(status, 'P') NOT IN ('E', 'F')
                AND (
                        token LIKE 'android-%'
                        OR token LIKE 'ios-%'
                        OR token LIKE 'web-%'
                    )
        """   

        params = {
            "id_notificacao": id_notificacao,
            "data_envio": data_envio_string,
        }

        dict_query = {
            "query":query,
            "params": params
        }

        dm.raw_sql_execute(**dict_query)

        # Atualizando os tokens enviados
        query = """
            UPDATE
                NOTIFICACAO_USUARIO

            SET
                status = 'E',
                data_envio = :data_envio,
                lida = NULL

            WHERE
                id_notificacao = :id_notificacao
                AND data_envio IS NULL
                AND ISNULL(status, 'P') NOT IN ('E', 'F')
        """   

        params = {
            "id_notificacao": id_notificacao,
            "data_envio": data_envio_string,
        }

        dict_query = {
            "query":query,
            "params": params
        }

        dm.raw_sql_execute(**dict_query)

    else:
        # Notificacao
        query = """
            UPDATE
                NOTIFICACAO

            SET
                status = 'F',
                data_envio = :data_envio,
                enviado_por = UPPER(:enviado_por)

            WHERE
                id_notificacao = :id_notificacao
        """

        params = {
            "id_notificacao": id_notificacao,
            "data_envio": data_envio_string,
            "enviado_por": enviado_por
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        # Usuarios
        query = """
            UPDATE
                NOTIFICACAO_USUARIO

            SET
                status = 'F',
                data_envio = :data_envio

            WHERE
                id_notificacao = :id_notificacao
        """   

        params = {
            "id_notificacao": id_notificacao,
            "data_envio": data_envio_string,
        }

        dict_query = {
            "query":query,
            "params": params
        }

        dm.raw_sql_execute(**dict_query)

    if id_rota in {2,3,4,9}:

        # Produto
        if id_rota == 2:

            query = """
                UPDATE
                    NOTIFICACAO_PRODUTO

                SET
                    data_envio = :data_envio

                WHERE
                    id_notificacao = :id_notificacao
            """

        # Oferta
        elif id_rota == 3:

            query = """
                UPDATE
                    NOTIFICACAO_OFERTA

                SET
                    data_envio = :data_envio

                WHERE
                    id_notificacao = :id_notificacao
            """

        # Cupom
        elif id_rota == 4:

            query = """
                UPDATE
                    NOTIFICACAO_CUPOM

                SET
                    data_envio = :data_envio

                WHERE
                    id_notificacao = :id_notificacao
            """

        # Noticia
        elif id_rota == 9:

            query = """
                UPDATE
                    NOTIFICACAO_NOTICIA

                SET
                    data_envio = :data_envio

                WHERE
                    id_notificacao = :id_notificacao
            """
    
        params = {
            "id_notificacao": id_notificacao,
            "data_envio": data_envio_string,
        }

        dict_query = {
            "query":query,
            "params": params
        }

        dm.raw_sql_execute(**dict_query)


def send_notification(id_notificacao: int, enviado_por: str = 'padrao') -> tuple:
    """
    Envia a notificação escolhida

    Args:
        id_notificacao (int): ID da notificação a ser enviada.
        enviado_por (str): Quem está enviando a notificação.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    data_envio = datetime.datetime.now()

    # Pegando as informações da notificação
    query = """
        SELECT
            TOP 1
                ntfc.id_notificacao,
                ntfc.id_distribuidor,
                lacapp.id_acao,
                lacapp.descricao as descricao_acao,
                rapp.id_rota,
                rapp.descricao_rota,
                ISNULL(rapp.parametro_ptbr, rapp.parametro_eng) as parametro_rota,
                UPPER(ntfc.titulo) as titulo,
                ntfc.corpo,
                ntfc.imagem,
                UPPER(ntfc.status) as status,
                ntfc.lista_clientes as id_lista_cliente

        FROM
            NOTIFICACAO ntfc

            INNER JOIN ROTAS_APP rapp ON
                ntfc.id_rota = rapp.id_rota

            LEFT JOIN LISTA_ACAO_APP lacapp ON
                ntfc.id_acao = lacapp.id_acao
                AND lacapp.status = 'A'

        WHERE
            ntfc.id_notificacao = :id_notificacao
    """

    params = {
        "id_notificacao": id_notificacao
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info(f"Notificação {id_notificacao} não existente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação não existente."},
               }, 200

        return False, msg

    status = response.get("status")

    if status == 'C':
        logger.info(f"Notificação {id_notificacao} com envio cancelado.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação com envio cancelado."},
               }, 200

        return False, msg

    elif status == 'F':
        logger.info(f"Notificação {id_notificacao} falhou no envio anterior.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação falhou no envio anterior."},
               }, 200

        return False, msg

    elif status == 'E':
        logger.info(f"Notificação {id_notificacao} já enviado anteriormente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação já enviado anteriormente."},
               }, 200

        return False, msg

    elif status != 'P':
        logger.info(f"Notificação {id_notificacao} com status - {status} inválido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação com status inválido."},
               }, 200

        return False, msg

    notificacao = response.copy()

    # Pegando os usuarios que receberão a notificação
    query = """
        SELECT
            DISTINCT token

        FROM
            NOTIFICACAO_USUARIO

        WHERE
            id_notificacao = :id_notificacao
            AND (
                    token NOT LIKE 'android-%'
                    AND token NOT LIKE 'ios-%'
                    AND token NOT LIKE 'web-%'
                )
    """

    params = {
        "id_notificacao": id_notificacao
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        
        id_lista_cliente = notificacao.get("id_lista_cliente")

        if id_lista_cliente:

            query = """
                INSERT INTO
                    NOTIFICACAO_USUARIO
                        (
                            id_notificacao,
                            id_usuario,
                            email,
                            token,
                            status,
                        )

                    SELECT
                        :id_notificacao,
                        u.id_usuario,
                        u.email,
                        u.token_firebase,
                        'P'

                    FROM
                        (

                            SELECT
                                DISTINCT
                                    u.id_usuario,
                                    u.email,
                                    s.token_firebase

                            FROM
                                USUARIO u

                                INNER JOIN USUARIO_CLIENTE uc ON
                                    u.id_usuario = uc.id_usuario

                                INNER JOIN CLIENTE c ON
                                    uc.id_cliente = c.id_cliente

                                INNER JOIN LISTA_SEGMENTACAO_USUARIO lsu ON
                                    c.id_cliente = lsu.id_cliente

                                INNER JOIN LISTA_SEGMENTACAO ls ON
                                    lsu.id_lista = ls.id_lista

                                INNER JOIN LOGIN l ON
                                    u.id_usuario = l.id_usuario

                                INNER JOIN USUARIO_SESSAO us ON
                                    l.id_usuario = us.id_usuario

                                INNER JOIN SESSAO s ON
                                    us.id_sessao = s.id_sessao
                                    AND l.token_aparelho = s.token_aparelho

                            WHERE
                                ls.id_lista = :id_lista_cliente
                                AND s.fim_sessao IS NULL
                                AND l.data_logout IS NULL
                                AND s.d_e_l_e_t_ = 0
                                AND l.d_e_l_e_t_ = 0
                                AND u.d_e_l_e_t_ = 0
                                AND uc.d_e_l_e_t_ = 0
                                AND ls.d_e_l_e_t_ = 0
                                AND u.status = 'A'
                                AND uc.status = 'A'
                                AND c.status = 'A'
                                AND ls.status = 'A'

                        ) u

                    WHERE
                        (
                            u.token_firebase NOT LIKE 'android-%'
                            AND u.token_firebase NOT LIKE 'ios-%'
                            AND u.token_firebase NOT LIKE 'web-%'
                        )
            """

            params = {
                "id_notificacao": id_notificacao,
                "id_lista_cliente": id_lista_cliente
            }

        else:

            query = """
                INSERT INTO
                    NOTIFICACAO_USUARIO
                        (
                            id_notificacao,
                            id_usuario,
                            email,
                            token,
                            data_envio,
                            status,
                            lida
                        )

                    SELECT
                        :id_notificacao,       
                        su.id_usuario,
                        su.email,
                        su.token,
                        NULL as data_envio,
                        'P' as status,
                        NULL lida

                    FROM
                        (
                            SELECT
                                DISTINCT
                                    su.id_usuario as id_usuario,
                                    su.email as email,
                                    s.token_firebase as token

                            FROM
                                SESSAO s

                                LEFT JOIN
                                (

                                    SELECT
                                        u.id_usuario,
                                        u.email,
                                        us.id_sessao

                                    FROM
                                        USUARIO u

                                        INNER JOIN
                                        (

                                            SELECT
                                                DISTINCT
                                                    us.id_usuario,
                                                    s.id_sessao

                                            FROM
                                                SESSAO s

                                                INNER JOIN USUARIO_SESSAO us ON
                                                    s.id_sessao = us.id_sessao
                                            
                                            WHERE
                                                s.d_e_l_e_t_ = 0
                                                                                
                                        ) us ON
                                            u.id_usuario = us.id_usuario

                                    WHERE
                                        u.d_e_l_e_t_ = 0

                                ) su ON
                                    s.id_sessao = su.id_sessao

                            WHERE
                                (
                                    s.token_firebase NOT LIKE 'android-%'
                                    AND s.token_firebase NOT LIKE 'ios-%'
                                    AND s.token_firebase NOT LIKE 'web-%'
                                )

                        ) su
            """

            params = {
                "id_notificacao": id_notificacao
            }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        query = """
            SELECT
                DISTINCT token

            FROM
                NOTIFICACAO_USUARIO

            WHERE
                id_notificacao = :id_notificacao
                AND LEN(token) > 0
        """

        params = {
            "id_notificacao": id_notificacao
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:

            dict_status = {
                "id_notificacao": id_notificacao,
                "status": 'F',
                "data_envio": data_envio,
                "enviado_por": enviado_por,
            }

            status_notification(**dict_status)

            if id_lista_cliente:
                logger.info(f"Notificação {id_notificacao} para a lista de segmentação {id_lista_cliente} não retornou tokens na releitura dos token.")

            else:    
                logger.info(f"Notificação {id_notificacao} não retornou tokens na releitura dos token.")

            msg = {"status":400,
                   "resposta":{
                       "tipo":"15",
                       "descricao":f"Erro na leitura dos tokens da notificação após o insert dos mesmos."},
                   }, 200

            return False, msg

    tokens = list(itertools.chain(*response))

    # Pegando os filtros da notificação
    id_rota = notificacao.get("id_rota")

    parametro_rota = notificacao.get("parametro_rota")
    id_distribuidor = notificacao.get("id_distribuidor")

    titulo = notificacao.get("titulo")
    corpo = notificacao.get("corpo")

    imagem = notificacao.get("imagem")
    if imagem:
        imagem = f"{image}/imagens/distribuidores/500/{id_distribuidor}/notificacao/{imagem}"

    else:
        imagem = None

    dict_filtros = {
        "rota": parametro_rota,
        "id_distribuidor": str(id_distribuidor),
        "id_notificacao": str(id_notificacao)
    }

    if id_rota not in {1,5,6,7,8}:

        if id_rota not in {2,3,4,9}:
            logger.info(f"Notificação {id_notificacao} com rota inválida.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Ação recusada: Notificação com rota inválida."},
                   }, 200

            return False, msg

        parametro = ""

        if id_rota == 2:

            parametro = "id_produto"

            query = """
                SELECT
                    codigo_produto as id_produto

                FROM
                    NOTIFICACAO_PRODUTO

                WHERE
                    id_notificacao = :id_notificacao
            """

        elif id_rota == 3:

            parametro = "id_oferta"

            query = """
                SELECT
                    CONVERT(VARCHAR, codigo_oferta) as id_oferta

                FROM
                    NOTIFICACAO_OFERTA

                WHERE
                    id_notificacao = :id_notificacao
            """

        elif id_rota == 4:

            parametro = "id_cupom"

            query = """
                SELECT
                    CONVERT(VARCHAR, id_cupom) as id_cupom

                FROM
                    NOTIFICACAO_CUPOM

                WHERE
                    id_notificacao = :id_notificacao
            """

        elif id_rota == 9:

            parametro = "id_noticia"

            query = """
                SELECT
                    CONVERT(VARCHAR, id_noticia) as id_noticia

                FROM
                    NOTIFICACAO_NOTICIA

                WHERE
                    id_notificacao = :id_notificacao
            """
 
        params = {
            "id_notificacao": id_notificacao
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:

            dict_status = {
                "id_notificacao": id_notificacao,
                "status": 'F',
                "data_envio": data_envio,
                "enviado_por": enviado_por,
            }

            status_notification(**dict_status)

            logger.info(f"Notificação {id_notificacao} sem filtros válidos.")
            msg = {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Notificação sem filtros válidos."},
                    }, 200

            return False, msg
        
        ids = list(itertools.chain(*response))

        dict_filtros.update({
            parametro: ",".join(ids)
        })

    dict_google = {
        "titulo": titulo,
        "corpo": corpo,
        "imagem": imagem,
        "filtros": dict_filtros,
        "tokens": tokens,
    }

    answer = google_notification(**dict_google)

    total = answer.get("envios")
    erros = answer.get("erros")
    user_error = answer.get("tokens_falhas")

    # Controle de resultados
    envio = erros < total

    dict_status = {
        "id_notificacao": id_notificacao,
        "status": 'E' if envio else 'F',
        "tokens_falhos": user_error,
        "data_envio": data_envio,
        "enviado_por": enviado_por,
    }

    status_notification(**dict_status)

    sucessos = total - erros

    if envio:
        logger.info(f"Notificação {id_notificacao} enviada com {sucessos} sucessos e {erros} falhas.")

    else:
        logger.info(f"Falha no envio da notificação {id_notificacao} devido a erros com os tokens ou com o certificado do google.")

    dados_envio = {
        "notificacao": id_notificacao,
        "resultado": envio,
        "total": total,
        "sucessos": sucessos,
        "erros": erros,
    }

    return True, dados_envio


def send_stock_notification(id_notificacao: int, enviado_por: str = "padrao") -> tuple:
    """
    Envia a notificação de estoque de produto escolhida

    Args:
        id_notificacao (int): ID da notificação a ser enviada.
        enviado_por (str): Quem está enviando a notificação.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    data_envio = datetime.datetime.now()

    # Pegando as informações da notificação
    query = """
        SELECT
            TOP 1 ntfce.id_usuario,
            ntfce.id_produto,
            ntfce.status        

        FROM
            NOTIFICACAO_ESTOQUE ntfce

        WHERE
            ntfce.id_notificacao = :id_notificacao
    """

    params = {
        "id_notificacao": id_notificacao
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} não existente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação não existente."},
               }, 200

        return False, msg

    status = response.get("status")
    id_produto = response.get("id_produto")
    id_usuario = response.get("id_usuario")

    # Checagem do status
    if status == 'C':
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com envio cancelado.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação com envio cancelado."},
               }, 200

        return False, msg

    elif status == 'F':
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} falhou no envio anterior.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação falhou no envio anterior."},
               }, 200

        return False, msg

    elif status == 'E':
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} já enviado anteriormente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação já enviado anteriormente."},
               }, 200

        return False, msg

    elif status != 'P':
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com status - {status} inválido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação com status inválido."},
               }, 200

        return False, msg

    # Checagem do produto
    query = """
        SELECT
            TOP 1 p.status as p_status,
            pd.status as pd_status,
            UPPER(p.descricao_completa) as descricao,
            pd.id_distribuidor

        FROM
            PRODUTO p

            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                p.id_produto = pd.id_produto

            INNER JOIN DISTRIBUIDOR d ON
                pd.id_distribuidor = d.id_distribuidor

        WHERE
            p.id_produto = :id_produto
            AND d.status = 'A'
    """

    params = {
        "id_produto": id_produto
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com produto {id_produto} inexistente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Produto inexistente."},
               }, 200

        dict_update = {
            "id_notificacao": id_notificacao,
            "status": 'F',
            "data_envio": data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

        key_columns = ["id_notificacao"]

        dm.core_sql_update("NOTIFICACAO_ESTOQUE", dict_update, key_columns)

        return False, msg

    p_status = response.get("p_status")
    pd_status = response.get("pd_status")

    if p_status != 'A' or pd_status != 'A':
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com produto {id_produto} de status inválido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao": "Ação recusada: Produto com status inválido."},
               }, 200

        dict_update = {
            "id_notificacao": id_notificacao,
            "status": 'F',
            "data_envio": data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

        key_columns = ["id_notificacao"]

        dm.core_sql_update("NOTIFICACAO_ESTOQUE", dict_update, key_columns)

        return False, msg

    descricao_produto = response.get("descricao")
    id_distribuidor = response.get("id_distribuidor")

    # Verificando o usuario
    query = """
        SELECT
            TOP 1 status

        FROM
            USUARIO u

        WHERE
            id_usuario = :id_usuario
            AND d_e_l_e_t_ = 0
    """

    params = {
        "id_usuario": id_usuario
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com usuario {id_usuario} inexistente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Usuario inexistente."},
               }, 200

        dict_update = {
            "id_notificacao": id_notificacao,
            "status": 'F',
            "data_envio": data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

        key_columns = ["id_notificacao"]

        dm.core_sql_update("NOTIFICACAO_ESTOQUE", dict_update, key_columns)

        return False, msg

    u_status = response[0]

    if u_status != 'A':
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com usuario {id_usuario}  de status inválido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Usuario com status inválido."},
               }, 200

        dict_update = {
            "id_notificacao": id_notificacao,
            "status": 'F',
            "data_envio": data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

        key_columns = ["id_notificacao"]

        dm.core_sql_update("NOTIFICACAO_ESTOQUE", dict_update, key_columns)

        return False, msg

    # Pegando o token
    query = """
        SELECT
            DISTINCT
                s.token_firebase

        FROM
            LOGIN l

            INNER JOIN USUARIO_SESSAO us ON
                l.id_usuario = us.id_usuario

            INNER JOIN SESSAO s ON
                us.id_sessao = s.id_sessao
                AND l.token_aparelho = s.token_aparelho

        WHERE
            l.id_usuario = :id_usuario
            AND (
                    s.token_firebase NOT LIKE 'android-%'
                    AND s.token_firebase NOT LIKE 'ios-%'
                    AND s.token_firebase NOT LIKE 'web-%'
                )
            AND s.fim_sessao IS NULL
            AND l.data_logout IS NULL
            AND s.d_e_l_e_t_ = 0
            AND l.d_e_l_e_t_ = 0
    """

    params = {
        "id_usuario": id_usuario
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} com usuario {id_usuario} não logado.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Usuario não logado."},
               }, 200

        dict_update = {
            "id_notificacao": id_notificacao,
            "status": 'F',
            "data_envio": data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        }

        key_columns = ["id_notificacao"]

        dm.core_sql_update("NOTIFICACAO_ESTOQUE", dict_update, key_columns)

        return False, msg

    tokens_firebase = list(itertools.chain(*response))

    titulo = "Produto disponível"
    corpo = f"O produto {descricao_produto} chegou ao nosso estoque."

    dict_google = {
        "titulo": titulo,
        "corpo": corpo,
        "imagem": None,
        "filtros": {
            "rota": "produtos",
            "id_distribuidor": str(id_distribuidor),
            "id_produto": id_produto,
            "id_notificacao": str(id_notificacao),
            "lembrete_estoque": "1",
        },
        "tokens": tokens_firebase,
    }

    answer = google_notification(**dict_google)

    sucessos = answer.get("sucessos")
    erros = answer.get("erros")
    tokens_falhos = answer.get("tokens_falhas")

    data = [
        {
            "id_notificacao": id_notificacao,
            "status": 'F' if token in tokens_falhos else 'E',
            "token_firebase": token,
        }
        for token in tokens_firebase
    ]

    dm.raw_sql_insert("NOTIFICACAO_ESTOQUE_TOKENS", data)

    if sucessos:
        status = 'E'

    else:
        status = 'F'

    dict_update = {
        "id_notificacao": id_notificacao,
        "status": status,
        "data_envio": data_envio.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
    }

    key_columns = ["id_notificacao"]

    dm.core_sql_update("NOTIFICACAO_ESTOQUE", dict_update, key_columns)

    if not sucessos:
        logger.info(f"Falha no envio de todas as notificação de lembrete de estoque com id {id_notificacao} para o usuario {id_usuario}.")

    elif erros:
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} enviada com {sucessos} sucessos e {erros} falhas.")
        
    else:
        logger.info(f"Notificação de lembrete de estoque {id_notificacao} enviada com sucesso.")

    dados_envio = {
        "notificacao": id_notificacao,
        "sucessos": sucessos
    }

    return True, dados_envio


def send_email_marketplace(id_email: int) -> tuple:
    """
    Envia a email para o usuario marketplace criado no jmanager

    Args:
        id_email (int): ID do email a ser enviado.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    data_envio = datetime.datetime.now()

    # Pegando as informações do email
    query = """
        SELECT
            TOP 1 e.id_email,
            e.assunto,
            e.status,
            e.html as corpo

        FROM
            EMAIL e

        WHERE
            e.id_email = :id_email
    """

    params = {
        "id_email": id_email
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info(f"Email {id_email} não existente ou sem usuarios atrelados.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Email não existente."},
               }, 200

        return False, msg

    # Checagem dos dados
    status = response.get("status")

    if status == 'C':
        logger.info(f"Email {id_email} com envio cancelado.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Email com envio cancelado."},
               }, 200

        return False, msg

    elif status == 'F':
        logger.info(f"Notificação {id_email} falhou no envio anterior.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação falhou no envio anterior."},
               }, 200

        return False, msg

    elif status == 'E':
        logger.info(f"Notificação {id_email} já enviado anteriormente.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação já enviado anteriormente."},
               }, 200

        return False, msg

    elif status != 'P':
        logger.info(f"Notificação {id_email} com status - {status} inválido.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Notificação com status inválido."},
               }, 200

        return False, msg

    assunto = response.get("assunto")

    if not assunto:
        
        dict_update = {
            "id_email": id_email,
            "status": "F",
            "data_envio": data_envio
        }

        key_columns = ["id_email"]

        dm.core_sql_update("EMAIL", dict_update, key_columns)
        dm.core_sql_update("EMAIL_USUARIO", dict_update, key_columns)

        logger.info(f"Email {id_email} sem assunto.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Email sem assunto."},
               }, 200

        return False, msg

    corpo = response.get("corpo")

    if not corpo:

        dict_update = {
            "id_email": id_email,
            "status": "F",
            "data_envio": data_envio
        }

        key_columns = ["id_email"]

        dm.core_sql_update("EMAIL", dict_update, key_columns)
        dm.core_sql_update("EMAIL_USUARIO", dict_update, key_columns)

        logger.info(f"Email {id_email} sem corpo.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Email sem corpo."},
               }, 200

        return False, msg

    # Pegando os usuario que lerão a mensagem
    query = """
        SELECT
            DISTINCT
                u.id_usuario,
                u.email

        FROM
            EMAIL_USUARIO eu

            INNER JOIN USUARIO u ON
                eu.id_usuario = u.id_usuario

        WHERE
            eu.id_email = :id_email
            AND LEN(u.email) > 0
            AND u.status = 'A'
            AND u.d_e_l_e_t_ = 0
    """

    params = {
        "id_email": id_email
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        
        dict_update = {
            "id_email": id_email,
            "status": "F",
            "data_envio": data_envio
        }

        key_columns = ["id_email"]

        dm.core_sql_update("EMAIL", dict_update, key_columns)
        dm.core_sql_update("EMAIL_USUARIO", dict_update, key_columns)

        logger.info(f"Email {id_email} sem usuarios válidos atrelados a si.")
        msg = {"status":400,
               "resposta":{
                   "tipo":"13",
                   "descricao":f"Ação recusada: Email sem usuarios válidos atrelados a si."},
               }, 200

        return False, msg

    dict_email_user = {}
    emails = []

    for user in response:

        dict_email_user[user["email"]] = user["id_usuario"]
        
        if user["email"] not in emails:
            emails.append(user["email"])

    total = len(emails)
    emails_per_time = 100
    paginas = (total // emails_per_time) + (total % emails_per_time > 0)

    user_error = []

    for pagina in range(paginas):

        send_emails = emails[pagina * emails_per_time: (pagina + 1) * emails_per_time]

        dict_send_email = {
            "reciever": send_emails, 
            "subject": assunto, 
            "html": corpo,
            "hide_to": True
        }
        
        answer, response = send_email(**dict_send_email)

        if not answer:

            hold_id = [
                dict_email_user[email]
                for email in send_emails
            ]

            user_error.extend(hold_id)

            continue

        fails = response.get("emails_falhos")

        hold_id = [
            dict_email_user[email]
            for email in fails
        ]

        user_error.extend(hold_id)

    erros = len(user_error)

    if erros >= total:

        dict_update = {
            "id_email": id_email,
            "status": "F",
            "data_envio": data_envio
        }

        key_columns = ["id_email"]

        dm.core_sql_update("EMAIL", dict_update, key_columns)
        dm.core_sql_update("EMAIL_USUARIO", dict_update, key_columns)

    else:

        dict_update = {
            "id_email": id_email,
            "status": "E",
            "data_envio": data_envio
        }

        key_columns = ["id_email"]

        dm.core_sql_update("EMAIL", dict_update, key_columns)

        if user_error:

            query = """
                UPDATE
                    EMAIL_USUARIO

                SET
                    data_envio = :data_envio,
                    status = 'F'

                WHERE
                    id_email = :id_email
                    AND id_usuario IN :usuarios
            """

            params = {
                "id_email": id_email,
                "usuarios": user_error,
                "data_envio": data_envio,
            }

            dict_query = {
                "query": query,
                "params": params,
            }

            dm.raw_sql_execute(**dict_query)

        query = """
            UPDATE
                EMAIL_USUARIO

            SET
                data_envio = :data_envio,
                status = 'E'

            WHERE
                id_email = :id_email
                AND status != 'F'
        """

        params = {
            "id_email": id_email,
            "data_envio": data_envio,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

    erros = len(user_error)
    sucessos = total - erros

    if sucessos > erros:
        logger.info(f"Email {id_email} enviado com {sucessos} sucessos e {erros} falhas.")

    else:
        logger.info(f"Falha no envio do email {id_email} devido a erros com os emails.")

    dados_envio = {
        "notificacao": id_email,
        "resultado": sucessos > erros,
        "total": total,
        "sucessos": sucessos,
        "erros": erros,
    }

    return True, dados_envio