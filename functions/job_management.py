#=== Importações de módulos externos ===#
import concurrent.futures
import os, itertools

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.payment_management as pm
import functions.data_management as dm
import functions.message_send as ms


def send_stock_notification_starter(id_notificacao: int, enviado_por: str = 'padrao') -> tuple:
    """
    Inicia o serviço de envio de notificação de estoque de produto

    Args:
        id_notificacao (int): ID da notificação a ser enviada.
        enviado_por (str): Quem está enviando a notificação.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    global_info.save_info_thread(**{
        "api": "job"
    })

    return ms.send_stock_notification(id_notificacao, enviado_por)


def stock_notification_job():
    """
    Pega as notificações de estoque de produto e as envia pelo job do marketplace
    """

    limite = os.environ.get("STOCK_NOTIFICATION_SEND_LIMIT_JOB_PS")

    if not str(limite).isdecimal():
        limite = 10

    else:
        limite = int(limite)

    hold_resultados = {
        "notificacoes": 0,
        "notificacoes_enviadas": 0,
        "notificacoes_falhas": 0,
    }

    query = f"""
        SELECT
            DISTINCT ntfce.id_notificacao

        FROM
            NOTIFICACAO_ESTOQUE ntfce

            LEFT JOIN PRODUTO_ESTOQUE pe ON
                ntfce.id_produto = pe.id_produto

        WHERE
            ntfce.status = 'P'
            AND ISNULL(pe.qtd_estoque, 0) > 0
            AND ntfce.data_envio IS NULL

        ORDER BY
            ntfce.id_notificacao
    """

    dict_query = {
        "query": query,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info("Sem notificações para enviar")
        return hold_resultados

    notificacoes = list(itertools.chain(*response))
    hold_resultados["notificacoes"] = len(notificacoes)

    with concurrent.futures.ThreadPoolExecutor() as executor:

        counter = len(notificacoes)
        pages = (counter // limite) + (counter % limite > 0)

        for page in range(pages):

            notificacao_stack = notificacoes[(page * limite) : ((page + 1) * limite)]
            futures = []
            
            for id_notificacao in notificacao_stack:

                notificacao = {
                    "id_notificacao": id_notificacao,
                    "enviado_por": "job"
                }

                futures.append(executor.submit(send_stock_notification_starter, **notificacao))
        
            for future in concurrent.futures.as_completed(futures):
                
                result, info = future.result()

                if result:
                    
                    if info.get("sucessos"):
                        hold_resultados["notificacoes_enviadas"] += 1

                    else:
                        hold_resultados["notificacoes_falhas"] += 1

                else:
                    hold_resultados["notificacoes_falhas"] += 1

    return hold_resultados


def send_notification_starter(id_notificacao: int, enviado_por: str = 'padrao') -> tuple:
    """
    Inicia o serviço de envio de notificação

    Args:
        id_notificacao (int): ID da notificação a ser enviada.
        enviado_por (str): Quem está enviando a notificação.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    global_info.save_info_thread(**{
        "api": "job"
    })

    return ms.send_notification(id_notificacao, enviado_por)


def notification_job():
    """
    Pega as notificações e as envia pelo job do marketplace
    """

    limite = os.environ.get("NOTIFICATION_SEND_LIMIT_JOB_PS")

    if not str(limite).isdecimal():
        limite = 10

    else:
        limite = int(limite)

    hold_resultados = {
        "notificacoes": 0,
        "notificacoes_enviadas": 0,
        "notificacoes_falhas": 0,
    }

    query = f"""
        SELECT
            DISTINCT ntfc.id_notificacao

        FROM
            NOTIFICACAO ntfc

            INNER JOIN ROTAS_APP rapp ON
                ntfc.id_rota = rapp.id_rota

        WHERE
            ntfc.data_agendado <= GETDATE()
            AND ntfc.data_envio IS NULL
            AND ntfc.status = 'P'

        ORDER BY
            ntfc.id_notificacao
    """

    dict_query = {
        "query": query,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info("Sem notificações para enviar")
        return hold_resultados

    notificacoes = list(itertools.chain(*response))
    hold_resultados["notificacoes"] = len(notificacoes)

    with concurrent.futures.ThreadPoolExecutor() as executor:

        counter = len(notificacoes)
        pages = (counter // limite) + (counter % limite > 0)

        for page in range(pages):

            notificacao_stack = notificacoes[(page * limite) : ((page + 1) * limite)]
            futures = []
            
            for id_notificacao in notificacao_stack:

                notificacao = {
                    "id_notificacao": id_notificacao,
                    "enviado_por": "job"
                }

                futures.append(executor.submit(send_notification_starter, **notificacao))
        
            for future in concurrent.futures.as_completed(futures):
                
                result, info = future.result()

                if result:

                    if info.get("total") == info.get("erros"):
                        hold_resultados["notificacoes_falhas"] += 1

                    else:
                        hold_resultados["notificacoes_enviadas"] += 1

                else:
                    hold_resultados["notificacoes_falhas"] += 1

    return hold_resultados


def send_email_starter(id_email: int) -> tuple:
    """
    Inicia o serviço de envio de notificação

    Args:
        id_email (int): ID da notificação a ser enviada.
        enviado_por (str): Quem está enviando a notificação.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    global_info.save_info_thread(**{
        "api": "job"
    })

    return ms.send_email_marketplace(id_email)


def email_job():
    """
    Pega os email e os envia pelo job do jmanager
    """

    limite = os.environ.get("EMAIL_SEND_LIMIT_JOB_PS")

    if not str(limite).isdecimal():
        limite = 10

    else:
        limite = int(limite)

    hold_resultados = {
        "emails": 0,
        "emails_enviados": 0,
        "emails_falhos": 0,
    }

    query = f"""
        SELECT
            DISTINCT
                e.id_email

        FROM
            EMAIL e

        WHERE
            e.data_agendamento < GETDATE()
            AND e.status = 'P'
    """

    dict_query = {
        "query": query,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info("Sem notificações para enviar")
        return hold_resultados

    emails = list(itertools.chain(*response))
    hold_resultados["emails"] = len(emails)

    with concurrent.futures.ThreadPoolExecutor() as executor:

        counter = len(emails)
        pages = (counter // limite) + (counter % limite > 0)

        for page in range(pages):

            email_stack = emails[(page * limite) : ((page + 1) * limite)]
            futures = []
            
            for id_email in email_stack:

                email = {
                    "id_email": id_email,
                }

                futures.append(executor.submit(send_email_starter, **email))
        
            for future in concurrent.futures.as_completed(futures):
                
                result, info = future.result()

                if result:

                    if info.get("total") == info.get("erros"):
                        hold_resultados["emails_falhos"] += 1

                    else:
                        hold_resultados["emails_enviados"] += 1

                else:
                    hold_resultados["emails_falhos"] += 1

    return hold_resultados


def verify_pix_starter(id_cobranca: int) -> tuple:
    """
    Inicia o serviço de checagem de cobranças pix

    Args:
        id_cobranca (int): ID da cobrança a ser checada.

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a 
        mensagem de erro no segundo caso algo dê errado.
    """

    global_info.save_info_thread(**{
        "api": "job"
    })

    return pm.pix_job(id_cobranca)


def pix_job():
    """
    Verifica o status dos pix ainda não verificados
    """

    limite = os.environ.get("PIX_CHECK_LIMIT_JOB_PS")

    if not str(limite).isdecimal():
        limite = 10

    else:
        limite = int(limite)

    hold_resultados = {
        "pix": 0,
        "pix_sucessos": 0,
        "pix_falhos": 0,
    }

    query = f"""
        SELECT
            id_cobranca    
            
        FROM
            PIX_COBRANCAS pxcob

        WHERE
            status = 'ATIVA'
            AND d_e_l_e_t_ = 0

        ORDER BY
            id_cobranca
    """

    dict_query = {
        "query": query,
        "raw": True
    }

    response = dm.raw_sql_return(**dict_query)

    if not response:
        logger.info("Sem cobrancas pix para verificar")
        return hold_resultados

    cobrancas = list(itertools.chain(*response))
    hold_resultados["pix"] = len(cobrancas)

    with concurrent.futures.ThreadPoolExecutor() as executor:

        counter = len(cobrancas)
        pages = (counter // limite) + (counter % limite > 0)

        for page in range(pages):

            pix_stack = cobrancas[(page * limite) : ((page + 1) * limite)]
            futures = []
            
            for id_cobranca in pix_stack:

                pix = {
                    "id_cobranca": id_cobranca,
                }

                futures.append(executor.submit(verify_pix_starter, **pix))
        
            for future in concurrent.futures.as_completed(futures):
                
                result = future.result()

                if result:
                    hold_resultados["pix_sucessos"] += 1

                else:
                    hold_resultados["pix_falhos"] += 1

    return hold_resultados