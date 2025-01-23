#=== Importações de módulos externos ===#
from flask_restful import Resource
import os, unidecode

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm


class ListarNotificacoesNaoLidas(Resource):

    @logger
    def post(self):
        """
        Método POST do Listar Notificações não-lidas do Marketplace

        Returns:
            [dict]: Status da transação
        """

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        not_null = ["token_firebase"]

        # Checa chaves inválidas, faltantes e valores incorretos
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = not_null,
                                not_null = not_null)):
            
            return validity

        # Verificando os dados de entrada
        pagina, limite = dm.page_limit_handler(response_data)

        token_firebase = str(response_data["token_firebase"])

        # Pegando as notificações
        query = """
            SELECT
                ntfc.id_notificacao,
                COUNT(*) OVER() 'count__'

            FROM
                (
                
                    SELECT
                        DISTINCT
                            ntfc.id_notificacao

                    FROM
                        NOTIFICACAO_USUARIO ntfcu

                        INNER JOIN NOTIFICACAO ntfc ON
                            ntfcu.id_notificacao = ntfc.id_notificacao

                        INNER JOIN ROTAS_APP rapp ON
                            ntfc.id_rota = rapp.id_rota

                    WHERE
                        ISNULL(ntfcu.lida, 0) != 1
                        AND token = :token_firebase
                        AND ntfc.status = 'E'
                        AND ntfc.data_envio IS NOT NULL
                        AND ntfcu.status = 'E'
                
                ) n

                INNER JOIN NOTIFICACAO ntfc ON
                    n.id_notificacao = ntfc.id_notificacao

            ORDER BY
                ntfc.data_envio DESC

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY
        """

        params = {
            "token_firebase": token_firebase,
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.info(f"Não existem notificacoes para o token {token_firebase}.")
            return {"status": 404,
                    "resposta": {
                        "tipo": "7", 
                        "descricao": "Sem dados para retornar."}}, 200

        counter = response[0][-1]
        ids_notificacao = [notificacao[0] for notificacao in response]

        query = """
            SELECT
                DISTINCT
                    ntfc.id_notificacao,
                    ntfc.id_distribuidor,
                    lacapp.id_acao,
                    rapp.id_rota,
                    rapp.descricao_rota,
                    ISNULL(rapp.parametro_ptbr, rapp.parametro_eng) as parametro_rota,
                    UPPER(ntfc.titulo) as titulo,
                    ntfc.corpo,
                    ntfc.imagem,
                    ntfc.data_envio

            FROM
                NOTIFICACAO_USUARIO ntfcu

                INNER JOIN NOTIFICACAO ntfc ON
                    ntfcu.id_notificacao = ntfc.id_notificacao

                INNER JOIN ROTAS_APP rapp ON
                    ntfc.id_rota = rapp.id_rota

                LEFT JOIN LISTA_ACAO_APP lacapp ON
                    ntfc.id_acao = lacapp.id_acao
                    AND lacapp.status = 'A'

            WHERE
                ntfc.id_notificacao IN :notificacoes

            ORDER BY
                ntfc.data_envio DESC
        """

        params = {
            "notificacoes": ids_notificacao
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        response = dm.raw_sql_return(**dict_query)

        notificacoes = []

        server = os.environ.get("PSERVER_PS").lower()

        if server == "production":
            image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
            
        else:
            image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

        for notificacao in response:

            id_notificacao = int(notificacao.get("id_notificacao"))
            id_distribuidor = int(notificacao.get("id_distribuidor"))

            id_rota = int(notificacao.get("id_rota"))
            descricao_rota = unidecode.unidecode(notificacao.get("descricao_rota")).capitalize()
            parametro_rota = str(notificacao.get("parametro_rota")).lower()
    
            conteudo = {
                "id_notificacao": id_notificacao,
                "id_distribuidor": id_distribuidor,
                "rota": parametro_rota
            }

            if id_rota not in {1,5,6,7,8}:

                if id_rota not in {2,3,4,9}:
                    continue

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
                            codigo_oferta as id_oferta

                        FROM
                            NOTIFICACAO_OFERTA

                        WHERE
                            id_notificacao = :id_notificacao
                    """

                elif id_rota == 4:

                    parametro = "id_cupom"

                    query = """
                        SELECT
                            id_cupom

                        FROM
                            NOTIFICACAO_CUPOM

                        WHERE
                            id_notificacao = :id_notificacao
                    """

                elif id_rota == 9:

                    parametro = "id_noticia"

                    query = """
                        SELECT
                            id_noticia

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
                    continue
                
                ids = [str(id_obj[0])   for id_obj in response]

                conteudo.update({
                    parametro: "&".join(ids)
                })

            titulo = notificacao.get("titulo")
            corpo = notificacao.get("corpo")
            imagem = notificacao.get("imagem")

            imagem = notificacao.get("imagem")

            if imagem:
                imagem = f"{image}/imagens/distribuidores/500/{id_distribuidor}/notificacao/{imagem}"

            else:
                imagem = None

            data_envio = notificacao.get("data_envio")

            notificacao = {
                "tipo": descricao_rota,
                "conteudo": conteudo,
                "notification": {
                    "title": titulo,
                    "body": corpo,
                    "image": imagem,
                },
                "data": data_envio,
                "lida": False,
            }

            notificacoes.append(notificacao)


        if not notificacoes:
            logger.info(f"Não existem notificacoes para o token {token_firebase}.")
            return {"status": 404,
                    "resposta": {
                        "tipo": "7", 
                        "descricao": "Sem dados para retornar."}}, 200

        logger.info(f"Notificações para o {token_firebase} encontradas.")
        return {"status": 200,
                "resposta": {
                    "tipo": "1", 
                    "descricao": "Dados encontrados."},
                "informacoes": {
                    "itens": counter,
                    "paginas": (counter // limite) + (counter % limite > 0),
                    "pagina_atual": pagina
                },
                "dados": notificacoes}, 200