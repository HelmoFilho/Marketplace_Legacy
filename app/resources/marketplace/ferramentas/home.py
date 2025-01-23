#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

from timeit import default_timer as timer
class Home(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self):
        """
        Método POST do Home do Marketplace

        Returns:
            [dict]: Status da transação
        """

        id_usuario_token = global_info.get("id_usuario")

        # Dados recebidos
        response_data = dm.get_request(trim_values = True)

        # Chaves que precisam ser mandadas
        no_use_columns = ["id_distribuidor", "id_cliente"]

        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int
        }

        # Checa chaves inválidas, faltantes e valores incorretos
        if (validity := dm.check_validity(request_response = response_data, 
                                no_use_columns = no_use_columns,
                                bool_check_password = False,
                                correct_types = correct_types)):
            
            return validity

        # Verificando os dados enviados
        id_distribuidor = int(response_data.get("id_distribuidor")) \
                                if response_data.get("id_distribuidor") else 0

        id_cliente = int(response_data.get("id_cliente")) \
                                if response_data.get("id_cliente") else None 

        if id_usuario_token:   
            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

        # Escolhendo o servido utilizado
        server = str(os.environ.get("PSERVER_PS")).lower()

        if server == "production":
            image_server = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
            
        elif server == "development":
            image_server = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

        else:
            image_server = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

        # Holder da home
        home = []

        # Pegando os distribuidores validos do cliente
        if not id_usuario_token:

            query = """
                SELECT
                    DISTINCT id_distribuidor

                FROM
                    DISTRIBUIDOR

                WHERE
                    id_distribuidor = CASE
                                          WHEN ISNULL(:id_distribuidor,0) = 0
                                              THEN id_distribuidor
                                          ELSE
                                              :id_distribuidor
                                      END
                    AND status = 'A'
            """

            params = {
                "id_distribuidor": id_distribuidor
            }

        else:

            query = """
                SELECT
                    DISTINCT d.id_distribuidor

                FROM
                    CLIENTE c

                    INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                        c.id_cliente = cd.id_cliente

                    INNER JOIN DISTRIBUIDOR d ON
                        cd.id_distribuidor = d.id_distribuidor

                WHERE
                    d.id_distribuidor = CASE
                                            WHEN ISNULL(:id_distribuidor, 0) = 0
                                                THEN d.id_distribuidor
                                            ELSE
                                                :id_distribuidor
                                        END
                    AND	c.id_cliente = :id_cliente
                    AND c.status = 'A'
                    AND c.status_receita = 'A'
                    AND cd.status = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND d.status = 'A'
            """

            params = {
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor
            }

        query_obj = {
            "query": query,
            "params": params,
            "raw": True
        }

        distribuidores = dm.raw_sql_return(**query_obj)

        if not distribuidores:
            logger.error("Sem distribuidores cadastrados válidos para a operação.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Sem distribuidores cadastrados."}}, 200
                        
        distribuidores = [distribuidor[0] for distribuidor in distribuidores]
        
        # Slide
        slide_query = f"""
            SELECT
                TOP 5 sh.id_distribuidor,
                sh.id,
                sh.url_imagem as imagem,
                rtapp.parametro_ptbr as rota,
				rtacapp.parametro_ptbr as acao,
				sh.produtos
            
            FROM
                SLIDESHOW_HOME sh
                
                INNER JOIN DISTRIBUIDOR d ON
                	sh.id_distribuidor = d.id_distribuidor

                INNER JOIN ROTAS_APP rtapp ON
					sh.rota = rtapp.id_rota

				LEFT JOIN ROTAS_ACAO_APP rtacapp ON
					sh.acao = rtacapp.id_acao
            
            WHERE
                sh.id_distribuidor = ISNULL(:id_distribuidor, 0)
                AND LEN(sh.url_imagem) > 0
                AND (
                        (
                            sh.data_indeterminada = 1
                        )
                            OR
                        (
                            sh.data_indeterminada = 0
                            AND data_inicio <= GETDATE()
                            AND sh.data_final >= GETDATE()
                        )
                    )
                AND d.status = 'A'
                AND sh.status = 'A' 
            
            ORDER BY
                sh.data_indeterminada DESC,
                sh.data_inicio DESC
        """

        params = {
            "id_distribuidor": id_distribuidor
        }

        dict_query = {
            "query": slide_query,
            "params": params,
        }

        slides = dm.raw_sql_return(**dict_query)

        if slides:

            for slide in slides:
                imagem = slide.get("imagem")
                distribuidor_slide = slide.get("id_distribuidor")
                slide["imagem"] = f"{image_server}/imagens/distribuidores/600/{distribuidor_slide}/slideshow/{imagem}"

                if slide.get("acao"):
                    if ":id_distribuidor" in slide.get("acao"):
                        slide["acao"] = f"{slide.get('acao')}".replace(":id_distribuidor", f"{distribuidor_slide}")

                else:
                    slide["acao"] = str(id_distribuidor)

            home.append({
                            "tipo": "slideshow",
                            "imagens": slides
                        })

        # Banner
        query_banner = f"""
            SELECT
                hb.id_banner,
                hb.id_distribuidor,
                hb.url_imagem,
                ISNULL(hb.funcao, 'produto/' + CONVERT(VARCHAR, hb.id_distribuidor)) as rota
                        
            FROM
                HOME_BANNER hb

            WHERE
                (
                    (
                        hb.id_distribuidor = ISNULL(:id_distribuidor, 0)
                    )
                        OR
                    (
                        ISNULL(:id_distribuidor, 0) = 0
                        AND hb.id_distribuidor != 0
                        AND hb.mostrar_principal = 1
                    )
                )
                AND LEN(hb.url_imagem) > 0
                AND hb.funcao IS NOT NULL
                AND (
                        (
                            hb.data_inicio IS NULL
                        )
                            OR
                        (
                            hb.data_inicio <= GETDATE()
                        )
                    )
                AND (
                        (
                            hb.data_fim IS NULL
                        )
                            OR
                        (
                            hb.data_fim >= GETDATE()
                        )
                    )
                AND hb.status = 'A'
                AND d_e_l_e_t_ = 0

            ORDER BY
                CASE WHEN :id_distribuidor = hb.id_distribuidor THEN 1 ELSE 0 END DESC,
                hb.data_inicio DESC
        """

        params = {
            "id_distribuidor": id_distribuidor
        }

        dict_query = {
            "query": query_banner,
            "params": params,
        }

        query_banner = dm.raw_sql_return(**dict_query)

        if query_banner:
            
            for banner in query_banner:
                distribuidor_banner = banner.get("id_distribuidor")
                url_imagem = banner.get("url_imagem")

                banner["url_imagem"] = f"{image_server}/imagens/distribuidores/600/{distribuidor_banner}/banners/{url_imagem}"            

            home.append({
                            "tipo": "banner",
                            "banner": query_banner
                        })

        if id_usuario_token:

            # # Card - Ofertas do dia
            # limite_produto_ofertas = 5
            # pagina_produto_ofertas = 1

            # dict_descontos = {
            #     "id_usuario": id_usuario_token,
            #     "id_cliente": id_cliente,
            #     "pagina": pagina_produto_ofertas,
            #     "limite": limite_produto_ofertas,
            #     "image_replace": "150"
            # }

            # data = dj.json_desconto(**dict_descontos)

            # if data:

            #     home.append({
            #                     "tipo": "card",
            #                     "titulo": "ofertas_do_dia",
            #                     "ofertas": data.get("produtos")
            #                 })

            # Card - Meus Favoritos
            limite_produto_favoritos = 8

            dict_favoritos = {
                "id_usuario": id_usuario_token,
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor,
                "image_replace": "150",
                "limite": limite_produto_favoritos,
                "pagina": 1
            }

            produtos = dj.json_favoritos(**dict_favoritos)

            if produtos:

                home.append({
                    "tipo": "card",
                    "titulo": "meus_favoritos",
                    "produtos": produtos.get("produtos")
                })

            # Card - Ultimos vistos
            dict_ultimos_vistos = {
                "id_usuario": id_usuario_token,
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor,
                "image_replace": "150",
                "limite": 3
            }

            produtos = dj.json_ultimos_vistos(**dict_ultimos_vistos)

            if produtos:

                home.append({
                    "tipo": "card",
                    "titulo": "ultimos_vistos",
                    "produtos": produtos
                })

        # Blog
        dict_noticia = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor,
            "pagina": 1,
            "limite": 2,
            "image_replace": "500"
        }

        noticias_hold = dj.json_noticias(**dict_noticia)

        if noticias_hold:
            home.append({
                            "tipo": "blog",
                            "noticias": noticias_hold.get("data")
                        })

        # # Mais Acessados
        # home.append({
        #                 "tipo": "mais_acessados",
        #                 "atalhos": [
        #                     {
        #                         "imagem": "https://midiasmarketplace-dev.guarany.com.br/imagens/produto/150/7899706187343/shampoo-preenchedor-elseve-200ml-hidra-hialuronico/1.png",
        #                         "titulo": "danielzinho",
        #                         "rota": "https://midiasmarketplace-dev.guarany.com.br/imagens/produto/150/7899706187343/shampoo-preenchedor-elseve-200ml-hidra-hialuronico/1.png"
        #                     }
        #                 ]
        #             })

        if home:
            logger.info(f"Dados do home enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados da home enviados."},
                    "situacao": 1 if id_usuario_token else 0,
                    "home": home}, 200
        
        logger.info(f"Dados nao encontrados.")
        return {"status": 404,
                "resposta": {
                    "tipo": "7", 
                    "descricao": "Sem dados para retornar."}}, 200