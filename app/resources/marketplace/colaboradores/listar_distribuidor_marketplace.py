#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarDistribuidorMarketplace(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def get(self, id_cliente = None):
        """
        Método GET do Listar Distribuidor
        Returns:
            [dict]: Status da transação
        """

        token = global_info.get("token")

        if token:

            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

        else:
            id_cliente = None

        # Servidor de imagem
        server = os.environ.get("PSERVER_PS").lower()

        if server == "production":
            image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
            
        elif server == "development":
            image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

        else:
            image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

        # Criando o JSON
        if token:
            query = """
                SELECT
                    d.id_distribuidor,
                    TRIM(UPPER(d.nome_fantasia)) as nome_fantasia,
                    TRIM(UPPER(d.razao_social)) as razao_social,
                    d.cor_a,
                    d.cor_b,
                    d.cor_c,
                    TRIM(LOWER(d.imagem)) as imagem

                FROM
                    DISTRIBUIDOR d

                WHERE
                    d.id_distribuidor IN (

                                            SELECT
                                                DISTINCT d.id_distribuidor

                                            FROM
                                                CLIENTE c

                                                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                                                    c.id_cliente = cd.id_cliente

                                                INNER JOIN DISTRIBUIDOR d ON
                                                    cd.id_distribuidor = d.id_distribuidor

                                            WHERE
                                                c.id_cliente = :id_cliente
                                                AND d.id_distribuidor != 0
                                                AND cd.data_aprovacao <= GETDATE()
                                                AND c.status = 'A'
                                                AND c.status_receita = 'A'
                                                AND cd.status = 'A'
                                                AND cd.d_e_l_e_t_ = 0
                                                AND d.status = 'A'

                                            UNION

                                            SELECT
                                                0 as id_distribuidor

                                         )
                    AND d.status = 'A'

                ORDER BY
                    d.id_distribuidor ASC
            """
                
            params = {
                "id_cliente": id_cliente,
            }

        else:
            query = """
                SELECT
                    d.id_distribuidor,
                    TRIM(UPPER(d.nome_fantasia)) as nome_fantasia,
                    TRIM(UPPER(d.razao_social)) as razao_social,
                    d.cor_a,
                    d.cor_b,
                    d.cor_c,
                    TRIM(LOWER(d.imagem)) as imagem

                FROM
                    DISTRIBUIDOR d

                WHERE
                    d.status = 'A'

                ORDER BY
                    d.id_distribuidor ASC
            """
                
            params = {}

        data = dm.raw_sql_return(query, params = params)

        for distribuidor in data:

            imagens = []
            imagem = None

            if distribuidor.get("imagem"):
                try:
                    imagem = distribuidor.get("imagem")

                    id_distribuidor = distribuidor.get("id_distribuidor")
                    
                    imagem = f"{image}/imagens/distribuidores/300/{id_distribuidor}/logos/{imagem}"

                except:
                    imagem = None

            if imagem:
                imagens.append(imagem)

            distribuidor["imagem"] = imagens

        if data:

            logger.info("Dados de distribuidor enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados."}, "dados": data}, 200
        
        logger.info("Nao existem dados de distribuidor para enviar.")
        return {"status":404,
                "resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200