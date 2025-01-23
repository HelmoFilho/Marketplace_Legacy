#=== Importações de módulos externos ===#
from flask_restful import Resource
from PIL import Image
import os

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.file_management as fm
import functions.security as secure
from app.server import logger

class UploadImagensProdutos(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1])
    def post(self) -> dict:
        """
        Método POST do Upload de Imagens do Produto

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        request_files = fm.get_files()

        image_folder = f"{os.environ.get('IMAGE_PATH_MAIN_FOLDER_PS')}\\imagens\\produto"
        
        # Holder das imagens
        imagens = dict()

        # Registro de falhas
        falhas = 0
        falhas_hold = dict()

        for key, value in request_files.items():

            # Verificando a chave
            if "_" not in key:
                falhas += 1

                if falhas_hold.get("chave_invalida"):
                    falhas_hold["chave_invalida"]['combinacao'].append({
                        "chave_do_arquivo": key
                    })
                
                else:
                    falhas_hold[f"chave_invalida"] = {
                        "descricao": f"Chave em formato inválido",
                        "combinacao": [
                            {
                                "chave_do_arquivo": key
                            }
                        ]
                    }
                continue

            # Verificando a posicao da chave
            sku, posicao = str(key).split("_")

            if posicao not in {"1", "2", "3"}:
                falhas += 1

                if falhas_hold.get("posicao_invalida"):
                    falhas_hold["posicao_invalida"]['combinacao'].append({
                        "chave_do_arquivo": key
                    })
                
                else:
                    falhas_hold[f"posicao_invalida"] = {
                        "descricao": f"Posição informada é inválida",
                        "combinacao": [
                            {
                                "chave_do_arquivo": key
                            }
                        ]
                    }
                continue

            # Verificando a existência do sku
            sku_query = """
                SELECT
                    TOP 1 sku

                FROM
                    PRODUTO

                WHERE
                    sku = :sku
            """

            params = {
                "sku": sku
            }

            sku_query = dm.raw_sql_return(sku_query, params = params, raw = True, first = True)

            if not sku_query:

                if falhas_hold.get("sku_inexistente"):
                    falhas_hold["sku_inexistente"]['combinacao'].append({
                        "chave_do_arquivo": key
                    })
                
                else:
                    falhas_hold[f"sku_inexistente"] = {
                        "descricao": f"Arquivo não é uma imagem",
                        "combinacao": [
                            {
                                "chave_do_arquivo": key
                            }
                        ]
                    }
                continue

            # Verificando a imagem em si
            try:
                Image.open(value).verify()
            
            except:
                falhas += 1

                if falhas_hold.get("imagem_invalida"):
                    falhas_hold["imagem_invalida"]['combinacao'].append({
                        "chave_do_arquivo": key
                    })
                
                else:
                    falhas_hold[f"imagem_invalida"] = {
                        "descricao": f"Arquivo não é uma imagem",
                        "combinacao": [
                            {
                                "chave_do_arquivo": key
                            }
                        ]
                    }
                continue

            # Salvando o link da imagem 
            imagens[key] = value
        
        if not imagens:

            if falhas <= 0:
                logger.error("Nenhuma imagem foi enviada")
                return {"status":400,
                        "resposta":{
                            "tipo": "13", 
                            "descricao": "Ação recusada: Nenhuma imagem enviada."}}, 200

            logger.error("Houveram erros no envio de imagem")
            return {"status":200,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Nenhuma imagem válida enviada."},
                    "situacao": {
                        "sucessos": 0,
                        "falhas": falhas,
                        "descricao": falhas_hold}}, 200

        hold_sku = []

        # Verificando as imagens
        for key, value in imagens.items():

            imagem = Image.open(value)
            formato = str(imagem.format)

            sku, posicao = str(key).split("_")

            if not os.path.exists(f"{image_folder}\\{sku}"):
                os.mkdir(f"{image_folder}\\{sku}")

            # Checando existência do registro
            query = """
                SELECT
                    TOP 1 sku

                FROM
                    PRODUTO_IMAGEM

                WHERE
                    sku = :sku
                    AND imagem LIKE :imagem
            """

            params = {
                "sku": sku,
                "imagem": f"{posicao}%"
            }

            query = dm.raw_sql_return(query, params = params, raw = True, first = True)

            # Salvando a posicao da imagem
            if not query:

                data = {
                    "sku": sku,
                    "imagem": f"{posicao}.{formato.lower()}",
                    "token": None
                }

                dm.raw_sql_insert("PRODUTO_IMAGEM", data)

            else:
                
                query = """
                    UPDATE
                        PRODUTO_IMAGEM

                    SET
                        token = NULL,
                        imagem = :new_image

                    WHERE
                        imagem LIKE :old_image
                        AND sku = :sku
                """

                params = {
                    "new_image": f"{posicao}.{formato.lower()}",
                    "sku": sku,
                    "old_image": f"{posicao}%"
                }

                dm.raw_sql_execute(query, params = params)

            # Salvando imagem nova
            hold_sku.append(sku)
            imagem.save(f"{image_folder}\\{sku}\\{posicao}.{formato.lower()}") 

        if hold_sku:
            fm.criar_token_imagem_produto(hold_sku)


        if falhas <= 0: 
            logger.info("Todas as transacoes foram realizadas com sucesso")
            return {"status":200,
                    "resposta":{"tipo":"1", "descricao": "Todas as transações foram feitas."}}, 200

        logger.error(f"Houveram {falhas} erros com a transacao")
        return {"status":200,
                    "resposta":{"tipo":"1", "descricao": "Houveram erros com a transação."}, 
                    "situacao": {
                        "sucessos": len(request_files) - falhas,
                        "falhas": falhas,
                        "descricao": falhas_hold
                    }}, 200