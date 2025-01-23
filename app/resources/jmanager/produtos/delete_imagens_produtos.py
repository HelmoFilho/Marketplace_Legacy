#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class DeleteImagensProdutos(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1])
    def post(self) -> dict:
        """
        Método POST do Deletar de Imagens do Produto

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True, trim_values = True)

        necessary_keys = ["sku"]

        correct_types = {
            "sku": [str, list]
        }

        # Checa chaves inválidas e faltantes, valores incorretos e nulos
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = necessary_keys, 
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            return validity
        
        # Verificando o sku enviado
        skus = response_data.get("sku")
        image_folder = os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS")
        
        if type(skus) is str:
            skus = [skus]
        
        # Holder das imagens
        imagens = []
        paths = []

        # Registro de falhas
        falhas = 0
        falhas_hold = {}

        # loop
        for sku in skus:
            
            sku = str(sku)
            
            # Verificando a chave enviada
            try:
                numero, posicao = sku.split("_")
            
            except:
                falhas += 1

                if falhas_hold.get("formato_inválido"):
                    falhas_hold["formato_inválido"]['combinacao'].append({
                        "sku": sku
                    })
                
                else:
                    falhas_hold[f"formato_inválido"] = {
                        "descricao": f"Sku em formato inválido",
                        "combinacao": [
                            {
                                "sku": sku
                            }
                        ]
                    }

                continue

            # Verificando a posição que deve ser deletada
            if posicao not in {"1", "2", "3"}:
                falhas += 1

                if falhas_hold.get("posicao_invalida"):
                    falhas_hold["posicao_invalida"]['combinacao'].append({
                        "sku": sku
                    })
                
                else:
                    falhas_hold[f"posicao_invalida"] = {
                        "descricao": f"Posição informada é inválida",
                        "combinacao": [
                            {
                                "sku": sku
                            }
                        ]
                    }

                continue

            # Verificando a existência do sku informado
            query_variante = f"""
                SELECT
                    TOP 1 sku

                FROM
                    PRODUTO

                WHERE 
                    sku = :sku
            """

            params = {
                "sku": numero
            }

            query_variante = dm.raw_sql_return(query_variante, params = params, raw = True, first = True)

            if not query_variante:
                falhas += 1

                if falhas_hold.get("sku_invalido"):
                    falhas_hold["sku_invalido"]['combinacao'].append({
                        "sku": sku
                    })
                
                else:
                    falhas_hold[f"sku_invalido"] = {
                        "descricao": f"Sku informado não encontrado",
                        "combinacao": [
                            {
                                "sku": sku
                            }
                        ]
                    }

                continue 

            # Verificando o arquivo a ser deletado
            imagem_query = """
                SELECT
                    TOP 1 imagem

                FROM
                    PRODUTO_IMAGEM

                WHERE
                    sku = :sku
                    AND imagem LIKE :imagem
            """

            params = {
                "sku": numero,
                "imagem": f"{posicao}%"
            }

            imagem_query = dm.raw_sql_return(imagem_query, params = params, raw = True, first = True)

            if imagem_query:

                old_file = imagem_query[0]

                paths.append(f"{image_folder}\\imagens\\produto\\{numero}\\{old_file}")
                    
                imagens.append({
                    "sku": numero,
                    "imagem": old_file
                })

        # Realizando os commits
        if imagens:

            dm.raw_sql_delete("Produto_Imagem", imagens)

            for path in paths:
                if os.path.exists(path):
                    os.remove(path)

        if falhas <= 0: 
            logger.info("Todas as transacoes foram realizadas com sucesso")
            return {"status":200,
                    "resposta":{"tipo":"1", "descricao": "Todas as transações foram feitas."}}, 200

        logger.error(f"Houveram {falhas} erros com a transacao")
        return {"status":200,
                "resposta":{"tipo":"1", "descricao": "Houveram erros com a transação."}, 
                "situacao": {
                    "sucessos": len(sku) - falhas,
                    "falhas": falhas,
                    "descricao": falhas_hold
                }}, 200