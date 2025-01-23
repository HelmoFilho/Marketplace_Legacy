#=== Importações de módulos externos ===#
from flask_restful import Resource
from PIL import Image
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.file_management as fm
import functions.security as secure

class UploadImagemDistribuidor(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1])
    def post(self) -> dict:
        """
        Método POST do Upload de Imagens do Distribuidor

        Returns:
            [dict]: Status da transação
        """        
        info = global_info.get("token_info")

        # Pega os dados do front-end
        request_files = fm.get_files()

        image_folder = os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS")
        used_folder = os.environ.get("IMAGE_USED_PATH_PS")

        caminhos = ["imagens", used_folder, "distribuidores"]

        if not request_files:
            logger.error("Nenhuma imagem enviada.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Nenhuma imagem enviada."}
                    },200

        # Holder das imagens
        key = next(iter(request_files))
        value = request_files[key]
        
        if not key or not value:
            logger.error("Imagem nao enviada no primeiro setor.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Nenhuma imagem válida enviada."}
                    },200

        # Verificando o key da imagem
        try:
            img_type, id_distribuidor = key.split("_")
            id_distribuidor = int(id_distribuidor)
        
        except:
            logger.error("Chave da imagem e invalida.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Chave da imagem é inválida."}
                    },200

        # Verificando a imagem do distribuidor
        try:
            Image.open(value).verify()
        except:
            logger.error("Imagem e invalida.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Imagem inválida."}
                    },200

        # Verificando o distribuidor da imagem
        answer, response = dm.distribuidor_check(id_distribuidor)

        if not answer:
            return response

        # Verificando o caminho da imagem
        path_url = image_folder
        caminhos.extend([f"{id_distribuidor}", "logos"])

        for path in caminhos:
            
            path_url += f"/{path}"

            if not os.path.exists(path_url):
                os.mkdir(path_url)

        # Salvando a imagem
        imagem = Image.open(value)
        image_format = imagem.format

        update_data = {
            "id_distribuidor": id_distribuidor,
            "imagem": f"1.{str(image_format).lower()}"
        }

        key_columns = ["id_distribuidor"]

        dm.raw_sql_update("DISTRIBUIDOR", update_data, key_columns)

        imagem.save(f"{path_url}/1.{str(image_format).lower()}")

        logger.info(f"Imagem do distribuidor {id_distribuidor} salva.")
        return {"status":200,
                "resposta":{
                    "tipo": "1", 
                    "descricao": "Imagem salva com sucesso."}
                },200