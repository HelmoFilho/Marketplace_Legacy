#=== Importações de módulos externos ===#
from flask_restful import Resource
from PIL import Image
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.file_management as fm
import functions.security as secure

class UploadImagemUsuario(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Upload de Imagens do Usuario

        Returns:
            [dict]: Status da transação
        """        
        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        request_files = fm.get_files()

        image_folder = os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS")
        used_folder = os.environ.get("IMAGE_USED_PATH_PS")

        caminhos = ["imagens", used_folder, "fotos", "usuarios"]

        if not request_files:
            logger.error("Nenhuma imagem enviada.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Nenhuma imagem enviada."}
                    },200

        value = request_files.get("imagem")
        
        if not value:
            logger.error("Imagem nao enviada no primeiro setor.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Nenhuma imagem válida enviada."}
                    },200            

        # Verificando a imagem enviada
        try:
            Image.open(value).verify()
        except:
            logger.error("Imagem e invalida.")
            return {"status":400,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Imagem inválida."}
                    },200

        # Verificando o caminho da imagem
        path_url = image_folder

        for path in caminhos:
            
            path_url += f"/{path}"

            if not os.path.exists(path_url):
                os.mkdir(path_url)

        # Verificando se já existe uma imagem salva
        query = """
            SELECT
                TOP 1 imagem

            FROM
                USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario,
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True
        }

        query = dm.raw_sql_return(**dict_query)
        if query:

            old_file = query[0]
            if os.path.isfile(f"{path_url}/{old_file}"):
                os.remove(f"{path_url}/{old_file}")

        # Salvando a imagem
        imagem = Image.open(value)
        image_format = imagem.format

        update_data = {
            "id_usuario": id_usuario,
            "imagem": f"{id_usuario}.png"
        }

        key_columns = ["id_usuario"]

        dm.raw_sql_update("USUARIO", update_data, key_columns)

        imagem.save(f"{path_url}/{id_usuario}.png")

        logger.info(f"Imagem do usuario salva.")
        return {"status":200,
                "resposta":{
                    "tipo": "1", 
                    "descricao": "Imagem salva com sucesso."}
                },200

        