#=== Importações de módulos externos ===#
from flask_restful import Resource
import os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class DeletarDadosUsuario(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Deletar dados do usuario

        Returns:
            [dict]: Status da transação
        """

        id_usuario = global_info.get('id_usuario')

        query = """
            SELECT
                TOP 1 imagem

            FROM
                USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
            "first": True
        }

        response = dm.raw_sql_return(**dict_query)

        if response:

            image_folder = os.environ.get("IMAGE_PATH_MAIN_FOLDER_PS")
            chosen_folder = os.environ.get("IMAGE_USED_PATH_PS")
            image = response[0]

            image_path = f"{image_folder}//imagens//{chosen_folder}//fotos//usuarios//{image}"

            if os.path.exists(image_path):
                if os.path.isfile(image_path):
                    os.remove(image_path)

        query = """
            UPDATE
                USUARIO

            SET
                nome = NULL,
                cpf = NULL,
                status = 'E',
                email = NULL,
                telefone = NULL,
                senha = NULL,
                senha_temporaria = NULL,
                alterar_senha = NULL,
                data_cadastro = NULL,
                data_nascimento = NULL,
                token_google = NULL,
                id_google = NULL,
                token_facebook = NULL,
                id_facebook = NULL,
                token_apple = NULL,
                id_apple = NULL,
                permite_notificacao = NULL,
                permite_email = NULL,
                aceite_termos = NULL,
                data_aceite = NULL,
                d_e_l_e_t_ = 1,
                observacao = NULL,
                imagem = NULL

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        query = """
            UPDATE
                LOGIN

            SET
                data_logout = GETDATE(),
                d_e_l_e_t_ = 1

            WHERE
                id_usuario = :id_usuario
                AND 
                    (
                        d_e_l_e_t_ = 0
                        OR data_logout IS NULL
                    )
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        logger.info(f"Usuario {id_usuario} deletado com sucesso.")

        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Usuario deletado com sucesso."}}, 200