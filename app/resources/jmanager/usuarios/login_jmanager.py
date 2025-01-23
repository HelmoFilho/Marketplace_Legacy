#=== Importações de módulos externos ===#
from datetime import datetime, timedelta
from flask_restful import Resource
import itertools

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class LoginJManager(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self):
        """
        Método POST do Login do JManager

        Returns:
            [dict]: Status da transação
        """
        hora_login = datetime.now()
        
        # Verificação do token
        info = global_info.get("token_info")

        if info:

            # Salvando o LOG do login
            new_log = {
                "id_usuario": info.get("id_usuario"),
                "data_login": hora_login
            }

            dm.raw_sql_insert("JMANAGER_LOGIN_LOG", new_log)

            logger.info("Token do cliente validado e token reenviado.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Token de sessão enviado."}, 
                    "token": global_info.get("token")}, 200

        # Dados recebidos
        response_data = dm.get_request(norm_keys = True, values_upper=True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["email", "senha", "manter_logado"]

        correct_types = {"manter_logado": [bool, str, None, int]}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys,  
                                          not_null = ["email", "senha"],
                                          correct_types = correct_types)):
            
            return validity

        email = response_data.get("email")

        # Checa se o email está cadastrado
        query = """
            SELECT
                TOP 1 ju.id_usuario,
                ju.senha,
                ju.senha_temporaria,
                ju.nome_completo as nome,
                ju.id_perfil,
                UPPER(jf.descricao) as perfil_descricao,
                ju.id_cargo,
                UPPER(jc.descricao) as cargo_descricao,
                UPPER(ju.status) as status,
                UPPER(ju.alterar_senha) as alterar_senha

            FROM
                JMANAGER_USUARIO ju

                INNER JOIN JMANAGER_CARGO jc ON
                    ju.id_cargo = jc.id

                INNER JOIN JMANAGER_PERFIL jf ON
                    ju.id_perfil = jf.id

            WHERE
                email = :email 
        """

        params = {
            "email": email
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.info(f"Usuario com email - {email} nao existente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        id_usuario = response.get("id_usuario")
        nome = response.get("nome")
        status = response.get("status")
        trocar = response.get("alterar_senha")
        new_password = response.get("senha_temporaria")
        true_password = response.get("senha")
        
        id_perfil = response.get("id_perfil")
        perfil_desc = response.get("perfil_descricao")

        id_cargo = response.get("id_cargo")
        cargo_desc = response.get("cargo_descricao")

        global_info.save_info_thread(id_usuario = id_usuario)

        if status == 'P':
            logger.error(f"Usuario com status pendente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário com status pendente."}}, 409

        if status != 'A':
            logger.error(f"Usuario com status inválido '{status}'.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário com status inválido."}}, 409

        # Verificando se o usuario está associado a algum distribuidor ativo
        query = """
            SELECT
                DISTINCT d.id_distribuidor

            FROM
                DISTRIBUIDOR d

                INNER JOIN JMANAGER_USUARIO_DISTRIBUIDOR jud ON
                    d.id_distribuidor = jud.id_distribuidor

                INNER JOIN JMANAGER_USUARIO ju ON
                    jud.id_usuario = ju.id_usuario

            WHERE
                ju.id_usuario = :id_usuario 
                AND d.status = 'A'
                AND jud.status = 'A'
                AND jud.d_e_l_e_t_ = 0
        """

        params = {
            "id_usuario": id_usuario
        }

        dict_query = {
            "query": query,
            "params": params,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.info("Usuario sem distribuidores atrelados a si.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        distribuidores = list(itertools.chain(*response))

        # Verificações primarias do usuario
        senha = response_data.get("senha")

        check_manter = response_data.get("manter_logado")

        if check_manter and (str(check_manter).upper() != 'N'):
            manter = True
        else:
            manter = False

        autorizacao_troca = False

        # Verificação de nova senha
        if new_password and trocar == "S":

            if secure.password_compare(senha, new_password):
                autorizacao_troca = True

            else:
                logger.error("Usuario errou senha temporaria.")
                return {"status":409,
                        "resposta":{
                            "tipo":"9",
                            "descricao":f"Usuário ou senha inválido."}}, 409

        else:
            if trocar == 'S':
                logger.error(f"Usuario com status de alterar senha ativo mas sem senha temporária.")
                return {"status":409,
                        "resposta":{
                            "tipo":"9",
                            "descricao":f"Usuário ou senha inválido."}}, 409

            if str(senha).upper() != str(true_password).upper():
                logger.error("Usuario digitou a senha errada.")
                return {"status":409,
                        "resposta":{
                            "tipo":"9",
                            "descricao":f"Usuário ou senha inválido."}}, 409

        # Salvando o LOG do login
        new_log = {
            "id_usuario": id_usuario,
            "data_login": hora_login
        }
        
        dm.raw_sql_insert("JMANAGER_LOGIN_LOG", new_log)

        if not autorizacao_troca:

            # Verificar se o usuario já possui token ativo
            query = """
                SELECT
                    TOP 1 token_sessao

                FROM
                    JMANAGER_LOGIN

                WHERE
                    id_usuario = :id_usuario
            """

            params = {
                "id_usuario": id_usuario
            }

            dict_query = {
                "query": query,
                "params": params,
                "first": True,
                "raw": True,
            }
            
            response = dm.raw_sql_return(**dict_query)

            if response:

                token = response[0]

                dict_verify_token = {
                    "agent": "jmanager",
                    "token_envio": token
                }

                answer, info = secure.verify_token(**dict_verify_token)
                
                if answer:
                    logger.info("Cliente ja Logado e Token reenviado.")
                    return {"status":200,
                            "resposta":{
                                "tipo":"1",
                                "descricao":f"Token de sessão enviado."}, 
                            "token": token}, 200
        
        # Pegando os distribuidores e descricoes
        distribuidores_query = """
            SELECT
                DISTINCT
                    d.id_distribuidor,
                    UPPER(d.nome_fantasia) as nome_fantasia,
                    d.status

            FROM
                DISTRIBUIDOR d

            WHERE
                (
                    (
                        0 IN :distribuidores
                    )
                        OR
                    (
                        0 NOT IN :distribuidores
                        AND d.id_distribuidor IN :distribuidores
                    )
                )

            ORDER BY
                d.id_distribuidor
        """

        params = {
            "distribuidores": distribuidores
        }

        dict_query = {
            "query": distribuidores_query,
            "params": params,
        }

        distribuidores_query = dm.raw_sql_return(**dict_query)

        payload_data = {
            "nome": str(nome).split()[0],
            "id_usuario": id_usuario,
            "id_distribuidor": distribuidores,
            "distribuidores": distribuidores_query,
            "id_perfil": id_perfil,
            "perfil_descricao": perfil_desc,
            "id_cargo": id_cargo,
            "cargo_descricao": cargo_desc,
            "data_login": hora_login.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "manter_logado": manter,
            "alterar_senha": autorizacao_troca
        }

        if not manter:
            payload_data["exp"] = hora_login.utcnow() + timedelta(hours = 8)

        private_key = open(file = f"C:\\Users\\t_jsantosfilho\\keys\\jmanager", mode = "r").read()

        dict_encode = {
            "payload": payload_data,
            "private_key": private_key,
        }

        token = secure.encode_token(**dict_encode)

        # Deletando os logins alteriores se existirem
        query = """
            IF EXISTS (SELECT 1 FROM JMANAGER_LOGIN WHERE id_usuario = :id_usuario)
            BEGIN

                UPDATE
                    JMANAGER_LOGIN

                SET
                    token_sessao = :token_sessao,
                    data_login = :data_login,
                    manter_logado = :manter_logado

                WHERE
                    id_usuario = :id_usuario

            END

            ELSE
            BEGIN

                INSERT INTO
                    JMANAGER_LOGIN
                        (
                            id_usuario,
                            email,
                            token_sessao,
                            data_login,
                            manter_logado
                        )
                    
                    VALUES
                        (
                            :id_usuario,
                            :email,
                            :token_sessao,
                            :data_login,
                            :manter_logado
                        )

            END
        """

        params = {
            "id_usuario": id_usuario,
            "email": str(email).upper(),
            "token_sessao": token,
            "data_login": hora_login,
            "manter_logado": int(manter)
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        # Retornando o token para o front-end
        logger.info("Cliente Logado e Token enviado.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Token de sessão enviado."},
                "token": token}, 200