#=== Importações de módulos externos ===#
from datetime import datetime, timedelta
from flask_restful import Resource
from flask import request
import itertools

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class LoginUsuario(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required = False)
    def post(self):
        """
        Método POST do Login do Usuario

        Returns:
            [dict]: Status da transação
        """

        hora_login = datetime.now()

        # Verifica o Token normal
        token_social = request.headers.get("auth-token-social")
        plataforma = request.headers.get("social-platform")

        social_email = None

        info = global_info.get("token_info")

        if info:

            logger.info("Token do cliente validado e token reenviado.")
            return {"status":201,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Token de sessão enviado."},
                    "token": global_info.get("token"),
                    "nome": info.get("nome"),
                    "id_usuario": info.get("id_usuario"),
                    "id_cliente": list(info.get("id_cliente"))}, 200

        # Verifica o token social
        elif token_social:

            answer, response = secure.login_social(plataforma, token_social)
            if not answer:
                return response

            social_email = response.get("social_email")

        # Dados recebidos
        response_data = dm.get_request(values_upper=True)

        # Chaves que precisam ser mandadas
        necessary_keys = ["email", "senha", "manter_logado", "token_aparelho"]
        
        not_null = ["email", "senha", "token_aparelho"]
        
        no_use_columns= ["latitude", "longitude"]

        if social_email:
            necessary_keys.remove("senha")
            not_null.remove("senha")
            no_use_columns.append("senha")

            response_data["email"] = social_email

        # chaves de comparação
        correct_types = {"manter_logado": bool}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys, 
                                          no_use_columns = no_use_columns,
                                          not_null = not_null,
                                          correct_types = correct_types)):
            
            return validity

        # Checagem da sessão
        token_aparelho = response_data.get("token_aparelho")

        query = """
            SELECT
                TOP 1 id_sessao

            FROM
                SESSAO

            WHERE
                token_aparelho = :token_aparelho
                AND d_e_l_e_t_ = 0
                AND status = 'A'

            ORDER BY
                inicio_sessao DESC
        """
        
        params = {
            "token_aparelho": token_aparelho
        }

        dict_query = {
            "query": query,
            "params": params,
            "first": True,
            "raw": True,
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error("Sessao nao existe para dispositivo informado.")
            return {"status": 400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Sessão inexistente."}}, 400

        id_sessao = response[0]

        # Checa se o email está cadastrado
        email = response_data.get("email")

        query = """
            SELECT
                TOP 1 u.id_usuario,
                UPPER(u.nome) as nome,
                UPPER(u.status) as status,
                u.senha,
                u.senha_temporaria,
                UPPER(u.alterar_senha) as alterar_senha

            FROM
                USUARIO u

            WHERE
                u.email = :email
                AND u.status IN ('A', 'P')
                AND u.d_e_l_e_t_ = 0
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
            logger.error(f"Usuario com email - {email} nao existente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário ou senha inválido."}}, 409

        user_data = response

        id_usuario = user_data.get("id_usuario")
        status = user_data.get("status")
        nome = user_data.get("nome")
        trocar = user_data.get("alterar_senha")
        new_password = user_data.get("senha_temporaria")
        true_password = user_data.get("senha")

        global_info.save_info_thread(id_usuario = id_usuario)

        if str(status).upper() == 'P':
            logger.info(f"Usuario com aprovacao pendente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário com aprovação pendente."}}, 409

        # Checa se o usuario possui clientes ativos atrelados ao mesmo
        query = """
            SELECT
                DISTINCT c.id_cliente

            FROM
                USUARIO u

                INNER JOIN USUARIO_CLIENTE uc ON
                    u.id_usuario = uc.id_usuario

                INNER JOIN CLIENTE c ON
                    uc.id_cliente = c.id_cliente

                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                    c.id_cliente = cd.id_cliente

                INNER JOIN.DISTRIBUIDOR d ON
                    cd.id_distribuidor = d.id_distribuidor

            WHERE
                u.id_usuario = :id_usuario
                AND LEN(c.chave_integracao) > 0
                AND d.id_distribuidor != 0
                AND uc.data_aprovacao <= GETDATE()
                AND cd.data_aprovacao <= GETDATE()
                AND uc.d_e_l_e_t_ = 0
                AND cd.d_e_l_e_t_ = 0
                AND u.status = 'A'
                AND uc.status = 'A'
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND d.status = 'A'
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

        if not query:
            logger.info(f"Usuario sem clientes validos atrelados ao mesmo.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuario com aprovação de clientes pendentes."}}, 409

        clientes = list(itertools.chain(*response))

        # Pegando informações necessarias
        input_password = response_data.get("senha")
        input_latitude = response_data.get("latitude") if response_data.get("latitude") else None
        input_longitude = response_data.get("longitude") if response_data.get("longitude") else None

        check_manter = response_data.get("manter_logado")

        if check_manter and (str(check_manter).upper() != 'N'):
            input_manter = True
        else:
            input_manter = False

        autorizacao_troca = False
        
        if not social_email:

            # Verificação de nova senha
            if new_password and trocar == "S":

                if str(input_password).upper() != str(new_password).upper():
                    logger.error(f"Usuario errou senha temporaria.")
                    return {"status":409,
                            "resposta":{
                                "tipo":"9",
                                "descricao":f"Usuário ou senha inválido."}}, 409
                
                autorizacao_troca = True

            else:
                # Verificando se o usuario pode pegar um token
                if trocar == 'S':
                    logger.error(f"Usuario com status de alterar senha ativo mas sem senha temporária.")
                    return {"status":409,
                            "resposta":{
                                "tipo":"9",
                                "descricao":f"Usuário ou senha inválido."}}, 409

                if str(input_password).upper() != str(true_password).upper():
                    logger.error("Usuario digitou a senha errada.")
                    return {"status":409,
                            "resposta":{
                                "tipo":"9",
                                "descricao":f"Usuário ou senha inválido."}}, 409

        # Linkando o usuario a sessão atual
        user_session = {
            "id_sessao": id_sessao,
            "id_usuario": id_usuario,
            "data_insert": hora_login
        }

        dm.raw_sql_insert("USUARIO_SESSAO", user_session)

        if not autorizacao_troca:
        
            # Verificar se o usuario já possui token ativo
            query = """
                SELECT
                    TOP 1 token

                FROM
                    LOGIN

                WHERE
                    id_usuario = :id_usuario
                    AND token_aparelho = :token_aparelho
                    AND d_e_l_e_t_ = 0
                    AND data_logout IS NULL
            """

            params = {
                "id_usuario": id_usuario,
                "token_aparelho": token_aparelho
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
                    "agent": "marketplace",
                    "token_envio": token
                }

                answer, info = secure.verify_token(**dict_verify_token)
                
                if answer:
                    logger.info("Token reenviado para usuario sem token.")
                    return {"status":200,
                            "resposta":{
                                "tipo":"1",
                                "descricao":f"Token de sessão enviado."}, 
                                "token": token,
                                "nome": info.get("nome"),
                                "id_usuario": info.get("id_usuario"),
                                "id_cliente": list(info.get("id_cliente"))}, 200

        # Criação do token
        payload_data = {
            "nome": str(nome).split()[0],
            "id_usuario": id_usuario,
            "id_cliente": clientes,
            "data_login": hora_login.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "alterar_senha": autorizacao_troca,
            "manter_logado": int(input_manter),
        }

        if not input_manter:
            payload_data["exp"] = hora_login.utcnow() + timedelta(hours = 8)

        private_key = open(file = f"C:\\Users\\t_jsantosfilho\\keys\\marketplace", mode = "r").read()

        dict_encode = {
            "payload": payload_data,
            "private_key": private_key,
        }

        token = secure.encode_token(**dict_encode)

        # Deletando os logins alteriores se existirem
        query = """
            UPDATE
                LOGIN

            SET
                data_logout = :hora_login,
                d_e_l_e_t_ = 1

            WHERE
                id_usuario = :id_usuario
                AND token_aparelho = :token_aparelho
        """

        params = {
            "id_usuario": id_usuario,
            "token_aparelho": token_aparelho,
            "hora_login": hora_login,
        }

        dict_query = {
            "query": query,
            "params": params,
        }

        dm.raw_sql_execute(**dict_query)

        # Criando o objeto de login
        new_login = {
            "id_usuario": id_usuario,
            "token": token,
            "token_aparelho": token_aparelho,
            "latitude": input_latitude,
            "longitude": input_longitude,
            "data_login": hora_login,
            "manter_logado": int(input_manter),
            "d_e_l_e_t_": 0
        }

        dm.core_sql_insert("LOGIN", new_login)

        # Retornando o token para o front-end
        logger.info("Token criado e enviado para o usuario.")
        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Token de sessão enviado."}, \
                "token": token,
                "nome": str(nome).upper().split()[0],
                "id_usuario": id_usuario,
                "id_cliente": clientes}, 201