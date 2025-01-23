#=== Importações de módulos externos ===#
from flask_restful import Resource
import re, os

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.message_send as ms
import functions.security as secure

server = os.environ.get("PSERVER_PS").lower()

if server == "production":
    image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
    
elif server == "development":
    image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

else:
    image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"

class EsquecerSenhaMarketplace(Resource):

    @logger
    def post(self) -> dict:
        """
        Método POST do Esquecer Senha do Marketplace

        Returns:
            [dict]: Status da transação
        """
        response_data = dm.get_request(trim_values = True)

        necessary_columns = ["email"]

        # Checa chaves inválidas e faltantes, valores incorretos e nulos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_columns, 
                                          not_null = necessary_columns)):

            return validity

        email = response_data.get('email')

        # Query no usuario
        user_query = """
            SELECT
                TOP 1 u.id_usuario,
                u.nome,
                u.telefone,
                UPPER(u.status) as status

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
                u.email LIKE :email
                AND d.id_distribuidor != 0
                AND uc.data_aprovacao <= GETDATE()
                AND cd.data_aprovacao <= GETDATE()
                AND uc.d_e_l_e_t_ = 0
                AND cd.d_e_l_e_t_ = 0
                AND uc.status = 'A'
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND d.status = 'A'
        """

        params = {
            'email': email
        }
    
        user_query = dm.raw_sql_return(user_query, params = params, first = True)

        if not user_query:

            logger.error(f"Usuario de email-{email} inexistente.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário inválido."}}, 409

        status = user_query.get("status")

        if status != "A":
            logger.error(f"Usuario de email-{email} de status {status}.")
            return {"status":409,
                    "resposta":{
                        "tipo":"9",
                        "descricao":f"Usuário inválido."}}, 409

        # Gera e comita uma senha temporaria para o usuario
        id_usuario = user_query.get("id_usuario")
        global_info.save_info_thread(id_usuario = id_usuario)
        
        first_name = str(user_query.get("nome")).split()[0]
        telefone = user_query.get("telefone")

        password, pass_hash = secure.new_password_generator()

        to_update = {
            "email": email,
            "id_usuario": id_usuario,
            "senha_temporaria": pass_hash.upper(),
            "alterar_senha": "S"
        }

        key_columns = ["email", "id_usuario"]

        dm.raw_sql_update("USUARIO", to_update, key_columns)

        # Pega as informações para criação das mensagens
        sms_message = f"Sua nova senha de acesso ao App DAG+:  {password}.\r\n" + \
            "Se você não fez essa solicitação, entre em contato com nosso suporte."

        # Pega o HTML na url e troca as informações pelas do usuario
        url = "app\\templates\\alterar_senha.html"
        html = ""

        with open(url, 'r', encoding = "utf-8") as f:
            html = str(f.read())

        to_change = {
            "nome": first_name.capitalize(),
            "senha": password.upper(),
            "top": f"{image}/imagens/fotos/400/alterar_senha/trabalhe-conosco.png",
            "middle": f"{image}/imagens/fotos/300/alterar_senha/alterar_senha.png"
        } 

        for key, value in to_change.items():
            html = html.replace(f"@{key.upper()}@", value)
        
        # Envia o sms e o email
        email_send = True
        sms_send = True

        if re.match("^[1-9]{2}9[6-9][0-9]{7}$", telefone):
            sms_send, _ = ms.send_sms(telefone, sms_message)
        else:
            logger.info("Sms nao enviado devido ao numero cadastrado nao ser um telefone celular.")
            sms_send = False

        email_send, _ = ms.send_email(response_data.get("email"), "Suporte DAG", html)   

        if email_send and sms_send:
            logger.info("Nova senha enviada para o usuario por sms e email.")

        elif email_send or sms_send:

            if email_send:
                logger.info("Nova senha enviada para o usuario por email.")

            elif email_send:
                logger.info("Nova senha enviada para o usuario por sms.")

        if email_send or sms_send:
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Nova senha enviada."},
                    }, 200

        logger.info("Nao foi possivel enviar a mensagem para o usuario.")
        return {"status":200,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Erro no envio de senha temporaria."},
                }, 200