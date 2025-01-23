#=== Importações de módulos externos ===#
from flask_restful import Resource
import datetime, base64, re
from flask import request

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.payment_management as pm
import functions.data_management as dm
import functions.default_json as dj
import functions.security as secure

class RegistroCartaoUsuarioMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def get(self, id_cliente = None):
        """
        Método GET do Registro de cartao do Usuario Marketplace Específico

        Returns:
            [dict]: Status da transação
        """

        id_usuario = int(global_info.get('id_usuario'))

        # Verificando os dados de cartão salvos
        response = dj.json_cartoes(id_usuario)

        if not response:
            logger.info(f"Nao existem cartoes salvos para o usuario.")
            return {"status":404,
                    "resposta":{
                        "tipo":"7",
                        "descricao":f"Sem dados para retornar."}}, 200

        logger.info(f"Encontrados {len(response)} cartoes salvos para o usuario.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Dados enviados."},
                "dados": response}, 200


    @logger
    @secure.auth_wrapper()
    def post(self, id_cliente = None):
        """
        Método POST do Registro de cartao do Usuario Marketplace Específico

        Returns:
            [dict]: Status da transação
        """

        data_hora = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        id_usuario = global_info.get("id_usuario")

        # Dados recebidos
        response_data = dm.get_request(values_upper = True, trim_values = True, norm_values = True)

        # Chaves que precisam ser mandadas
        necessary_keys = [
            "nome_titular", "logradouro", "numero", "salvar_cvv", "cidade", "estado", 
            "cep", "ddd", "telefone", "sexo", "numero_cartao", "vencimento",  "cpf",
            "bairro", "data_nascimento"
        ]

        no_use_columns = ["complemento", "cvv", "bandeira"]

        correct_types = {
            "numero_cartao": int, "cvv": int, "numero": int, "salvar_cvv": bool, 
            "ddd": int, "telefone": int, "vencimento": int
        }

        response_data["salvar_cvv"] = bool(response_data.get("salvar_cvv"))

        if response_data.get("salvar_cvv"):
            no_use_columns.remove("cvv")
            necessary_keys.append("cvv")
        
        else:
            response_data["cvv"] = None

        # Checagem dos dados de entrada
        if (validity := dm.check_validity(request_response = response_data,
                                          comparison_columns = necessary_keys,
                                          not_null = necessary_keys,
                                          no_use_columns = no_use_columns,
                                          correct_types = correct_types)):
            
            return validity

        # Verificando os dados de entrada

        ## Nome
        nome = response_data.get("nome_titular")
        if len(nome.split(" ")) == 1:
            logger.error("Usuario forneceu nome do titular invalido")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Forneca nome e sobrenome."}}, 200 

        ## Localizacao
        estado = response_data.get("estado")
        if len(estado) != 2:
            logger.error("Usuario forneceu estado invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Estado invalido."}}, 200 

        cidade = response_data.get("cidade")
        bairro = response_data.get("bairro")
        endereco = response_data.get("logradouro")
        complemento = response_data.get("complemento")\
                        if response_data.get("complemento") else ""
        numero = str(response_data.get("numero"))

        cep = re.sub("[^0-9]", "", response_data.get("cep"))
        if len(cep) != 8:
            logger.error("Usuario tentou se cadastrar com cep invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: CEP invalido."}}, 200 

        ## Cartao
        numero_cartao = str(response_data.get("numero_cartao"))
        if not secure.credit_card_number_validator(numero_cartao):
            logger.error("Usuario forneceu numero do cartao invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Numero de cartão invalido."}}, 200

        salvar_cvv = False if response_data.get("salvar_cvv") in [False, "N", "n", None] else True
        cvv = str(response_data.get("cvv")) if salvar_cvv else None

        bandeira = str(secure.credit_card_brand_validator(numero_cartao)).upper()

        if not bandeira:
            logger.error("Usuario forneceu numero do cartao sem bandeira.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Numero de cartão invalido."}}, 200

        vencimento = str(response_data.get("vencimento"))
        mes_vencimento = vencimento[:2]
        ano_vencimento = vencimento[2:]

        ## Info pessoal
        try:
            data_nascimento = str(response_data["data_nascimento"])
            pattern = "%Y-%m-%d"
            data_nascimento = datetime.datetime.strptime(data_nascimento, pattern)
        
        except:
            logger.error("Usuario tentou fornecer data de nascimento invalida.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: Data de nascimento invalida."}}, 200  

        ddd = str(response_data.get("ddd"))
        if not re.match(r"^0?[1-9]{2}$", ddd):
            logger.error("Usuario tentou fornecer ddd invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: ddd invalido."}}, 200 
        
        if len(ddd) == 3:
            ddd = ddd[1:]

        try:
            cpf = re.sub("[^0-9]", "", response_data["cpf"])
            if not secure.cpf_validator(cpf):
                logger.error("Usuario tentou se cadastrar com cpf invalido.")
                return {"status":400,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: cpf invalido."}}, 200 

        except:
            logger.error("Usuario tentou se cadastrar com cpf invalido.")
            return {"status":400,
                    "resposta":{
                        "tipo":"13",
                        "descricao":f"Ação recusada: cpf invalido."}}, 200 

        telefone = str(response_data.get("telefone"))
        sexo = response_data.get("sexo")

        # Pegando o email do usuario
        email_query = """
            SELECT
                TOP 1 email

            FROM
                USUARIO

            WHERE
                id_usuario = :id_usuario
        """

        params = {
            "id_usuario": id_usuario
        }

        email = str(dm.raw_sql_return(email_query, params = params, first = True, raw = True)[0]).upper()

        # Verificando se o usuario já esta cadastrado
        user_maxipago_query = """
            SELECT
                d.id_distribuidor,
                udm.id_maxipago

            FROM
                DISTRIBUIDOR d

                LEFT JOIN USUARIO_DISTRIBUIDOR_MAXIPAGO udm ON
                    d.id_distribuidor = udm.id_distribuidor
                    AND udm.id_usuario = :id_usuario

            WHERE
                d.status = 'A'
                AND d.id_distribuidor != 0

            ORDER BY
                d.id_distribuidor
        """

        params = {
            'id_usuario': id_usuario
        }

        dict_query = {
            "query": user_maxipago_query,
            "params": params,
            "raw": True
        }

        user_maxipago = dm.raw_sql_return(**dict_query)

        if not user_maxipago:

            logger.error(f"Não existem distribuidores salvos na base para realizar cadastro de cartão.")
            msg = {"status":400,
                   "resposta":{
                       "tipo":"13",
                       "descricao":f"Falha na transação. Entre em contato com o suporte."}}, 200

            return msg

        for distr_card in user_maxipago:

            id_distribuidor, id_maxipago = distr_card

            user_data = {
                "id_usuario": id_usuario,
                "nome": nome,
                "endereco": endereco, 
                "numero": numero,
                "complemento": complemento,
                "cidade": cidade,
                "estado": estado,
                "cep": cep,
                "telefone": ddd + telefone,
                "email": email,
                "data_nascimento": data_nascimento,
                "sexo": str(sexo[0]).upper(),
            }

            if not id_maxipago:

                user_data["id_distribuidor"] = id_distribuidor

                answer, response = pm.maxipago_cadastrar_cliente(**user_data)
                if not answer:
                    return response

        # Cadastrando o cartão de crédito
        credit_card_data = {
            "id_usuario": id_usuario,
            "numero_cartao": numero_cartao,
            "mes_vencimento": mes_vencimento,
            "ano_vencimento": ano_vencimento,
            "nome": nome,
            "endereco": endereco,
            "numero_endereco": str(numero),
            "complemento": complemento,
            "cidade": cidade,
            "estado": estado,
            "cep": cep,
            "ddd": ddd,
            "telefone": telefone,
            "email": email,
            "bandeira": bandeira,
            "bairro": bairro,
            "cpf": cpf,
            "data_nascimento": data_nascimento,
            "sexo": sexo,
            "data_criacao": data_hora,
            "cvv": cvv
        }

        answer, response = pm.maxipago_cadastrar_cartao(**credit_card_data)

        if not answer:
            return response

        dict_cartoes = {
            "id_usuario": id_usuario
        }

        response = dj.json_cartoes(**dict_cartoes)

        logger.info(f"Novo cartao cadastrado para o usuario.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Cartão salvo com sucesso."},
                "dados": response}, 200 