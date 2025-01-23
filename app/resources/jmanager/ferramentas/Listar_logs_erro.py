#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class ListarLogsErro(Resource):

    @logger
    @secure.auth_wrapper(permission_range = [1])
    def post(self) -> dict:
        """
        Método POST do Listar logs de erro do marketplace

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(norm_values = True)
        
        unnecessary_keys = ["data_erro_de", "data_erro_ate"]

        # Checa chaves inválidas e faltantes e valores nulos
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = unnecessary_keys,
                                bool_missing = False)):
            return validity

        # Checando a pagina e o limite atual
        pagina, limite = dm.page_limit_handler(response_data)

        string_date_1 = ""
        string_date_2 = ""

        params = {
            "offset": (pagina - 1) * limite,
            "limite": limite
        }

        # Checando a data de cadastro
        date_1 = response_data.get("data_erro_de")
        date_2 = response_data.get("data_erro_ate")

        if date_1:

            try:
                date_1 = datetime.strptime(date_1, '%Y-%m-%d')
                date_1 = date_1.strftime('%Y-%m-%d')
                if date_1 < "1900-01-01":
                    date_1 = "1900-01-01"
            except:
                date_1 = "1900-01-01"

            params.update({
                "date_1": date_1,
            })

            string_date_1 = "AND CONVERT(DATE, le.error_date) >= :date_1"

        if date_2:

            try:
                date_2 = datetime.strptime(date_2, '%Y-%m-%d')
                date_2 = date_2.strftime('%Y-%m-%d')
                if date_2 < "1900-01-01":
                    date_2 = "1900-01-01"
            except:
                date_2 = "1900-01-01"

            params.update({
                "date_2": date_2,
            })

            string_date_2 = "AND CONVERT(DATE, le.error_date) <= :date_2"

        query = f"""
            SELECT
                phone_token,
                session,
                error_date,
                message,
                request,
                COUNT(*) OVER() count__

            FROM
                (

                    SELECT
                        le.phone_token,
                        le.session,
                        le.error_date,
                        le.message,
                        le.request

                    FROM
                        LOG_ERROS le

                    WHERE
                        LEN(phone_token) > 0
                        {string_date_1}
                        {string_date_2}

                ) le

            ORDER BY
                error_date DESC

            OFFSET
                :offset ROWS

            FETCH
                NEXT :limite ROWS ONLY
        """

        dict_query = {
            "query": query,
            "params": params,
        }

        response = dm.raw_sql_return(**dict_query)
        if not response:
            logger.info("Sem logs cadastrados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"7",
                        "descricao":f"Sem dados para retornar."}
                    }, 200 

        counter = response[0].get("count__")

        for log in response:
            log.pop("count__")
            
        logger.info("Logs enviados.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Logs encontrados."},
                "informacoes": {
                    "itens": counter,
                    "paginas": counter//limite + (counter % limite > 0),
                    "pagina_atual": pagina},
                "dados": response}, 200