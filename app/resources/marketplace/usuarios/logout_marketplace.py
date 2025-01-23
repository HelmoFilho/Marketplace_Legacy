#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class LogoutMarketplace(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Logout do Marketplace

        Returns:
            [dict]: Status da transação
        """
        token = global_info.get("token")

        update_data = {
            "token": token,
            "d_e_l_e_t_": 1,
            "data_logout": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        }

        key_columns = ["token"]

        dm.raw_sql_update("LOGIN", update_data, key_columns)

        logger.info("Cliente deslogado e token desabilitado.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Usuario deslogado."}}, 200