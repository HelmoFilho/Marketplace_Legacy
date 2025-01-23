#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class ListarListasProdutos(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Listas de Produtos

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)
        
        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            return validity

        # Tratamento do distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        if id_distribuidor != 0:
            answer, response = dm.distr_usuario_check(id_distribuidor)
            if not answer:
                return response

            distribuidores = [id_distribuidor]

        else:
            global_info.save_info_thread(id_distribuidor = id_distribuidor)
            distribuidores = info.get('id_distribuidor')

        # Realizando a query
        lista_query = f"""
            SELECT
                DISTINCT
                    lp.id_lista,
                    lp.id_distribuidor,
                    lp.descricao,
                    lp.data_criacao,
                    lp.status
            
            FROM
                LISTA_PRODUTOS lp

                INNER JOIN LISTA_PRODUTOS_RELACAO lpr ON
                    lp.id_lista = lpr.id_lista

            WHERE
                (
                    (
                        0 IN :distribuidores
                    )
                        OR
                    (
                        0 NOT IN :distribuidores 
                        AND lp.id_distribuidor IN :distribuidores
                    )
                )
                AND lpr.status = 'A'
                AND lp.d_e_l_e_t_ = 0

            ORDER BY
                lp.data_criacao ASC
        """

        params = {
            "distribuidores": distribuidores
        }

        dict_query = {
            "query": lista_query,
            "params": params
        }

        data = dm.raw_sql_return(**dict_query)
        
        if data:

            logger.info(f"Dados do produto foram enviados.")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados encontrados."}, 
                    "dados": data}, 200

        logger.info(f"Listas de produtos nao foram encontradas.")
        return {"status":400,
                "resposta":{
                    "tipo":"6",
                    "descricao":f"Dados não encontrados para estes filtros."}}, 200