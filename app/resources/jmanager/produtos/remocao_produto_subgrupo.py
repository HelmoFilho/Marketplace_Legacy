#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RemocaoProdutoSubgrupo(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2])
    def post(self) -> dict:
        """
        Método POST do Remoção do Subgrupo do Produto

        Returns:
            [dict]: Status da transação
        """
        info = global_info.get("token_info")

        # Pega os dados do front-end
        response_data = dm.get_request()

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor","id_subgrupo", "id_produto"]

        correct_types = {
            "id_distribuidor": int,
            "id_subgrupo": int,
            "id_produto": [list, str]
        }

        # Checa chaves inválidas
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys,
                                            correct_types = correct_types,
                                            not_null = necessary_keys)):
            
            return validity

        # Verificação do id distribuidor
        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        # Pegando as informações recebidas
        id_subgrupo = int(response_data.get("id_subgrupo"))  \
                        if response_data.get("id_subgrupo") else None
        
        produtos = response_data.get("id_produto")

        # Reajustando o id_produto
        if type(produtos) is str:
            produtos = [produtos]

        produtos = list(set(produtos))

        # Hold usuado para commit
        produto_hold = list()
        
        # Hold dos erros
        falhas_hold = dict()
        falhas = 0

        # Verificando a existência do grupo_pai
        subgrupo_pai_query = """
            SELECT
                TOP 1 id_subgrupo

            FROM
                SUBGRUPO

            WHERE
                id_distribuidor = :id_distribuidor
                AND id_subgrupo = :subgrupo_pai
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "subgrupo_pai": id_subgrupo
        }

        subgrupo_pai_query = dm.raw_sql_return(subgrupo_pai_query, params = params, 
                                                raw = True, first = True)

        if not subgrupo_pai_query:
            logger.error(f"Subgrupo pai nao existe.")
            return {"status":200,
                    "resposta":{
                        "tipo": "13", 
                        "descricao": "Ação recusada: Subgrupo pai fornecido não existe."}
                    }, 200

        # Verificando os sku
        for id_produto in produtos:

            # Verificar se o sku já esta associado ao subgrupo pai
            produto_subgrupo_query = """
                SELECT
                    DISTINCT sku,
                    id_distribuidor,
                    id_subgrupo
                
                FROM
                    PRODUTO_SUBGRUPO
                
                WHERE
                    id_subgrupo = :id_subgrupo
                    AND codigo_produto = :id_produto
                    AND id_distribuidor = :id_distribuidor
            """

            params = {
                "id_subgrupo": id_subgrupo,
                "id_produto": id_produto,
                "id_distribuidor": id_distribuidor
            }

            produto_subgrupo_query = dm.raw_sql_return(produto_subgrupo_query, params = params)

            if not produto_subgrupo_query:

                falhas += 1

                if falhas_hold.get("produto_subgrupo"):
                    falhas_hold["produto_subgrupo"]['combinacao'].append({
                        "id_produto": id_produto
                    })
                
                else:
                    falhas_hold[f"produto_subgrupo"] = {
                        "descricao": f"Produto não associado a subgrupo informado",
                        "combinacao": [
                            {
                                "id_produto": id_produto
                            }
                        ]
                    }
                continue

            # Adicionando objeto do commit
            produto_hold.extend(produto_subgrupo_query)

        # Realizando os commits
        if produto_hold:

            dm.raw_sql_delete("Produto_Subgrupo", produto_hold)

        if falhas > 0:

            sucesso = len(produtos) - falhas

            logger.error(f"Houve erro no delete de {falhas} do total de {sucesso} registros.")
            return {"status": 200,
                    "resposta": {"tipo": "15",
                                 "descricao": f"Houve erros com a transação"},
                    "situacao": {
                        "sucessos": sucesso,
                        "falhas": falhas,
                        "descricao": falhas_hold
                    }},200

        logger.info("Todos os registros foram deletados com sucesso")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Registros deletados com sucesso."}}, 200