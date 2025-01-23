#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure

class RegistroListaProdutos(Resource):

    @logger
    @secure.auth_wrapper(permission_range=[1,2,3,4,5])
    def post(self) -> dict:
        """
        Método POST do Registro de lista de produtos

        Returns:
            [dict]: Status da transação
        """
        data_hora = datetime.now()
        
        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True, trim_values = True)

        necessary_keys = ["id_distribuidor", "descricao"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys, 
                                          not_null = necessary_keys,
                                          correct_types = correct_types)):
            return validity

        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response
        
        descricao = response_data.get("descricao")
        
        # Verifica a existência da lista
        lista_query = f"""
            SELECT
                TOP 1 lp.id_lista

            FROM
                LISTA_PRODUTOS lp
                
                INNER JOIN DISTRIBUIDOR d ON
                    lp.id_distribuidor = d.id_distribuidor

            WHERE
                lp.id_distribuidor = :id_distribuidor
                AND lp.descricao = :descricao COLLATE SQL_Latin1_General_CP1_CI_AI
                AND lp.status = 'A'
                AND lp.d_e_l_e_t_ = 0
                AND d.status = 'A'
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "descricao": descricao
        }

        dict_query = {
            "query": lista_query,
            "params": params,
            "first": True,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)
        
        if response:
            logger.error("Lista com descrição exata ja existe salva.")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Lista com exata descrição já existe."}}, 200
        
        # query de insert
        insert = {
            "id_distribuidor": id_distribuidor,
            "descricao": descricao,
            "data_criacao": data_hora.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "status": "A",
            "d_e_l_e_t_": 0
        }

        dm.raw_sql_insert("Lista_Produtos", insert)

        logger.info("Cadastro da lista de produto foi concluida.")
        return {"status":201,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Cadastro da lista de produto foi concluida."}}, 201


    @logger
    @secure.auth_wrapper(permission_range=[1,2,3,4,5])
    def put(self) -> dict:
        """
        Método PUT do Registro de lista de produtos

        Returns:
            [dict]: Status da transação
        """

        # Pega os dados do front-end
        response_data = dm.get_request(values_upper = True, trim_values = True)

        necessary_keys = ["id_lista", "id_produto", "id_distribuidor"]

        correct_types = {
            "id_lista": int, 
            "id_distribuidor": int, 
            "id_produto": [list, str]
        }

        # Checa chaves inválidas e faltantes, valores nulos e se o registro não existe
        if (validity := dm.check_validity(request_response = response_data, 
                                          comparison_columns = necessary_keys, 
                                          not_null = necessary_keys,
                                          correct_types = correct_types)):
            return validity

        id_distribuidor = int(response_data.get("id_distribuidor"))

        answer, response = dm.distr_usuario_check(id_distribuidor)
        if not answer:
            return response

        # Verificando id_lista
        id_lista = int(response_data.get("id_lista"))

        lista_query = f"""
            SELECT 
                TOP 1 lp.id_lista
            
            FROM 
                LISTA_PRODUTOS lp 
            
            WHERE 
                lp.id_lista = :id_lista 
                AND lp.id_distribuidor = :id_distribuidor 
                AND lp.status = 'A' 
                AND lp.d_e_l_e_t_ = 0
        """

        params = {
            "id_distribuidor": id_distribuidor,
            "id_lista": id_lista
        }

        dict_query = {
            "query": lista_query,
            "params": params,
            "first": True,
            "raw": True
        }

        response = dm.raw_sql_return(**dict_query)

        if not response:
            logger.error(f"Lista {id_lista} informada nao existe.")
            return {"status":409,
                    "resposta":{
                        "tipo":"5",
                        "descricao":f"Lista não existe."}}, 200

        # Verificando sku
        id_produtos = response_data.get("id_produto")

        if isinstance(id_produtos, str):
            id_produtos = [id_produtos]

        id_produtos = list(set(id_produtos))

        insert_holder = []

        # Hold das falhas
        falhas = 0
        falhas_hold = dict()

        for id_produto in id_produtos:

            # Verifica se o distribuidor tem p produto salvo
            produto_query = f"""
                SELECT
			    	TOP 1 p.id_produto
			    
                FROM
			    	PRODUTO p
    
			    	INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
			    		p.id_produto = pd.id_produto
			    
                WHERE
			    	pd.id_distribuidor = :id_distribuidor
			    	AND p.id_produto = :id_produto
                    AND p.status = 'A'
                    AND pd.status = 'A'
            """

            params = {
                "id_distribuidor": id_distribuidor,
                "id_produto": id_produto
            }

            dict_query = {
                "query": produto_query,
                "params": params, 
                "first": True,
                "raw": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if not response:

                falhas += 1

                if falhas_hold.get("produto"):
                    falhas_hold["produto"]['combinacao'].append({
                        'id_produto': id_produto,
                        'id_distribuidor': id_distribuidor
                    })
                    
                else:
                    falhas_hold["produto"] = {
                        'motivo': 'Produto nao existe ou não-ativo',
                        "combinacao": [{
                            'id_produto': id_produto,
                            'id_distribuidor': id_distribuidor
                        }]
                    }
                
                continue

            # Verificar se o produto já está cadastrado na lista
            produto_lista = f"""
                SELECT 
			    	TOP 1 lpr.id_lista
			    
                FROM
			    	LISTA_PRODUTOS_RELACAO lpr 
    
			    WHERE 
			    	lpr.id_lista = :id_lista
			    	AND lpr.codigo_produto = :id_produto
            """

            params = {
                "id_lista": id_lista,
                "id_produto": id_produto
            }

            dict_query = {
                "query": produto_lista,
                "params": params, 
                "first": True,
                "raw": True,
            }

            response = dm.raw_sql_return(**dict_query)

            if response:

                falhas += 1

                if falhas_hold.get("lista_produto"):
                    falhas_hold["lista_produto"]['combinacao'].append({
                        'id_produto': id_produto,
                        'id_lista': id_lista
                    })
                    
                else:
                    falhas_hold["lista_produto"] = {
                        'motivo': 'Produto já cadastrado na lista',
                        "combinacao": [{
                            'id_produto': id_produto,
                            'id_lista': id_lista
                        }]
                    }
                
                continue
                
            insert_holder.append({
                "id_lista": id_lista,
                "codigo_produto": id_produto,
                "status": 'A'
            })

        if insert_holder:
            dm.raw_sql_insert("LISTA_PRODUTOS_RELACAO", insert_holder)
        
        if falhas <= 0:
            logger.info("Todos os registros foram adicionados sem problemas.")
            return {"status":201,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Registros adicionados a lista com sucesso."}}, 201
        
        logger.info(f"{falhas} registros nao foram adicionados.")
        return {"status":200,
                "resposta":{
                    "tipo":"1",
                    "descricao":f"Houveram erros com a transação."},
                "situacao": {
                    "falhas": falhas,
                    "sucessos": len(id_produtos) - falhas,
                    "situacao": falhas_hold
                }}, 200