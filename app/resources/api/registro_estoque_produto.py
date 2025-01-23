#=== Importações de módulos externos ===#
from flask_restful import Resource
from datetime import datetime

#=== Importações de módulos internos ===#
import functions.data_management as dm
import functions.security as secure
from app.server import logger

class RegistroEstoqueProduto(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self):
        """
        Método POST do Registro de Estoque dos Produtos

        Returns:
            [dict]: Status da transação
        """
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Verificando informações de entrada
        response_data = dm.get_request(values_upper = True, trim_values = True, bool_save_request = False)

        necessary_keys = ["estoque"]

        correct_types = {"estoque": [list, dict]}

        # Checa chaves inválidas e faltantes, valores incorretos e se o registro já existe
        if (validity := dm.check_validity(request_response = response_data,
                                comparison_columns = necessary_keys,
                                not_null = necessary_keys,
                                correct_types = correct_types)):
            
            return validity[0], 200        

        # Verificando os dados enviados
        estoques = response_data.get("estoque")
        
        if type(estoques) is dict:
            estoques = [estoques]

        # Registro de falhas
        total = len(estoques)
        falhas = 0
        falhas_hold = {}

        accepted_keys = ["id_distribuidor","agrupamento_variante","cod_prod_distr",
                            "cod_frag_distr","status","saldo_estoque","giro_produto"]

        # Hold dos inserts
        estoque_insert = []      
        estoque_update = []

        for estoque in estoques:
            if estoque:

                if not isinstance(estoque, dict):

                    falhas += 1

                    if falhas_hold.get("estoque"):
                        falhas_hold["estoque"]['combinacao'][0]["contador"] += 1
                    
                    else:
                        falhas_hold[f"estoque"] = {
                            "descricao": f"estoque não está em formato válido (objeto|dicionário)",
                            "combinacao": [
                                {
                                    "contador": 1,
                                }
                            ]
                        }
                    continue
                
                copy_dict = estoque.copy()

                estoque = {
                    key: copy_dict.get(key)
                    for key in accepted_keys
                }

                estoque["status"] = 'I' if estoque.get("status") in ["I", 'i', False, None] else 'A'

                # Verificação dos dados enviados

                ## id_distribuidor
                id_distribuidor = estoque.get("id_distribuidor")
                cod_prod_distr = estoque.get("cod_prod_distr")

                try:
                    id_distribuidor = int(id_distribuidor)

                except:
                    falhas += 1

                    if falhas_hold.get("distribuidor_id_invalido"):
                        falhas_hold["distribuidor_id_invalido"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr
                        })
                    
                    else:
                        falhas_hold["distribuidor_id_invalido"] = {
                            "motivo": f"Distribuidor com valor não-numérico.",
                            "combinacao": [{
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr
                            }],
                        }
                    continue
                
                if id_distribuidor == 0:
                    falhas += 1

                    if falhas_hold.get("distribuidor_0"):
                        falhas_hold["distribuidor_0"]["combinacao"].append({
                            "id_distribuidor": 0,
                            "cod_prod_distr": cod_prod_distr
                        })
                    
                    else:
                        falhas_hold["distribuidor_0"] = {
                            "motivo": f"Distribuidor 0 não pode cadastrar produtos",
                            "combinacao": [{
                                "id_distribuidor": 0,
                                "cod_prod_distr": cod_prod_distr
                            }],
                        }
                    continue

                # Checando a existencia do distribuidor
                distri_query = f"""
                    SELECT
                        TOP 1 id_distribuidor

                    FROM
                        DISTRIBUIDOR
                    
                    WHERE
                        id_distribuidor = :id_distribuidor
                        AND status = 'A'
                """

                params = {
                    "id_distribuidor": id_distribuidor
                }

                distri_query = dm.raw_sql_return(distri_query, params = params, raw = True, first = True)

                if not distri_query:

                    falhas += 1

                    if falhas_hold.get("distribuidor"):
                        falhas_hold["distribuidor"]['combinacao'].append({
                                "id_distribuidor": id_distribuidor,
                                "cod_prod_distr": cod_prod_distr
                        })
                    
                    else:
                        falhas_hold[f"distribuidor"] = {
                            "descricao": f"Distribuidor não existente ou inativo",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cod_prod_distr": cod_prod_distr
                                }
                            ]
                        }
                    continue
                
                # Verificação de valores nulos
                null = []
                for key in accepted_keys:
                    if str(estoque.get(key)) != "0":
                        if str(estoque.get(key)).upper() != "FALSE":
                            if not bool(estoque.get(key)):
                                null.append(key)
                
                if null:
                    null = list(set(null))

                    falhas += 1

                    if falhas_hold.get("valores_nulos"):
                        falhas_hold["valores_nulos"]['combinacao'].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr,
                            "chaves": null
                        })
                    
                    else:
                        falhas_hold[f"valores_nulos"] = {
                            "descricao": f"Chaves com valor vazio ou nulo",
                            "combinacao": [
                                {
                                    "id_distribuidor": id_distribuidor,
                                    "cod_prod_distr": cod_prod_distr,
                                    "chaves": null
                                }
                            ]
                        }
                    continue

                estoque_query = f"""
                    SELECT 
                        TOP 1 id_distribuidor

                    FROM 
                        API_PRODUTO_ESTOQUE

                    WHERE
                        id_distribuidor = :id_distribuidor
                        AND cod_prod_distr = :cod_prod_distr                        
                """

                params = {
                    "id_distribuidor": id_distribuidor,
                    "cod_prod_distr": cod_prod_distr
                }

                estoque_query = dm.raw_sql_return(estoque_query, params = params, first = True, raw = True)

                if estoque_query:

                    estoque_update.append({
                        "id_distribuidor": id_distribuidor,
                        "cod_prod_distr": cod_prod_distr,
                        "saldo_estoque": estoque.get('saldo_estoque'),
                        "giro_produto": estoque.get('giro_produto'),
                        "status": estoque.get("status"),
                        "atualizado": "N",
                        "dt_update": data_hora
                    })

                    continue
                    
                
                # cod_prod_distr
                agrupamento_variante = str(estoque.get("agrupamento_variante"))
                cod_frag_distr = str(estoque.get("cod_frag_distr"))

                if cod_prod_distr != f"{agrupamento_variante}{cod_frag_distr}":
                    falhas += 1

                    if falhas_hold.get("cod_prod_distr"):
                        falhas_hold["cod_prod_distr"]["combinacao"].append({
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr
                        })

                    falhas_hold["cod_prod_distr"] = {
                        "motivo": f"Chaves 'agrupamento_variante' e 'cod_frag_distr' com valores diferentes de 'cod_prod_distr'",
                        "combinacao": [{
                            "id_distribuidor": id_distribuidor,
                            "cod_prod_distr": cod_prod_distr
                        }]
                    }
                    continue

                estoque_insert.append({
                    "id_distribuidor": id_distribuidor,
                    "agrupamento_variante": agrupamento_variante,
                    "cod_prod_distr": cod_prod_distr,
                    "cod_frag_distr": cod_frag_distr,
                    "status": estoque.get('status'),
                    "saldo_estoque": estoque.get('saldo_estoque'),
                    "giro_produto": estoque.get('giro_produto'),
                    "atualizado": "N",
                    "dt_insert": data_hora
                })

        # Commits
        if estoque_insert:

            dm.raw_sql_insert("API_PRODUTO_ESTOQUE", estoque_insert)
        
        if estoque_update:

            key_columns = ["cod_prod_distr", "id_distribuidor"]
            dm.raw_sql_update("API_PRODUTO_ESTOQUE", estoque_update, key_columns)

        # Salvando os estoques feitos
        if estoque_insert or estoque_update:

            query = f"""
                -- Travar registros 
                UPDATE 
                    A
                
                SET 
                    A.atualizado = 'A'

                FROM
                    API_PRODUTO_ESTOQUE A
                    
                    INNER JOIN PRODUTO_DISTRIBUIDOR B ON 
                        B.ID_DISTRIBUIDOR = A.ID_DISTRIBUIDOR
                        AND B.COD_PROD_DISTR = A.COD_PROD_DISTR
                    
                WHERE 
                    ISNULL(A.atualizado, 'N') = 'N';

                -- Update para atualizar os dados ja existentes
                UPDATE 
                    A
                
                SET 
                    A.QTD_ESTOQUE = C.SALDO_ESTOQUE, 
                    A.ULTIMA_ATUALIZACAO = GETDATE()
                
                FROM 
                    PRODUTO_ESTOQUE A
                    
                    INNER JOIN PRODUTO_DISTRIBUIDOR B ON 
                        B.ID_DISTRIBUIDOR = A.ID_DISTRIBUIDOR
                        AND B.ID_PRODUTO = A.ID_PRODUTO
                
                    INNER JOIN API_PRODUTO_ESTOQUE C ON 
                        C.ID_DISTRIBUIDOR = B.ID_DISTRIBUIDOR
                        AND C.COD_PROD_DISTR = B.COD_PROD_DISTR
                        AND C.AGRUPAMENTO_VARIANTE = B.AGRUPAMENTO_VARIANTE
                
                WHERE 
                    C.ATUALIZADO = 'A' 
                    AND C.SALDO_ESTOQUE <> A.QTD_ESTOQUE;

                -- Insert para atualizar os dados ja existentes
                INSERT INTO 
                    PRODUTO_ESTOQUE
                
                    SELECT 
                        B.ID_PRODUTO, 
                        A.ID_DISTRIBUIDOR, 
                        A.SALDO_ESTOQUE QTD_ESTOQUE, 
                        GETDATE() ULTIMA_ATUALIZACAO
                
                    FROM 
                        API_PRODUTO_ESTOQUE A
                
                        INNER JOIN PRODUTO_DISTRIBUIDOR B ON 
                            A.ID_DISTRIBUIDOR = B.ID_DISTRIBUIDOR
                            AND A.COD_PROD_DISTR = B.COD_PROD_DISTR
                            AND A.AGRUPAMENTO_VARIANTE = B.AGRUPAMENTO_VARIANTE

                    WHERE 
                        A.ATUALIZADO = 'A'
                        AND NOT EXISTS (
                            SELECT 
                                1
                            
                            FROM 
                                PRODUTO_ESTOQUE C
                                
                            WHERE 
                                B.ID_DISTRIBUIDOR = C.ID_DISTRIBUIDOR
                                AND B.ID_PRODUTO=C.ID_PRODUTO
                        );

                -- Salvando na tabela produto distribuidor
                UPDATE
                    pd

                SET
                    pd.estoque = CASE WHEN pe.qtd_estoque > 0 THEN pe.qtd_estoque ELSE 0 END	

                FROM
                    PRODUTO_DISTRIBUIDOR pd

                    LEFT JOIN PRODUTO_ESTOQUE pe ON
                        pd.id_produto = pe.id_produto
                        AND pd.id_distribuidor = pe.id_distribuidor
            """

            dm.raw_sql_execute(query)
        
        sucessos = total - falhas

        if falhas == 0:
            logger.info("Todas as transacoes ocorreram sem problemas")
            return {"status":200,
                    "resposta":{
                        "tipo":"1",
                        "descricao":f"Dados enviados para a administração para análise."},
                    "situacao": {
                        "sucessos": total - falhas,
                        "falhas": falhas,
                        "descricao": falhas_hold}}, 200
        
        logger.info(f"Ocorreram {falhas} falhas e {sucessos} sucessos durante a transacao")
        return {"status":200,
                "resposta":{
                    "tipo":"15",
                    "descricao":f"Ocorreram erros na transação."},
                "situacao": {
                    "sucessos": total - falhas,
                    "falhas": falhas,
                    "descricao": falhas_hold}}, 200