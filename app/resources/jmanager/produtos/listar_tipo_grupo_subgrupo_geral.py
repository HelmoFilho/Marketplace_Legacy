#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm
import functions.security as secure 

class ListarTipoGrupoSubgrupoGeral(Resource):

    @logger
    @secure.auth_wrapper()
    def post(self) -> dict:
        """
        Método POST do Listar Tipo Grupo Subgrupo do Geral

        Returns:
            [dict]: Status da transação
        """

        info = global_info.get("token_info")
        
        id_distribuidor_token = info.get("id_distribuidor")
        id_perfil = info.get("id_perfil")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]

        correct_types = {"id_distribuidor": int}

        # Checa chaves inválidas e chaves faltantes
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            correct_types = correct_types)):
            return validity

        id_distribuidor = int(response_data.get("id_distribuidor"))

        if id_perfil != 1:
            if id_distribuidor not in id_distribuidor_token:
                logger.error(f"Usuario tentou realizar acao por D:{id_distribuidor} nao atrelado ao mesmo.")
                return {"status":403,
                        "resposta":{
                            "tipo":"13",
                            "descricao":f"Ação recusada: Usuario tentando realizar ação por cliente não atrelado ao mesmo."}}, 403

        # Criando o json
        query = r"""
            SET NOCOUNT ON;

            SELECT
                DISTINCT t.id_tipo,
                t.descricao as descricao_tipo,
                CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END t_ranking,
                t.status as status_tipo,
                g.id_grupo,
                g.descricao as descricao_grupo,
                CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END g_ranking,
                g.status as status_grupo,
                s.id_subgrupo,
                s.descricao as descricao_subgrupo,
                CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END s_ranking,
                s.status as status_subgrupo,
                gs.status as status_grupo_subgrupo

            FROM
                DISTRIBUIDOR d

                INNER JOIN SUBGRUPO s ON
                    d.id_distribuidor = s.id_distribuidor
                                                    
                INNER JOIN GRUPO_SUBGRUPO gs ON
                    s.id_subgrupo = gs.id_subgrupo

                INNER JOIN GRUPO g ON
                    gs.id_grupo = g.id_grupo

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

            WHERE
                s.id_distribuidor = :id_distribuidor

            ORDER BY
                t_ranking,
                t.descricao,
                g_ranking,
                g.descricao,
                s_ranking,
                s.descricao
        """

        params = {
            "id_distribuidor": id_distribuidor,
        }

        dict_query = {
            "query": query,
            "params": params
        }

        data = dm.raw_sql_return(**dict_query)

        if data:

            dados = []

            for tgs in data:

                for saved_tgs in dados:
                    if saved_tgs["id_tipo"] == tgs["id_tipo"]:
                        
                        grupos = saved_tgs["grupo"]

                        for grupo in grupos:
                            if grupo["id_grupo"] == tgs["id_grupo"]:

                                subgrupos = grupo["subgrupo"]

                                for subgrupo in subgrupos:
                                    if subgrupo["id_subgrupo"] == tgs["id_subgrupo"]:
                                        break

                                else:
                                    subgrupos.append({
                                        "id_subgrupo": tgs["id_subgrupo"],
                                        "descricao_subgrupo": tgs["descricao_subgrupo"],
                                        "status_subgrupo": tgs["status_subgrupo"],
                                        "status_grupo_subgrupo": tgs["status_grupo_subgrupo"]
                                    })

                                break

                        else:
                            grupos.append({
                                "id_grupo": tgs["id_grupo"],
                                "descricao_grupo": tgs["descricao_grupo"],
                                "status_grupo": tgs["status_grupo"],
                                "subgrupo": [{
                                    "id_subgrupo": tgs["id_subgrupo"],
                                    "descricao_subgrupo": tgs["descricao_subgrupo"],
                                    "status_subgrupo": tgs["status_subgrupo"],
                                    "status_grupo_subgrupo": tgs["status_grupo_subgrupo"]
                                }]
                            })
                            
                        break


                else:
                    dados.append({
                        "id_tipo": tgs["id_tipo"],
                        "descricao_tipo": tgs["descricao_tipo"],
                        "status_tipo": tgs["status_tipo"],
                        "grupo": [{
                            "id_grupo": tgs["id_grupo"],
                            "descricao_grupo": tgs["descricao_grupo"],
                            "status_grupo": tgs["status_grupo"],
                            "subgrupo": [{
                                "id_subgrupo": tgs["id_subgrupo"],
                                "descricao_subgrupo": tgs["descricao_subgrupo"],
                                "status_subgrupo": tgs["status_subgrupo"],
                                "status_grupo_subgrupo": tgs["status_grupo_subgrupo"]
                            }]
                        }]
                    })
            
            if dados:
                    
                logger.info(f"Dados enviados para o front-end.")
                return {"status":200,
                        "resposta":{"tipo":"1","descricao":f"Dados enviados."},"dados":dados}, 200

        logger.info(f"Dados nao encontrados")
        return {"status":404,
                "resposta":{
                    "tipo":"7",
                    "descricao":f"Sem dados para retornar."}}, 200