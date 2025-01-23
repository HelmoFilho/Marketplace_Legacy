#=== Importações de módulos externos ===#
from flask_restful import Resource

#=== Importações de módulos internos ===#
from app.server import logger, global_info
import functions.data_management as dm 
import functions.security as secure

class ListarMarcasMarketplace(Resource):

    @logger
    @secure.auth_wrapper(bool_auth_required=False)
    def post(self) -> dict:
        """
        Método GET do Listar Marcas Marketplace

        Returns:
            [dict]: Status da transação
        """
        
        id_usuario = global_info.get("id_usuario")

        # Pega os dados do front-end
        response_data = dm.get_request(trim_values = True)

        # Serve para criar as colunas para verificação de chaves inválidas
        necessary_keys = ["id_distribuidor"]

        no_use_columns = ["id_cliente", "id_marca", "id_tipo", "id_grupo", "id_subgrupo",
                            "tipo_oferta", "id_oferta", "desconto", "estoque"]

        correct_types = {
            "id_distribuidor": int,
            "id_cliente": int,
            "id_marca": [list, int],
            "id_produto": [list, int],
            "id_tipo": int,
            "id_grupo": int,
            "id_subgrupo": int,
            "tipo_oferta": int,
            "id_oferta": int,
            "estoque": bool,
            "desconto": bool
        }

        # Checa dados enviados
        if (validity := dm.check_validity(request_response = response_data, 
                                            comparison_columns = necessary_keys, 
                                            not_null = necessary_keys,
                                            no_use_columns = no_use_columns,
                                            correct_types = correct_types)):
            
            return validity

        ## Dados de entrada
        id_cliente = int(response_data.get("id_cliente")) if response_data.get("id_cliente") else None
        id_distribuidor = int(response_data.get("id_distribuidor"))

        if id_usuario:
            answer, response = dm.cliente_check(id_cliente)
            if not answer:
                return response

        else:
            id_usuario = None
            id_cliente = None

        id_tipo = int(response_data.get("id_tipo")) if response_data.get("id_tipo") else None
        id_grupo = int(response_data.get("id_grupo")) if response_data.get("id_grupo") else None
        id_subgrupo = int(response_data.get("id_subgrupo")) if response_data.get("id_subgrupo") else None

        id_produto = response_data.get("id_produto") if response_data.get("id_produto") else None
        id_oferta = response_data.get("id_oferta") if response_data.get("id_oferta") else None

        tipo_oferta = int(response_data.get("tipo_oferta"))\
                        if response_data.get("tipo_oferta") and id_usuario else None

        desconto = True if response_data.get("desconto") and id_usuario else False
        estoque = True if response_data.get("estoque") and id_usuario else False

        id_marca = response_data.get("id_marca") if response_data.get("id_marca") else None

        # Pegando os distribuidores
        if not id_usuario:
            query = """
                SELECT
                    DISTINCT id_distribuidor

                FROM
                    DISTRIBUIDOR

                WHERE
                    status = 'A'
                    AND id_distribuidor = CASE
                                                WHEN :id_distribuidor = 0
                                                    THEN id_distribuidor
                                                ELSE
                                                    :id_distribuidor
                                            END
            """

            params = {
                "id_distribuidor": id_distribuidor
            }

        else:
            query = """
                SELECT
                    DISTINCT d.id_distribuidor

                FROM
                    CLIENTE c

                    INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                        c.id_cliente = cd.id_cliente

                    INNER JOIN DISTRIBUIDOR d ON
                        cd.id_distribuidor = d.id_distribuidor

                WHERE
                    c.status = 'A'
                    AND c.status_receita = 'A'
                    AND cd.status = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND d.status = 'A'
                    AND	c.id_cliente = :id_cliente
                    AND d.id_distribuidor = CASE
                                                WHEN :id_distribuidor = 0
                                                    THEN d.id_distribuidor
                                                ELSE
                                                    :id_distribuidor
                                            END
            """

            params = {
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor
            }

        distribuidores = dm.raw_sql_return(query, params = params, raw = True)
        if not distribuidores:
            logger.error("Sem distribuidores cadastrados válidos para a operação.")
            return {"status": 400,
                    "resposta": {
                        "tipo": "13", 
                        "descricao": "Ação recusada: Sem distribuidores cadastrados."}}, 200
                        
        distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

        # Cria o JSON
        params = {
            "id_distribuidor": id_distribuidor,
            "distribuidores": distribuidores,
            "id_cliente": id_cliente
        }

        bindparams = ["distribuidores"]

        string_id_marca = ""
        string_tgs = ""
        string_id_oferta = ""
        string_id_produto = ""
        string_ofertas = ""
        string_desconto = ""
        string_estoque = ""

        if id_marca:

            if type(id_marca) not in [list, tuple, set]:
                id_marca = [id_marca]

            else:
                id_marca = list(id_marca)

            bindparams.append("id_marca")

            params.update({
                "id_marca": id_marca
            })

            string_id_marca = "AND m.id_marca IN :id_marca"

        if id_tipo or id_grupo or id_subgrupo:

            if id_subgrupo:
                string_tgs += f"AND s.id_subgrupo = :id_subgrupo "
                params["id_subgrupo"] = id_subgrupo

            if id_grupo:
                string_tgs += "AND g.id_grupo = :id_grupo "
                params["id_grupo"] = id_grupo

            if id_tipo:
                string_tgs += "AND t.id_tipo = :id_tipo "
                params["id_tipo"] = id_tipo

        if id_produto:

            if type(id_produto) not in [list, tuple, set]:
                id_produto = [id_produto]

            else:
                id_produto = list(id_produto)

            bindparams.append("id_produto")

            params.update({
                "id_produto": id_produto
            })

            string_id_produto = "AND p.id_produto IN :id_produto"

        if id_oferta:

            if type(id_oferta) not in [list, tuple, set]:
                id_oferta = [id_oferta]

            else:
                id_oferta = list(id_oferta)

            bindparams.append("id_oferta")

            params.update({
                "id_oferta": id_oferta
            })

            string_id_oferta = "AND offers.id_oferta IN :id_oferta"

        if tipo_oferta:
            if tipo_oferta == 1:
                string_ofertas = "AND offers.tipo_oferta = 2"
            
            elif tipo_oferta == 2:
                string_ofertas = "AND offers.tipo_oferta = 3"

            elif tipo_oferta == 3:
                string_ofertas = "AND offers.tipo_oferta IN (2,3)"

        if desconto:
            string_desconto = "AND odesc.desconto > 0"

        if estoque:
            string_estoque = "AND pe.qtd_estoque > 0"

        marca_query = f"""
            SET NOCOUNT ON;

            SELECT
                DISTINCT m.id_distribuidor,
                m.id_marca,
                m.desc_marca
            
            FROM 
                PRODUTO p

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto
            
                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca
                    AND pd.id_distribuidor = m.id_distribuidor

                INNER JOIN PRODUTO_SUBGRUPO ps ON
                    pd.id_produto = ps.codigo_produto 

                INNER JOIN SUBGRUPO s ON
                    ps.id_subgrupo = s.id_subgrupo
                    AND ps.id_distribuidor = s.id_distribuidor
                                                                
                INNER JOIN GRUPO_SUBGRUPO gs ON
                    s.id_subgrupo = gs.id_subgrupo

                INNER JOIN GRUPO g ON
                    gs.id_grupo = g.id_grupo

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

                LEFT JOIN PRODUTO_ESTOQUE pe ON
                    pd.id_produto = pe.id_produto 
                    AND pd.id_distribuidor = pe.id_distribuidor

                LEFT JOIN 
                (
                    SELECT
                        op.id_produto,
                        o.id_distribuidor,
                        MAX(od.desconto) desconto,
                        o.data_inicio,
                        o.data_final

                    FROM
                        (

                            SELECT
                                o.id_oferta

                            FROM
                                OFERTA o

                            WHERE
                                id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                            UNION

                            SELECT
                                o.id_oferta

                            FROM
                                OFERTA o

                                INNER JOIN OFERTA_CLIENTE oc ON
                                    o.id_oferta = oc.id_oferta

                            WHERE
                                oc.id_cliente = :id_cliente
                                AND oc.status = 'A'
                                AND oc.d_e_l_e_t_ = 0

                        ) offers

                        INNER JOIN OFERTA o ON
                            offers.id_oferta = o.id_oferta

                        INNER JOIN OFERTA_PRODUTO op ON
                            o.id_oferta = op.id_oferta

                        INNER JOIN OFERTA_DESCONTO od ON
                            o.id_oferta = od.id_oferta

                    WHERE
                        o.tipo_oferta = 1
                        AND o.id_distribuidor IN :distribuidores
                        AND o.status = 'A'
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()
                        AND op.status = 'A'
                        AND od.status = 'A'

                    GROUP BY
                        op.id_produto,
                        o.id_distribuidor,
                        o.data_inicio,
                        o.data_final

                ) odesc ON
                    p.id_produto = odesc.id_produto
                    AND pd.id_distribuidor = odesc.id_distribuidor

                LEFT JOIN 
                (
                    SELECT
                        offers.id_oferta,
                        offers_produto.id_produto,
                        offers_produto.tipo_oferta

                    FROM
                        (
                            SELECT
                                op.id_oferta,
                                op.id_produto,
                                o.tipo_oferta

                            FROM
                                (

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                    WHERE
                                        id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                                    UNION

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                        INNER JOIN OFERTA_CLIENTE oc ON
                                            o.id_oferta = oc.id_oferta

                                    WHERE
                                        oc.id_cliente = :id_cliente
                                        AND oc.status = 'A'
                                        AND oc.d_e_l_e_t_ = 0

                                ) offers

                                INNER JOIN OFERTA o ON
                                    offers.id_oferta = o.id_oferta

                                INNER JOIN OFERTA_PRODUTO op ON
                                    o.id_oferta = op.id_oferta

                            WHERE
                                o.tipo_oferta IN (2,3)
                                AND o.id_distribuidor IN :distribuidores
                                AND o.status = 'A'
                                AND o.data_inicio <= GETDATE()
                                AND o.data_final >= GETDATE()
                                AND op.status = 'A'

                            UNION

                            SELECT
                                ob.id_oferta,
                                ob.id_produto,
                                o.tipo_oferta

                            FROM
                                (

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                    WHERE
                                        id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE WHERE status = 'A' AND d_e_l_e_t_ = 0)

                                    UNION

                                    SELECT
                                        o.id_oferta

                                    FROM
                                        OFERTA o

                                        INNER JOIN OFERTA_CLIENTE oc ON
                                            o.id_oferta = oc.id_oferta

                                    WHERE
                                        oc.id_cliente = :id_cliente
                                        AND oc.status = 'A'
                                        AND oc.d_e_l_e_t_ = 0

                                ) offers

                                INNER JOIN OFERTA o ON
                                    offers.id_oferta = o.id_oferta

                                INNER JOIN OFERTA_BONIFICADO ob ON
                                    o.id_oferta = ob.id_oferta	

                                WHERE
                                    o.tipo_oferta = 2
                                    AND o.id_distribuidor IN :distribuidores
                                    AND o.status = 'A'
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND ob.status = 'A'
                        
                        ) offers_produto

                    INNER JOIN 
                    (

                        (
                            SELECT
                                av.id_oferta

                            FROM

                                (
                                    SELECT
                                        o.id_oferta
                                                    
                                    FROM
                                        OFERTA o
                                                        
                                        INNER JOIN OFERTA_PRODUTO op ON
                                            o.id_oferta = op.id_oferta
                                                            
                                        INNER JOIN PRODUTO p ON
                                            op.id_produto = p.id_produto

                                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                            p.id_produto = pd.id_produto
                                            AND o.id_distribuidor = pd.id_distribuidor
                                                            
                                    WHERE
                                        o.tipo_oferta = 2
                                        AND o.status = 'A'
                                        AND op.status = 'A'
                                        AND p.status = 'A'
                                        AND pd.status = 'A'
                                                
                                ) AS av
                                                
                                INNER JOIN (
                                                                
                                                SELECT
                                                    o.id_oferta
                                                    
                                                FROM
                                                    OFERTA o
                                                        
                                                    INNER JOIN OFERTA_BONIFICADO ob ON
                                                        o.id_oferta = ob.id_oferta
                                                            
                                                    INNER JOIN PRODUTO p ON
                                                        ob.id_produto = p.id_produto

                                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                                        p.id_produto = pd.id_produto
                                                        AND o.id_distribuidor = pd.id_distribuidor
                                                            
                                                WHERE
                                                    o.tipo_oferta = 2
                                                    AND o.status = 'A'
                                                    AND ob.status = 'A'
                                                    AND p.status = 'A'
                                                    AND pd.status = 'A'
                                                            
                                        ) AS bv ON
                                av.id_oferta = bv.id_oferta
                        )

                        UNION

                        (
                            SELECT
                                o.id_oferta
                                                    
                            FROM
                                OFERTA o
                                                        
                                INNER JOIN OFERTA_PRODUTO op ON
                                    o.id_oferta = op.id_oferta
                                                            
                                INNER JOIN PRODUTO p ON
                                    op.id_produto = p.id_produto

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto
                                    AND o.id_distribuidor = pd.id_distribuidor

                                INNER JOIN OFERTA_ESCALONADO_FAIXA oef ON
                                    o.id_oferta = oef.id_oferta
                                                            
                            WHERE
                                o.tipo_oferta = 3
                                AND oef.status = 'A'
                                AND oef.desconto > 0
                                AND o.status = 'A'
                                AND op.status = 'A'
                                AND p.status = 'A'
                                AND pd.status = 'A'
                        )

                    ) offers ON
                        offers_produto.id_oferta = offers.id_oferta

                    WHERE
                        1 = 1                 

                ) offers ON
                    p.id_produto = offers.id_produto

            WHERE
                1 = 1
                {string_id_produto}
                {string_id_marca}
                {string_tgs}
                {string_id_oferta}
                {string_ofertas} 
                {string_desconto}
                {string_estoque}
                AND pd.id_distribuidor IN :distribuidores
                AND ps.id_distribuidor = :id_distribuidor
                AND p.status = 'A'
                AND pd.status = 'A'
                AND m.status = 'A'
                AND ps.status = 'A'
                AND s.status = 'A'
                AND gs.status = 'A'
                AND g.status = 'A'
                AND t.status = 'A'

            ORDER BY
                m.id_distribuidor,
                m.desc_marca
        """

        data = dm.raw_sql_return(marca_query, params = params, bindparams = bindparams)

        if data:
            logger.info("Dados de marcas enviados")
            return {"status":200,
                    "resposta":{"tipo":"1","descricao":f"Dados enviados."}, "dados": data}, 200
        
        logger.info("Nao existem dados de marca para serem retornados.")
        return {"status":404,"resposta":{"tipo":"7","descricao":f"Sem dados para retornar."}}, 200