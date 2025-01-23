#=== Importações de módulos externos ===#
from datetime import datetime
import numpy as np
import os
from timeit import default_timer as timer

#=== Importações de módulos internos ===#
import functions.data_management as dm

server = os.environ.get("PSERVER_PS").lower()

if server == "production":
    route = str(os.environ.get("SERVER_GUARANY_PS")).lower()
    image = str(os.environ.get("IMAGE_SERVER_GUARANY_PS")).lower()
    
elif server == "development":
    route = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('IMAGE_PORT_PS')}"

else:
    route = f"http://localhost:{os.environ.get('MAIN_PORT_PS')}"
    image = f"http://localhost:{os.environ.get('IMAGE_PORT_PS')}"


# Geradores de estrutura padrão

def json_products_like_gen(prod_dist_list: list, id_cliente: int = None, bool_marketplace: bool = True,
                            image_replace: str = 'auto') -> list:
    """
    Gera a estrutura padrão de produtos atravêz do resultado da query

    Args:
        prod_cod_list (list): lista de objetos contendo os dados que devem ser normalizados
        id_cliente (int, optional): ID do cliente selecionado caso for do marketplace. Defaults to None.
        bool_marketplace (bool, optional): Se a resposta pertence ao jmanager ou marketplace. Defaults to True.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    Returns:
        list: Uma lista com a lista de produtos normalizadas
    """

    assert isinstance(prod_dist_list, (list, dict)), "product with invalid format"

    # Tratamento dos dados  
    if isinstance(prod_dist_list, dict):
        prod_dist_list = [prod_dist_list]

    produtos = []

    for produto in prod_dist_list:

        for saved_produto in produtos:
            if saved_produto["distribuidores"][0].get("id_produto") == produto.get("id_produto"):

                distribuidores = saved_produto["distribuidores"][0]

                # id_tipo/id_grupo/id_subgrupo                
                if int(produto.get("id_tipo")) not in distribuidores.get("id_tipo"):
                    distribuidores["id_tipo"].append(int(produto.get("id_tipo")))

                if int(produto.get("id_grupo")) not in distribuidores.get("id_grupo"):
                    distribuidores["id_grupo"].append(int(produto.get("id_grupo")))      

                if int(produto.get("id_subgrupo")) not in distribuidores.get("id_subgrupo"):
                    distribuidores["id_subgrupo"].append(int(produto.get("id_subgrupo"))) 

                # categorização
                for saved_tgs in distribuidores.get("categorizacao"):
                    if saved_tgs["id_tipo"] == produto["id_tipo"]:
                        
                        grupos = saved_tgs["grupo"]

                        for grupo in grupos:
                            if grupo["id_grupo"] == produto["id_grupo"]:

                                subgrupos = grupo["subgrupo"]

                                for subgrupo in subgrupos:
                                    if subgrupo["id_subgrupo"] == produto["id_subgrupo"]:
                                        break

                                else:
                                    subgrupos.append({
                                        "id_subgrupo": produto["id_subgrupo"],
                                        "descricao_subgrupo": produto["descricao_subgrupo"],
                                    })

                                break

                        else:
                            grupos.append({
                                "id_grupo": produto["id_grupo"],
                                "descricao_grupo": produto["descricao_grupo"],
                                "subgrupo": [{
                                    "id_subgrupo": produto["id_subgrupo"],
                                    "descricao_subgrupo": produto["descricao_subgrupo"],
                                }]
                            })
                            
                        break

                else:
                    distribuidores.get("categorizacao").append({
                        "id_tipo": produto["id_tipo"],
                        "descricao_tipo": produto["descricao_tipo"],
                        "grupo": [{
                            "id_grupo": produto["id_grupo"],
                            "descricao_grupo": produto["descricao_grupo"],
                            "subgrupo": [{
                                "id_subgrupo": produto["id_subgrupo"],
                                "descricao_subgrupo": produto["descricao_subgrupo"],
                                }]
                            }]
                    })

                # Ofertas
                #if (id_cliente and bool_marketplace) or not (bool_marketplace):
                check_ativador = produto.get("ativador")
                check_bonificado = produto.get("bonificado")
                check_escalonado = produto.get("escalonado")

                ofertas = distribuidores.get("ofertas")

                if check_ativador:
                    if ofertas:
                        if ofertas.get("bonificada"):
                            if ofertas.get("bonificada").get("produto_ativador"):
                                if produto.get("id_oferta") not in ofertas.get("bonificada").get("produto_ativador"):
                                    ofertas.get("bonificada").get("produto_ativador").append(produto.get("id_oferta"))
                            
                            else:
                                ofertas.get("bonificada").update({
                                    "produto_ativador": [produto.get("id_oferta")]
                                })
                        
                        else:
                            ofertas.update({
                            "bonificada": {
                                "produto_ativador": [produto.get("id_oferta")]
                            }
                        })

                    else:
                        ofertas = {
                            "bonificada": {
                                "produto_ativador": [produto.get("id_oferta")]
                            }
                        }


                if check_bonificado:
                    if ofertas:
                        if ofertas.get("bonificada"):
                            if ofertas.get("bonificada").get("produto_bonificado"):
                                if produto.get("id_oferta") not in ofertas.get("bonificada").get("produto_bonificado"):
                                    ofertas.get("bonificada").get("produto_bonificado").append(produto.get("id_oferta"))
                            
                            else:
                                ofertas.get("bonificada").update({
                                    "produto_bonificado": [produto.get("id_oferta")]
                                })
                        
                        else:
                            ofertas.update({
                            "bonificada": {
                                "produto_bonificado": [produto.get("id_oferta")]
                            }
                        })

                    else:
                        ofertas = {
                            "bonificada": {
                                "produto_bonificado": [produto.get("id_oferta")]
                            }
                        }

                
                if check_escalonado:
                    if ofertas:
                        if ofertas.get("escalonada"):
                            if ofertas.get("escalonada").get("produto_escalonado"):
                                if produto.get("id_oferta") not in ofertas.get("escalonada").get("produto_escalonado"):
                                    ofertas.get("escalonada").get("produto_escalonado").append(produto.get("id_oferta"))
                            
                            else:
                                ofertas.get("escalonada").update({
                                    "produto_escalonado": [produto.get("id_oferta")]
                                })
                        
                        else:
                            ofertas.update({
                            "escalonada": {
                                "produto_escalonado": [produto.get("id_oferta")]
                            }
                        })

                    else:
                        ofertas = {
                            "escalonada": {
                                "produto_escalonado": [produto.get("id_oferta")]
                            }
                        }

                distribuidores["ofertas"] = ofertas

                break

        else:

            imagens_list = []

            imagens = produto.get("tokens")
            if imagens:

                imagens = imagens.split(",")
                imagens_list = [
                    f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                    for imagem in imagens
                    if imagem
                ]

            preco_tabela = None
            estoque = None
            desconto = {}
            ofertas = {}

            if (bool_marketplace and id_cliente) or not bool_marketplace:
                estoque = produto.get("estoque")
                preco_tabela = produto.get("preco_tabela")

                if preco_tabela:
                    
                    desconto_perc = produto.get("desconto")
                    if desconto_perc:

                        if desconto_perc > 100:
                            desconto_perc = 100

                        elif desconto_perc < 0:
                            desconto_perc = 0

                        preco_desconto = preco_tabela * (1 - (desconto_perc/100))

                        desconto = {
                            "preco_desconto": round(preco_desconto, 3),
                            "desconto": desconto_perc,
                            "data_inicio": produto.get("data_inicio_desconto"),
                            "data_final": produto.get("data_final_desconto")
                        }

            if produto.get("id_oferta"):
                check_ativador = produto.get("ativador")
                check_bonificado = produto.get("bonificado")
                check_escalonado = produto.get("escalonado")

                bonificada = {}
                produto_ativador = []
                produto_bonificado = []

                escalonada = {}
                produto_escalonado = []

                if check_ativador == 1:
                    produto_ativador = [produto.get("id_oferta")]
                    bonificada.update({
                        'produto_ativador': produto_ativador,
                    })

                if check_bonificado == 1:
                    produto_bonificado = [produto.get("id_oferta")]
                    bonificada.update({
                        'produto_bonificado': produto_bonificado,
                    })

                if check_escalonado == 1:
                    produto_escalonado = [produto.get("id_oferta")]
                    escalonada.update({
                        'produto_escalonado': produto_escalonado,
                    })

                if bonificada:
                    ofertas.update({
                        "bonificada": bonificada,
                    })

                if escalonada:
                    ofertas.update({
                        "escalonada": escalonada,
                    })

            # Pegando as ofertas
            produto_hold = {
                "sku": produto.get("sku"),
                "descricao_produto": produto.get("descricao_produto"),
                "status": produto.get("status"),
                "id_marca": produto.get("id_marca"),
                "descricao_marca": produto.get("descricao_marca"),
                "dun14": produto.get("dun14"),
                "tipo_produto": produto.get("tipo_produto"),
                "variante": produto.get("variante"),
                "volumetria": produto.get("volumetria"),
                "unidade_embalagem": produto.get("unidade_embalagem"),
                "quantidade_embalagem": produto.get("quantidade_embalagem"),
                "unidade_venda": produto.get("unidade_venda"),
                "quant_unid_venda": produto.get("quant_unid_venda"),
                "imagem": imagens_list,
                "distribuidores": []
            }

            distribuidores = {
                "id_produto": produto.get("id_produto"),
                "id_distribuidor": produto.get("id_distribuidor"),
                "descricao_distribuidor": produto.get("descricao_distribuidor"),
                "agrupamento_variante": produto.get("agrupamento_variante"),
                "cod_prod_distr": produto.get("cod_prod_distr"),
                "cod_frag_distr": produto.get("cod_frag_distr"),
                "id_fornecedor": produto.get("id_fornecedor"),
                "desc_fornecedor": produto.get("desc_fornecedor"),
                "multiplo_venda": produto.get("multiplo_venda"),
                "ranking": produto.get("ranking"),
                "unidade_venda": produto.get("unidade_venda_distribuidor"),
                "quant_unid_venda": produto.get("quant_unid_venda_distribuidor"),
                "giro": produto.get("giro"),
                "agrup_familia": produto.get("agrup_familia"),
                "id_tipo": [
                    produto.get("id_tipo"),
                ],
                "id_grupo": [
                    produto.get("id_grupo"),
                ],
                "id_subgrupo": [
                    produto.get("id_subgrupo"),
                ],
                "categorizacao": [{
                    "id_tipo": produto.get("id_tipo"),
                    "descricao_tipo": produto.get("descricao_tipo"),
                    "grupo": [{
                        "id_grupo": produto.get("id_grupo"),
                        "descricao_grupo": produto.get("descricao_grupo"),
                        "subgrupo": [{
                            "id_subgrupo": produto.get("id_subgrupo"),
                            "descricao_subgrupo": produto.get("descricao_subgrupo")
                        }]
                    }]
                }],
                "status": produto.get("status_distribuidor"),
                "data_cadastro": produto.get("data_cadastro"),
                "detalhes": produto.get("detalhes"),
                "ofertas": ofertas,
            }

            if (bool_marketplace and id_cliente):
                distribuidores.update({
                    "avise_me": bool(produto.get("avise_me")),
                    "favorito": bool(produto.get("favorito")),
                })

            if (bool_marketplace and id_cliente) or not bool_marketplace:

                distribuidores.update({
                    "estoque": estoque,
                    "preco_tabela": preco_tabela,
                    "desconto": desconto,
                })

            produto_hold["distribuidores"].append(distribuidores)
            produtos.append(produto_hold)

    return produtos


def json_orcamento_like_gen(prod_dist_list: list, image_replace: str = 'auto') -> dict:
    """
    Retorna a resposta da query de orcamento normalizada

    Args:
        prod_dist_list (list): lista de dicionarios contendo os dados que devem ser normalizados
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    Returns:
        dict: retorna um dicionário com a lista de produtos de orcamento e as ofertas destes orcamento 
    """

    assert isinstance(prod_dist_list, (list, dict)), "product with invalid format"

    # Tratamento dos dados  
    if isinstance(prod_dist_list, dict):
        prod_dist_list = [prod_dist_list]

    orcamentos = []
    ofertas = set()

    for produto in prod_dist_list:

        id_produto = produto.get("id_produto")
        tipo = str(produto.get("tipo")).upper()
        id_oferta = produto.get("id_oferta")
        tipo_oferta = produto.get("tipo_oferta")

        if not id_oferta or tipo_oferta != 2:
            bonificado = None

        else:
            if tipo == 'B': bonificado = True
            else: bonificado = False

        if id_oferta and id_oferta not in ofertas:
            ofertas.add(id_oferta)

        for saved_orcamento in orcamentos:
            
            id_orcamento = produto.get("id_orcamento")

            if saved_orcamento.get("id_orcamento") == id_orcamento:

                for saved_produto in saved_orcamento.get("itens"):

                    if saved_produto.get("id_campanha"):
                        saved_oferta = saved_produto.get("id_campanha")

                    elif saved_produto.get("id_escalonado"):
                        saved_oferta = saved_produto.get("id_escalonado")

                    else:
                        saved_oferta = None
                    
                    if id_produto == saved_produto.get("id_produto") and id_oferta == saved_oferta and bonificado == saved_produto.get("bonificado"):

                        if int(produto.get("id_tipo")) not in saved_produto.get("id_tipo"):
                            saved_produto["id_tipo"].append(int(produto.get("id_tipo")))

                        if int(produto.get("id_grupo")) not in saved_produto.get("id_grupo"):
                            saved_produto["id_grupo"].append(int(produto.get("id_grupo")))      

                        if int(produto.get("id_subgrupo")) not in saved_produto.get("id_subgrupo"):
                            saved_produto["id_subgrupo"].append(int(produto.get("id_subgrupo"))) 

                        for saved_tgs in saved_produto.get("categorizacao"):
                            if saved_tgs.get("id_tipo") == produto.get("id_tipo"):
                                
                                grupos = saved_tgs.get("grupo")

                                for grupo in grupos:
                                    if grupo.get("id_grupo") == produto.get("id_grupo"):

                                        subgrupos = grupo.get("subgrupo")

                                        for subgrupo in subgrupos:
                                            if subgrupo.get("id_subgrupo") == produto.get("id_subgrupo"):
                                                break

                                        else:
                                            subgrupos.append({
                                                "id_subgrupo": produto.get("id_subgrupo"),
                                                "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                            })

                                        break

                                else:
                                    grupos.append({
                                        "id_grupo": produto.get("id_grupo"),
                                        "descricao_grupo": produto.get("descricao_grupo"),
                                        "subgrupo": [{
                                            "id_subgrupo": produto.get("id_subgrupo"),
                                            "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                        }]
                                    })
                                    
                                break


                        else:
                            saved_produto.get("categorizacao").append({
                                "id_tipo": produto.get("id_tipo"),
                                "descricao_tipo": produto.get("descricao_tipo"),
                                "grupo": [{
                                    "id_grupo": produto.get("id_grupo"),
                                    "descricao_grupo": produto.get("descricao_grupo"),
                                    "subgrupo": [{
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                        }]
                                    }]
                            })

                        break

                else:
                    if not id_oferta:
                        tipo_oferta = None

                    preco_tabela = produto.get("preco_tabela")
                    desconto = produto.get("desconto")

                    if desconto is None: desconto = 0
                    if desconto < 0: desconto = 0
                    if desconto > 100: desconto = 100

                    desconto_escalonado = produto.get("desconto_escalonado")

                    if desconto_escalonado is None: desconto_escalonado = 0
                    if desconto_escalonado < 0: desconto_escalonado = 0
                    if desconto_escalonado > 100: desconto_escalonado = 100

                    imagens_list = []

                    imagens = produto.get("tokens")
                    if imagens:

                        imagens = imagens.split(",")
                        imagens_list = [
                            f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                            for imagem in imagens
                            if imagem
                        ]

                    produto_orcamento = {
                        "id_produto": id_produto,
                        "cod_prod_distr": produto.get("cod_prod_distr"),
                        "id_distribuidor": produto.get("id_distribuidor"),
                        "preco_tabela": round(preco_tabela, 2),
                        "preco_financeiro": None,
                        "preco_desconto": None,
                        "preco_aplicado": None,
                        "quantidade": produto.get("quantidade"),
                        "estoque": produto.get("estoque"),
                        "desconto": round(desconto, 2),
                        "desconto_tipo": 1 if desconto else 0,
                        "imagem": imagens_list,
                        "categorizacao": [
                            {
                                "id_tipo": produto.get("id_tipo"),
                                "descricao_tipo": produto.get("descricao_tipo"),
                                "grupo": [
                                    {
                                        "id_grupo": produto.get("id_grupo"),
                                        "descricao_grupo": produto.get("descricao_grupo"),
                                        "subgrupo": [
                                            {
                                                "id_subgrupo": produto.get("id_subgrupo"),
                                                "descricao_subgrupo": produto.get("descricao_subgrupo")
                                            }
                                        ]
                                    }
                                ]
                            }
                        ],
                        "id_tipo": [
                            produto.get("id_tipo"),
                        ],
                        "id_grupo": [
                            produto.get("id_grupo"),
                        ],
                        "id_subgrupo": [
                            produto.get("id_subgrupo"),
                        ],
                        "sku": produto.get("sku"),
                        "descricao_produto": produto.get("descricao_produto"),
                        "id_marca":  produto.get("id_marca"),
                        "descricao_marca": produto.get("descricao_marca"),
                        "status": "A",
                        "unidade_embalagem": produto.get("unidade_embalagem"),
                        "quantidade_embalagem": produto.get("quantidade_embalagem"),
                        "unidade_venda": produto.get("unidade_venda"),
                        "quant_unid_venda": produto.get("quant_unid_venda"),
                        "multiplo_venda": produto.get("multiplo_venda"),
                    }

                    if id_oferta:

                        if tipo_oferta == 2:
                            produto_orcamento.update({
                                "id_campanha": id_oferta,
                                "bonificado": bonificado,
                                "tipo_oferta": 2
                            })

                        else:
                            produto_orcamento.update({
                                "id_escalonado": id_oferta,
                                "desconto_escalonado": round(desconto_escalonado, 2),
                                "tipo_oferta": 3
                            })

                            if desconto_escalonado > desconto:
                                produto_orcamento["desconto_tipo"] = 2

                    saved_orcamento["itens"].append(produto_orcamento)

                break            

        else:
            id_oferta = produto.get("id_oferta")
            tipo_oferta = produto.get("tipo_oferta")

            if not id_oferta:
                tipo_oferta = None

            preco_tabela = produto.get("preco_tabela")
            desconto = produto.get("desconto")

            if desconto is None: desconto = 0
            if desconto < 0: desconto = 0
            if desconto > 100: desconto = 100

            desconto_escalonado = produto.get("desconto_escalonado")

            if desconto_escalonado is None: desconto_escalonado = 0
            if desconto_escalonado < 0: desconto_escalonado = 0
            if desconto_escalonado > 100: desconto_escalonado = 100

            imagens_list = []

            imagens = produto.get("tokens")
            if imagens:

                imagens = imagens.split(",")
                imagens_list = [
                    f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                    for imagem in imagens
                    if imagem
                ]

            produto_orcamento = {
                "id_produto": id_produto,
                "cod_prod_distr": produto.get("cod_prod_distr"),
                "id_distribuidor": produto.get("id_distribuidor"),
                "preco_tabela": round(preco_tabela, 2),
                "preco_financeiro": None,
                "preco_desconto": None,
                "preco_aplicado": None,
                "quantidade": produto.get("quantidade"),
                "estoque": produto.get("estoque"),
                "desconto": round(desconto, 2),
                "desconto_tipo": 1 if desconto else 0,
                "imagem": imagens_list,
                "categorizacao": [
                    {
                        "id_tipo": produto.get("id_tipo"),
                        "descricao_tipo": produto.get("descricao_tipo"),
                        "grupo": [
                            {
                                "id_grupo": produto.get("id_grupo"),
                                "descricao_grupo": produto.get("descricao_grupo"),
                                "subgrupo": [
                                    {
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo")
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "id_tipo": [
                    produto.get("id_tipo"),
                ],
                "id_grupo": [
                    produto.get("id_grupo"),
                ],
                "id_subgrupo": [
                    produto.get("id_subgrupo"),
                ],
                "sku": produto.get("sku"),
                "descricao_produto": produto.get("descricao_produto"),
                "id_marca":  produto.get("id_marca"),
                "descricao_marca": produto.get("descricao_marca"),
                "status": "A",
                "unidade_embalagem": produto.get("unidade_embalagem"),
                "quantidade_embalagem": produto.get("quantidade_embalagem"),
                "unidade_venda": produto.get("unidade_venda"),
                "quant_unid_venda": produto.get("quant_unid_venda"),
                "multiplo_venda": produto.get("multiplo_venda"),
            }

            if id_oferta:

                if tipo_oferta == 2:
                    produto_orcamento.update({
                        "id_campanha": id_oferta,
                        "bonificado": bonificado,
                        "tipo_oferta": 2
                    })

                else:
                    produto_orcamento.update({
                        "id_escalonado": id_oferta,
                        "desconto_escalonado": round(desconto_escalonado, 2),
                        "tipo_oferta": 3
                    })

                    if desconto_escalonado > desconto:
                        produto_orcamento["desconto_tipo"] = 2
                        
            orcamento_cabecalho = {
                "id_orcamento": produto.get("id_orcamento"),
                "id_distribuidor": produto.get("id_distribuidor"),
                "itens": [produto_orcamento],
            }

            orcamentos.append(orcamento_cabecalho)

    return {"orcamentos": orcamentos, "ofertas": ofertas}


def json_pedido_produto_like_gen(prod_dist_list: list, image_replace: str = 'auto') -> dict:
    """
    Retorna os produtos do pedido normalizados

    Args:
        prod_dist_list (list): lista de dicionarios contendo os dados que devem ser normalizados
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    Returns:
        dict: retorna um dicionário com a lista de produtos de orcamento e as ofertas destes orcamento 
    """

    assert isinstance(prod_dist_list, (list, dict)), "product with invalid format"

    # Tratamento dos dados  
    if isinstance(prod_dist_list, dict):
        prod_dist_list = [prod_dist_list]

    produtos = []

    for produto in prod_dist_list:

        id_produto = produto.get("id_produto")
        tipo = str(produto.get("tipo_venda")).upper()
        id_oferta = produto.get("id_oferta")
        tipo_oferta = produto.get("tipo_oferta") if id_oferta else None

        if not id_oferta or tipo_oferta != 2:
            bonificado = None

        else:
            if tipo == 'B': bonificado = True
            else: bonificado = False

        for saved_produto in produtos:

            if saved_produto.get("id_campanha"):
                saved_oferta = saved_produto.get("id_campanha")

            elif saved_produto.get("id_escalonado"):
                saved_oferta = saved_produto.get("id_escalonado")

            else:
                saved_oferta = None
                
            if id_produto == saved_produto.get("id_produto") and id_oferta == saved_oferta and bonificado == saved_produto.get("bonificado"):

                if int(produto.get("id_tipo")) not in saved_produto.get("id_tipo"):
                    saved_produto["id_tipo"].append(int(produto.get("id_tipo")))

                if int(produto.get("id_grupo")) not in saved_produto.get("id_grupo"):
                    saved_produto["id_grupo"].append(int(produto.get("id_grupo")))      

                if int(produto.get("id_subgrupo")) not in saved_produto.get("id_subgrupo"):
                    saved_produto["id_subgrupo"].append(int(produto.get("id_subgrupo"))) 

                for saved_tgs in saved_produto.get("categorizacao"):
                    if saved_tgs.get("id_tipo") == produto.get("id_tipo"):
                        
                        grupos = saved_tgs.get("grupo")

                        for grupo in grupos:
                            if grupo.get("id_grupo") == produto.get("id_grupo"):

                                subgrupos = grupo.get("subgrupo")

                                for subgrupo in subgrupos:
                                    if subgrupo.get("id_subgrupo") == produto.get("id_subgrupo"):
                                        break

                                else:
                                    subgrupos.append({
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                    })

                                break

                        else:
                            grupos.append({
                                "id_grupo": produto.get("id_grupo"),
                                "descricao_grupo": produto.get("descricao_grupo"),
                                "subgrupo": [{
                                    "id_subgrupo": produto.get("id_subgrupo"),
                                    "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                }]
                            })
                            
                        break


                else:
                    saved_produto.get("categorizacao").append({
                        "id_tipo": produto.get("id_tipo"),
                        "descricao_tipo": produto.get("descricao_tipo"),
                        "grupo": [{
                            "id_grupo": produto.get("id_grupo"),
                            "descricao_grupo": produto.get("descricao_grupo"),
                            "subgrupo": [{
                                "id_subgrupo": produto.get("id_subgrupo"),
                                "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                }]
                            }]
                    })

                break

        else:

            preco_tabela = produto.get("preco_tabela")
            preco_aplicado = produto.get("preco_aplicado")
            desconto = produto.get("desconto")
            desconto_tipo = produto.get("desconto_tipo") if desconto else None

            if desconto is None: desconto = 0
            if desconto < 0: desconto = 0
            if desconto > 100: desconto = 100

            imagens_list = []

            imagens = produto.get("tokens")
            if imagens:

                imagens = imagens.split(",")
                imagens_list = [
                    f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                    for imagem in imagens
                ]

            produto_orcamento = {
                "id_produto": id_produto,
                "id_distribuidor": produto.get("id_distribuidor"),
                "preco_tabela": round(preco_tabela, 2),
                "preco_aplicado": round(preco_aplicado, 2),
                "quantidade": produto.get("quantidade"),
                "estoque": produto.get("estoque"),
                "desconto": round(desconto, 2),
                "desconto_tipo": desconto_tipo,
                "imagem": imagens_list,
                "categorizacao": [
                    {
                        "id_tipo": produto.get("id_tipo"),
                        "descricao_tipo": produto.get("descricao_tipo"),
                        "grupo": [
                            {
                                "id_grupo": produto.get("id_grupo"),
                                "descricao_grupo": produto.get("descricao_grupo"),
                                "subgrupo": [
                                    {
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo")
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "id_tipo": [
                    produto.get("id_tipo"),
                ],
                "id_grupo": [
                    produto.get("id_grupo"),
                ],
                "id_subgrupo": [
                    produto.get("id_subgrupo"),
                ],
                "sku": produto.get("sku"),
                "descricao_produto": produto.get("descricao_produto"),
                "id_marca":  produto.get("id_marca"),
                "descricao_marca": produto.get("descricao_marca"),
                "status": "A",
                "unidade_embalagem": produto.get("unidade_embalagem"),
                "quantidade_embalagem": produto.get("quantidade_embalagem"),
                "unidade_venda": produto.get("unidade_venda"),
                "quant_unid_venda": produto.get("quant_unid_venda"),
                "multiplo_venda": produto.get("multiplo_venda"),
            }

            if id_oferta:

                if tipo_oferta == 2:
                    produto_orcamento.update({
                        "id_campanha": id_oferta,
                        "bonificado": bonificado,
                        "tipo_oferta": 2
                    })

                else:
                    produto_orcamento.update({
                        "id_escalonado": id_oferta,
                        "tipo_oferta": 3
                    })

            produtos.append(produto_orcamento)

    return produtos


def json_pedido_produto_jsl_like_gen(prod_dist_list: list, percentual: float, 
                                        image_replace: str = 'auto') -> dict:
    """
    Retorna os produtos do pedido normalizados após o pedido ter sido processado pelo distribuidor e
        algumas informações terem se perdido. Tenta recriar ao maximo o mesmo padrão de antes.

    Args:
        prod_dist_list (list): lista de dicionarios contendo os dados que devem ser normalizados
        percentual (float): Percentual da condição de pagamento.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    Returns:
        dict: retorna um dicionário com a lista de produtos de orcamento e as ofertas destes orcamento 
    """

    assert isinstance(prod_dist_list, (list, dict)), "product with invalid format"
    assert isinstance(percentual, float), "Chosen percent is not float"

    # Tratamento dos dados  
    if isinstance(prod_dist_list, dict):
        prod_dist_list = [prod_dist_list]

    produtos = []

    for produto in prod_dist_list:

        id_produto = produto.get("id_produto")
        tipo = str(produto.get("tipo_venda")).upper()
        bonificado = None

        if tipo == 'B': bonificado = True

        for saved_produto in produtos:
                
            if id_produto == saved_produto.get("id_produto") and bonificado == saved_produto.get("bonificado"):

                if int(produto.get("id_tipo")) not in saved_produto.get("id_tipo"):
                    saved_produto["id_tipo"].append(int(produto.get("id_tipo")))

                if int(produto.get("id_grupo")) not in saved_produto.get("id_grupo"):
                    saved_produto["id_grupo"].append(int(produto.get("id_grupo")))      

                if int(produto.get("id_subgrupo")) not in saved_produto.get("id_subgrupo"):
                    saved_produto["id_subgrupo"].append(int(produto.get("id_subgrupo"))) 

                for saved_tgs in saved_produto.get("categorizacao"):
                    if saved_tgs.get("id_tipo") == produto.get("id_tipo"):
                        
                        grupos = saved_tgs.get("grupo")

                        for grupo in grupos:
                            if grupo.get("id_grupo") == produto.get("id_grupo"):

                                subgrupos = grupo.get("subgrupo")

                                for subgrupo in subgrupos:
                                    if subgrupo.get("id_subgrupo") == produto.get("id_subgrupo"):
                                        break

                                else:
                                    subgrupos.append({
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                    })

                                break

                        else:
                            grupos.append({
                                "id_grupo": produto.get("id_grupo"),
                                "descricao_grupo": produto.get("descricao_grupo"),
                                "subgrupo": [{
                                    "id_subgrupo": produto.get("id_subgrupo"),
                                    "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                }]
                            })
                            
                        break


                else:
                    saved_produto.get("categorizacao").append({
                        "id_tipo": produto.get("id_tipo"),
                        "descricao_tipo": produto.get("descricao_tipo"),
                        "grupo": [{
                            "id_grupo": produto.get("id_grupo"),
                            "descricao_grupo": produto.get("descricao_grupo"),
                            "subgrupo": [{
                                "id_subgrupo": produto.get("id_subgrupo"),
                                "descricao_subgrupo": produto.get("descricao_subgrupo"),
                                }]
                            }]
                    })

                break

        else:

            preco_tabela = produto.get("preco_tabela")
            preco_aplicado = produto.get("preco_aplicado")
            
            preco_financeiro = np.round(preco_tabela * (1 + (percentual / 100)), 2)

            desconto = 0

            if tipo != 'B':
                desconto = np.round(100 * (1 - (preco_aplicado / preco_financeiro)), 2)

            if desconto < 0: desconto = 0
            if desconto > 100: desconto = 100

            imagens_list = []

            imagens = produto.get("tokens")
            if imagens:

                imagens = imagens.split(",")
                imagens_list = [
                    f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                    for imagem in imagens
                ]

            produto_orcamento = {
                "id_produto": id_produto,
                "id_distribuidor": produto.get("id_distribuidor"),
                "preco_tabela": round(preco_tabela, 2),
                "preco_aplicado": round(preco_aplicado, 2),
                "quantidade": produto.get("quantidade"),
                "estoque": produto.get("estoque"),
                "desconto": round(desconto, 2),
                "imagem": imagens_list,
                "categorizacao": [
                    {
                        "id_tipo": produto.get("id_tipo"),
                        "descricao_tipo": produto.get("descricao_tipo"),
                        "grupo": [
                            {
                                "id_grupo": produto.get("id_grupo"),
                                "descricao_grupo": produto.get("descricao_grupo"),
                                "subgrupo": [
                                    {
                                        "id_subgrupo": produto.get("id_subgrupo"),
                                        "descricao_subgrupo": produto.get("descricao_subgrupo")
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "id_tipo": [
                    produto.get("id_tipo"),
                ],
                "id_grupo": [
                    produto.get("id_grupo"),
                ],
                "id_subgrupo": [
                    produto.get("id_subgrupo"),
                ],
                "sku": produto.get("sku"),
                "descricao_produto": produto.get("descricao_produto"),
                "id_marca":  produto.get("id_marca"),
                "descricao_marca": produto.get("descricao_marca"),
                "status": "A",
                "unidade_embalagem": produto.get("unidade_embalagem"),
                "quantidade_embalagem": produto.get("quantidade_embalagem"),
                "unidade_venda": produto.get("unidade_venda"),
                "quant_unid_venda": produto.get("quant_unid_venda"),
                "multiplo_venda": produto.get("multiplo_venda"),
            }

            if bonificado:
                produto_orcamento["bonificado"] = True

            produtos.append(produto_orcamento)

    return produtos


def json_ofertas_like_gen(prod_dist_list: list, id_cliente: int, image_replace: str = 'auto') -> list:
    """
    Retorna a resposta da query de ofertas normalizada

    Args:
        prod_dist_list (list): lista de dicionarios contendo os dados que devem ser normalizados.
        id_cliente (int): ID do cliente selecionado.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    Returns:
        list: retorna a lista de ofertas normalizadas
    """

    assert isinstance(prod_dist_list, (list, dict)), "product with invalid format"

    # Tratamento dos dados  
    if isinstance(prod_dist_list, dict):
        prod_dist_list = [prod_dist_list]

    oferta_hold = []

    for oferta in prod_dist_list:

        id_oferta = oferta.get('id_oferta')
        tipo_oferta = oferta.get("tipo_oferta")

        if tipo_oferta == 2:

            for saved_oferta in oferta_hold:
                if not saved_oferta.get("id_campanha"):
                    continue

                if saved_oferta["id_campanha"] == id_oferta:
                    
                    id_produto = oferta.get("id_produto")

                    # Caso o produto já esteja salvo
                    saved_product_bool = False
                    
                    if oferta.get("ativador"):
                        for saved_produto in saved_oferta["produto_ativador"]:
                            if saved_produto.get("id_produto") == id_produto:

                                # id_tipo/id_grupo/id_subgrupo                
                                if int(oferta.get("id_tipo")) not in saved_produto.get("id_tipo"):
                                    saved_produto["id_tipo"].append(int(oferta.get("id_tipo")))

                                if int(oferta.get("id_grupo")) not in saved_produto.get("id_grupo"):
                                    saved_produto["id_grupo"].append(int(oferta.get("id_grupo")))      

                                if int(oferta.get("id_subgrupo")) not in saved_produto.get("id_subgrupo"):
                                    saved_produto["id_subgrupo"].append(int(oferta.get("id_subgrupo"))) 

                                # categorização
                                for saved_tgs in saved_produto.get("categorizacao"):
                                    if saved_tgs["id_tipo"] == oferta["id_tipo"]:
                                        
                                        grupos = saved_tgs["grupo"]

                                        for grupo in grupos:
                                            if grupo["id_grupo"] == oferta["id_grupo"]:

                                                subgrupos = grupo["subgrupo"]

                                                for subgrupo in subgrupos:
                                                    if subgrupo["id_subgrupo"] == oferta["id_subgrupo"]:
                                                        break

                                                else:
                                                    subgrupos.append({
                                                        "id_subgrupo": oferta["id_subgrupo"],
                                                        "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                    })

                                                break

                                        else:
                                            grupos.append({
                                                "id_grupo": oferta["id_grupo"],
                                                "descricao_grupo": oferta["descricao_grupo"],
                                                "subgrupo": [{
                                                    "id_subgrupo": oferta["id_subgrupo"],
                                                    "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                }]
                                            })
                                            
                                        break

                                else:
                                    saved_produto.get("categorizacao").append({
                                        "id_tipo": oferta["id_tipo"],
                                        "descricao_tipo": oferta["descricao_tipo"],
                                        "grupo": [{
                                            "id_grupo": oferta["id_grupo"],
                                            "descricao_grupo": oferta["descricao_grupo"],
                                            "subgrupo": [{
                                                "id_subgrupo": oferta["id_subgrupo"],
                                                "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                }]
                                            }]
                                    })

                                saved_product_bool = True
                                break

                    if oferta.get("bonificado"):
                        for saved_produto in saved_oferta["produto_bonificado"]:
                            if saved_produto.get("id_produto") == id_produto:

                                # id_tipo/id_grupo/id_subgrupo                
                                if int(oferta.get("id_tipo")) not in saved_produto.get("id_tipo"):
                                    saved_produto["id_tipo"].append(int(oferta.get("id_tipo")))

                                if int(oferta.get("id_grupo")) not in saved_produto.get("id_grupo"):
                                    saved_produto["id_grupo"].append(int(oferta.get("id_grupo")))      

                                if int(oferta.get("id_subgrupo")) not in saved_produto.get("id_subgrupo"):
                                    saved_produto["id_subgrupo"].append(int(oferta.get("id_subgrupo"))) 

                                # categorização
                                for saved_tgs in saved_produto.get("categorizacao"):
                                    if saved_tgs["id_tipo"] == oferta["id_tipo"]:
                                        
                                        grupos = saved_tgs["grupo"]

                                        for grupo in grupos:
                                            if grupo["id_grupo"] == oferta["id_grupo"]:

                                                subgrupos = grupo["subgrupo"]

                                                for subgrupo in subgrupos:
                                                    if subgrupo["id_subgrupo"] == oferta["id_subgrupo"]:
                                                        break

                                                else:
                                                    subgrupos.append({
                                                        "id_subgrupo": oferta["id_subgrupo"],
                                                        "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                    })

                                                break

                                        else:
                                            grupos.append({
                                                "id_grupo": oferta["id_grupo"],
                                                "descricao_grupo": oferta["descricao_grupo"],
                                                "subgrupo": [{
                                                    "id_subgrupo": oferta["id_subgrupo"],
                                                    "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                }]
                                            })
                                            
                                        break

                                else:
                                    saved_produto.get("categorizacao").append({
                                        "id_tipo": oferta["id_tipo"],
                                        "descricao_tipo": oferta["descricao_tipo"],
                                        "grupo": [{
                                            "id_grupo": oferta["id_grupo"],
                                            "descricao_grupo": oferta["descricao_grupo"],
                                            "subgrupo": [{
                                                "id_subgrupo": oferta["id_subgrupo"],
                                                "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                }]
                                            }]
                                    })

                                saved_product_bool = True
                                break

                    if saved_product_bool:
                        break

                    # Caso não...
                    desconto = {}

                    if id_cliente and oferta.get("desconto") and oferta.get("preco_tabela"):
                
                        desconto_perc = oferta.get("desconto")
                        if desconto_perc:

                            if desconto_perc > 100:
                                desconto_perc = 100

                            elif desconto_perc < 0:
                                desconto_perc = 0

                            preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                            desconto = {
                                "preco_desconto": round(preco_desconto, 3),
                                "desconto": desconto_perc,
                                "data_inicio": oferta.get("data_inicio_desconto"),
                                "data_final": oferta.get("data_final_desconto")
                            }

                    imagens_list = []

                    imagens = oferta.get("tokens")
                    if imagens:

                        imagens = imagens.split(",")
                        imagens_list = [
                            f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                            for imagem in imagens
                            if imagem
                        ]

                    produto_campanha = {
                        "id_campanha": oferta.get("id_oferta"),
                        "id_distribuidor": oferta.get("id_distribuidor"),
                        "id_produto": oferta.get("id_produto"),
                        "sku": oferta.get("sku"),
                        "id_marca": oferta.get("id_marca"),
                        "descricao_marca": oferta.get("descricao_marca"),
                        "descricao_produto": oferta.get("descricao_produto"),
                        "imagem": imagens_list,
                        "id_tipo": [
                            oferta.get("id_tipo"),
                        ],
                        "id_grupo": [
                            oferta.get("id_grupo"),
                        ],
                        "id_subgrupo": [
                            oferta.get("id_subgrupo"),
                        ],
                        "categorizacao": [{
                            "id_tipo": oferta.get("id_tipo"),
                            "descricao_tipo": oferta.get("descricao_tipo"),
                            "grupo": [{
                                "id_grupo": oferta.get("id_grupo"),
                                "descricao_grupo": oferta.get("descricao_grupo"),
                                "subgrupo": [{
                                    "id_subgrupo": oferta.get("id_subgrupo"),
                                    "descricao_subgrupo": oferta.get("descricao_subgrupo")
                                }]
                            }]
                        }],
                        "status": oferta.get("status_produto"),
                        "quantidade": oferta.get("quantidade"),
                        "quantidade_minima": oferta.get("quantidade_minima"),
                        "valor_minimo": oferta.get("valor_minimo"),
                        "quantidade_min_ativacao": oferta.get("quantidade_min_ativacao"),
                        "valor_min_ativacao": oferta.get("valor_min_ativacao"),
                        "unidade_embalagem": oferta.get("unidade_embalagem"),
                        "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                        "unidade_venda": oferta.get("unidade_venda"),
                        "quant_unid_venda": oferta.get("quant_unid_venda"),
                        "multiplo_venda": oferta.get("multiplo_venda"),
                    }

                    if id_cliente:
                        produto_campanha.update({
                            "preco_tabela": oferta.get("preco_tabela"),
                            "estoque": oferta.get("estoque"),
                            "desconto": desconto,
                        })

                    if oferta.get("ativador"):       
                        saved_oferta["quantidade_produto"] += 1

                        if not saved_oferta.get("unidade_venda"):
                            saved_oferta["unidade_venda"] = oferta.get("unidade_venda")
                            
                        saved_oferta["produto_ativador"].append(produto_campanha)

                    if oferta.get("bonificado"):
                        produto_campanha_bonificado = produto_campanha.copy()
                        produto_campanha_bonificado["quantidade_bonificada"] = oferta.get("quantidade_bonificada")
                        saved_oferta["produto_bonificado"].append(produto_campanha_bonificado)

                    break

            else:
                campanha_cabecalho = {
                    "id_campanha": id_oferta,
                    "id_distribuidor": oferta.get("id_distribuidor"),
                    "status": oferta.get("status_oferta"),
                    "tipo_campanha": oferta.get("tipo_campanha"),
                    "descricao": oferta.get("descricao_oferta"),
                    "data_inicio": oferta.get("data_inicio"),
                    "data_final": oferta.get("data_final"),
                    "destaque": oferta.get("destaque"),
                    "quantidade_limite": oferta.get("quantidade_limite"),
                    "quantidade_pontos": oferta.get("quantidade_pontos"),
                    "quantidade_produto": oferta.get("quantidade_produto"),
                    "quantidade_ativar": oferta.get("quantidade_ativar"),
                    "maxima_ativacao": oferta.get("maxima_ativacao"),
                    "quantidade_bonificado": oferta.get("quantidade_bonificado"),
                    "valor_ativar": oferta.get("valor_ativar"),
                    "automatica": False,
                    "unidade_venda": None,
                    "unificada": oferta.get("unificada"),
                    "produto_ativador": [],
                    "produto_bonificado": []
                }

                desconto = {}

                if id_cliente and oferta.get("desconto") and oferta.get("preco_tabela"):
            
                    desconto_perc = oferta.get("desconto")
                    if desconto_perc:

                        if desconto_perc > 100:
                            desconto_perc = 100

                        elif desconto_perc < 0:
                            desconto_perc = 0

                        preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                        desconto = {
                            "preco_desconto": round(preco_desconto, 3),
                            "desconto": desconto_perc,
                            "data_inicio": oferta.get("data_inicio_desconto"),
                            "data_final": oferta.get("data_final_desconto")
                        }

                imagens_list = []

                imagens = oferta.get("tokens")
                if imagens:

                    imagens = imagens.split(",")
                    imagens_list = [
                        f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                        for imagem in imagens
                        if imagem
                    ]

                produto_campanha = {
                    "id_campanha": oferta.get("id_oferta"),
                    "id_distribuidor": oferta.get("id_distribuidor"),
                    "id_produto": oferta.get("id_produto"),
                    "sku": oferta.get("sku"),
                    "id_marca": oferta.get("id_marca"),
                    "descricao_marca": oferta.get("descricao_marca"),
                    "descricao_produto": oferta.get("descricao_produto"),
                    "imagem": imagens_list,
                    "id_tipo": [
                        oferta.get("id_tipo"),
                    ],
                    "id_grupo": [
                        oferta.get("id_grupo"),
                    ],
                    "id_subgrupo": [
                        oferta.get("id_subgrupo"),
                    ],
                    "categorizacao": [{
                        "id_tipo": oferta.get("id_tipo"),
                        "descricao_tipo": oferta.get("descricao_tipo"),
                        "grupo": [{
                            "id_grupo": oferta.get("id_grupo"),
                            "descricao_grupo": oferta.get("descricao_grupo"),
                            "subgrupo": [{
                                "id_subgrupo": oferta.get("id_subgrupo"),
                                "descricao_subgrupo": oferta.get("descricao_subgrupo")
                            }]
                        }]
                    }],
                    "status": oferta.get("status_produto"),
                    "quantidade": oferta.get("quantidade"),
                    "quantidade_minima": oferta.get("quantidade_minima"),
                    "valor_minimo": oferta.get("valor_minimo"),
                    "quantidade_min_ativacao": oferta.get("quantidade_min_ativacao"),
                    "valor_min_ativacao": oferta.get("valor_min_ativacao"),
                    "unidade_embalagem": oferta.get("unidade_embalagem"),
                    "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                    "unidade_venda": oferta.get("unidade_venda"),
                    "quant_unid_venda": oferta.get("quant_unid_venda"),
                    "multiplo_venda": oferta.get("multiplo_venda"),
                }

                if id_cliente:
                    produto_campanha.update({
                        "preco_tabela": oferta.get("preco_tabela"),
                        "estoque": oferta.get("estoque"),
                        "desconto": desconto,
                    })

                if oferta.get("ativador"):       
                    campanha_cabecalho["quantidade_produto"] += 1
                    campanha_cabecalho["unidade_venda"] = oferta.get("unidade_venda")
                    campanha_cabecalho["produto_ativador"].append(produto_campanha)

                if oferta.get("bonificado"):
                    produto_campanha_bonificado = produto_campanha.copy()
                    produto_campanha_bonificado["quantidade_bonificada"] = oferta.get("quantidade_bonificada")
                    campanha_cabecalho["produto_bonificado"].append(produto_campanha_bonificado)

                oferta_hold.append(campanha_cabecalho)

        elif tipo_oferta == 3:
            
            for saved_oferta in oferta_hold:
                if not saved_oferta.get("id_escalonado"):
                    continue

                if saved_oferta["id_escalonado"] == id_oferta:
                    
                    for faixa in saved_oferta["faixa_escalonado"]:
                        if faixa["sequencia"] == oferta.get('sequencia'):
                            break
                    
                    else:
                        saved_oferta["faixa_escalonado"].append({
                            "id_escalonado": id_oferta,
                            "id_distribuidor": oferta.get("id_distribuidor"),
                            "sequencia": oferta.get("sequencia"),
                            "faixa": oferta.get("faixa"),
                            "desconto": oferta.get("desconto_faixa"),
                            "unidade_venda": saved_oferta["faixa_escalonado"][0].get("unidade_venda"),
                        })

                    # Caso o produto já esteja salvo
                    id_produto = oferta.get("id_produto")

                    for saved_produto in saved_oferta["produto_escalonado"]:
                        
                        if saved_produto.get("id_produto") == id_produto:

                            # id_tipo/id_grupo/id_subgrupo                
                            if int(oferta.get("id_tipo")) not in saved_produto.get("id_tipo"):
                                saved_produto["id_tipo"].append(int(oferta.get("id_tipo")))

                            if int(oferta.get("id_grupo")) not in saved_produto.get("id_grupo"):
                                saved_produto["id_grupo"].append(int(oferta.get("id_grupo")))      

                            if int(oferta.get("id_subgrupo")) not in saved_produto.get("id_subgrupo"):
                                saved_produto["id_subgrupo"].append(int(oferta.get("id_subgrupo"))) 

                            # categorização
                            for saved_tgs in saved_produto.get("categorizacao"):
                                if saved_tgs["id_tipo"] == oferta["id_tipo"]:
                                    
                                    grupos = saved_tgs["grupo"]

                                    for grupo in grupos:
                                        if grupo["id_grupo"] == oferta["id_grupo"]:

                                            subgrupos = grupo["subgrupo"]

                                            for subgrupo in subgrupos:
                                                if subgrupo["id_subgrupo"] == oferta["id_subgrupo"]:
                                                    break

                                            else:
                                                subgrupos.append({
                                                    "id_subgrupo": oferta["id_subgrupo"],
                                                    "descricao_subgrupo": oferta["descricao_subgrupo"],
                                                })

                                            break

                                    else:
                                        grupos.append({
                                            "id_grupo": oferta["id_grupo"],
                                            "descricao_grupo": oferta["descricao_grupo"],
                                            "subgrupo": [{
                                                "id_subgrupo": oferta["id_subgrupo"],
                                                "descricao_subgrupo": oferta["descricao_subgrupo"],
                                            }]
                                        })
                                        
                                    break

                            else:
                                saved_produto.get("categorizacao").append({
                                    "id_tipo": oferta["id_tipo"],
                                    "descricao_tipo": oferta["descricao_tipo"],
                                    "grupo": [{
                                        "id_grupo": oferta["id_grupo"],
                                        "descricao_grupo": oferta["descricao_grupo"],
                                        "subgrupo": [{
                                            "id_subgrupo": oferta["id_subgrupo"],
                                            "descricao_subgrupo": oferta["descricao_subgrupo"],
                                            }]
                                        }]
                                })
                            
                            break

                    else:

                        imagens_list = []

                        imagens = oferta.get("tokens")
                        if imagens:

                            imagens = imagens.split(",")
                            imagens_list = [
                                f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                                for imagem in imagens
                                if imagem
                            ]

                        desconto = {}

                        if id_cliente and oferta.get("desconto") and oferta.get("preco_tabela"):
                    
                            desconto_perc = oferta.get("desconto")
                            if desconto_perc:

                                if desconto_perc > 100:
                                    desconto_perc = 100

                                elif desconto_perc < 0:
                                    desconto_perc = 0

                                preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                                desconto = {
                                    "preco_desconto": round(preco_desconto, 3),
                                    "desconto": desconto_perc,
                                    "data_inicio": oferta.get("data_inicio_desconto"),
                                    "data_final": oferta.get("data_final_desconto")
                                }

                        produto_escalonado = {
                            "id_escalonado": id_oferta,
                            "id_distribuidor": oferta.get("id_distribuidor"),
                            "id_produto": oferta.get("id_produto"),
                            "sku": oferta.get("sku"),
                            "id_marca": oferta.get("id_marca"),
                            "descricao_marca": oferta.get("descricao_marca"),
                            "descricao_produto": oferta.get("descricao_produto"),
                            "imagem": imagens_list,
                            "id_tipo": [
                                oferta.get("id_tipo"),
                            ],
                            "id_grupo": [
                                oferta.get("id_grupo"),
                            ],
                            "id_subgrupo": [
                                oferta.get("id_subgrupo"),
                            ],
                            "categorizacao": [{
                                "id_tipo": oferta.get("id_tipo"),
                                "descricao_tipo": oferta.get("descricao_tipo"),
                                "grupo": [{
                                    "id_grupo": oferta.get("id_grupo"),
                                    "descricao_grupo": oferta.get("descricao_grupo"),
                                    "subgrupo": [{
                                        "id_subgrupo": oferta.get("id_subgrupo"),
                                        "descricao_subgrupo": oferta.get("descricao_subgrupo")
                                    }]
                                }]
                            }],
                            "status": oferta.get("status_produto"),
                            "quantidade": oferta.get("quantidade"),
                            "unidade_embalagem": oferta.get("unidade_embalagem"),
                            "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                            "unidade_venda": oferta.get("unidade_venda"),
                            "quant_unid_venda": oferta.get("quant_unid_venda"),
                            "multiplo_venda": oferta.get("multiplo_venda"),
                        }

                        if id_cliente:
                            produto_escalonado.update({
                                "preco_tabela": oferta.get("preco_tabela"),
                                "estoque": oferta.get("estoque"),
                                "desconto": desconto,
                            })

                        saved_oferta["produto_escalonado"].append(produto_escalonado)

                    break

            else:
                imagens_list = []

                imagens = oferta.get("tokens")
                if imagens:

                    imagens = imagens.split(",")
                    imagens_list = [
                        f"{image}/imagens/{imagem}".replace("/auto/", f"/{image_replace}/")
                        for imagem in imagens
                        if imagem
                    ]

                desconto = {}

                if id_cliente and oferta.get("desconto") and oferta.get("preco_tabela"):
            
                    desconto_perc = oferta.get("desconto")
                    if desconto_perc:

                        if desconto_perc > 100:
                            desconto_perc = 100

                        elif desconto_perc < 0:
                            desconto_perc = 0

                        preco_desconto = oferta.get("preco_tabela") * (1 - (desconto_perc/100))

                        desconto = {
                            "preco_desconto": round(preco_desconto, 3),
                            "desconto": desconto_perc,
                            "data_inicio": oferta.get("data_inicio_desconto"),
                            "data_final": oferta.get("data_final_desconto")
                        }

                produto_escalonado = {
                    "id_escalonado": id_oferta,
                    "id_distribuidor": oferta.get("id_distribuidor"),
                    "id_produto": oferta.get("id_produto"),
                    "sku": oferta.get("sku"),
                    "id_marca": oferta.get("id_marca"),
                    "descricao_marca": oferta.get("descricao_marca"),
                    "descricao_produto": oferta.get("descricao_produto"),
                    "imagem": imagens_list,
                    "id_tipo": [
                        oferta.get("id_tipo"),
                    ],
                    "id_grupo": [
                        oferta.get("id_grupo"),
                    ],
                    "id_subgrupo": [
                        oferta.get("id_subgrupo"),
                    ],
                    "categorizacao": [{
                        "id_tipo": oferta.get("id_tipo"),
                        "descricao_tipo": oferta.get("descricao_tipo"),
                        "grupo": [{
                            "id_grupo": oferta.get("id_grupo"),
                            "descricao_grupo": oferta.get("descricao_grupo"),
                            "subgrupo": [{
                                "id_subgrupo": oferta.get("id_subgrupo"),
                                "descricao_subgrupo": oferta.get("descricao_subgrupo")
                            }]
                        }]
                    }],
                    "status": oferta.get("status_produto"),
                    "quantidade": oferta.get("quantidade"),
                    "unidade_embalagem": oferta.get("unidade_embalagem"),
                    "quantidade_embalagem": oferta.get("quantidade_embalagem"),
                    "unidade_venda": oferta.get("unidade_venda"),
                    "quant_unid_venda": oferta.get("quant_unid_venda"),
                    "multiplo_venda": oferta.get("multiplo_venda"),
                }

                if id_cliente:
                    produto_escalonado.update({
                        "preco_tabela": oferta.get("preco_tabela"),
                        "estoque": oferta.get("estoque"),
                        "desconto": desconto,
                    })

                oferta_hold.append({
                    "id_escalonado": id_oferta,
                    "id_distribuidor": oferta.get("id_distribuidor"),
                    "descricao": oferta.get("descricao_oferta"),
                    "status": oferta.get("status_oferta"),
                    "total_desconto": 0,
                    "unificada": oferta.get("unificada"),
                    "escalonado": [
                        {
                            "id_escalonado": id_oferta,
                            "id_distribuidor": oferta.get("id_distribuidor"),
                            "data_inicio": oferta.get("data_inicio"),
                            "data_final": oferta.get("data_final"),
                            "limite": oferta.get("quantidade_limite"),
                            "status_escalonado": oferta.get("status_oferta"),
                        }
                    ],
                    "faixa_escalonado": [
                        {
                            "id_escalonado": id_oferta,
                            "id_distribuidor": oferta.get("id_distribuidor"),
                            "sequencia": oferta.get("sequencia"),
                            "faixa": oferta.get("faixa"),
                            "desconto": oferta.get("desconto_faixa"),
                            "unidade_venda": oferta.get("unidade_venda"),
                        }
                    ],
                    "produto_escalonado": [produto_escalonado]
                })

    return oferta_hold


def json_cupons_like_gen(cupom_dist_list: list) -> list:
    """
    Retorna a resposta da query de cupons normalizada

    Args:
        cupom_dist_list (list): lista de dicionarios contendo os dados que devem ser normalizados.

    Returns:
        list: retorna a lista de cupons normalizadas
    """

    hold_cupons = []

    for cupom in cupom_dist_list:

        for saved_cupom in hold_cupons:
            if cupom["id_cupom"] == saved_cupom["id_cupom"] and saved_cupom["tipo_itens"] != 1:
                if str(cupom.get("itens")) not in saved_cupom["itens"]:
                    saved_cupom["itens"].append(str(cupom.get("itens")))
                break

        else:
            hold_cupons.append({
                "id_cupom": cupom.get("id_cupom"),
                "id_distribuidor": cupom.get("id_distribuidor"),
                "codigo_cupom": cupom.get("codigo_cupom"),
                "titulo": cupom.get("titulo"),
                "descricao": cupom.get("descricao"),
                "data_inicio": cupom.get("data_inicio"),
                "data_final": cupom.get("data_final"),
                "tipo_desconto": cupom.get("tipo_desconto"),
                "tipo_desconto_descricao": str(cupom.get("tipo_desconto_descricao")).capitalize() if cupom.get("tipo_desconto_descricao") else None,
                "desconto_valor": cupom.get("desconto_valor"),
                "desconto_percentual": cupom.get("desconto_percentual"),
                "valor_limite": cupom.get("valor_limite"),
                "valor_ativar": cupom.get("valor_ativar"),
                "termos_uso": cupom.get("termos_uso"),
                "status": cupom.get("status"),
                "reutiliza": cupom.get("reutiliza"),
                "oculto":  bool(cupom.get("oculto")),
                "bloqueado":  bool(cupom.get("bloqueado")),
                "motivo_bloqueio": cupom.get("motivo_bloqueio") if cupom.get("motivo_bloqueio") else None,
                "tipo_cupom": cupom.get("tipo_cupom"),
                "tipo_cupom_descricao": cupom.get("descricao_tipo_cupom"),
                "tipo_itens": cupom.get("tipo_itens"),
                "tipo_itens_descricao": cupom.get("descricao_tipo_itens"),
                "itens": [str(cupom.get("itens"))] if cupom.get("itens") else [],
            })

    return hold_cupons


# Consultas genêricas

def json_fretes_distribuidor(id_cliente: int) -> list:
    """
    Retorna os fretes associados a um cliente pelos distribuidores

    Args:
        id_cliente (int): ID do cliente escolhido

    Returns:
        list: Lista com os fretes
    """

    query = f"""
        SELECT
            cd.id_distribuidor,
            ISNULL(pd.valor_frete,0) valor_frete,
            ISNULL(pd.valor_minimo_frete,0) valor_minimo_frete,
            ISNULL(pd.valor_minimo_pedido,0) valor_minimo_pedido,
            ISNULL(pd.maximo_item_pedido,0) maximo_item_pedido

        FROM
            (
            
                SELECT
                    DISTINCT d.id_distribuidor

                FROM
                    DISTRIBUIDOR d

                    INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                        d.id_distribuidor = cd.id_distribuidor

                    INNER JOIN CLIENTE c ON
                        cd.id_cliente = c.id_cliente

                WHERE
                    LEN(c.nome_fantasia) > 0
                    AND c.id_cliente = :id_cliente
                    AND c.status = 'A'
                    AND cd.status = 'A'
                    AND cd.d_e_l_e_t_ = 0
                    AND cd.data_aprovacao <= GETDATE()
                    AND d.status = 'A'
                    AND d.id_distribuidor != 0

            ) CD

            LEFT JOIN PARAMETRO_DISTRIBUIDOR pd ON
                cd.id_distribuidor = pd.id_distribuidor

        ORDER BY
            pd.id_distribuidor
    """        
    
    params = {
        "id_cliente": id_cliente
    }

    data = dm.raw_sql_return(query, params = params)

    if not data:
        return []

    return data


def json_limite_credito(id_cliente: int, id_distribuidor: list[int] = None) -> list:
    """
    Retorna os valores de crédito do cliente

    Args:
        id_usuario (int): ID do usuario
        id_cliente (int): ID do distribuidor
        id_distribuidor (list[int], optional): lista com os distribuiodes dos quais se quer ver limite. Defaults to None.

    Returns:
        list: lista com os limites de creditos
    """

    if id_distribuidor is not None:

        if isinstance(id_distribuidor, (int, str)):
            id_distribuidor = [str(id_distribuidor)]

        elif isinstance(id_distribuidor, (list, tuple, set)):
            id_distribuidor = list(id_distribuidor)

        else:
            id_distribuidor = [0]

    else:
        id_distribuidor = [0]

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
            c.id_cliente = :id_cliente
            AND d.id_distribuidor != 0
            AND (
                    (
                        0 NOT IN :distribuidores
                        AND d.id_distribuidor IN :distribuidores
                    )
                        OR
                    (
                        0 IN :distribuidores
                    )
                )
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND cd.d_e_l_e_t_ = 0
            AND d.status = 'A'            
    """

    params = {
        "id_cliente": id_cliente,
        "distribuidores": id_distribuidor
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
    }

    distribuidores = dm.raw_sql_return(**dict_query)
    if not distribuidores:
        return []

    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    # Salvando o saldo do cliente
    for distribuidor in distribuidores:

        query = """
            SELECT
                ISNULL(SUM(valor), 0) saldo_negativo

            FROM
                PEDIDO

            WHERE
                id_cliente = :id_cliente
                AND id_distribuidor = :id_distribuidor
                AND forma_pagamento = 1
                AND data_finalizacao IS NULL
                AND d_e_l_e_t_ = 0
        """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": distribuidor
        }

        query = dm.raw_sql_return(query, params = params, first = True, raw = True)

        if query:

            saldo_negativo = query[0]

            query = """
                IF NOT EXISTS (
                                    SELECT 
                                        TOP 1 1 
                                    
                                    FROM 
                                        PARAMETRO_CLIENTE 
                                        
                                    WHERE 
                                        id_distribuidor = :id_distribuidor 
                                        AND id_cliente = :id_cliente
                              )
                BEGIN

                    INSERT INTO
                        PARAMETRO_CLIENTE
                            (
                                id_cliente,
                                id_distribuidor,
                                limite_credito,
                                saldo_limite,
                                valor_minimo_pedido
                            )

                        VALUES
                            (
                                :id_cliente,
                                :id_distribuidor,
                                0,
                                0 - :saldo_negativo,
                                0
                            )

                END

                ELSE
                BEGIN

                    UPDATE
                        PARAMETRO_CLIENTE

                    SET
                        saldo_limite = limite_credito - :saldo_negativo

                    WHERE
                        id_cliente = :id_cliente
                        AND id_distribuidor = :id_distribuidor
                    
                END
            """

            params = {
                "id_cliente": id_cliente,
                "id_distribuidor": distribuidor,
                "saldo_negativo": saldo_negativo,
            }

            dm.raw_sql_execute(query, params = params)

    query = """
        SELECT
            id_distribuidor,
            limite_credito,
            saldo_limite as credito_atual,
            valor_minimo_pedido as pedido_minimo

        FROM
            PARAMETRO_CLIENTE

        WHERE
            id_cliente = :id_cliente
            AND id_distribuidor != 0
            AND (
                    (
                        0 NOT IN :distribuidores
                        AND id_distribuidor IN :distribuidores
                    )
                        OR
                    (
                        0 IN :distribuidores
                    )
                )
            
        ORDER BY
            id_distribuidor
    """

    params = {
        "id_cliente": id_cliente,
        "distribuidores": distribuidores
    }

    bindparams = ["distribuidores"]

    query = dm.raw_sql_return(query, params = params, bindparams = bindparams)

    if not query:
        return []

    return query


def json_limites(id_cliente: int, id_distribuidor: list[int] = None) -> list:
    """
    Junção do limite de credito com os limites de frete

    Args:
        id_usuario (int): ID do usuario
        id_cliente (int): ID do cliente

    Returns:
        list: Lista com os valores de limite do usuario-cliente
    """

    if isinstance(id_distribuidor, int):
        id_distribuidor = [id_distribuidor]

    if not isinstance(id_distribuidor, list):
        id_distribuidor = None

    dict_limite_credito = {
        "id_cliente": id_cliente
    }

    limite_credito = json_limite_credito(**dict_limite_credito)

    # Pegando o frete
    dict_fretes = {
        "id_cliente": id_cliente
    }

    fretes = json_fretes_distribuidor(**dict_fretes)

    if not limite_credito or not fretes:
        return []

    dados = []

    for limite in limite_credito:
        for frete in fretes:
            if limite.get("id_distribuidor") == frete.get("id_distribuidor"):
                if id_distribuidor is None or frete.get("id_distribuidor") in id_distribuidor:
                    dados.append({
                        "id_distribuidor": limite.get("id_distribuidor"),
                        "limite_credito": limite.get("limite_credito"),
                        "credito_atual": limite.get("credito_atual"),
                        "pedido_minimo": limite.get("pedido_minimo"),
                        "valor_frete": frete.get("valor_frete"),
                        "valor_minimo_frete": frete.get("valor_minimo_frete"),
                        "maximo_item_pedido": frete.get("maximo_item_pedido"),
                    })
                    break

    return dados


def json_formas_pagamento(id_usuario: int, id_cliente: int, id_distribuidor: int) -> list:
    """
    Retorna as formas de pagamento do cliente

    Args:
        id_usuario (int): ID do usuario
        id_cliente (int): ID do cliente
        id_distribuidor (int): ID do distribuidor

    Returns:
        list: Lista com os valores de formas de pagamento do usuario-cliente
    """

    # Grupo de pagamento
    query = """
        IF EXISTS (
                    SELECT
                        TOP 1 id_grupo

                    FROM
                        CLIENTE_GRUPO_PAGTO

                    WHERE
                        id_cliente = :id_cliente
                        AND status = 'A'
                  )
        BEGIN

            SELECT
                TOP 1 id_grupo

            FROM
                CLIENTE_GRUPO_PAGTO

            WHERE
                id_cliente = :id_cliente
                AND status = 'A'

            ORDER BY
                data_cadastro ASC

        END

        ELSE
        BEGIN

            SELECT
                TOP 1 id_grupo

            FROM
                GRUPO_PAGTO

            WHERE
                status = 'A'
                AND padrao = 'S'
        
        END
    """

    params = {
        "id_cliente": id_cliente
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
        "first": True,
    }

    query = dm.raw_sql_return(**dict_query)

    if not query:
        return []

    id_grupo_pagamento = query[0]

    # Condicoes de pagamento
    query = """
        SELECT
            id_condpgto,
            descricao,
            percentual,
            id_formapgto,
            desc_formapgto,
            padrao,
            juros_parcela

        FROM
            (
            
                SELECT
                    DISTINCT cp.id_condpgto,
                    cp.descricao,
                    cp.percentual,
                    fp.id_formapgto,
                    fp.descricao as desc_formapgto,
                    ISNULL(gpi.padrao, 'N') padrao,
                    ISNULL(cp.juros_parcela, 0) as juros_parcela

                FROM
                    GRUPO_PAGTO_ITEM gpi

                    INNER JOIN CONDICAO_PAGAMENTO cp ON 
                        gpi.id_condpgto = cp.id_condpgto

                    INNER JOIN FORMA_PAGAMENTO fp ON
                        cp.id_formapgto = fp.id_formapgto

                WHERE
                    gpi.id_grupo = :id_grupo_pagamento
                    AND gpi.id_distribuidor = :id_distribuidor
                    AND gpi.status = 'A'
                    AND cp.status = 'A'
                    AND fp.status = 'A'

            ) tabela

        ORDER BY
            id_condpgto
    """

    params = {
        "id_grupo_pagamento": id_grupo_pagamento,
        "id_distribuidor": id_distribuidor
    }

    condicoes_pagamento = dm.raw_sql_return(query, params = params)

    if not condicoes_pagamento:
        return []

    s_index = None

    for index, condicao_pagamento in enumerate(condicoes_pagamento):
        if condicao_pagamento.get("padrao") == 'S':
            s_index = index
            break

    if s_index is None:
        s_index = 0

    for index, condicao_pagamento in enumerate(condicoes_pagamento):
        if index != s_index:
            condicao_pagamento["padrao"] = 'N'
        else:
            condicao_pagamento["padrao"] = 'S'
    
    # Montando o json
    formas_pagamento = []   

    for condicao_pagamento in condicoes_pagamento:
        
        id_formapgto = condicao_pagamento.get("id_formapgto")

        for saved_forma in formas_pagamento:
            
            if saved_forma.get("id") == id_formapgto:
                padrao = True if condicao_pagamento.get("padrao") == 'S' else False

                if not saved_forma.get("padrao"):
                    saved_forma["padrao"] = padrao

                descricao_cond = condicao_pagamento.get("descricao")

                condicoes = {
                    "id_condpgto": condicao_pagamento.get("id_condpgto"),
                    "descricao": descricao_cond,
                    "percentual": condicao_pagamento.get("percentual"),
                    "juros": condicao_pagamento.get("juros_parcela"),
                    "padrao": padrao
                }

                if id_formapgto == 2:

                    parcelas = (str(descricao_cond).split()[-1]).strip()[0]
                    if not str(parcelas).isdecimal():
                        parcelas = 1

                    else:
                        parcelas = int(parcelas)

                    condicoes["parcelas"] = parcelas

                saved_forma["condicoes"].append(condicoes)
                break

        else:
            dict_formapgto = {
                "id": id_formapgto,
                "nome": str(condicao_pagamento.get("desc_formapgto")).capitalize(),
                "padrao": True if condicao_pagamento.get("padrao") == 'S' else False,
                "condicoes": [],
            }

            descricao_cond = condicao_pagamento.get("descricao")

            condicoes = {
                "id_condpgto": condicao_pagamento.get("id_condpgto"),
                "descricao": descricao_cond,
                "percentual": condicao_pagamento.get("percentual"),
                "juros": condicao_pagamento.get("juros_parcela"),
                "padrao": True if condicao_pagamento.get("padrao") == 'S' else False
            }

            if id_formapgto == 2:

                parcelas = (str(descricao_cond).split()[-1]).strip()[0]
                if not str(parcelas).isdecimal():
                    parcelas = 1

                else:
                    parcelas = int(parcelas)

                condicoes["parcelas"] = parcelas

                dict_cartao = {
                    "id_usuario": id_usuario,
                }

                response = json_cartoes(**dict_cartao)

                cartoes = []

                for cartao in response:
                    cartoes.append({
                        "id_cartao": cartao.get("id_cartao"),
                        "numero_cartao": cartao.get("numero_cartao"),
                        "bandeira": cartao.get("bandeira"),
                        "cvv_check": cartao.get("cvv_check")
                    })

                dict_formapgto["cartoes"] = cartoes

            dict_formapgto["condicoes"].append(condicoes)
            formas_pagamento.append(dict_formapgto)

    return formas_pagamento


def json_cliente(id_usuario: int, id_cliente: list[int] = None) -> list:
    """
    Mostra as informações do cliente

    Args:
        id_usuario (int): ID do usuario
        id_cliente (list[int]): IDs dos clientes que se quer informação

    Returns:
        list: Lista com os clientes associados ao usuario
    """

    assert isinstance(id_usuario, int), "id_usuario with invalid format"
    
    if type(id_cliente) is list:
        assert len(id_cliente) > 0, "id_cliente with invalid length"

    if type(id_cliente) is int:
        id_cliente = [id_cliente]

    if not id_cliente:
        id_cliente = None

    # Pegando os dados dos distribuidores
    query = """
        SELECT
            d.id_distribuidor,
            UPPER(d.nome_fantasia) as nome_fantasia

        FROM
            DISTRIBUIDOR d

        WHERE
            d.id_distribuidor != 0
            AND status = 'A'
    """

    response = dm.raw_sql_return(query)
    if not response:
        return []

    saved_distribuidores = {
        distribuidor["id_distribuidor"]: distribuidor
        for distribuidor in response
    }

    # Query do cliente
    string_id_cliente = ""

    params = {
        "id_usuario": id_usuario
    }

    if id_cliente:
        string_id_cliente = "AND c.id_cliente IN :id_cliente"
        params["id_cliente"] = id_cliente

    cliente_query = f"""
        SELECT
            DISTINCT c.id_cliente,
            c.cnpj,
            c.nome_fantasia,
            c.razao_social,
            c.endereco,
            c.endereco_num,
            c.endereco_complemento,
            c.bairro,
            c.cep,
            c.cidade,
            c.estado,
            c.telefone,
            c.data_cadastro,
            uc.data_aprovacao,
            c.status_receita

        FROM
            USUARIO_CLIENTE uc
            
            INNER JOIN CLIENTE c ON
                uc.id_cliente = c.id_cliente

            INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                c.id_cliente = cd.id_cliente

            INNER JOIN DISTRIBUIDOR d ON
                cd.id_distribuidor = d.id_distribuidor

        WHERE
            uc.id_usuario = :id_usuario
            {string_id_cliente}
            AND uc.status = 'A'
            AND cd.status = 'A'
            AND d.status = 'A'
            AND uc.d_e_l_e_t_ = 0
            AND cd.d_e_l_e_t_ = 0

        ORDER BY
            uc.data_aprovacao;
    """

    dict_query = {
        "query": cliente_query,
        "params": params,
    }

    cliente_query = dm.raw_sql_return(**dict_query)

    # Pegando os distribuidores dos clientes
    data = []

    for cliente in cliente_query:

        id_cliente = cliente.get("id_cliente")
        
        # Pegando dados do limite de credito
        dict_limite_credito = {
            "id_cliente": id_cliente,
        }

        dados = json_limites(**dict_limite_credito)

        cliente["limites"] = dados

        # Pegando dados do distribuidor
        distribuidor_query = """
            SELECT
                DISTINCT d.id_distribuidor

            FROM
                CLIENTE c

                INNER JOIN CLIENTE_DISTRIBUIDOR cd ON
                    c.id_cliente = cd.id_cliente

                INNER JOIN.DISTRIBUIDOR d ON
                    cd.id_distribuidor = d.id_distribuidor

            WHERE
                c.id_cliente = :id_cliente
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'

            ORDER BY
                d.id_distribuidor
        """

        params = {
            "id_cliente": cliente.get("id_cliente")
        }

        distribuidor_query = dm.raw_sql_return(distribuidor_query, params = params)

        cliente["distribuidor"] = []
        metodos = []

        for distribuidor in distribuidor_query:

            id_distribuidor = distribuidor.get("id_distribuidor")

            # Pegando dados da forma de pagamento
            dict_formas_pagamento = {
                "id_usuario": id_usuario,
                "id_cliente": id_cliente,
                "id_distribuidor": id_distribuidor,
            }

            formpgtos = json_formas_pagamento(**dict_formas_pagamento)

            if formpgtos:
                cliente["distribuidor"].append(saved_distribuidores.get(id_distribuidor))
                metodos.append({
                    "id_distribuidor": id_distribuidor,
                    "pagamentos": formpgtos
                })

        cliente["metodos"] = metodos

        if cliente["distribuidor"] and cliente["metodos"]:
            data.append(cliente)

    return data


def json_usuario(id_usuario: int) -> dict:
    """
    Mostra as informações do cliente

    Args:
        id_usuario (int): ID do usuario

    Returns:
        dict: Objeto com as informações do usuario
    """

    query = """
        SELECT
            TOP 1 u.id_usuario,
            u.nome,
            u.cpf,
            u.email,
            u.telefone,
            u.data_cadastro,
            u.data_nascimento,
            u.data_aceite,
            u.imagem

        FROM
            USUARIO u

        WHERE
            u.id_usuario = :id_usuario
            AND u.status = 'A'
    """
    
    params = {
        "id_usuario": id_usuario
    }

    query = dm.raw_sql_return(query, params = params, first = True)

    if not query:
        return {}

    # Trazendo os clientes atrelados ao usuario
    data = query

    imagem = data.get("imagem")
    if imagem:
        imagem = f"{image}/imagens/fotos/300/usuarios/{imagem}"
        data["imagem"] = imagem

    else:
        data["imagem"] = None

    # Pegando os clientes do usuario
    dict_cliente = {
        "id_usuario":id_usuario,
    }

    clientes = json_cliente(**dict_cliente)
    if not clientes:
        return {}

    data["cliente"] = clientes

    return data


def json_products(prod_dist_list: list, agent: str = "marketplace", id_cliente = None,
                    id_usuario = None, image_replace: str = 'auto') -> list:
    """
    Gera o json padrão do produto

    Args:
        produto_cod_list (list): lista de dicionarios contendo id_produto e id_distribuidor
        distribuidor (int): distribuidor principal das buscas
        agent (str, optional): Quem é o agente que pede o json. Defaults to "marketplace".
        image_replace (str, optional): Quem é o agente que pede o json. Defaults to "marketplace".

    Returns:
        list: Resultado da query já normalizado e validado
    """
    agent = str(agent).lower()

    assert agent in {"marketplace", "jmanager"}, "agent with invalid value."
    assert type(prod_dist_list) in [list, dict], "product with invalid format"

    # Tratamento dos dados  
    if type(prod_dist_list) is dict:
        prod_dist_list = [prod_dist_list]

    produtos = []

    for produto in prod_dist_list:

        id_distribuidor = produto.get("id_distribuidor")
        id_produto = produto.get("id_produto")

        if id_distribuidor and id_produto:
            produtos.append({
                "id_distribuidor": id_distribuidor,
                "id_produto": id_produto
            })

    # Verificando qual o tipo de usuario 
    if agent == "marketplace":
        bool_marketplace = True
    
    else:
        bool_marketplace = False

    # Pegando os distribuidores do cliente
    if bool_marketplace and id_cliente:
        
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
                c.id_cliente = :id_cliente
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'
        """

        params = {
            "id_cliente": id_cliente
        }        

    else:
        
        query = """
            SELECT
                DISTINCT id_distribuidor

            FROM
                DISTRIBUIDOR

            WHERE
                status = 'A'
        """

        params = {}

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
    }

    distribuidores = dm.raw_sql_return(**dict_query)
    if not distribuidores:
        return []

    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    params = {
        "produto_input": str(produtos).replace("'", '"'),
        "distribuidores": distribuidores,
        "id_cliente": id_cliente,
    }

    # Query de produto
    if bool_marketplace:

        if id_usuario:

            produto_query = f"""
                SELECT
                    DISTINCT pq.id, 
                    p.id_produto,
                    p.sku,
                    p.descricao_completa as descricao_produto,
                    p.status,
                    p.dun14,
                    p.tipo_produto,
                    p.variante,
                    p.volumetria,
                    p.unidade_embalagem,
                    p.quantidade_embalagem,
                    p.unidade_venda,
                    p.quant_unid_venda,
                    p.tokens_imagem as tokens,
                    pd.id_distribuidor,
                    d.nome_fantasia as descricao_distribuidor,
                    pd.agrupamento_variante,
                    pd.cod_prod_distr,
                    SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
                    pd.multiplo_venda,
                    pd.ranking,
                    pd.unidade_venda as unidade_venda_distribuidor,
                    pd.quant_unid_venda as quant_unid_venda_distribuidor,
                    pd.giro,
                    pd.agrup_familia,
                    pd.status as status_distribuidor,
                    pd.data_cadastro,
                    m.id_marca,
                    m.desc_marca as descricao_marca,
                    f.id_fornecedor,
                    f.desc_fornecedor,
                    s.id_subgrupo,
                    s.descricao as descricao_subgrupo,
                    g.id_grupo,
                    g.descricao as descricao_grupo,
                    t.id_tipo,
                    t.descricao as descricao_tipo,
                    pd.estoque,
                    pq.preco_tabela,
                    odesc.desconto,
                    odesc.data_inicio as data_inicio_desconto,
                    odesc.data_final as data_final_desconto,
                    offers.id_oferta,
                    CASE
                        WHEN offers.id_oferta IS NULL 
                            THEN NULL
                        WHEN offers.tipo_oferta = 2 AND offers.ativador = 1
                            THEN 1
                        ELSE
                            0
                    END ativador,
                    CASE
                        WHEN offers.id_oferta IS NULL 
                            THEN NULL
                        WHEN offers.tipo_oferta = 2 AND offers.bonificado = 1
                            THEN 1
                        ELSE
                            0
                    END bonificado,
                    CASE
                        WHEN offers.id_oferta IS NULL 
                            THEN NULL
                        WHEN offers.tipo_oferta = 3 AND offers.ativador = 1
                            THEN 1
                        ELSE
                            0
                    END escalonado,
                    CASE
                        WHEN ntfce.id_produto IS NULL OR pd.estoque > 0
                            THEN 0
                        ELSE
                            1
                    END avise_me,
                    CASE
                        WHEN uf.id_usuario IS NULL 
                            THEN 0
                        ELSE
                            1
                    END favorito,
                    pdet.detalhes

                FROM
                    (

                        SELECT
                            thip.id,
                            tpp.id_produto,
                            tpp.id_distribuidor,
                            MIN(tpp.preco_tabela) preco_tabela

                        FROM
                            (

                                SELECT
                                    nda.id,
                                    nda.id_produto,
                                    nda.id_distribuidor

                                FROM
                                    (

                                        SELECT
                                            ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as id,
                                            id_produto,
                                            id_distribuidor

                                        FROM
                                            OPENJSON(:produto_input)
                                    
                                        WITH
                                            (
                                                id_produto VARCHAR(200)   '$.id_produto',  
                                                id_distribuidor INT       '$.id_distribuidor'
                                            )

                                    ) nda
                            
                            ) thip

                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                thip.id_produto = tpp.id_produto
                                AND thip.id_distribuidor = tpp.id_distribuidor

                            INNER JOIN TABELA_PRECO tp ON
                                tpp.ID_DISTRIBUIDOR = tp.ID_DISTRIBUIDOR
                                AND tpp.ID_TABELA_PRECO = tp.ID_TABELA_PRECO

                        WHERE
                            tp.id_distribuidor IN :distribuidores
                            AND (
                                    (
                                        tp.tabela_padrao = 'S'
                                    )
                                        OR
                                    (
                                        tp.id_tabela_preco IN (
                                                                    SELECT
                                                                        id_tabela_preco

                                                                    FROM
                                                                        TABELA_PRECO_CLIENTE

                                                                    WHERE
                                                                        id_cliente = :id_cliente
                                                                        AND id_distribuidor IN :distribuidores
                                                                )
                                    )
                                )
                            AND tp.status = 'A'
                            AND tp.dt_inicio <= GETDATE()
                            AND tp.dt_fim >= GETDATE()
                            
                        GROUP BY
                            thip.id,
                            tpp.id_produto,
                            tpp.id_distribuidor

                    ) pq

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        pq.id_produto = pd.id_produto
                        AND pq.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN DISTRIBUIDOR d ON
                        pd.id_distribuidor = d.id_distribuidor

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                    INNER JOIN FORNECEDOR f ON
                        pd.id_fornecedor = f.id_fornecedor

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        pd.id_produto = ps.codigo_produto

                    INNER JOIN SUBGRUPO s ON
                        ps.id_subgrupo = s.id_subgrupo
                        AND ps.id_distribuidor = s.id_distribuidor

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        s.id_subgrupo = gs.id_subgrupo
                        AND s.id_distribuidor = gs.id_distribuidor

                    INNER JOIN GRUPO g ON
                        gs.id_grupo = g.id_grupo

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo

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
                                    o.id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE)

                                UNION

                                SELECT
                                    oc.id_oferta

                                FROM
                                    OFERTA_CLIENTE oc

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
                            AND o.limite_ativacao_oferta > 0
                            AND od.desconto > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND o.status = 'A'
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
                            o.id_oferta,
                            o.id_produto,
                            o.tipo_oferta,
                            o.id_distribuidor,
                            o.ativador,
                            o.bonificado

                        FROM
                            (
                                SELECT
                                    o.id_oferta,
                                    o.id_distribuidor,
                                    op.id_produto,
                                    o.tipo_oferta,
                                    op.ativador,
                                    op.bonificado

                                FROM
                                    (
                                                    
                                        SELECT
                                            op.id_oferta,
                                            op.id_produto,
                                            1 ativador,
                                            0 bonificado

                                        FROM
                                            OFERTA_PRODUTO op

                                        WHERE
                                            op.status = 'A'

                                        UNION

                                        SELECT
                                            ob.id_oferta,
                                            ob.id_produto,
                                            0 ativador,
                                            1 bonificado

                                        FROM
                                            OFERTA_BONIFICADO ob

                                        WHERE
                                            ob.status = 'A'

                                    ) op

                                    INNER JOIN OFERTA o ON
                                        op.id_oferta = o.id_oferta

                                    INNER JOIN
                                    (

                                        SELECT
                                            o.id_oferta

                                        FROM
                                            OFERTA o

                                        WHERE
                                            o.id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE)

                                        UNION

                                        SELECT
                                            oc.id_oferta

                                        FROM
                                            OFERTA_CLIENTE oc

                                        WHERE
                                            oc.id_cliente = :id_cliente
                                            AND oc.status = 'A'
                                            AND oc.d_e_l_e_t_ = 0

                                    ) oc ON
                                        o.id_oferta = oc.id_oferta

                                    INNER JOIN PRODUTO p ON
                                        op.id_produto = p.id_produto

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        p.id_produto = pd.id_produto
                                        AND o.id_distribuidor = pd.id_distribuidor

                                WHERE
                                    o.tipo_oferta IN (2,3)
                                    AND o.id_distribuidor IN :distribuidores
                                    AND o.limite_ativacao_oferta > 0
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND o.status = 'A'
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                                            
                            ) o

                            INNER JOIN
                            (
                            
                                SELECT
                                    DISTINCT
                                        o.id_oferta,
                                        o.id_distribuidor

                                FROM
                                    PRODUTO_DISTRIBUIDOR pd

                                    INNER JOIN PRODUTO p ON
                                        pd.id_produto = p.id_produto

                                    INNER JOIN OFERTA o ON
                                        pd.id_distribuidor = o.id_distribuidor

                                    INNER JOIN OFERTA_PRODUTO op ON
                                        p.id_produto = op.id_produto
                                        AND o.id_oferta = op.id_oferta

                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                        pd.id_produto = tpp.id_produto
                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                    INNER JOIN TABELA_PRECO tp ON
                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                WHERE
                                    pd.estoque > 0
                                    AND o.id_distribuidor IN :distribuidores
                                    AND (

                                            (
                                                o.tipo_oferta = 2 
                                                AND o.id_oferta IN (

                                                                        SELECT
                                                                            ob.id_oferta

                                                                        FROM
                                                                            PRODUTO_DISTRIBUIDOR pd

                                                                            INNER JOIN PRODUTO p ON
                                                                                pd.id_produto = p.id_produto

                                                                            INNER JOIN OFERTA o ON
                                                                                pd.id_distribuidor = o.id_distribuidor

                                                                            INNER JOIN OFERTA_BONIFICADO ob ON
                                                                                p.id_produto = ob.id_produto
                                                                                AND o.id_oferta = ob.id_oferta

                                                                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                                                pd.id_produto = tpp.id_produto
                                                                                AND pd.id_distribuidor = tpp.id_distribuidor

                                                                            INNER JOIN TABELA_PRECO tp ON
                                                                                tpp.id_tabela_preco = tp.id_tabela_preco
                                                                                AND tpp.id_distribuidor = tp.id_distribuidor

                                                                        WHERE
                                                                            pd.estoque > 0
                                                                            AND o.id_distribuidor IN :distribuidores
                                                                            AND o.tipo_oferta = 2
                                                                            AND tp.dt_inicio <= GETDATE()
                                                                            AND tp.dt_fim >= GETDATE()
                                                                            AND tp.tabela_padrao = 'S'
                                                                            AND tp.status = 'A'
                                                                            AND ob.status = 'A'
                                                                            AND p.status = 'A'
                                                                            AND pd.status = 'A'

                                                                    )
                                            )

                                                OR

                                            (
                                                    
                                                o.tipo_oferta = 3
                                                AND o.id_oferta IN (
                                                                    
                                                                        SELECT
                                                                            id_oferta

                                                                        FROM
                                                                            OFERTA_ESCALONADO_FAIXA

                                                                        WHERE
                                                                            desconto > 0
                                                                            AND status = 'A'

                                                                    )
                                                    
                                            )

                                        )
                                    AND o.limite_ativacao_oferta > 0
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND tp.tabela_padrao = 'S'
                                    AND tp.dt_inicio <= GETDATE()
                                    AND tp.dt_fim >= GETDATE()
                                    AND op.status = 'A'
                                    AND tp.status = 'A'
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND o.status = 'A'

                            ) op ON
                                o.id_oferta = op.id_oferta
                                AND o.id_distribuidor = op.id_distribuidor

                    ) offers ON
                        pd.id_produto = offers.id_produto
                        AND pd.id_distribuidor = offers.id_distribuidor

                    LEFT JOIN PRODUTO_DETALHES pdet ON
                        p.id_produto = pdet.id_produto

                    LEFT JOIN NOTIFICACAO_ESTOQUE ntfce ON
                        p.id_produto = ntfce.id_produto
                        AND ntfce.id_usuario = :id_usuario

                    LEFT JOIN USUARIO_FAVORITO uf ON
                        uf.id_usuario = :id_usuario
                        AND pd.id_produto = uf.id_produto
                        AND pd.id_distribuidor = uf.id_distribuidor

                WHERE
                    ps.id_distribuidor != 0
                    AND ps.status = 'A'
                    AND s.status = 'A'
                    AND gs.status = 'A'
                    AND g.status = 'A'
                    AND t.status = 'A'

                ORDER BY
                    pq.id;
            """

            params.update({
                "id_usuario": id_usuario
            })

        else:

            produto_query = f"""
                SELECT
                    DISTINCT pq.id, 
                    p.id_produto,
                    p.sku,
                    p.descricao_completa as descricao_produto,
                    p.status,
                    p.dun14,
                    p.tipo_produto,
                    p.variante,
                    p.volumetria,
                    p.unidade_embalagem,
                    p.quantidade_embalagem,
                    p.unidade_venda,
                    p.quant_unid_venda,
                    p.tokens_imagem as tokens,
                    pd.id_distribuidor,
                    d.nome_fantasia as descricao_distribuidor,
                    pd.agrupamento_variante,
                    pd.cod_prod_distr,
                    SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
                    pd.multiplo_venda,
                    pd.ranking,
                    pd.unidade_venda as unidade_venda_distribuidor,
                    pd.quant_unid_venda as quant_unid_venda_distribuidor,
                    pd.giro,
                    pd.agrup_familia,
                    pd.status as status_distribuidor,
                    pd.data_cadastro,
                    m.id_marca,
                    m.desc_marca as descricao_marca,
                    f.id_fornecedor,
                    f.desc_fornecedor,
                    s.id_subgrupo,
                    s.descricao as descricao_subgrupo,
                    g.id_grupo,
                    g.descricao as descricao_grupo,
                    t.id_tipo,
                    t.descricao as descricao_tipo,
                    offers.id_oferta,
                    CASE
                        WHEN offers.id_oferta IS NULL 
                            THEN NULL
                        WHEN offers.tipo_oferta = 2 AND offers.ativador = 1
                            THEN 1
                        ELSE
                            0
                    END ativador,
                    CASE
                        WHEN offers.id_oferta IS NULL 
                            THEN NULL
                        WHEN offers.tipo_oferta = 2 AND offers.bonificado = 1
                            THEN 1
                        ELSE
                            0
                    END bonificado,
                    CASE
                        WHEN offers.id_oferta IS NULL 
                            THEN NULL
                        WHEN offers.tipo_oferta = 3 AND offers.ativador = 1
                            THEN 1
                        ELSE
                            0
                    END escalonado,
                    pdet.detalhes

                FROM
                    (

                        SELECT
                            nda.id,
                            nda.id_produto,
                            nda.id_distribuidor

                        FROM
                            (

                                SELECT
                                    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as id,
                                    id_produto,
                                    id_distribuidor

                                FROM
                                    OPENJSON(:produto_input)
                            
                                WITH
                                    (
                                        id_produto VARCHAR(200)   '$.id_produto',  
                                        id_distribuidor INT       '$.id_distribuidor'
                                    )

                            ) nda
                    
                    ) pq

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        pq.id_produto = pd.id_produto
                        AND pq.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN DISTRIBUIDOR d ON
                        pd.id_distribuidor = d.id_distribuidor

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                    INNER JOIN FORNECEDOR f ON
                        pd.id_fornecedor = f.id_fornecedor

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        pd.id_produto = ps.codigo_produto

                    INNER JOIN SUBGRUPO s ON
                        ps.id_subgrupo = s.id_subgrupo
                        AND ps.id_distribuidor = s.id_distribuidor

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        s.id_subgrupo = gs.id_subgrupo
                        AND s.id_distribuidor = gs.id_distribuidor

                    INNER JOIN GRUPO g ON
                        gs.id_grupo = g.id_grupo

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo

                    LEFT JOIN
                    (

                        SELECT
                            o.id_oferta,
                            o.id_produto,
                            o.tipo_oferta,
                            o.id_distribuidor,
                            o.ativador,
                            o.bonificado

                        FROM
                            (
                                SELECT
                                    o.id_oferta,
                                    o.id_distribuidor,
                                    op.id_produto,
                                    o.tipo_oferta,
                                    op.ativador,
                                    op.bonificado

                                FROM
                                    (
                                                    
                                        SELECT
                                            op.id_oferta,
                                            op.id_produto,
                                            1 ativador,
                                            0 bonificado

                                        FROM
                                            OFERTA_PRODUTO op

                                        WHERE
                                            op.status = 'A'

                                        UNION

                                        SELECT
                                            ob.id_oferta,
                                            ob.id_produto,
                                            0 ativador,
                                            1 bonificado

                                        FROM
                                            OFERTA_BONIFICADO ob

                                        WHERE
                                            ob.status = 'A'

                                    ) op

                                    INNER JOIN OFERTA o ON
                                        op.id_oferta = o.id_oferta

                                    INNER JOIN PRODUTO p ON
                                        op.id_produto = p.id_produto

                                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                        p.id_produto = pd.id_produto
                                        AND o.id_distribuidor = pd.id_distribuidor

                                WHERE
                                    o.tipo_oferta IN (2,3)
                                    AND o.id_distribuidor IN :distribuidores
                                    AND o.limite_ativacao_oferta > 0
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND o.status = 'A'
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                                            
                            ) o

                            INNER JOIN
                            (
                            
                                SELECT
                                    DISTINCT
                                        o.id_oferta,
                                        o.id_distribuidor

                                FROM
                                    PRODUTO_DISTRIBUIDOR pd

                                    INNER JOIN PRODUTO p ON
                                        pd.id_produto = p.id_produto

                                    INNER JOIN OFERTA o ON
                                        pd.id_distribuidor = o.id_distribuidor

                                    INNER JOIN OFERTA_PRODUTO op ON
                                        p.id_produto = op.id_produto
                                        AND o.id_oferta = op.id_oferta

                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                        pd.id_produto = tpp.id_produto
                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                    INNER JOIN TABELA_PRECO tp ON
                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                WHERE
                                    pd.estoque > 0
                                    AND o.id_distribuidor IN :distribuidores
                                    AND (

                                            (
                                                o.tipo_oferta = 2 
                                                AND o.id_oferta IN (

                                                                        SELECT
                                                                            ob.id_oferta

                                                                        FROM
                                                                            PRODUTO_DISTRIBUIDOR pd

                                                                            INNER JOIN PRODUTO p ON
                                                                                pd.id_produto = p.id_produto

                                                                            INNER JOIN OFERTA o ON
                                                                                pd.id_distribuidor = o.id_distribuidor

                                                                            INNER JOIN OFERTA_BONIFICADO ob ON
                                                                                p.id_produto = ob.id_produto
                                                                                AND o.id_oferta = ob.id_oferta

                                                                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                                                pd.id_produto = tpp.id_produto
                                                                                AND pd.id_distribuidor = tpp.id_distribuidor

                                                                            INNER JOIN TABELA_PRECO tp ON
                                                                                tpp.id_tabela_preco = tp.id_tabela_preco
                                                                                AND tpp.id_distribuidor = tp.id_distribuidor

                                                                        WHERE
                                                                            pd.estoque > 0
                                                                            AND o.id_distribuidor IN :distribuidores
                                                                            AND o.tipo_oferta = 2
                                                                            AND tp.dt_inicio <= GETDATE()
                                                                            AND tp.dt_fim >= GETDATE()
                                                                            AND tp.tabela_padrao = 'S'
                                                                            AND tp.status = 'A'
                                                                            AND ob.status = 'A'
                                                                            AND p.status = 'A'
                                                                            AND pd.status = 'A'

                                                                    )
                                            )

                                                OR

                                            (
                                                    
                                                o.tipo_oferta = 3
                                                AND o.id_oferta IN (
                                                                    
                                                                        SELECT
                                                                            id_oferta

                                                                        FROM
                                                                            OFERTA_ESCALONADO_FAIXA

                                                                        WHERE
                                                                            desconto > 0
                                                                            AND status = 'A'

                                                                    )
                                                    
                                            )

                                        )
                                    AND o.limite_ativacao_oferta > 0
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND tp.tabela_padrao = 'S'
                                    AND tp.dt_inicio <= GETDATE()
                                    AND tp.dt_fim >= GETDATE()
                                    AND op.status = 'A'
                                    AND tp.status = 'A'
                                    AND p.status = 'A'
                                    AND pd.status = 'A'
                                    AND o.status = 'A'

                            ) op ON
                                o.id_oferta = op.id_oferta
                                AND o.id_distribuidor = op.id_distribuidor

                    ) offers ON
                        pd.id_produto = offers.id_produto
                        AND pd.id_distribuidor = offers.id_distribuidor

                    LEFT JOIN PRODUTO_DETALHES pdet ON
                        p.id_produto = pdet.id_produto

                WHERE
                    ps.id_distribuidor != 0
                    AND ps.status = 'A'
                    AND s.status = 'A'
                    AND gs.status = 'A'
                    AND g.status = 'A'
                    AND t.status = 'A'

                ORDER BY
                    pq.id;
            """

    else:

        produto_query = """
            SELECT
                DISTINCT pq.id, 
                p.id_produto,
                p.sku,
                p.descricao_completa as descricao_produto,
                p.status,
                p.dun14,
                p.tipo_produto,
                p.variante,
                p.volumetria,
                p.unidade_embalagem,
                p.quantidade_embalagem,
                p.unidade_venda,
                p.quant_unid_venda,
                p.tokens_imagem as tokens,
                pd.id_distribuidor,
                d.nome_fantasia as descricao_distribuidor,
                pd.agrupamento_variante,
                pd.cod_prod_distr,
                SUBSTRING(pd.cod_prod_distr, LEN(pd.agrupamento_variante) + 1, LEN(pd.cod_prod_distr)) as cod_frag_distr,
                pd.multiplo_venda,
                pd.ranking,
                pd.unidade_venda as unidade_venda_distribuidor,
                pd.quant_unid_venda as quant_unid_venda_distribuidor,
                pd.giro,
                pd.agrup_familia,
                pd.status as status_distribuidor,
                pd.data_cadastro,
                m.id_marca,
                m.desc_marca as descricao_marca,
                f.id_fornecedor,
                f.desc_fornecedor,
                s.id_subgrupo,
                s.descricao as descricao_subgrupo,
                g.id_grupo,
                g.descricao as descricao_grupo,
                t.id_tipo,
                t.descricao as descricao_tipo,
                pd.estoque,
                ISNULL(ppr.preco_tabela, 0) as preco_tabela,
                odesc.desconto,
                odesc.data_inicio as data_inicio_desconto,
                odesc.data_final as data_final_desconto,
                offers.id_oferta,
                CASE
                    WHEN offers.id_oferta IS NULL 
                        THEN NULL
                    WHEN offers.tipo_oferta = 2 AND offers.ativador = 1
                        THEN 1
                    ELSE
                        0
                END ativador,
                CASE
                    WHEN offers.id_oferta IS NULL 
                        THEN NULL
                    WHEN offers.tipo_oferta = 2 AND offers.bonificado = 1
                        THEN 1
                    ELSE
                        0
                END bonificado,
                CASE
                    WHEN offers.id_oferta IS NULL 
                        THEN NULL
                    WHEN offers.tipo_oferta = 3 AND offers.ativador = 1
                        THEN 1
                    ELSE
                        0
                END escalonado,
                pdet.detalhes

            FROM
                (

                    SELECT
                        thip.id,
                        thip.id_produto,
                        thip.id_distribuidor

                    FROM
                        (

                            SELECT
                                nda.id,
                                nda.id_produto,
                                nda.id_distribuidor

                            FROM
                                (

                                    SELECT
                                        ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as id,
                                        id_produto,
                                        id_distribuidor

                                    FROM
                                        OPENJSON(:produto_input)
                                
                                    WITH
                                        (
                                            id_produto VARCHAR(200)   '$.id_produto',  
                                            id_distribuidor INT       '$.id_distribuidor'
                                        )

                                ) nda
                        
                        ) thip

                ) pq

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    pq.id_produto = pd.id_produto
                    AND pq.id_distribuidor = pd.id_distribuidor

                INNER JOIN PRODUTO p ON
                    pd.id_produto = p.id_produto

                INNER JOIN DISTRIBUIDOR d ON
                    pd.id_distribuidor = d.id_distribuidor

                INNER JOIN MARCA m ON
                    pd.id_marca = m.id_marca
                    AND pd.id_distribuidor = m.id_distribuidor

                INNER JOIN FORNECEDOR f ON
                    pd.id_fornecedor = f.id_fornecedor

                INNER JOIN PRODUTO_SUBGRUPO ps ON
                    pd.id_produto = ps.codigo_produto

                INNER JOIN SUBGRUPO s ON
                    ps.id_subgrupo = s.id_subgrupo
                    AND ps.id_distribuidor = s.id_distribuidor

                INNER JOIN GRUPO_SUBGRUPO gs ON
                    s.id_subgrupo = gs.id_subgrupo
                    AND s.id_distribuidor = gs.id_distribuidor

                INNER JOIN GRUPO g ON
                    gs.id_grupo = g.id_grupo

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

                LEFT JOIN
                (
                    SELECT
                        tpp.id_produto,
                        tpp.id_distribuidor,
                        MIN(tpp.preco_tabela) preco_tabela

                    FROM
                        TABELA_PRECO tp

                        INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                            tp.id_tabela_preco = tpp.id_tabela_preco
                            AND tp.id_distribuidor = tpp.id_distribuidor

                    WHERE
                        tp.id_distribuidor IN :distribuidores
                        AND tp.tabela_padrao = 'S'
                        AND tp.status = 'A'
                        AND tp.dt_inicio <= GETDATE()
                        AND tp.dt_fim >= GETDATE()
                        
                    GROUP BY
                        tpp.id_produto,
                        tpp.id_distribuidor

                ) ppr ON
                    pq.id_produto = ppr.id_produto
                    AND pq.id_distribuidor = ppr.id_distribuidor

                LEFT JOIN 
                (

                    SELECT
                        op.id_produto,
                        o.id_distribuidor,
                        MAX(od.desconto) desconto,
                        o.data_inicio,
                        o.data_final

                    FROM
                        OFERTA o

                        INNER JOIN OFERTA_PRODUTO op ON
                            o.id_oferta = op.id_oferta

                        INNER JOIN OFERTA_DESCONTO od ON
                            o.id_oferta = od.id_oferta

                    WHERE
                        o.tipo_oferta = 1
                        AND o.id_distribuidor IN :distribuidores
                        AND o.limite_ativacao_oferta > 0
                        AND od.desconto > 0
                        AND o.data_inicio <= GETDATE()
                        AND o.data_final >= GETDATE()
                        AND o.status = 'A'
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
                        o.id_oferta,
                        o.id_produto,
                        o.tipo_oferta,
                        o.id_distribuidor,
                        o.ativador,
                        o.bonificado

                    FROM
                        (
                            SELECT
                                o.id_oferta,
                                o.id_distribuidor,
                                op.id_produto,
                                o.tipo_oferta,
                                op.ativador,
                                op.bonificado

                            FROM
                                (
                                                
                                    SELECT
                                        op.id_oferta,
                                        op.id_produto,
                                        1 ativador,
                                        0 bonificado

                                    FROM
                                        OFERTA_PRODUTO op

                                    WHERE
                                        op.status = 'A'

                                    UNION

                                    SELECT
                                        ob.id_oferta,
                                        ob.id_produto,
                                        0 ativador,
                                        1 bonificado

                                    FROM
                                        OFERTA_BONIFICADO ob

                                    WHERE
                                        ob.status = 'A'

                                ) op

                                INNER JOIN OFERTA o ON
                                    op.id_oferta = o.id_oferta

                                INNER JOIN PRODUTO p ON
                                    op.id_produto = p.id_produto

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto
                                    AND o.id_distribuidor = pd.id_distribuidor

                            WHERE
                                o.tipo_oferta IN (2,3)
                                AND o.id_distribuidor IN :distribuidores
                                AND o.limite_ativacao_oferta > 0
                                AND o.data_inicio <= GETDATE()
                                AND o.data_final >= GETDATE()
                                AND o.status = 'A'
                                AND p.status = 'A'
                                AND pd.status = 'A'
                                                        
                        ) o

                ) offers ON
                    pd.id_produto = offers.id_produto
                    AND pd.id_distribuidor = offers.id_distribuidor

                LEFT JOIN PRODUTO_DETALHES pdet ON
                    p.id_produto = pdet.id_produto

            WHERE
                ps.id_distribuidor != 0

            ORDER BY
                pq.id;
        """

    dict_query = {
        "query": produto_query,
        "params": params,
    }

    produtos = dm.raw_sql_return(**dict_query)

    if not produtos:
        return []

    dict_produtos = {
        "prod_dist_list": produtos,
        "id_cliente": id_cliente,
        "bool_marketplace": bool_marketplace,
        "image_replace": image_replace
    }

    response = json_products_like_gen(**dict_produtos)

    return response


def json_ofertas(id_cliente: int = None, **kwargs) -> dict:
    """
    Retorna a contagem total de ofertas e as ofertas paginadas

    Args:
        id_cliente (int, optional): ID do cliente utilizado. Defaults to None.

    kwargs:
        pagina (int): Qual a pagina que deve ser pegue.

        limite (int): Número de ofertas por página.

        id_tipo (int): ID do tipo de produtos da ofertas.

        id_grupo (int): ID do grupo de produtos da ofertas.

        id_subgrupo (int): ID do subgrupo de produtos da ofertas.

        busca (str): Frase utilizada no campo de busca de produtos.

        id_oferta (list|int): Lista de ofertas que devem ser filtradas.

        sku (list|int): Lista de sku de produtos das ofertas que devem ser filtradas.

        id_produto (list|int): Lista de produtos das ofertas que devem ser filtradas.

        estoque (bool): Procura ofertas que tenham produtos com estoque.

        desconto (bool): Procura ofertas que tenham produtos com desconto.

        multiplo_venda (int): Procura ofertas que tenham produtos com os multiplos de venda informado.

        tipo_oferta (int): Procura ofertas de um determinado tipo
            1: Ofertas campanha
            2: Ofertas escalonadas

        marca (list|int): Procura ofertas com produtos das marcas informadas.

        grupos (list|int): Procura ofertas com produtos dos grupos informados.

        subgrupos (list|int): Procura ofertas com produtos dos subgrupos informados.

        ordenar (int): Ordena as ofertas.
            1: Recomendados
            2: Mais recentes
            3: Mais antigas

    Returns:
        dict: Dicionario com a contagem das ofertas e as ofertas paginadas
    """

    # Pegando os dados enviados
    id_distribuidor = int(kwargs.get('id_distribuidor')) if kwargs.get('id_distribuidor') else 0

    busca = kwargs.get("busca") if kwargs.get("busca") else None

    id_tipo = int(kwargs.get("id_tipo")) if kwargs.get("id_tipo") else None
    id_grupo = int(kwargs.get("id_grupo")) if kwargs.get("id_grupo") else None
    id_subgrupo = int(kwargs.get("id_subgrupo")) if kwargs.get("id_subgrupo") else None

    id_oferta = kwargs.get("id_oferta") if kwargs.get("id_oferta") else None

    id_produto = kwargs.get("id_produto") if kwargs.get("id_produto") else None
    sku = kwargs.get("sku") if kwargs.get("sku") else None

    pagina, limite = dm.page_limit_handler(kwargs)

    estoque = True if kwargs.get("estoque") and id_cliente else False
    desconto = True if kwargs.get("desconto") and id_cliente else False
    multiplo_venda = int(kwargs.get("multiplo_venda")) if kwargs.get("multiplo_venda") else 0
    tipo_oferta = int(kwargs.get("tipo_oferta")) if kwargs.get("tipo_oferta") and id_cliente else None
    marca = kwargs.get("marca")
    grupos = kwargs.get("grupos")
    subgrupos = kwargs.get("subgrupos")

    ordenar_por = int(kwargs.get("ordenar")) if kwargs.get("ordenar") else 1 # padrão mais vendidos
    if (ordenar_por > 3 or ordenar_por < 1):
        ordenar_por = 1

    # Pegando os distribuidores validos do cliente
    if not id_cliente:
        query = """
            SELECT
                DISTINCT id_distribuidor

            FROM
                DISTRIBUIDOR

            WHERE
                id_distribuidor != 0
                AND id_distribuidor = CASE
                                          WHEN ISNULL(:id_distribuidor, 0) = 0
                                              THEN id_distribuidor
                                          ELSE
                                              :id_distribuidor
                                      END
                AND status = 'A'
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
                c.id_cliente = :id_cliente
                AND d.id_distribuidor = CASE
                                            WHEN ISNULL(:id_distribuidor, 0) = 0
                                                THEN d.id_distribuidor
                                            ELSE
                                                :id_distribuidor
                                        END
                AND d.id_distribuidor != 0
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'
        """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

    query_obj = {
        "query": query,
        "params": params,
        "raw": True
    }

    distribuidores = dm.raw_sql_return(**query_obj)
    if not distribuidores:
        return {}

    distribuidores = [distribuidor[0] for distribuidor in distribuidores]

    # Fazendo as queries
    params = {
        "id_distribuidor": id_distribuidor,
        "distribuidores": distribuidores,
        "id_cliente": id_cliente,
        "offset": (pagina - 1) * limite,
        "limite": limite,
    }

    string_id_distribuidor_tgs = "AND ps.id_distribuidor = :id_distribuidor"
    string_busca = ""
    string_tgs = ""
    string_id_produto = ""
    string_sku = ""
    string_id_oferta = ""
    string_ordenar_por = ""
    string_estoque = ""
    string_multiplo_venda = ""
    string_desconto = ""
    string_tipo_ofertas = "AND o.tipo_oferta IN (2,3)"
    string_marca = ""
    string_grupos = ""
    string_subgrupos = ""

    ## Procura por campo de busca
    if busca:

        busca = str(busca).upper().split()

        string_busca = ""

        for index, word in enumerate(busca):

            params.update({
                f"word_{index}": f"%{word}%"
            })

            string_busca += f"""AND (
                p.descricao_completa COLLATE Latin1_General_CI_AI LIKE :word_{index}
                OR p.sku LIKE :word_{index}
                OR p.dun14 LIKE :word_{index}
                OR p.id_produto LIKE :word_{index}
                OR m.desc_marca COLLATE Latin1_General_CI_AI LIKE :word_{index}
            )"""

    ## Procura por id_produto
    if id_produto:

        if not isinstance(id_produto, (list, tuple, set)):
            id_produto = [id_produto]

        else:
            id_produto = list(id_produto)

        params.update({
            "id_produto": id_produto
        })

        string_id_produto = "AND p.id_produto IN :id_produto"

    ## Procura por id_produto
    if sku:

        if not isinstance(sku, (list, tuple, set)):
            sku = [sku]

        else:
            sku = list(sku)

        params.update({
            "sku": sku
        })

        string_sku = "AND p.sku IN :sku"

    ## Procura por tipo, grupo e/ou subgrupo
    if id_tipo or id_grupo or id_subgrupo:

        # Pegando os subgrupos
        if id_subgrupo:
            string_tgs += f"AND s.id_subgrupo = :id_subgrupo "
            params["id_subgrupo"] = id_subgrupo

        if id_grupo:
            string_tgs += "AND g.id_grupo = :id_grupo "
            params["id_grupo"] = id_grupo

        if id_tipo:
            string_tgs += "AND t.id_tipo = :id_tipo "
            params["id_tipo"] = id_tipo

    ## Procura por id_oferta
    if id_oferta:

        if not isinstance(id_oferta, (list, tuple, set)):
            id_oferta = [id_oferta]

        else:
            id_oferta = list(id_oferta)

        params.update({
            "id_oferta": id_oferta
        })

        string_id_oferta = "AND o.id_oferta IN :id_oferta"

    ## Procura por estoque
    if estoque and id_cliente:
        string_estoque = "AND pe.qtd_estoque > 0"

    ## Procura por multiplo venda
    if multiplo_venda > 0:
        string_multiplo_venda = "AND pd.multiplo_venda = :multiplo_venda"
        params.update({
            "multiplo_venda": multiplo_venda,
        })

    # Procura por desconto
    if desconto and id_cliente:
        string_desconto = "AND odesc.desconto > 0"

    # Procura por tipo_oferta
    if tipo_oferta:

        if tipo_oferta == 1:
            string_tipo_ofertas = "AND o.tipo_oferta = 2"
        
        elif tipo_oferta == 2:
            string_tipo_ofertas = "AND o.tipo_oferta = 3"

    ## Procura por marca
    if marca:

        if not isinstance(marca, (list, tuple, set)):
            marca = [marca]

        else:
            marca = list(marca)

        params.update({
            "marca": marca
        })

        string_marca = "AND m.id_marca IN :marca"

    ## Procura por grupo e subgrupo
    if grupos:

        string_id_distribuidor_tgs = "AND ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END"

        if not isinstance(grupos, (list, tuple, set)):
            grupos = [grupos]

        else:
            grupos = list(grupos)

        params.update({
            "grupos": grupos
        })

        string_grupos = "AND g.id_grupo IN :grupos"

    if subgrupos:

        string_id_distribuidor_tgs = "AND ps.id_distribuidor = CASE WHEN ISNULL(:id_distribuidor, 0) = 0 THEN ps.id_distribuidor ELSE :id_distribuidor END"

        if not isinstance(subgrupos, (list, tuple, set)):
            subgrupos = [subgrupos]

        else:
            subgrupos = list(subgrupos)

        params.update({
            "subgrupos": subgrupos
        })

        string_subgrupos = "AND s.id_subgrupo IN :subgrupos"

    ## Ordenacao da busca
    dict_ordenacao = {
        1:"o.ordem, o.descricao_oferta",
        2:"o.data_cadastro DESC, o.data_inicio DESC, o.descricao_oferta",
        3:"o.data_cadastro ASC, o.data_inicio ASC, o.descricao_oferta",
    }

    string_ordenar_por = dict_ordenacao.get(ordenar_por)

    query = f"""
        SELECT
            o.id_oferta,
            COUNT(*) OVER() count__

        FROM
            (

                SELECT
                    op.id_oferta,
                    op.id_distribuidor

                FROM
                    (

                        SELECT
                            DISTINCT o.id_oferta,
                            o.id_distribuidor

                        FROM
                            OFERTA o

                            INNER JOIN
                            (
                            
                                SELECT
                                    op.id_oferta,
                                    op.id_produto

                                FROM
                                    OFERTA_PRODUTO op

                                WHERE
                                    op.status = 'A'

                                UNION

                                SELECT
                                    ob.id_oferta,
                                    ob.id_produto

                                FROM
                                    OFERTA_BONIFICADO ob

                                WHERE
                                    ob.status = 'A'
                            
                            ) op ON
                                o.id_oferta = op.id_oferta

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                op.id_produto = pd.id_produto
                                AND o.id_distribuidor = pd.id_distribuidor

                            INNER JOIN PRODUTO p ON
                                pd.id_produto = p.id_produto

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

                            INNER JOIN MARCA m ON
                                pd.id_marca = m.id_marca
                                AND pd.id_distribuidor = m.id_distribuidor

                            LEFT JOIN	
                            (
                                SELECT
                                    op.id_produto,
                                    o.id_distribuidor,
                                    od.desconto

                                FROM
                                    (

                                        SELECT
                                            o.id_oferta

                                        FROM
                                            OFERTA o

                                        WHERE
                                            id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE)

                                        UNION

                                        SELECT
                                            oc.id_oferta

                                        FROM
                                            OFERTA_CLIENTE oc

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
                                    AND o.limite_ativacao_oferta > 0
                                    AND od.desconto > 0
                                    AND o.data_inicio <= GETDATE()
                                    AND o.data_final >= GETDATE()
                                    AND o.status = 'A'
                                    AND op.status = 'A'
                                    AND od.status = 'A'
                                
                            ) odesc ON
                                pd.id_produto = odesc.id_produto
                                AND pd.id_distribuidor = odesc.id_distribuidor

                        WHERE
                            pd.id_distribuidor IN :distribuidores
                            {string_id_oferta}
                            {string_tipo_ofertas}
                            {string_id_produto}
                            {string_sku}
                            {string_busca}
                            {string_tgs}
                            {string_subgrupos}
                            {string_grupos}
                            {string_estoque}
                            {string_multiplo_venda}
                            {string_desconto}
                            {string_marca}
                            {string_id_distribuidor_tgs}
                            AND ps.status = 'A'
                            AND s.status  = 'A'
                            AND gs.status = 'A'
                            AND g.status  = 'A'
                            AND t.status  = 'A'
                            AND m.status  = 'A'

                    ) op

                    INNER JOIN
                    (
                    
                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

                        WHERE
                            oc.id_cliente = :id_cliente
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) oc ON
                        op.id_oferta = oc.id_oferta

                    INNER JOIN VIEW_OFERTAS_MARKETPLACE vom ON
                        op.id_oferta = vom.id_oferta
                        AND op.id_distribuidor = vom.id_distribuidor

            ) op 

            INNER JOIN OFERTA o ON
                op.id_oferta = o.id_oferta
                AND op.id_distribuidor = o.id_distribuidor

        ORDER BY
            {string_ordenar_por}

        OFFSET
            :offset ROWS

        FETCH
            NEXT :limite ROWS ONLY
    """

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
    }

    response = dm.raw_sql_return(**dict_query)
    if not response:
        return {}

    id_oferta = [oferta[0]   for oferta in response]
    counter = response[0][1]

    # Pegando as informações da oferta paginada
    query = f"""
        SELECT
            id_oferta,
            tipo_oferta,
            id_produto,
            id_distribuidor,
            id_marca,
            descricao_marca,
            sku,
            status_produto,
            descricao_produto,
            unidade_embalagem,
            quantidade_embalagem,
            unidade_venda,
            quant_unid_venda,
            preco_tabela,
            multiplo_venda,
            ranking,
            id_subgrupo,
            descricao_subgrupo,
            id_grupo,
            descricao_grupo,
            id_tipo,
            descricao_tipo,
            estoque,
            tokens,
            desconto,
            data_inicio_desconto,
            data_final_desconto,
            descricao_oferta,
            quantidade_limite,
            quantidade_pontos,
            maxima_ativacao,
            data_inicio,
            data_final,
            quantidade_produto,
            quantidade_bonificado,
            quantidade,
            automatica,
            total_desconto,
            sequencia,
            faixa,
            desconto_faixa,
            unificada,
            quantidade_bonificada,
            ordem,
            destaque,
            tipo_campanha,
            quantidade_ativar,
            valor_ativar,
            quantidade_minima,
            valor_minimo,
            status_oferta,
            quantidade_min_ativacao,
            valor_min_ativacao,
            quantidade_bonificada,
            status_bonificado,
            ativador,
            bonificado,
            data_cadastro

        FROM
            (
                SELECT
                    DISTINCT o.id_oferta,
                    o.tipo_oferta,
                    p.id_produto,
                    pd.id_distribuidor,
                    m.id_marca,
                    m.desc_marca AS descricao_marca,
                    p.sku,
                    p.status as status_produto,
                    p.descricao_completa AS descricao_produto,
                    p.unidade_embalagem,
                    p.quantidade_embalagem,
                    p.unidade_venda,
                    p.quant_unid_venda,
                    pp.preco_tabela,
                    pd.multiplo_venda,
                    pd.ranking,
                    s.id_subgrupo,
                    s.descricao as descricao_subgrupo,
                    s.ranking as ranking_subgrupo,
                    g.id_grupo,
                    g.descricao as descricao_grupo,
                    g.ranking as ranking_grupo,
                    t.id_tipo,
                    t.descricao as descricao_tipo,
                    t.ranking as ranking_tipo,
                    ISNULL(pe.qtd_estoque,0) AS estoque,
                    p.tokens_imagem as tokens,
                    odesc.desconto,
                    odesc.data_inicio as data_inicio_desconto,
                    odesc.data_final as data_final_desconto,
                    o.descricao_oferta,
                    o.limite_ativacao_oferta AS quantidade_limite,
                    o.limite_ativacao_oferta AS quantidade_pontos,
                    o.limite_ativacao_cliente AS maxima_ativacao,
                    o.data_inicio,
                    o.data_final,
                    0 AS quantidade_produto,
                    0 AS quantidade_bonificado,
                    0 AS quantidade,
                    0 AS automatica,
                    0 total_desconto,
                    oef.sequencia,
                    oef.faixa,
                    oef.desconto as desconto_faixa,
                    o.produto_agrupado as "unificada",
                    o.ordem,
                    CASE WHEN o.ordem = 1 THEN 1 ELSE 0 END AS destaque,
                    CASE WHEN o.tipo_oferta = 2 THEN 'W' ELSE NULL END AS tipo_campanha,
                    CASE WHEN o.operador = 'Q' THEN necessario_para_ativar ELSE 0 END AS quantidade_ativar,
                    CASE WHEN o.operador = 'V' THEN necessario_para_ativar ELSE 0 END AS valor_ativar,
                    CASE WHEN o.operador = 'Q' THEN necessario_para_ativar ELSE 0 END AS quantidade_minima,
                    CASE WHEN o.operador = 'V' THEN necessario_para_ativar ELSE 0 END AS valor_minimo,
                    o.status as status_oferta,
                    CASE WHEN o.operador = 'Q' THEN op.quantidade_min_ativacao ELSE 0 END AS quantidade_min_ativacao,
                    CASE WHEN o.operador = 'V' THEN op.valor_min_ativacao ELSE 0 END AS valor_min_ativacao,
                    ob.quantidade_bonificada,
                    ob.status status_bonificado,
                    CASE
                        WHEN o.tipo_oferta = 2 AND EXISTS (SELECT 1 FROM OFERTA_PRODUTO WHERE id_oferta = o.id_oferta AND id_produto = p.id_produto AND status = 'A') THEN 1
                        ELSE 0
                    END ativador,
                    CASE 
                        WHEN o.tipo_oferta = 2 AND EXISTS (SELECT 1 FROM OFERTA_BONIFICADO WHERE id_oferta = o.id_oferta AND id_produto = p.id_produto AND status = 'A') THEN 1
                        ELSE 0
                    END bonificado,
                    o.data_cadastro

                FROM
                    OFERTA o

                    INNER JOIN
                    (

                        SELECT
                            op.id_oferta,
                            op.id_produto

                        FROM
                            OFERTA_PRODUTO op

                        WHERE
                            op.status = 'A'

                        UNION

                        SELECT
                            ob.id_oferta,
                            ob.id_produto

                        FROM
                            OFERTA_BONIFICADO ob	

                        WHERE
                            ob.status = 'A'

                    ) offers_produto ON
                        o.id_oferta = offers_produto.id_oferta

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        offers_produto.id_produto = pd.id_produto
                        AND o.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN 
                    (
                        SELECT
                            tpp.id_produto,
                            tpp.id_distribuidor,
                            MIN(tpp.preco_tabela) preco_tabela

                        FROM
                            TABELA_PRECO tp

                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                tp.id_tabela_preco = tpp.id_tabela_preco
                                AND tp.id_distribuidor = tpp.id_distribuidor

                        WHERE
                            tp.id_distribuidor IN :distribuidores
                            AND (
                                    (
                                        tp.tabela_padrao = 'S'
                                    )
                                        OR
                                    (
                                        tp.id_tabela_preco IN (
                                                                    SELECT
                                                                        id_tabela_preco

                                                                    FROM
                                                                        TABELA_PRECO_CLIENTE

                                                                    WHERE
                                                                        status = 'A'
                                                                        AND id_distribuidor IN :distribuidores
                                                                        AND id_cliente = :id_cliente
                                                                )
                                    )
                                )
                            AND tp.status = 'A'
                            AND tp.dt_inicio <= GETDATE()
                            AND tp.dt_fim >= GETDATE()

                        GROUP BY
                            tpp.id_produto,
                            tpp.id_distribuidor

                    ) pp ON
                        pd.id_produto = pp.id_produto
                        AND pd.id_distribuidor = pp.id_distribuidor

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
                                                
                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

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
                            AND o.limite_ativacao_oferta > 0
                            AND od.desconto > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND o.status = 'A'
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

                    LEFT JOIN PRODUTO_ESTOQUE pe ON
                        pd.id_produto = pe.id_produto
                        AND pd.id_distribuidor = pe.id_distribuidor 

                    LEFT JOIN OFERTA_PRODUTO op ON
                        o.id_oferta = op.id_oferta
                        AND op.status = 'A'

                    LEFT JOIN OFERTA_BONIFICADO ob ON
                        o.id_oferta = ob.id_oferta
                        AND ob.status = 'A'

                    LEFT JOIN OFERTA_ESCALONADO_FAIXA oef ON
                        o.id_oferta = oef.id_oferta
                        AND oef.status = 'A'

                WHERE
                    o.id_oferta IN :ofertas
                    AND ps.id_distribuidor != 0
                    AND ps.status = 'A'
                    AND s.status = 'A'
                    AND gs.status = 'A'
                    AND g.status = 'A'
                    AND t.status = 'A'
            ) o

        ORDER BY
            {string_ordenar_por},
            CASE WHEN estoque > 0 THEN 1 ELSE 0 END DESC,
            CASE WHEN ranking > 0 THEN ranking ELSE 999999 END,
            sequencia,
            faixa,
            CASE WHEN ranking_tipo > 0 THEN ranking_tipo ELSE 999999 END,
            CASE WHEN ranking_grupo > 0 THEN ranking_grupo ELSE 999999 END,
            CASE WHEN ranking_subgrupo > 0 THEN ranking_subgrupo ELSE 999999 END
    """

    params = {
        "distribuidores": distribuidores,
        "id_cliente": id_cliente,
        "ofertas": id_oferta,
    }

    dict_query = {
        "query": query,
        "params": params,
    }

    response = dm.raw_sql_return(**dict_query)
    if not response:
        return {}

    dict_ofertas = {
        "prod_dist_list": response,
        "id_cliente": id_cliente,
        "image_replace": "150"
    }

    ofertas = json_ofertas_like_gen(**dict_ofertas)

    if not ofertas:
        return {}

    data = {
        "counter": counter,
        "ofertas": ofertas
    }

    return data


def json_cupons(id_cliente: int, id_distribuidor: int, **kwargs) -> dict:
    """
    Retorna a contagem total de cupons e os cupons paginados

    Args:
        id_cliente (int): ID do cliente utilizado.
        id_distribuidor (int): ID do distribuidor utilizado.

    kwargs:
        pagina (int): Qual a pagina que deve ser pegue.

        limite (int): Número de ofertas por página.

        id_cupom (list|int): Lista de cupons que devem ser filtradas.

    Returns:
        dict: Dicionario com a contagem dos cupons e os cupons paginados
    """
    
    # Pegando os dados enviados
    id_cliente = int(id_cliente) if id_cliente else None
    id_distribuidor = int(id_distribuidor) if id_distribuidor else 0

    id_cupom = kwargs.get("id_cupom") if kwargs.get("id_cupom") else None

    pagina, limite = dm.page_limit_handler(kwargs)

    # Pegando os distribuidores validos do cliente
    if not id_cliente:
        query = """
            SELECT
                DISTINCT id_distribuidor

            FROM
                DISTRIBUIDOR

            WHERE
                id_distribuidor = CASE
                                      WHEN ISNULL(:id_distribuidor, 0) = 0
                                          THEN id_distribuidor
                                      ELSE
                                          :id_distribuidor
                                  END
                AND id_distribuidor != 0
                AND status = 'A'
                
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
                c.id_cliente = :id_cliente
                AND d.id_distribuidor != 0
                AND d.id_distribuidor = CASE
                                            WHEN ISNULL(:id_distribuidor, 0) = 0
                                                THEN d.id_distribuidor
                                            ELSE
                                                :id_distribuidor
                                        END
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'                
        """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

    query_obj = {
        "query": query,
        "params": params,
        "raw": True
    }

    distribuidores = dm.raw_sql_return(**query_obj)
    distribuidores = [distribuidor[0] for distribuidor in distribuidores]

    if not distribuidores:
        return {}

    # Fazendo as queries
    params = {
        "id_distribuidor": id_distribuidor,
        "distribuidores": distribuidores,
        "id_cliente": id_cliente,
        "offset": (pagina - 1) * limite,
        "limite": limite
    }

    string_id_cupom = ""

    ## Procura por id_cupom
    if id_cupom:

        if not isinstance(id_cupom, (list, tuple, set)):
            id_cupom = [id_cupom]

        else:
            id_cupom = list(id_cupom)

        params.update({
            "id_cupom": id_cupom
        })

        string_id_cupom = "AND cp.id_cupom IN :id_cupom"

    # Pegando os Cupons
    query = f"""
        SELECT
            cp.id_cupom,
            cp.id_distribuidor,
            cp.codigo_cupom,
            cp.titulo,
            cp.descricao,
            cp.data_inicio,
            cp.data_final,
            cp.tipo_cupom,
            tpcp.descricao as descricao_tipo_cupom,
            ISNULL(cp.desconto_valor, 0) as desconto_valor,
            ISNULL(cp.desconto_percentual, 0) as desconto_percentual,
            CASE
                WHEN ISNULL(cp.desconto_valor, 0) != 0
                    THEN 2
                ELSE
                    1
            END tipo_desconto,
            CASE
                WHEN ISNULL(cp.desconto_valor, 0) != 0
                    THEN 'Valor'
                ELSE
                    'Percentual'
            END tipo_desconto_descricao,
            cp.valor_limite,
            cp.valor_ativar,
            cp.termos_uso,
            cp.status,
            ISNULL(cp.qt_reutiliza, 0) as reutiliza,
            0 oculto,
            CASE WHEN cp.bloqueado = 'N' THEN 0 ELSE 1 END bloqueado,
            cp.tipo_itens,
            tpcpit.descricao as descricao_tipo_itens,
            citns.id_objeto as itens,
            CUPONS.count__

        FROM
            (
            
                SELECT
                    CUPONS.id_cupom,
                    CUPONS.id_distribuidor,
                    CUPONS.id_tipo_cupom,
                    CUPONS.id_tipo_itens,
                    CUPONS_VALIDOS.data_cadastro,
                    COUNT(*) OVER() count__

                FROM
                    (
                        -- Para todos
                        SELECT
                            DISTINCT cp.id_cupom,
                            cp.id_distribuidor,
                            cp.tipo_cupom as id_tipo_cupom,
                            cp.tipo_itens as id_tipo_itens

                        FROM
                            CUPOM cp

                        WHERE
                            cp.tipo_itens = 1

                        UNION

                        -- Para o produto
                        SELECT
                            DISTINCT cp.id_cupom,
                            cp.id_distribuidor,
                            cp.tipo_cupom as id_tipo_cupom,
                            cp.tipo_itens as id_tipo_itens

                        FROM
                            CUPOM cp

                            INNER JOIN CUPOM_ITENS citns ON
                                cp.id_cupom = citns.id_cupom

                            INNER JOIN 
                            (
                                SELECT
                                    pd.id_produto,
                                    0 as id_distribuidor

                                FROM
                                    PRODUTO_DISTRIBUIDOR pd

                                WHERE
                                    status = 'A'
                                    AND pd.id_distribuidor IN :distribuidores

                                UNION

                                SELECT
                                    pd.id_produto,
                                    pd.id_distribuidor

                                FROM
                                    PRODUTO_DISTRIBUIDOR pd
                                
                                WHERE
                                    status = 'A'
                                    AND pd.id_distribuidor IN :distribuidores

                            ) pds ON
                                cp.id_distribuidor = pds.id_distribuidor
                                AND citns.id_objeto = pds.id_produto

                            INNER JOIN PRODUTO p ON
                                pds.id_produto = p.id_produto

                        WHERE
                            cp.tipo_itens = 2
                            AND citns.status = 'A'
                            AND p.status = 'A'

                        UNION

                        -- Para marcas
                        SELECT
                            DISTINCT cp.id_cupom,
                            cp.id_distribuidor,
                            cp.tipo_cupom as id_tipo_cupom,
                            cp.tipo_itens as id_tipo_itens

                        FROM
                            CUPOM cp

                            INNER JOIN CUPOM_ITENS citns ON
                                cp.id_cupom = citns.id_cupom

                            INNER JOIN
                            (
                                SELECT
                                    CONVERT(VARCHAR(10), m.id_marca) as id_marca,
                                    0 as id_distribuidor

                                FROM
                                    MARCA m

                                WHERE
                                    status = 'A'
                                    AND m.id_distribuidor IN :distribuidores

                                UNION

                                SELECT
                                    CONVERT(VARCHAR(10), m.id_marca) as id_marca,
                                    m.id_distribuidor

                                FROM
                                    MARCA m

                                WHERE
                                    status = 'A'
                                    AND m.id_distribuidor IN :distribuidores
                            ) m ON
                                citns.id_objeto = m.id_marca
                                AND cp.id_distribuidor = m.id_distribuidor
                    
                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON 
                                m.id_marca = pd.id_marca 
                        
                            INNER JOIN PRODUTO p ON 
                                pd.id_produto = p.id_produto  

                        WHERE
                            cp.tipo_itens = 3
                            AND p.status = 'A' 
                            AND pd.status = 'A'
                            AND citns.status = 'A'

                        UNION

                        -- Para grupo
                        SELECT
                            DISTINCT cp.id_cupom,
                            cp.id_distribuidor,
                            cp.tipo_cupom as id_tipo_cupom,
                            cp.tipo_itens as id_tipo_itens

                        FROM
                            CUPOM cp

                            INNER JOIN CUPOM_ITENS citns ON
                                cp.id_cupom = citns.id_cupom

                            INNER JOIN GRUPO g ON
                                citns.id_objeto = CONVERT(VARCHAR(10), g.id_grupo)
                                AND cp.id_distribuidor = g.id_distribuidor

                            INNER JOIN TIPO t ON
                                g.tipo_pai = t.id_tipo

                            INNER JOIN GRUPO_SUBGRUPO gs ON
                                g.id_grupo = gs.id_grupo

                            INNER JOIN SUBGRUPO s ON
                                gs.id_subgrupo = s.id_subgrupo

                            INNER JOIN PRODUTO_SUBGRUPO ps ON
                                s.id_subgrupo = ps.id_subgrupo
                                AND s.id_distribuidor = ps.id_distribuidor

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                ps.codigo_produto = pd.id_produto
                                AND ps.id_distribuidor = pd.id_distribuidor

                            INNER JOIN PRODUTO p ON
                                pd.id_produto = p.id_produto

                        WHERE
                            cp.tipo_itens = 4
                            AND citns.status = 'A'
                            AND p.status = 'A'
                            AND pd.status = 'A'
                            AND t.status = 'A'
                            AND g.status = 'A'
                            AND gs.status = 'A'
                            AND s.status = 'A'
                            AND ps.status = 'A'

                        UNION

                        -- Para subgrupo
                        SELECT
                            DISTINCT cp.id_cupom,
                            cp.id_distribuidor,
                            cp.tipo_cupom as id_tipo_cupom,
                            cp.tipo_itens as id_tipo_itens

                        FROM
                            CUPOM cp

                            INNER JOIN CUPOM_ITENS citns ON
                                cp.id_cupom = citns.id_cupom

                            INNER JOIN SUBGRUPO s ON
                                citns.id_objeto = CONVERT(VARCHAR(10), s.id_subgrupo)
                                AND cp.id_distribuidor = s.id_distribuidor

                            INNER JOIN GRUPO_SUBGRUPO gs ON
                                s.id_subgrupo = gs.id_subgrupo
                                AND s.id_distribuidor = gs.id_distribuidor

                            INNER JOIN GRUPO g ON
                                gs.id_grupo = g.id_grupo

                            INNER JOIN TIPO t ON
                                g.tipo_pai = t.id_tipo

                            INNER JOIN PRODUTO_SUBGRUPO ps ON
                                s.id_subgrupo = ps.id_subgrupo
                                AND s.id_distribuidor = ps.id_distribuidor

                            INNER JOIN PRODUTO p ON
                                ps.codigo_produto = p.id_produto

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                p.id_produto = pd.id_produto
                                AND ps.id_distribuidor = pd.id_distribuidor


                        WHERE
                            cp.tipo_itens = 5
                            AND citns.status = 'A'
                            AND p.status = 'A'
                            AND pd.status = 'A'
                            AND t.status = 'A'
                            AND g.status = 'A'
                            AND gs.status = 'A'
                            AND s.status = 'A'
                            AND ps.status = 'A'

                    ) CUPONS

                    INNER JOIN 
                    (
                        
                        SELECT
                            cp.id_cupom,
                            cp.id_distribuidor,
                            tpcp.id_tipo_cupom,
                            tpcpit.id_tipo_itens,
                            CUPONS.data_cadastro
            
                        FROM
                            (

                                (
                                    SELECT
                                        cp.id_cupom,
                                        cp.id_distribuidor,
                                        cp.data_cadastro

                                    FROM
                                        CUPOM cp

                                    WHERE
                                        id_cupom NOT IN (SELECT id_cupom FROM CUPOM_CLIENTE)
                                        AND lista_cliente IS NULL
                                        AND oculto = 'N'

                                    UNION

                                    SELECT
                                        cp.id_cupom,
                                        cp.id_distribuidor,
                                        cpco.data_habilitado as data_cadastro

                                    FROM
                                        CUPOM cp

                                        INNER JOIN CUPOM_CLIENTE_OCULTO cpco ON
                                            cp.id_cupom = cpco.id_cupom

                                    WHERE
                                        cpco.id_cliente = :id_cliente
                                        AND cp.lista_cliente IS NULL
                                        AND cp.oculto != 'N'
                                        AND cpco.status = 'A'
                                )

                                UNION

                                (
                                    SELECT
                                        cp.id_cupom,
                                        cp.id_distribuidor,
                                        cp.data_cadastro

                                    FROM
                                        CUPOM cp

                                        INNER JOIN CUPOM_CLIENTE cpc ON
                                            cp.id_cupom = cpc.id_cupom
                                            AND cp.lista_cliente = cpc.id_lista_cliente

                                        INNER JOIN LISTA_SEGMENTACAO_USUARIO lsu ON
                                            cpc.id_lista_cliente = lsu.id_lista
                                            AND cpc.id_cliente = lsu.id_cliente

                                        INNER JOIN LISTA_SEGMENTACAO ls ON
                                            lsu.id_lista = ls.id_lista

                                    WHERE
                                        lsu.id_cliente = :id_cliente
                                        AND ls.id_distribuidor IN :distribuidores
                                        AND cp.lista_cliente IS NOT NULL
                                        AND cp.oculto = 'N'
                                        AND ls.d_e_l_e_t_ = 0
                                        AND ls.status = 'A'          
                                        AND cpc.status = 'A'   

                                    UNION

                                    SELECT
                                        cp.id_cupom,
                                        cp.id_distribuidor,
                                        cpco.data_habilitado as data_cadastro

                                    FROM
                                        CUPOM cp

                                        INNER JOIN CUPOM_CLIENTE cpc ON
                                            cp.id_cupom = cpc.id_cupom
                                            AND cp.lista_cliente = cpc.id_lista_cliente

                                        INNER JOIN LISTA_SEGMENTACAO_USUARIO lsu ON
                                            cpc.id_lista_cliente = lsu.id_lista
                                            AND cpc.id_cliente = lsu.id_cliente

                                        INNER JOIN LISTA_SEGMENTACAO ls ON
                                            lsu.id_lista = ls.id_lista

                                        INNER JOIN CUPOM_CLIENTE_OCULTO cpco ON
                                            cpc.id_cupom = cpco.id_cupom
                                            AND cpc.id_cliente = cpco.id_cliente

                                    WHERE
                                        ls.id_distribuidor IN :distribuidores
                                        AND lsu.id_cliente = :id_cliente
                                        AND cp.lista_cliente IS NOT NULL
                                        AND cp.oculto != 'N'
                                        AND ls.d_e_l_e_t_ = 0
                                        AND cpc.status = 'A'
                                        AND ls.status = 'A'     
                                        AND cpco.status = 'A'

                                )

                            ) CUPONS

                            INNER JOIN CUPOM cp ON
                                CUPONS.id_cupom = cp.id_cupom
                                AND CUPONS.id_distribuidor = cp.id_distribuidor

                            INNER JOIN TIPO_CUPOM tpcp ON
                                cp.tipo_cupom = tpcp.id_tipo_cupom

                            INNER JOIN TIPO_CUPOM_ITENS tpcpit ON
                                cp.tipo_itens = tpcpit.id_tipo_itens

                        WHERE
                            (
                                (
                                    cp.id_distribuidor IN :distribuidores
                                )
                                    OR
                                (
                                    cp.id_distribuidor = 0
                                    AND :id_distribuidor = 0
                                )
                            )
                            {string_id_cupom}
                            AND cp.qt_reutiliza > 0
                            AND (
                                    (
                                        cp.desconto_percentual > 0
                                        AND cp.desconto_percentual <= 100
                                        AND ISNULL(cp.desconto_valor, 0) = 0
                                    )
                                        OR
                                    (
                                        cp.desconto_valor > 0
                                        AND ISNULL(cp.desconto_percentual, 0) = 0
                                    )
                                )
                            AND cp.status = 'A'
                            AND cp.bloqueado = 'N'
                            AND tpcp.status = 'A'
                            AND tpcpit.status = 'A'

                    ) CUPONS_VALIDOS ON
                        CUPONS.id_cupom = CUPONS_VALIDOS.id_cupom
                        AND CUPONS.id_distribuidor = CUPONS_VALIDOS.id_distribuidor
                        AND CUPONS.id_tipo_cupom = CUPONS_VALIDOS.id_tipo_cupom
                        AND CUPONS.id_tipo_itens = CUPONS_VALIDOS.id_tipo_itens

                    INNER JOIN CUPOM cp ON
                        CUPONS_VALIDOS.id_cupom = cp.id_cupom
                        AND CUPONS_VALIDOS.id_distribuidor = cp.id_distribuidor
                        AND CUPONS_VALIDOS.id_tipo_cupom = cp.tipo_cupom
                        AND CUPONS_VALIDOS.id_tipo_itens = cp.tipo_itens

                ORDER BY
                    cp.titulo,
                    cp.descricao

                OFFSET
                    :offset ROWS

                FETCH
                    NEXT :limite ROWS ONLY
                    
            ) CUPONS

            INNER JOIN CUPOM cp ON
                CUPONS.id_cupom = cp.id_cupom
                AND CUPONS.id_distribuidor = cp.id_distribuidor
                AND CUPONS.id_tipo_cupom = cp.tipo_cupom
                AND CUPONS.id_tipo_itens = cp.tipo_itens

            INNER JOIN TIPO_CUPOM tpcp ON
                cp.tipo_cupom = tpcp.id_tipo_cupom

            INNER JOIN TIPO_CUPOM_ITENS tpcpit ON
                cp.tipo_itens = tpcpit.id_tipo_itens

            INNER JOIN
            (
                SELECT
                    DISTINCT cp.id_cupom,
                    cp.id_distribuidor,
                    NULL as id_objeto

                FROM
                    CUPOM cp

                WHERE
                    cp.tipo_itens = 1

                UNION

                -- Para o produto
                SELECT
                    DISTINCT cp.id_cupom,
                    cp.id_distribuidor,
                    citns.id_objeto

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_ITENS citns ON
                        cp.id_cupom = citns.id_cupom

                    INNER JOIN 
                    (
                        SELECT
                            pd.id_produto,
                            0 as id_distribuidor

                        FROM
                            PRODUTO_DISTRIBUIDOR pd

                        WHERE
                            status = 'A'
                            AND pd.id_distribuidor IN :distribuidores

                        UNION

                        SELECT
                            pd.id_produto,
                            pd.id_distribuidor

                        FROM
                            PRODUTO_DISTRIBUIDOR pd
                                
                        WHERE
                            status = 'A'
                            AND pd.id_distribuidor IN :distribuidores

                    ) pds ON
                        cp.id_distribuidor = pds.id_distribuidor
                        AND citns.id_objeto = pds.id_produto

                    INNER JOIN PRODUTO p ON
                        pds.id_produto = p.id_produto

                WHERE
                    cp.tipo_itens = 2
                    AND citns.status = 'A'
                    AND p.status = 'A'

                UNION

                -- Para marcas
                SELECT
                    DISTINCT cp.id_cupom,
                    cp.id_distribuidor,
                    citns.id_objeto

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_ITENS citns ON
                        cp.id_cupom = citns.id_cupom

                    INNER JOIN
                    (
                        SELECT
                            CONVERT(VARCHAR(10), m.id_marca) as id_marca,
                            0 as id_distribuidor

                        FROM
                            MARCA m

                        WHERE
                            status = 'A'
                            AND m.id_distribuidor IN :distribuidores

                        UNION

                        SELECT
                            CONVERT(VARCHAR(10), m.id_marca) as id_marca,
                            m.id_distribuidor

                        FROM
                            MARCA m

                        WHERE
                            status = 'A'
                            AND m.id_distribuidor IN :distribuidores

                    ) m ON
                        citns.id_objeto = m.id_marca
                        AND cp.id_distribuidor = m.id_distribuidor
                    
                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON 
                        m.id_marca = pd.id_marca 
                        
                    INNER JOIN PRODUTO p ON 
                        pd.id_produto = p.id_produto  

                WHERE
                    cp.tipo_itens = 3
                    AND p.status = 'A' 
                    AND pd.status = 'A'
                    AND citns.status = 'A'

                UNION

                -- Para grupo
                SELECT
                    DISTINCT cp.id_cupom,
                    cp.id_distribuidor,
                    citns.id_objeto

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_ITENS citns ON
                        cp.id_cupom = citns.id_cupom

                    INNER JOIN GRUPO g ON
                        citns.id_objeto = CONVERT(VARCHAR(10), g.id_grupo)
                        AND cp.id_distribuidor = g.id_distribuidor

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        g.id_grupo = gs.id_grupo

                    INNER JOIN SUBGRUPO s ON
                        gs.id_subgrupo = s.id_subgrupo

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        s.id_subgrupo = ps.id_subgrupo
                        AND s.id_distribuidor = ps.id_distribuidor

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        ps.codigo_produto = pd.id_produto 
                        AND ps.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                WHERE
                    cp.tipo_itens = 4
                    AND citns.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND t.status = 'A'
                    AND g.status = 'A'
                    AND gs.status = 'A'
                    AND s.status = 'A'
                    AND ps.status = 'A'

                UNION

                -- Para subgrupo
                SELECT
                    DISTINCT cp.id_cupom,
                    cp.id_distribuidor,
                    citns.id_objeto

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_ITENS citns ON
                        cp.id_cupom = citns.id_cupom

                    INNER JOIN SUBGRUPO s ON
                        citns.id_objeto = CONVERT(VARCHAR(10), s.id_subgrupo)
                        AND cp.id_distribuidor = s.id_distribuidor

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        s.id_subgrupo = gs.id_subgrupo
                        AND s.id_distribuidor = gs.id_distribuidor

                    INNER JOIN GRUPO g ON
                        gs.id_grupo = g.id_grupo

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        s.id_subgrupo = ps.id_subgrupo
                        AND s.id_distribuidor = ps.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        ps.codigo_produto = p.id_produto

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto
                        AND ps.id_distribuidor = pd.id_distribuidor

                WHERE
                    cp.tipo_itens = 5
                    AND citns.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND t.status = 'A'
                    AND g.status = 'A'
                    AND gs.status = 'A'
                    AND s.status = 'A'
                    AND ps.status = 'A'

            ) citns ON
                cp.id_cupom = citns.id_cupom
                AND cp.id_distribuidor = citns.id_distribuidor
            
        ORDER BY
            CASE WHEN cp.oculto = 'N' THEN 0 ELSE 1 END DESC,
            CUPONS.data_cadastro DESC,
            cp.data_inicio DESC,
            cp.titulo,
            cp.descricao,
            cp.id_cupom;
    """

    dict_query = {
        "query": query,
        "params": params,
    }

    query = dm.raw_sql_return(**dict_query)

    if not query:
        return {}

    counter = query[0].get('count__')
    cupons = json_cupons_like_gen(query)

    if not cupons:
        return {}

    data = {
        "counter": counter,
        "cupons": cupons
    }

    return data


def json_cupom_validado(id_cliente: int, id_cupom: int, **kwargs) -> list:
    """
    Valida um cupom para um cliente

    Args:
        id_cliente (int): ID do cliente utilizado.
        id_cupom (int): ID do cupom que será validado.

    kwargs:
        data_hora (datetime): data e hora na qual o cupom foi considerado valido.

    Returns:
        dict: Dicionario com a contagem dos cupons e os cupons paginados
    """

    data_hora = kwargs.get("data_hora")
    if not data_hora or not isinstance(data_hora, datetime):
        data_hora = datetime.now()

    # Distribuidores do clinte
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
            c.id_cliente = :id_cliente
            AND d.id_distribuidor != 0
            AND cd.d_e_l_e_t_ = 0
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND d.status = 'A'
    """

    params = {
        "id_cliente": id_cliente
    }

    query_obj = {
        "query": query,
        "params": params,
        "raw": True
    }

    distribuidores = dm.raw_sql_return(**query_obj)
    if not distribuidores:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Sem distribuidores cadastrados."
                      })

    distribuidores = [distribuidor[0] for distribuidor in distribuidores]

    # Cupons do cliente
    query = """
        SELECT
            TOP 1 cp.id_distribuidor,
            cp.codigo_cupom,
            cp.data_inicio,
            cp.data_final,
            ISNULL(cp.desconto_valor, 0) as desconto_valor,
            ISNULL(cp.desconto_percentual, 0) as desconto_percentual,
            ISNULL(cp.qt_reutiliza, 0) as qt_reutilizacao,
            UPPER(cp.status) as cupom_status,
            cp.qt_reutiliza,
            UPPER(cp.oculto) as oculto,
            UPPER(cp.bloqueado) as bloqueado,
            tpcp.id_tipo_cupom,
            UPPER(tpcp.status) as tipo_cupom_status,
            tpcpit.id_tipo_itens,
            UPPER(tpcpit.status) as tipo_itens_cupom_status

        FROM
            CUPOM cp

            INNER JOIN TIPO_CUPOM tpcp ON
                cp.tipo_cupom = tpcp.id_tipo_cupom

            INNER JOIN TIPO_CUPOM_ITENS tpcpit ON
                cp.tipo_itens = tpcpit.id_tipo_itens

        WHERE
            cp.id_cupom = :id_cupom
    """

    params = {
        "id_cupom": id_cupom
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
    }

    cupom = dm.raw_sql_return(**dict_query)

    if not cupom:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom informado não cadastrado."
                      })

    # Desconto do cupom
    valor = cupom.get("desconto_valor")
    percentual = cupom.get("desconto_percentual")

    if (valor <= 0 and percentual <= 0):
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom sem desconto aplicado."
                      })

    if valor > 0 and percentual > 0:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom com desconto em valor e percentual."
                      })

    if percentual > 100:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom de porcentagem com desconto acima de 100%."
                      })

    # Status do cupom
    status_cupom = cupom.get("cupom_status")
    status_tipo = cupom.get("tipo_cupom_status")
    status_itens = cupom.get("tipo_itens_cupom_status")

    if status_cupom != 'A':
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom inativado pelo distribuidor."
                      })

    if status_tipo != 'A':
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom com tipo invalidado."
                      })

    if status_itens != 'A':
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom com tipo de itens invalidado."
                      })

    # Distribuidor do cupom
    id_distribuidor = cupom.get("id_distribuidor")

    if str(id_distribuidor) != '0':
        if id_distribuidor not in distribuidores:
            return False, ({
                              "id_cupom": id_cupom,
                              "valido": False,
                              "motivo": "Cupom não acessível ao cliente escolhido."
                          })

    # Data do cupom
    data_final = cupom.get("data_final")

    if data_final:
        data_final = datetime.strptime(data_final, '%Y-%m-%d %H:%M:%S.%f')
    else:
        data_final = datetime(3000,1,1,0,0,0,0)

    if data_final < data_hora:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom expirado."
                      })

    data_inicio = cupom.get("data_inicio")
    
    if data_inicio:
        data_inicio = datetime.strptime(data_inicio, '%Y-%m-%d %H:%M:%S.%f')
    else:
        data_inicio = datetime(1900,1,1,0,0,0,0)

    if data_inicio > data_hora:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom não valido ainda."
                      })

    # Bloqueio
    bloqueado = cupom.get("bloqueado")

    if bloqueado != 'N':
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom bloqueado."
                      })

    # Reutilização
    qt_reutilizacao = cupom.get("qt_reutilizacao")

    if qt_reutilizacao <= 0:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom bloqueado excesso de utilização."
                      })

    # Cliente-Cupom
    query = """
        SELECT
            TOP 1 id_cupom

        FROM
            (

                SELECT
                    cp.id_cupom,
                    cp.id_distribuidor

                FROM
                    CUPOM cp

                WHERE
                    lista_cliente IS NULL
                    AND id_cupom NOT IN (SELECT id_cupom FROM CUPOM_CLIENTE)

                UNION

                SELECT
                    cp.id_cupom,
                    cp.id_distribuidor

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_CLIENTE cpc ON
                        cp.id_cupom = cpc.id_cupom
                        AND cp.lista_cliente = cpc.id_lista_cliente

                    INNER JOIN LISTA_SEGMENTACAO_USUARIO lsu ON
                        cpc.id_lista_cliente = lsu.id_lista
                        AND cpc.id_cliente = lsu.id_cliente

                    INNER JOIN LISTA_SEGMENTACAO ls ON
                        lsu.id_lista = ls.id_lista

                WHERE
                    cp.lista_cliente IS NOT NULL
                    AND lsu.id_cliente = :id_cliente
                    AND ls.id_distribuidor IN :distribuidores
                    AND ls.d_e_l_e_t_ = 0
                    AND ls.status = 'A'          
                    AND cpc.status = 'A'         

                UNION

                SELECT
                    cp.id_cupom,
                    cp.id_distribuidor

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_CLIENTE_OCULTO cpco ON
                        cp.id_cupom = cpco.id_cupom

                WHERE
                    cpco.id_cliente = :id_cliente
                    AND cpco.status = 'A'

            ) cp

        WHERE
            cp.id_cupom = :id_cupom
    """

    params = {
        "id_cupom": id_cupom,
        "id_cliente": id_cliente,
        "distribuidores": distribuidores
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True
    }

    cupom_cliente = dm.raw_sql_return(**dict_query)

    if not cupom_cliente:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom não acessível ao cliente escolhido."
                      })

    # itens do cupom
    id_tipo_itens = cupom.get("id_tipo_itens")

    query = """
        SET NOCOUNT ON;

        IF :id_tipo_itens = 1
        BEGIN

            SELECT
                TOP 1 cp.id_cupom,
                cp.id_distribuidor

            FROM
                CUPOM cp

            WHERE
                cp.id_cupom = :id_cupom
                AND cp.tipo_itens = 1

        END

        ELSE IF :id_tipo_itens = 2
        BEGIN

            SELECT
                TOP 1 cp.id_cupom,
                cp.id_distribuidor

            FROM
                CUPOM cp

                INNER JOIN CUPOM_ITENS citns ON
                    cp.id_cupom = citns.id_cupom

                INNER JOIN 
                (
                    SELECT
                        pd.id_produto,
                        0 as id_distribuidor

                    FROM
                        PRODUTO_DISTRIBUIDOR pd

                    WHERE
                        status = 'A'
                        AND pd.id_distribuidor IN :distribuidores

                    UNION

                    SELECT
                        pd.id_produto,
                        pd.id_distribuidor

                    FROM
                        PRODUTO_DISTRIBUIDOR pd
                                    
                    WHERE
                        status = 'A'
                        AND pd.id_distribuidor IN :distribuidores

                ) pds ON
                    cp.id_distribuidor = pds.id_distribuidor
                    AND citns.id_objeto = pds.id_produto

                INNER JOIN PRODUTO p ON
                    pds.id_produto = p.id_produto

            WHERE
                cp.id_cupom = :id_cupom
                AND cp.tipo_itens = 2
                AND citns.status = 'A'
                AND p.status = 'A'

        END

        ELSE IF :id_tipo_itens = 3
        BEGIN

            SELECT
                TOP 1 cp.id_cupom,
                cp.id_distribuidor

            FROM
                CUPOM cp

                INNER JOIN CUPOM_ITENS citns ON
                    cp.id_cupom = citns.id_cupom

                INNER JOIN
                (
                    SELECT
                        CONVERT(VARCHAR(10), m.id_marca) as id_marca,
                        0 as id_distribuidor

                    FROM
                        MARCA m

                    WHERE
                        status = 'A'
                        AND m.id_distribuidor IN :distribuidores

                    UNION

                    SELECT
                        CONVERT(VARCHAR(10), m.id_marca) as id_marca,
                        m.id_distribuidor

                    FROM
                        MARCA m

                    WHERE
                        status = 'A'
                        AND m.id_distribuidor IN :distribuidores

                ) m ON
                    citns.id_objeto = m.id_marca
                    AND cp.id_distribuidor = m.id_distribuidor
                        
                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON 
                    m.id_marca = pd.id_marca 
                            
                INNER JOIN PRODUTO p ON 
                    pd.id_produto = p.id_produto  

            WHERE
                cp.id_cupom = :id_cupom
                AND cp.tipo_itens = 3
                AND p.status = 'A' 
                AND pd.status = 'A'
                AND citns.status = 'A'

        END

        ELSE IF :id_tipo_itens = 4
        BEGIN

            SELECT
                TOP 1 cp.id_cupom,
                cp.id_distribuidor

            FROM
                CUPOM cp

                INNER JOIN CUPOM_ITENS citns ON
                    cp.id_cupom = citns.id_cupom

                INNER JOIN GRUPO g ON
                    citns.id_objeto = CONVERT(VARCHAR(10), g.id_grupo)
                    AND cp.id_distribuidor = g.id_distribuidor

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

                INNER JOIN GRUPO_SUBGRUPO gs ON
                    g.id_grupo = gs.id_grupo

                INNER JOIN SUBGRUPO s ON
                    gs.id_subgrupo = s.id_subgrupo

                INNER JOIN PRODUTO_SUBGRUPO ps ON
                    s.id_subgrupo = ps.id_subgrupo
                    AND s.id_distribuidor = ps.id_distribuidor

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    ps.codigo_produto = pd.id_produto
                    AND ps.id_distribuidor = pd.id_distribuidor

                INNER JOIN PRODUTO p ON
                    pd.id_produto = p.id_produto

            WHERE
                cp.id_cupom = :id_cupom
                AND cp.tipo_itens = 4
                AND citns.status = 'A'
                AND p.status = 'A'
                AND pd.status = 'A'
                AND t.status = 'A'
                AND g.status = 'A'
                AND gs.status = 'A'
                AND s.status = 'A'
                AND ps.status = 'A'

        END

        ELSE IF :id_tipo_itens = 5
        BEGIN

            SELECT
                TOP 1 cp.id_cupom,
                cp.id_distribuidor

            FROM
                CUPOM cp

                INNER JOIN CUPOM_ITENS citns ON
                    cp.id_cupom = citns.id_cupom

                INNER JOIN SUBGRUPO s ON
                    citns.id_objeto = CONVERT(VARCHAR(10), s.id_subgrupo)
                    AND cp.id_distribuidor = s.id_distribuidor

                INNER JOIN GRUPO_SUBGRUPO gs ON
                    s.id_subgrupo = gs.id_subgrupo
                    AND s.id_distribuidor = gs.id_distribuidor

                INNER JOIN GRUPO g ON
                    gs.id_grupo = g.id_grupo

                INNER JOIN TIPO t ON
                    g.tipo_pai = t.id_tipo

                INNER JOIN PRODUTO_SUBGRUPO ps ON
                    s.id_subgrupo = ps.id_subgrupo
                    AND s.id_distribuidor = ps.id_distribuidor

                INNER JOIN PRODUTO p ON
                    ps.codigo_produto = p.id_produto

                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                    p.id_produto = pd.id_produto
                    AND cp.id_distribuidor = pd.id_distribuidor

            WHERE
                cp.id_cupom = :id_cupom
                AND cp.tipo_itens = 5
                AND citns.status = 'A'
                AND p.status = 'A'
                AND pd.status = 'A'
                AND t.status = 'A'
                AND g.status = 'A'
                AND gs.status = 'A'
                AND s.status = 'A'
                AND ps.status = 'A'

        END

        ELSE
        BEGIN
            SELECT 1 as saida WHERE 1 = 0
        END
    """

    params = {
        "id_cupom": id_cupom,
        "distribuidores": distribuidores,
        "id_tipo_itens": id_tipo_itens
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True
    }

    cupom_item = dm.raw_sql_return(**dict_query)

    if not cupom_item:
        return False, ({
                          "id_cupom": id_cupom,
                          "valido": False,
                          "motivo": "Cupom sem itens válidos."
                      })

    return True, ({
                     "id_cupom": id_cupom,
                     "valido": True,
                     "motivo": None
                 })


def json_cartoes(id_usuario: int) -> tuple:
    """
    Lista os cartões de créditos salvos do usuário cliente

    Args:
        id_usuario (int): ID do usuario

    Returns:
        tuple: Tupla com o resultado da transação no primeiro elemento e a
                    lista com os cartoes no segundo elemento
    """

    # Verificando os dados de cartão salvos
    query = """
        SELECT
            id_cartao,
            id_usuario,
            REPLICATE('X', 12) + numero_cartao as numero_cartao,
            bandeira,
            ano_vencimento,
            mes_vencimento,
            portador,
            CASE WHEN LEN(cvv) > 0 THEN 1 ELSE 0 END as cvv_check

        FROM
            USUARIO_CARTAO_DE_CREDITO

        WHERE
            id_usuario = :id_usuario

        ORDER BY
            data_criacao
    """

    params = {
        "id_usuario": id_usuario,
    }

    dados = dm.raw_sql_return(query, params = params)

    if not dados:
        return []

    for dado in dados:
        dado["cvv_check"] = bool(dado.get("cvv_check"))

    return dados


def json_orcamento(id_usuario: int, id_cliente: int, id_distribuidor:int = None, 
                    id_orcamento: list = None, image_replace: str = '150') -> tuple:
    """
    Pega os produtos do orcamento do usuario-cliente

    Args:
        id_usuario (int): ID do usuario pai do orcamento
        id_cliente (int): ID do cliente pai do orcamento
        id_distribuidor (int, optional): ID do distribuidor dos produtos do orcamento. Defaults to None.
        id_orcamento (list, optional): IDs do orcamento especifico, caso queira. Defaults to None e retorna somentes os ativos atualmentes.
        image_replace (str, optional): Larguras das imagens vindas. Defaults to '150'.

    tuple: Tupla com o resultado da transação no primeiro elemento e o
        objeto com os produtos no segundo elemento
    """

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
            c.id_cliente = :id_cliente
            AND d.id_distribuidor = CASE
                                        WHEN ISNULL(:id_distribuidor, 0) = 0
                                            THEN d.id_distribuidor
                                        ELSE
                                            :id_distribuidor
                                    END
            AND d.id_distribuidor != 0
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND cd.d_e_l_e_t_ = 0
            AND d.status = 'A'
            
    """

    params = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor
    }

    distribuidores = dm.raw_sql_return(query, params = params, raw = True)

    if not distribuidores:
        return False, None

    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    # Trazendo os produtos do carrinho
    string_orcamento = ""

    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "distribuidores": distribuidores,
    }

    if id_orcamento:

        if isinstance(id_orcamento, (int, str)):
            id_orcamento = [id_orcamento]

        string_orcamento = "AND orcm.id_orcamento IN :id_orcamento"
        params["id_orcamento"] = id_orcamento

    else:
        string_orcamento = "AND orcm.status = 'A'"


    carrinho_query = f"""
        SELECT
            DISTINCT orcm.id_orcamento,
            orcm.id_distribuidor,
            p.id_produto,
            pd.cod_prod_distr,
            p.sku,
            p.descricao_completa as descricao_produto,
            m.id_marca,
            m.desc_marca as descricao_marca,
            ppr.preco_tabela,
            CASE
                WHEN orcmitm.tipo = 'B'
                    THEN 0
                ELSE
                    orcmitm.preco_venda
            END as preco_venda,
            orcmitm.quantidade,
            p.unidade_embalagem,
            p.quantidade_embalagem,
            p.unidade_venda,
            p.quant_unid_venda,
            pd.multiplo_venda,
            s.id_subgrupo,
            s.descricao as descricao_subgrupo,
            g.id_grupo,
            g.descricao as descricao_grupo,
            t.id_tipo,
            t.descricao as descricao_tipo,
            orcmitm.id_oferta,
            offers.tipo_oferta,
            orcmitm.tipo,
            pd.estoque,
            orcmitm.data_criacao,
            odesc.desconto,
            CASE 
                WHEN 
                    orcmitm.id_oferta IS NOT NULL 
                    AND offers.tipo_oferta = 3 
                THEN
                    (
                        CASE
                            WHEN 
                                orcmitm.desconto_escalonado IN (SELECT desconto FROM OFERTA_ESCALONADO_FAIXA WHERE status = 'A' AND id_oferta = orcmitm.id_oferta)
                            THEN
                                orcmitm.desconto_escalonado
                            ELSE
                                0
                        END
                    )
                ELSE
                    NULL			
            END as desconto_escalonado,	
            orcmitm.desconto_tipo,
            p.tokens_imagem as tokens,
            orcmitm.ordem

        FROM
            ORCAMENTO orcm

            INNER JOIN ORCAMENTO_ITEM orcmitm ON
                orcm.id_orcamento = orcmitm.id_orcamento
                AND orcm.id_distribuidor = orcmitm.id_distribuidor
                AND orcm.id_cliente = orcmitm.id_cliente

            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                orcmitm.id_produto = pd.id_produto
                AND orcmitm.id_distribuidor = pd.id_distribuidor

            INNER JOIN PRODUTO p ON
                pd.id_produto = p.id_produto

            INNER JOIN
            (
                SELECT
                    tpp.id_produto,
                    tpp.id_distribuidor,
                    MIN(tpp.preco_tabela) preco_tabela

                FROM
                    TABELA_PRECO tp

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        tp.id_tabela_preco = tpp.id_tabela_preco
                        AND tp.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN ORCAMENTO_ITEM orcmitm ON
                        tpp.id_produto = orcmitm.id_produto
                        AND tpp.id_distribuidor = orcmitm.id_distribuidor

                WHERE
                    tp.id_distribuidor IN :distribuidores
                    AND (
                            (
                                tp.tabela_padrao = 'S'
                            )
                                OR
                            (
                                tp.id_tabela_preco IN (
                                                            SELECT
                                                                id_tabela_preco

                                                            FROM
                                                                TABELA_PRECO_CLIENTE tpc

                                                            WHERE
                                                                id_distribuidor IN :distribuidores
                                                                AND id_cliente = :id_cliente
                                                        )
                            )
                        )
                    AND orcmitm.status = 'A'
                    AND orcmitm.d_e_l_e_t_ = 0
                    AND tp.status = 'A'
                    AND tp.dt_inicio <= GETDATE()
                    AND tp.dt_fim >= GETDATE()

                GROUP BY
                    tpp.id_produto,
                    tpp.id_distribuidor

            ) ppr ON
                pd.id_produto = ppr.id_produto
                AND pd.id_distribuidor = ppr.id_distribuidor

            INNER JOIN PRODUTO_SUBGRUPO ps ON
                pd.id_produto = ps.codigo_produto 
                AND pd.id_distribuidor = ps.id_distribuidor

            INNER JOIN SUBGRUPO s ON
                ps.id_subgrupo = s.id_subgrupo
                AND ps.id_distribuidor = s.id_distribuidor
                                                                                        
            INNER JOIN GRUPO_SUBGRUPO gs ON
                s.id_subgrupo = gs.id_subgrupo

            INNER JOIN GRUPO g ON
                gs.id_grupo = g.id_grupo

            INNER JOIN TIPO t ON
                g.tipo_pai = t.id_tipo

            INNER JOIN MARCA m ON
                pd.id_marca = m.id_marca

            LEFT JOIN 
            (
                SELECT
                    op.id_produto,
                    o.id_distribuidor,
                    MAX(od.desconto) desconto

                FROM
                    (
                    
                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

                        WHERE
                            oc.id_cliente = :id_cliente
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) offers

                    INNER JOIN OFERTA o ON
                        offers.id_oferta = o.id_oferta

                    INNER JOIN OFERTA_PRODUTO op ON
                        o.id_oferta = op.id_oferta

                    INNER JOIN ORCAMENTO_ITEM orcmitm ON
                        op.id_produto = orcmitm.id_produto

                    INNER JOIN ORCAMENTO orcm ON
                        orcmitm.id_orcamento = orcm.id_orcamento

                    INNER JOIN OFERTA_DESCONTO od ON
                        o.id_oferta = od.id_oferta

                WHERE
                    o.tipo_oferta = 1
                    AND orcm.id_usuario = :id_usuario
                    AND orcm.id_cliente = :id_cliente
                    AND o.id_distribuidor IN :distribuidores
                    AND o.limite_ativacao_oferta > 0
                    AND od.desconto > 0
                    AND orcmitm.status = 'A'
                    AND orcm.d_e_l_e_t_ = 0
                    AND orcmitm.d_e_l_e_t_ = 0
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
                pd.id_produto = odesc.id_produto
                AND pd.id_distribuidor = odesc.id_distribuidor

            LEFT JOIN
            (

                SELECT
                    o.id_oferta,
                    o.id_produto,
                    o.tipo_oferta,
                    o.id_distribuidor

                FROM
                    (
                        SELECT
                            o.id_oferta,
                            o.id_distribuidor,
                            op.id_produto,
                            o.tipo_oferta

                        FROM
                            (
                                            
                                SELECT
                                    op.id_oferta,
                                    op.id_produto

                                FROM
                                    OFERTA_PRODUTO op

                                WHERE
                                    op.status = 'A'

                                UNION

                                SELECT
                                    ob.id_oferta,
                                    ob.id_produto

                                FROM
                                    OFERTA_BONIFICADO ob

                                WHERE
                                    ob.status = 'A'

                            ) op

                            INNER JOIN OFERTA o ON
                                op.id_oferta = o.id_oferta

                            INNER JOIN
                            (

                                SELECT
                                    o.id_oferta

                                FROM
                                    OFERTA o

                                WHERE
                                    o.id_oferta NOT IN (SELECT id_oferta FROM OFERTA_CLIENTE)

                                UNION

                                SELECT
                                    oc.id_oferta

                                FROM
                                    OFERTA_CLIENTE oc

                                WHERE
                                    oc.id_cliente = :id_cliente
                                    AND oc.status = 'A'
                                    AND oc.d_e_l_e_t_ = 0

                            ) oc ON
                                o.id_oferta = oc.id_oferta

                            INNER JOIN PRODUTO p ON
                                op.id_produto = p.id_produto

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                p.id_produto = pd.id_produto
                                AND o.id_distribuidor = pd.id_distribuidor

                        WHERE
                            o.tipo_oferta IN (2,3)
                            AND o.id_distribuidor IN :distribuidores
                            AND o.limite_ativacao_oferta > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND o.status = 'A'
                            AND p.status = 'A'
                            AND pd.status = 'A'
                                                    
                    ) o

                    INNER JOIN
                    (
                    
                        SELECT
                            DISTINCT
                                o.id_oferta,
                                o.id_distribuidor

                        FROM
                            PRODUTO_DISTRIBUIDOR pd

                            INNER JOIN PRODUTO p ON
                                pd.id_produto = p.id_produto

                            INNER JOIN OFERTA o ON
                                pd.id_distribuidor = o.id_distribuidor

                            INNER JOIN OFERTA_PRODUTO op ON
                                p.id_produto = op.id_produto
                                AND o.id_oferta = op.id_oferta

                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                pd.id_produto = tpp.id_produto
                                AND pd.id_distribuidor = tpp.id_distribuidor

                            INNER JOIN TABELA_PRECO tp ON
                                tpp.id_tabela_preco = tp.id_tabela_preco
                                AND tpp.id_distribuidor = tp.id_distribuidor

                        WHERE
                            pd.estoque > 0
                            AND o.id_distribuidor IN :distribuidores
                            AND (

                                    (
                                        o.tipo_oferta = 2 
                                        AND o.id_oferta IN (

                                                                SELECT
                                                                    ob.id_oferta

                                                                FROM
                                                                    PRODUTO_DISTRIBUIDOR pd

                                                                    INNER JOIN PRODUTO p ON
                                                                        pd.id_produto = p.id_produto

                                                                    INNER JOIN OFERTA o ON
                                                                        pd.id_distribuidor = o.id_distribuidor

                                                                    INNER JOIN OFERTA_BONIFICADO ob ON
                                                                        p.id_produto = ob.id_produto
                                                                        AND o.id_oferta = ob.id_oferta

                                                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                                        pd.id_produto = tpp.id_produto
                                                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                                                    INNER JOIN TABELA_PRECO tp ON
                                                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                                                WHERE
                                                                    pd.estoque > 0
                                                                    AND o.id_distribuidor IN :distribuidores
                                                                    AND o.tipo_oferta = 2
                                                                    AND tp.dt_inicio <= GETDATE()
                                                                    AND tp.dt_fim >= GETDATE()
                                                                    AND tp.tabela_padrao = 'S'
                                                                    AND tp.status = 'A'
                                                                    AND ob.status = 'A'
                                                                    AND p.status = 'A'
                                                                    AND pd.status = 'A'

                                                            )
                                    )

                                        OR

                                    (
                                            
                                        o.tipo_oferta = 3
                                        AND o.id_oferta IN (
                                                            
                                                                SELECT
                                                                    id_oferta

                                                                FROM
                                                                    OFERTA_ESCALONADO_FAIXA

                                                                WHERE
                                                                    desconto > 0
                                                                    AND status = 'A'

                                                            )
                                            
                                    )

                                )
                            AND o.limite_ativacao_oferta > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND tp.tabela_padrao = 'S'
                            AND tp.dt_inicio <= GETDATE()
                            AND tp.dt_fim >= GETDATE()
                            AND op.status = 'A'
                            AND tp.status = 'A'
                            AND p.status = 'A'
                            AND pd.status = 'A'
                            AND o.status = 'A'

                    ) op ON
                        o.id_oferta = op.id_oferta
                        AND o.id_distribuidor = op.id_distribuidor

            ) offers ON
                pd.id_produto = offers.id_produto
                AND pd.id_distribuidor = offers.id_distribuidor
                AND orcmitm.id_oferta = offers.id_oferta

        WHERE
            orcm.id_usuario = :id_usuario
            AND orcm.id_cliente = :id_cliente
            {string_orcamento}
            AND pd.id_distribuidor IN :distribuidores
            AND pd.estoque > 0
            AND orcmitm.status = 'A'
            AND orcm.d_e_l_e_t_ = 0
            AND orcmitm.d_e_l_e_t_ = 0
            AND p.status = 'A'
            AND pd.status = 'A'
            AND ps.status = 'A'
            AND s.status = 'A'
            AND gs.status = 'A'
            AND g.status = 'A'
            AND t.status = 'A'

        ORDER BY
            orcmitm.data_criacao,
            orcmitm.ordem
    """

    dict_query = {
        "query": carrinho_query,
        "params": params,
    }

    carrinho_query = dm.raw_sql_return(**dict_query)

    if not carrinho_query:
        return False, None

    response = json_orcamento_like_gen(carrinho_query, image_replace)

    orcamentos = []
    cupons = None

    if response.get("orcamentos"):
        for orcamento in response.get("orcamentos"):
            orcamento_id = orcamento.get("id_orcamento")
            if orcamento_id not in orcamentos and orcamento_id:
                orcamentos.append(orcamento_id)

        # Pegando os cupons
        cupons_query = """
            SELECT
                o2.id_orcamento,
                o2.id_distribuidor,
                o2.id_cupom

            FROM
                (
                    SELECT
                        ROW_NUMBER() OVER(PARTITION BY o.id_distribuidor ORDER BY o.data_criacao) ordem,
                        o.id_orcamento,
                        o.id_distribuidor

                    FROM
                        ORCAMENTO o

                    WHERE
                        o.id_usuario = :id_usuario
                        AND o.id_cliente = :id_cliente
                        AND o.id_orcamento IN :id_orcamento
                        AND o.status = 'A'
                        AND o.d_e_l_e_t_ = 0
                ) o1

                INNER JOIN ORCAMENTO o2 ON
                    o1.id_orcamento = o2.id_orcamento
                    AND o1.id_distribuidor = o2.id_distribuidor

            WHERE
                ordem = 1
                AND o2.id_cupom IS NOT NULL
        """

        params = {
            "id_usuario": id_usuario,
            "id_cliente": id_cliente,
            "id_orcamento": orcamentos
        }

        cupons = dm.raw_sql_return(cupons_query, params = params)

    to_return = {
        "orcamentos": response.get("orcamentos"), 
        "ofertas": response.get("ofertas"),
        "cupons": cupons if cupons else None
    }

    return True, to_return


def json_pedido_produto(id_pedido: int, image_replace: str = '150', percentual: float = None) -> list:
    """
    Lista os produtos de um pedido especifico

    Args:
        id_pedido (int): ID do pedido
        image_replace (str, optional): definicao da imagem. Defaults to '150'.
        percentual (float, optional): Percentual do metodo de pagamento para o caso de produto
        exportado. Defaults to None. 

    Returns:
        list: lista com os produtos do pedido
    """

    # Checando se o pedido já foi exportado
    query = """
        SELECT
            1

        FROM
            PEDIDO_ITEM_RETORNO

        WHERE
            id_pedido = :id_pedido
            AND d_e_l_e_t_ = 0
    """

    params = {
        "id_pedido": id_pedido
    }

    dict_query = {
        "query": query,
        "params": params,
        "first": True,
        "raw": True,
    }

    check = dm.raw_sql_return(**dict_query)

    if check:

        assert isinstance(percentual, float), "Percentual da condição não enviado para pedido já exportado."

        query = """
            SELECT
                pdditmret.id_produto,
                pdditmret.id_distribuidor,
                pdditmret.preco_tabela,
                CASE WHEN pdditmret.tipo_venda = 'B' THEN 0 ELSE pdditmret.preco_venda END as preco_aplicado,
                pdditmret.quantidade_faturada as quantidade,
                p.estoque,
                pdditmret.tipo_venda,
                p.tokens,
                p.id_tipo,
                p.descricao_tipo,
                p.id_grupo,
                p.descricao_grupo,
                p.id_subgrupo,
                p.descricao_subgrupo,
                p.sku,
                p.descricao_produto,
                p.id_marca,
                p.desc_marca as descricao_marca,
                'A' as status,
                p.unidade_venda,
                p.quant_unid_venda,
                p.unidade_embalagem,
                p.quantidade_embalagem,
                p.multiplo_venda

            FROM
                PEDIDO pdd

                INNER JOIN PEDIDO_ITEM_RETORNO pdditmret ON
                    pdd.id_pedido = pdditmret.id_pedido
                    AND pdd.id_distribuidor = pdditmret.id_distribuidor

                LEFT JOIN
                (

                    SELECT
                        pd.id_produto,
                        pd.id_distribuidor,
                        p.sku,
                        p.descricao_completa as descricao_produto,
                        m.id_marca,
                        m.desc_marca,
                        pd.estoque,
                        p.tokens_imagem as tokens,
                        cat.id_tipo,
                        cat.descricao_tipo,
                        cat.t_ranking,
                        cat.id_grupo,
                        cat.descricao_grupo,
                        cat.g_ranking,
                        cat.id_subgrupo,
                        cat.descricao_subgrupo,
                        cat.s_ranking,
                        p.unidade_venda,
                        p.quant_unid_venda,
                        p.unidade_embalagem,
                        p.quantidade_embalagem,
                        pd.multiplo_venda

                    FROM
                        PRODUTO p

                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto

                        LEFT JOIN MARCA m ON
                            pd.id_marca = m.id_marca

                        LEFT JOIN PRODUTO_ESTOQUE pe ON
                            p.id_produto = pe.id_produto

                        LEFT JOIN
                        (

                            SELECT
                                DISTINCT t.id_tipo,
                                t.descricao as descricao_tipo,
                                CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END t_ranking,
                                g.id_grupo,
                                g.descricao as descricao_grupo,
                                CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END g_ranking,
                                s.id_subgrupo,
                                s.descricao as descricao_subgrupo,
                                CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END s_ranking,
                                pd.id_produto,
                                pd.id_distribuidor

                            FROM
                                PRODUTO p

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                    pd.id_produto = ps.codigo_produto
                                    AND pd.id_distribuidor = ps.id_distribuidor

                                INNER JOIN PEDIDO_ITEM_RETORNO pdditmret ON
                                    ps.codigo_produto = pdditmret.id_produto
                                    AND ps.id_distribuidor = pdditmret.id_distribuidor

                                INNER JOIN DISTRIBUIDOR d ON
                                    pd.id_distribuidor = d.id_distribuidor

                                INNER JOIN SUBGRUPO s ON
                                    ps.id_subgrupo = s.id_subgrupo
                                    AND ps.id_distribuidor = s.id_distribuidor
                                                                
                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                d.id_distribuidor != 0
                                AND pdditmret.id_pedido = :id_pedido
                                AND ps.status = 'A'
                                AND p.status = 'A'
                                AND pd.status = 'A'
                                AND d.status = 'A'
                                AND t.status = 'A'
                                AND g.status = 'A'
                                AND gs.status = 'A'
                                AND s.status = 'A'

                        ) cat ON
                            pd.id_produto = cat.id_produto
                            AND pd.id_distribuidor = cat.id_distribuidor

                ) p ON
                    pdditmret.id_produto = p.id_produto
                    AND pdditmret.id_distribuidor = p.id_distribuidor
                            
            WHERE
                pdd.id_pedido = :id_pedido
                AND pdd.d_e_l_e_t_ = 0
                AND pdditmret.d_e_l_e_t_ = 0

            ORDER BY
                CASE WHEN pdditmret.tipo_venda = 'B' THEN 0 ELSE 1 END DESC,
                p.t_ranking,
                p.g_ranking,
                p.s_ranking
        """

    else:

        query = """
            SET NOCOUNT ON;

            SELECT
                pdditm.id_produto,
                pdditm.id_distribuidor,
                pdditm.preco_produto as preco_tabela,
                (pdditm.preco_produto * (1 - (pdditm.desconto_aplicado/100))) as preco_desconto,
                CASE WHEN pdditm.tipo_venda = 'B' THEN 0 ELSE pdditm.preco_aplicado END as preco_aplicado,
                CASE
                    WHEN ISNULL(pdditm.quantidade_faturada, 0) = 0
                        THEN pdditm.quantidade_venda
                    ELSE
                        pdditm.quantidade_faturada
                END as quantidade,
                p.estoque,
                pdditm.desconto_aplicado as desconto,
                pdditm.desconto_tipo,
                pdditm.id_oferta,
                pdditm.tipo_oferta,
                pdditm.tipo_venda,
                p.tokens,
                p.id_tipo,
                p.descricao_tipo,
                p.id_grupo,
                p.descricao_grupo,
                p.id_subgrupo,
                p.descricao_subgrupo,
                p.sku,
                p.descricao_produto,
                p.id_marca,
                p.desc_marca as descricao_marca,
                'A' as status,
                p.unidade_venda,
                p.quant_unid_venda,
                p.unidade_embalagem,
                p.quantidade_embalagem,
                p.multiplo_venda

            FROM
                PEDIDO pdd

                INNER JOIN PEDIDO_ITEM pdditm ON
                    pdd.id_pedido = pdditm.id_pedido
                    AND pdd.id_distribuidor = pdditm.id_distribuidor

                LEFT JOIN
                (

                    SELECT
                        pd.id_produto,
                        pd.id_distribuidor,
                        p.sku,
                        p.descricao_completa as descricao_produto,
                        m.id_marca,
                        m.desc_marca,
                        pd.estoque,
                        p.tokens_imagem as tokens,
                        cat.id_tipo,
                        cat.descricao_tipo,
                        cat.t_ranking,
                        cat.id_grupo,
                        cat.descricao_grupo,
                        cat.g_ranking,
                        cat.id_subgrupo,
                        cat.descricao_subgrupo,
                        cat.s_ranking,
                        p.unidade_venda,
                        p.quant_unid_venda,
                        p.unidade_embalagem,
                        p.quantidade_embalagem,
                        pd.multiplo_venda

                    FROM
                        PRODUTO p

                        INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                            p.id_produto = pd.id_produto

                        LEFT JOIN MARCA m ON
                            pd.id_marca = m.id_marca

                        LEFT JOIN
                        (

                            SELECT
                                DISTINCT t.id_tipo,
                                t.descricao as descricao_tipo,
                                CASE WHEN t.ranking > 0 THEN t.ranking ELSE 999999 END t_ranking,
                                g.id_grupo,
                                g.descricao as descricao_grupo,
                                CASE WHEN g.ranking > 0 THEN g.ranking ELSE 999999 END g_ranking,
                                s.id_subgrupo,
                                s.descricao as descricao_subgrupo,
                                CASE WHEN s.ranking > 0 THEN s.ranking ELSE 999999 END s_ranking,
                                pd.id_produto,
                                pd.id_distribuidor

                            FROM
                                PRODUTO p

                                INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                    p.id_produto = pd.id_produto

                                INNER JOIN PRODUTO_SUBGRUPO ps ON
                                    pd.id_produto = ps.codigo_produto
                                    AND pd.id_distribuidor = ps.id_distribuidor

                                INNER JOIN PEDIDO_ITEM pdditm ON
                                    ps.codigo_produto = pdditm.id_produto
                                    AND ps.id_distribuidor = pdditm.id_distribuidor

                                INNER JOIN DISTRIBUIDOR d ON
                                    pd.id_distribuidor = d.id_distribuidor

                                INNER JOIN SUBGRUPO s ON
                                    ps.id_subgrupo = s.id_subgrupo
                                    AND ps.id_distribuidor = s.id_distribuidor
                                                    
                                INNER JOIN GRUPO_SUBGRUPO gs ON
                                    s.id_subgrupo = gs.id_subgrupo

                                INNER JOIN GRUPO g ON
                                    gs.id_grupo = g.id_grupo

                                INNER JOIN TIPO t ON
                                    g.tipo_pai = t.id_tipo

                            WHERE
                                d.id_distribuidor != 0
                                AND pdditm.id_pedido = :id_pedido
                                AND ps.status = 'A'
                                AND p.status = 'A'
                                AND pd.status = 'A'
                                AND d.status = 'A'
                                AND t.status = 'A'
                                AND g.status = 'A'
                                AND gs.status = 'A'
                                AND s.status = 'A'

                        ) cat ON
                            pd.id_produto = cat.id_produto
                            AND pd.id_distribuidor = cat.id_distribuidor

                ) p ON
                    pdditm.id_produto = p.id_produto
                    AND pdditm.id_distribuidor = p.id_distribuidor
                
            WHERE
                pdd.id_pedido = :id_pedido
                AND pdd.d_e_l_e_t_ = 0
                AND pdditm.d_e_l_e_t_ = 0

            ORDER BY
                pdditm.ordem_produto,
                p.t_ranking,
                p.g_ranking,
                p.s_ranking
        """

    params = {
        "id_pedido": id_pedido,
    }

    produtos = dm.raw_sql_return(query, params = params)

    if not produtos:
        return []

    dict_produto = {
        "prod_dist_list": produtos,
        "image_replace": image_replace,
    }

    if check:
        dict_produto["percentual"] = percentual
        produtos = json_pedido_produto_jsl_like_gen(**dict_produto)

    else:
        produtos = json_pedido_produto_like_gen(**dict_produto)

    if not produtos:
        return []

    return produtos


def json_pedido_cupom(id_cupom: list[int]) -> list:
    """
    Retorna os cupons listados acima para o pedido

    Args:
        id_cupom (list|int): Lista de cupons que devem ser filtradas.

    Returns:
        dict: Dicionario com a contagem dos cupons e os cupons paginados
    """

    # Fazendo as queries
    params = {}

    string_id_cupom = ""

    ## Procura por id_cupom
    if id_cupom:

        if not isinstance(id_cupom, (list, tuple, set)):
            id_cupom = [id_cupom]

        else:
            id_cupom = list(id_cupom)

        params.update({
            "id_cupom": id_cupom
        })

        string_id_cupom = "AND cp.id_cupom IN :id_cupom"

    # Pegando os Cupons
    query = f"""
        SELECT
            id_cupom,
            id_distribuidor,
            codigo_cupom,
            titulo,
            descricao,
            data_inicio,
            data_final,
            tipo_cupom,
            descricao_tipo_cupom,
            desconto_valor,
            desconto_percentual,
            valor_limite,
            valor_ativar,
            termos_uso,
            status,
            reutiliza,
            0 oculto,
            bloqueado,
            tipo_itens,
            descricao_tipo_itens,
            itens

        FROM
            (

                SELECT
                    DISTINCT cp.id_cupom,
                    cp.id_distribuidor,
                    cp.codigo_cupom,
                    cp.titulo,
                    cp.descricao,
                    cp.data_inicio,
                    cp.data_final,
                    cp.tipo_cupom,
                    tpcp.descricao as descricao_tipo_cupom,
                    cp.desconto_valor,
                    cp.desconto_percentual,
                    cp.valor_limite,
                    cp.valor_ativar,
                    cp.termos_uso,
                    cp.status,
                    cp.qt_reutiliza as reutiliza,
                    cp.oculto,
                    CASE WHEN cp.bloqueado = 'N' THEN 0 ELSE 1 END bloqueado,
                    cp.tipo_itens,
                    tpcpit.descricao as descricao_tipo_itens,
                    citns.id_objeto as itens

                FROM
                    CUPOM cp

                    INNER JOIN CUPOM_ITENS citns ON
                        cp.id_cupom = citns.id_cupom

                    INNER JOIN TIPO_CUPOM tpcp ON
                        cp.tipo_cupom = tpcp.id_tipo_cupom

                    INNER JOIN TIPO_CUPOM_ITENS tpcpit ON
                        cp.tipo_itens = tpcpit.id_tipo_itens

                WHERE
                    1 = 1
                    {string_id_cupom}

            ) cp
                    
        ORDER BY
            CASE WHEN cp.oculto = 'N' THEN 0 ELSE 1 END DESC,
            cp.titulo,
            cp.descricao,
            cp.id_cupom;
    """

    dict_query = {
        "query": query,
        "params": params,
    }

    query = dm.raw_sql_return(**dict_query)

    if not query:
        return []

    return json_cupons_like_gen(query)


def json_noticias(id_distribuidor: int, id_cliente: int = None, id_noticia: int = None,
                    image_replace: str = 'auto', **kwargs) -> dict:
    """
    Json padrão de noticias

    Args:
        id_distribuidor (int): id do distribuidor.
        id_cliente (int, optional): id do cliente. Defaults to None.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    kwargs:
        pagina (int): Qual a pagina que deve ser pegue.
        limite (int): Número de ofertas por página.

    Returns:
        dict: Objeto com os resultados e com a contagem
    """

    pagina = kwargs.get('pagina')
    limite = kwargs.get('limite')

    # Pegando os distribuidores validos do cliente
    if not id_cliente:
        query = """
            SELECT
                DISTINCT id_distribuidor

            FROM
                DISTRIBUIDOR

            WHERE
                id_distribuidor = CASE 
                                      WHEN :id_distribuidor = 0 
                                          THEN id_distribuidor 
                                      ELSE 
                                          :id_distribuidor 
                                  END
                AND status = 'A'
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
                c.id_cliente = :id_cliente
                AND d.id_distribuidor = CASE
                                            WHEN :id_distribuidor = 0
                                                THEN d.id_distribuidor
                                            ELSE
                                                :id_distribuidor
                                        END
                AND c.status = 'A'
                AND c.status_receita = 'A'
                AND cd.status = 'A'
                AND cd.d_e_l_e_t_ = 0
                AND d.status = 'A'

            UNION
            
            SELECT
                0 as id_distribuidor

            WHERE
                :id_distribuidor = 0
        """

        params = {
            "id_cliente": id_cliente,
            "id_distribuidor": id_distribuidor
        }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
    }

    distribuidores = dm.raw_sql_return(**dict_query)
    if not distribuidores:
        return {}
    
    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    # Pegando as noticias
    noticias_query = """
        SELECT
            n.id_noticia,
            n.id_distribuidor,
            n.titulo,
            n.imagem,
            n.corpo,
            n.data_inicio,
            n.data_final,
            d.nome_fantasia,
            COUNT(*) OVER() count__

        FROM
            NOTICIA n

            INNER JOIN DISTRIBUIDOR d ON
                n.id_distribuidor = d.id_distribuidor

        WHERE
            n.id_noticia = CASE WHEN ISNULL(:id_noticia, 0) = 0 THEN n.id_noticia ELSE :id_noticia END
            AND n.id_distribuidor IN :distribuidores
            AND n.data_inicio <= GETDATE()
            AND n.data_final >= GETDATE()
            AND n.status = 'A'
            AND LEN(n.corpo) > 0
            AND LEN(n.titulo) > 0

        ORDER BY
            n.data_inicio DESC

        OFFSET
            :offset ROWS

        FETCH
            NEXT :limite ROWS ONLY
    """

    params = {
        "id_noticia": id_noticia,
        "distribuidores": distribuidores,
        "offset": (pagina - 1) * limite,
        "limite": limite
    }

    dict_query = {
        "query": noticias_query,
        "params": params,
    }

    noticias_query = dm.raw_sql_return(**dict_query)

    if not noticias_query:
        return {}

    count = noticias_query[0].get("count__")
    for data in noticias_query:
        data.pop("count__", None)

        imagem = data.get("imagem")
        data["imagem"] = None
            
        if imagem and "." in str(imagem):
            id_distribuidor = data.get('id_distribuidor')
            data["imagem"] = f"{image}/imagens/distribuidores/{image_replace}/{id_distribuidor}/noticias/{imagem}"

    return {
        "count": count,
        "data": noticias_query,
    }


def json_ultimos_vistos(id_usuario: int, id_cliente: int, image_replace: str = 'auto', **kwargs) -> list:
    """
    Json padrão de ultimos vistos

    Args:
        id_usuario (int): id do usuario.
        id_cliente (int): id do cliente.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    kwargs:
        id_distribuidor (int): id do distribuidor
        limite (int): Quantidade de produtos que devem ser mostrados
        ordernar_por (int): Qual deve ser a ordenação dos dados

            1 - Preco do maior para o menor

            2 - Preco do menor para o maior 

            3 - Ordem de rankeamento (1 para maior)

            4 - Não implementado (padroniza para o 3)

            5 - Ordenação por ordem alfabética (A à Z)

            6 - Ordenação por ordem alfabética (Z à A)

            7 - Ordenação por ultimo adicionado

            8 - Ordenação por primeiro adicionado

        desconto (bool): Caso seja necessário retornar somente produtos com desconto
        tipo_oferta (int): Qual o tipo de oferta que deve ser retornada

            1 - Ofertas de compre e ganhe

            2 - Ofertas de escalonamento

        marca (list): Lista contendos marcas dos produtos que devem retornar
        grupos (list): Lista contendos grupos dos produtos que devem retornar
        subgrupos (list): Lista contendos subgrupos dos produtos que devem retornar        

    Returns:
        dict: Objeto com os resultados e com a contagem
    """

    id_distribuidor = kwargs.get("id_distribuidor")

    limite = kwargs.get("limite")

    max_ultimos_vistos = int(os.environ.get('MAX_LIMITE_ULTIMOS_VISTOS_PS'))

    ordenar_por = int(kwargs.get("ordenar")) if kwargs.get("ordenar") else 7 # padrão mais novos
    if (ordenar_por > 8 or ordenar_por < 1):
        ordenar_por = 7

    desconto = True if kwargs.get("desconto") and id_usuario else False

    tipo_oferta = int(kwargs.get("tipo_oferta")) if kwargs.get("tipo_oferta") and id_usuario else None

    marca = kwargs.get("marca")

    grupos = kwargs.get("grupos")
    subgrupos = kwargs.get("subgrupos")

    # Pegando os distribuidores do cliente
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
            c.id_cliente = :id_cliente
            AND d.id_distribuidor = CASE
                                        WHEN ISNULL(:id_distribuidor, 0) = 0
                                            THEN d.id_distribuidor
                                        ELSE
                                            :id_distribuidor
                                    END
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND cd.d_e_l_e_t_ = 0
            AND d.status = 'A'
    """

    params = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
    }

    distribuidores = dm.raw_sql_return(**dict_query)
    if not distribuidores:
        return []

    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    # Adicionando filtros do usuario
    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor,
        "distribuidores": distribuidores,
        "limite": limite,
    }

    string_desconto = ""
    string_oferta = ""
    string_marca = ""
    string_grupos = ""
    string_subgrupos = ""

    # Ordenação
    dict_ordenacao = {
        1:"preco_tabela DESC, descricao_completa",
        2:"preco_tabela ASC, descricao_completa",
        3:"ordem_rankeamento ASC, descricao_completa",
        4:"ordem_rankeamento ASC, descricao_completa",
        5:"descricao_completa ASC",
        6:"descricao_completa DESC",
        7:"data_cadastro_ultimos_vistos DESC, descricao_completa",
        8:"data_cadastro_ultimos_vistos ASC, descricao_completa"
    }

    string_ordenar_por = dict_ordenacao.get(ordenar_por)

    # Procura por desconto
    if desconto:
        string_desconto = """
            INNER JOIN
            (
            
                SELECT
                    op.id_produto,
                    o.id_distribuidor

                FROM
                    (

                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

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
                    AND o.limite_ativacao_oferta > 0
                    AND od.desconto > 0
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND o.status = 'A'
                    AND op.status = 'A'
                    AND od.status = 'A'
            
            ) odesc ON
                pd.id_produto = odesc.id_produto
                AND pd.id_distribuidor = odesc.id_distribuidor
        """

    # Procura por tipo_oferta
    if tipo_oferta:

        if tipo_oferta == 1:
            string_tipo_oferta = """
                AND (
                        o.tipo_oferta = 2 
                        AND o.id_oferta IN (

                                                SELECT
                                                    ob.id_oferta

                                                FROM
                                                    PRODUTO_DISTRIBUIDOR pd

                                                    INNER JOIN PRODUTO p ON
                                                        pd.id_produto = p.id_produto

                                                    INNER JOIN OFERTA o ON
                                                        pd.id_distribuidor = o.id_distribuidor

                                                    INNER JOIN
                                                    (
                                                    
                                                        SELECT
                                                            o.id_oferta

                                                        FROM
                                                            OFERTA o

                                                        WHERE
                                                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                                                        UNION

                                                        SELECT
                                                            oc.id_oferta

                                                        FROM
                                                            OFERTA_CLIENTE oc

                                                        WHERE
                                                            oc.id_cliente = :id_cliente
                                                            AND oc.status = 'A'
                                                            AND oc.d_e_l_e_t_ = 0

                                                    ) oc ON
                                                        o.id_oferta = oc.id_oferta

                                                    INNER JOIN OFERTA_BONIFICADO ob ON
                                                        p.id_produto = ob.id_produto
                                                        AND o.id_oferta = ob.id_oferta

                                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                        pd.id_produto = tpp.id_produto
                                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                                    INNER JOIN TABELA_PRECO tp ON
                                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                                WHERE
                                                    pd.estoque > 0
                                                    AND o.id_distribuidor IN :distribuidores
                                                    AND o.tipo_oferta = 2
                                                    AND tp.dt_inicio <= GETDATE()
                                                    AND tp.dt_fim >= GETDATE()
                                                    AND tp.tabela_padrao = 'S'
                                                    AND tp.status = 'A'
                                                    AND ob.status = 'A'
                                                    AND p.status = 'A'
                                                    AND pd.status = 'A'

                                            )
                    )
            """
        
        elif tipo_oferta == 2:
            string_tipo_oferta = """
                AND (
                                    
                        o.tipo_oferta = 3
                        AND o.id_oferta IN (
                                            
                                                SELECT
                                                    id_oferta

                                                FROM
                                                    OFERTA_ESCALONADO_FAIXA

                                                WHERE
                                                    desconto > 0
                                                    AND status = 'A'

                                            )
                            
                    )
            """

        string_oferta = f"""
            INNER JOIN
            (

                SELECT
                    o.id_oferta,
                    o.id_distribuidor,
                    o.tipo_oferta,
                    op.id_produto

                FROM
                    PRODUTO_DISTRIBUIDOR pd

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN OFERTA o ON
                        pd.id_distribuidor = o.id_distribuidor

                    INNER JOIN
                    (
                    
                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

                        WHERE
                            oc.id_cliente = 1
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) oc ON
                        o.id_oferta = oc.id_oferta

                    INNER JOIN OFERTA_PRODUTO op ON
                        p.id_produto = op.id_produto
                        AND o.id_oferta = op.id_oferta

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        pd.id_produto = tpp.id_produto
                        AND pd.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN TABELA_PRECO tp ON
                        tpp.id_tabela_preco = tp.id_tabela_preco
                        AND tpp.id_distribuidor = tp.id_distribuidor

                WHERE
                    pd.estoque > 0
                    AND o.id_distribuidor IN :distribuidores
                    {string_tipo_oferta}
                    AND o.limite_ativacao_oferta > 0
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND tp.tabela_padrao = 'S'
                    AND tp.dt_inicio <= GETDATE()
                    AND tp.dt_fim >= GETDATE()
                    AND op.status = 'A'
                    AND tp.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND o.status = 'A'
            
            ) op ON
                pd.id_produto = op.id_produto
                AND pd.id_distribuidor = op.id_distribuidor
        """

    ## Procura por marca
    if marca:

        if not isinstance(marca, (list, tuple, set)):
            marca = [marca]

        else:
            marca = list(marca)

        params.update({
            "marca": marca
        })

        string_marca = "AND m.id_marca IN :marca"

    # Procura por grupos
    if grupos:

        if not isinstance(grupos, (list, tuple, set)):
            grupos = [grupos]

        else:
            grupos = list(grupos)

        params.update({
            "grupos": grupos
        })

        string_grupos = "AND g.id_grupo IN :grupos"

    # Procura por subgrupos
    if subgrupos:

        if not isinstance(subgrupos, (list, tuple, set)):
            subgrupos = [subgrupos]

        else:
            subgrupos = list(subgrupos)

        params.update({
            "subgrupos": subgrupos
        })

        string_subgrupos = "AND s.id_subgrupo IN :subgrupos"
        
    ultimos_vistos_query = f"""
        SELECT
            id_produto,
            id_distribuidor,
            COUNT(*) OVER() count__

        FROM
            (
                
                SELECT
                    TOP {max_ultimos_vistos}
                        id_produto,
                        id_distribuidor,
                        ranking,
                        descricao_completa,
                        preco_tabela,
                        data_cadastro_ultimos_vistos,
                        ordem_rankeamento

                FROM
                    (
                    
                        SELECT
                            DISTINCT
                                p.id_produto,
                                pd.id_distribuidor,
                                pd.ranking,
                                p.descricao_completa,
                                MIN(tpp.preco_tabela) OVER(PARTITION BY tpp.id_produto) as preco_tabela,
                                uuv.data_cadastro as data_cadastro_ultimos_vistos,
                                CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

                        FROM 
                            USUARIO_ULTIMOS_VISTOS uuv

                            INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                                uuv.id_usuario = :id_usuario
                                AND uuv.id_produto = pd.id_produto
                                AND uuv.id_distribuidor = pd.id_distribuidor

                            INNER JOIN PRODUTO p ON
                                pd.id_produto = p.id_produto

                            INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                pd.id_produto = tpp.id_produto
                                AND pd.id_distribuidor = tpp.id_distribuidor

                            INNER JOIN TABELA_PRECO tp ON
                                tpp.id_tabela_preco = tp.id_tabela_preco
                                AND tpp.id_distribuidor = tp.id_distribuidor

                            INNER JOIN PRODUTO_SUBGRUPO ps ON
                                pd.id_produto = ps.codigo_produto

                            INNER JOIN SUBGRUPO s ON
                                ps.id_subgrupo = s.id_subgrupo
                                AND ps.id_distribuidor = s.id_distribuidor

                            INNER JOIN GRUPO_SUBGRUPO gs ON
                                s.id_subgrupo = gs.id_subgrupo
                                AND s.id_distribuidor = gs.id_distribuidor

                            INNER JOIN GRUPO g ON
                                gs.id_grupo = g.id_grupo

                            INNER JOIN TIPO t ON
                                g.tipo_pai = t.id_tipo

                            INNER JOIN MARCA m ON
                                pd.id_marca = m.id_marca
                                AND pd.id_distribuidor = m.id_distribuidor

                            INNER JOIN FORNECEDOR f ON
                                pd.id_fornecedor = f.id_fornecedor

                            {string_desconto}
                                
                            {string_oferta}

                        WHERE
                            pd.id_distribuidor IN :distribuidores
                            AND pd.estoque > 0
                            {string_marca}
                            {string_grupos}
                            {string_subgrupos}
                            AND (
                                    (
                                        tp.tabela_padrao = 'S'
                                    )
                                        OR
                                    (
                                        tp.id_tabela_preco IN (
                                                                    SELECT
                                                                        id_tabela_preco

                                                                    FROM
                                                                        TABELA_PRECO_CLIENTE

                                                                    WHERE
                                                                        id_distribuidor IN :distribuidores
                                                                        AND id_cliente = :id_cliente
                                                                )
                                    )
                                )
                            AND tp.dt_inicio <= GETDATE()
                            AND tp.dt_fim >= GETDATE()
                            AND tp.status = 'A'
                            AND p.status = 'A'
                            AND pd.status = 'A'
                            AND ps.status = 'A'
                            AND s.status = 'A'
                            AND gs.status = 'A'
                            AND g.status = 'A'
                            AND t.status = 'A'
                            AND m.status = 'A'
                            AND f.status = 'A'

                    ) uuv

            ) FILTER_RESULT

        ORDER BY
            {string_ordenar_por}

        OFFSET
            0 ROWS

        FETCH
            NEXT :limite ROWS ONLY
    """
    
    # Criando o JSON
    data = []

    dict_query = {
        "query": ultimos_vistos_query,
        "params": params,
    }

    ultimos_vistos_query = dm.raw_sql_return(**dict_query)

    if not ultimos_vistos_query:
        return []

    dict_produtos = {
        "prod_dist_list": ultimos_vistos_query,
        "id_cliente": id_cliente,
        "id_usuario": id_usuario,
        "image_replace": image_replace
    }

    data = json_products(**dict_produtos)

    return data


def json_desconto(id_usuario: int, id_cliente: int, image_replace: str = 'auto', **kwargs):
    """
    Json padrão de produtos com desconto

    Args:
        id_usuario (int): id do usuario.
        id_cliente (int): id do cliente.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    kwargs:
        id_usuario (int): id do usuario.
        id_distribuidor (int): id do distribuidor
        pagina (int): Qual a pagina de procura atual
        limite (int): Quantidade de produtos que devem ser mostrados por pagina
        ordernar_por (int): Qual deve ser a ordenação dos dados

            1 - Preco do maior para o menor

            2 - Preco do menor para o maior 

            3 - Ordem de rankeamento (1 para maior)

            4 - Não implementado (padroniza para o 3)

            5 - Ordenação por ordem alfabética (A à Z)

            6 - Ordenação por ordem alfabética (Z à A)

        multiplo_venda (int): Multiplos de venda dos produtos procurados
        tipo_oferta (int): Qual o tipo de oferta que deve ser retornada

            1 - Ofertas de compre e ganhe

            2 - Ofertas de escalonamento

        marca (list): Lista contendos marcas dos produtos que devem retornar
        grupos (list): Lista contendos grupos dos produtos que devem retornar
        subgrupos (list): Lista contendos subgrupos dos produtos que devem retornar        

    Returns:
        dict: Objeto com os resultados e com a contagem
    """

    id_distribuidor = kwargs.get("id_distribuidor")

    pagina = kwargs.get("pagina")
    limite = kwargs.get("limite")

    ordenar_por = int(kwargs.get("ordenar")) if kwargs.get("ordenar") else 3 # padrão mais vendidos
    if (ordenar_por > 6 or ordenar_por < 1):
        ordenar_por = 3

    tipo_oferta = int(kwargs.get("tipo_oferta")) if kwargs.get("tipo_oferta") else None

    marca = kwargs.get("marca")

    grupos = kwargs.get("grupos")
    subgrupos = kwargs.get("subgrupos")

    multiplo_venda = int(kwargs.get("multiplo_venda")) if kwargs.get("multiplo_venda") else 0

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
            c.id_cliente = :id_cliente
            AND d.id_distribuidor = CASE
                                        WHEN ISNULL(:id_distribuidor, 0) = 0
                                            THEN d.id_distribuidor
                                        ELSE
                                            :id_distribuidor
                                    END
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND cd.d_e_l_e_t_ = 0
            AND d.status = 'A'
    """

    params = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor
    }

    dict_query = {
        "query": query,
        "params": params,
        "raw": True,
    }

    distribuidores = dm.raw_sql_return(**dict_query)
    if not distribuidores:
        return {}

    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    # Fazendo as queries
    counter = 0

    string_ordenar_por = ""
    string_multiplo_venda = ""
    string_oferta = ""
    string_marca = ""
    string_grupos = ""
    string_subgrupos = ""

    params = {
        "distribuidores": distribuidores,
        "id_cliente": id_cliente,
    }

    # Procuras

    ## Procura por multiplo venda
    if multiplo_venda > 0:
        string_multiplo_venda = "AND pd.multiplo_venda = :multiplo_venda"
        params.update({
            "multiplo_venda": multiplo_venda,
        })

    # Procura por tipo_oferta
    if tipo_oferta:

        if tipo_oferta == 1:
            string_tipo_oferta = """
                AND (
                        o.tipo_oferta = 2 
                        AND o.id_oferta IN (

                                                SELECT
                                                    ob.id_oferta

                                                FROM
                                                    PRODUTO_DISTRIBUIDOR pd

                                                    INNER JOIN PRODUTO p ON
                                                        pd.id_produto = p.id_produto

                                                    INNER JOIN OFERTA o ON
                                                        pd.id_distribuidor = o.id_distribuidor

                                                    INNER JOIN
                                                    (
                                                    
                                                        SELECT
                                                            o.id_oferta

                                                        FROM
                                                            OFERTA o

                                                        WHERE
                                                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                                                        UNION

                                                        SELECT
                                                            oc.id_oferta

                                                        FROM
                                                            OFERTA_CLIENTE oc

                                                        WHERE
                                                            oc.id_cliente = :id_cliente
                                                            AND oc.status = 'A'
                                                            AND oc.d_e_l_e_t_ = 0

                                                    ) oc ON
                                                        o.id_oferta = oc.id_oferta

                                                    INNER JOIN OFERTA_BONIFICADO ob ON
                                                        p.id_produto = ob.id_produto
                                                        AND o.id_oferta = ob.id_oferta

                                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                        pd.id_produto = tpp.id_produto
                                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                                    INNER JOIN TABELA_PRECO tp ON
                                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                                WHERE
                                                    pd.estoque > 0
                                                    AND o.id_distribuidor IN :distribuidores
                                                    AND o.tipo_oferta = 2
                                                    AND tp.dt_inicio <= GETDATE()
                                                    AND tp.dt_fim >= GETDATE()
                                                    AND tp.tabela_padrao = 'S'
                                                    AND tp.status = 'A'
                                                    AND ob.status = 'A'
                                                    AND p.status = 'A'
                                                    AND pd.status = 'A'

                                            )
                    )
            """
        
        elif tipo_oferta == 2:
            string_tipo_oferta = """
                AND (
                                    
                        o.tipo_oferta = 3
                        AND o.id_oferta IN (
                                            
                                                SELECT
                                                    id_oferta

                                                FROM
                                                    OFERTA_ESCALONADO_FAIXA

                                                WHERE
                                                    desconto > 0
                                                    AND status = 'A'

                                            )
                            
                    )
            """

        string_oferta = f"""
            INNER JOIN
            (

                SELECT
                    o.id_oferta,
                    o.id_distribuidor,
                    o.tipo_oferta,
                    op.id_produto

                FROM
                    PRODUTO_DISTRIBUIDOR pd

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN OFERTA o ON
                        pd.id_distribuidor = o.id_distribuidor

                    INNER JOIN
                    (
                    
                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

                        WHERE
                            oc.id_cliente = 1
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) oc ON
                        o.id_oferta = oc.id_oferta

                    INNER JOIN OFERTA_PRODUTO op ON
                        p.id_produto = op.id_produto
                        AND o.id_oferta = op.id_oferta

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        pd.id_produto = tpp.id_produto
                        AND pd.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN TABELA_PRECO tp ON
                        tpp.id_tabela_preco = tp.id_tabela_preco
                        AND tpp.id_distribuidor = tp.id_distribuidor

                WHERE
                    pd.estoque > 0
                    AND o.id_distribuidor IN :distribuidores
                    {string_tipo_oferta}
                    AND o.limite_ativacao_oferta > 0
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND tp.tabela_padrao = 'S'
                    AND tp.dt_inicio <= GETDATE()
                    AND tp.dt_fim >= GETDATE()
                    AND op.status = 'A'
                    AND tp.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND o.status = 'A'
            
            ) op ON
                pd.id_produto = op.id_produto
                AND pd.id_distribuidor = op.id_distribuidor
        """

    ## Procura por marca
    if marca:

        if not isinstance(marca, (list, tuple, set)):
            marca = [marca]

        else:
            marca = list(marca)

        params.update({
            "marca": marca
        })

        string_marca = "AND m.id_marca IN :marca"

    if grupos:

        if not isinstance(grupos, (list, tuple, set)):
            grupos = [grupos]

        else:
            grupos = list(grupos)

        params.update({
            "grupos": grupos
        })

        string_grupos = "AND g.id_grupo IN :grupos"

    if subgrupos:

        if not isinstance(subgrupos, (list, tuple, set)):
            subgrupos = [subgrupos]

        else:
            subgrupos = list(subgrupos)

        params.update({
            "subgrupos": subgrupos
        })

        string_subgrupos = "AND s.id_subgrupo IN :subgrupos"

    ## Ordenacao da busca
    dict_ordenacao = {
        1:"preco_tabela DESC",
        2:"preco_tabela ASC",
        3:"ordem_rankeamento ASC, descricao_completa",
        4:"ordem_rankeamento ASC, descricao_completa",
        5:"descricao_completa ASC",
        6:"descricao_completa DESC"
    }

    string_ordenar_por = dict_ordenacao.get(ordenar_por)

    # Paginação
    params.update({
        "offset": (pagina - 1) * limite,
        "limite": limite
    })
    
    desconto_query = f"""
        SELECT
            id_produto,
            id_distribuidor,
            desconto,
            COUNT(*) OVER() count__

        FROM
            (
            
                SELECT
                    DISTINCT
                        p.id_produto,
                        pd.id_distribuidor,
                        pd.ranking,
                        p.descricao_completa,
                        MIN(tpp.preco_tabela) OVER(PARTITION BY tpp.id_produto) preco_tabela,
                        odesc.desconto,
                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

                FROM 
                    PRODUTO p

                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        p.id_produto = pd.id_produto

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        pd.id_produto = tpp.id_produto
                        AND pd.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN TABELA_PRECO tp ON
                        tpp.id_tabela_preco = tp.id_tabela_preco
                        AND tpp.id_distribuidor = tp.id_distribuidor

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        pd.id_produto = ps.codigo_produto

                    INNER JOIN SUBGRUPO s ON
                        ps.id_subgrupo = s.id_subgrupo
                        AND ps.id_distribuidor = s.id_distribuidor

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        s.id_subgrupo = gs.id_subgrupo
                        AND s.id_distribuidor = gs.id_distribuidor

                    INNER JOIN GRUPO g ON
                        gs.id_grupo = g.id_grupo

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                    INNER JOIN FORNECEDOR f ON
                        pd.id_fornecedor = f.id_fornecedor

                    INNER JOIN
                    (
                    
                        SELECT
                            op.id_produto,
                            o.id_distribuidor,
                            od.desconto

                        FROM
                            (

                                SELECT
                                    o.id_oferta

                                FROM
                                    OFERTA o

                                WHERE
                                    id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                                UNION

                                SELECT
                                    oc.id_oferta

                                FROM
                                    OFERTA_CLIENTE oc

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
                            AND o.limite_ativacao_oferta > 0
                            AND od.desconto > 0
                            AND o.data_inicio <= GETDATE()
                            AND o.data_final >= GETDATE()
                            AND o.status = 'A'
                            AND op.status = 'A'
                            AND od.status = 'A'
                    
                    ) odesc ON
                        pd.id_produto = odesc.id_produto
                        AND pd.id_distribuidor = odesc.id_distribuidor

                    {string_oferta}
                        
                WHERE
                    pd.id_distribuidor IN :distribuidores
                    AND pd.estoque > 0
                    {string_grupos}
                    {string_subgrupos}
                    {string_multiplo_venda}
                    {string_marca}
                    AND (
                            (
                                tp.tabela_padrao = 'S'
                            )
                                OR
                            (
                                tp.id_tabela_preco IN (
                                                            SELECT
                                                                id_tabela_preco

                                                            FROM
                                                                TABELA_PRECO_CLIENTE

                                                            WHERE
                                                                id_distribuidor IN :distribuidores
                                                                AND id_cliente = :id_cliente
                                                        )
                            )
                        )
                    AND tp.dt_inicio <= GETDATE()
                    AND tp.dt_fim >= GETDATE()
                    AND tp.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND ps.status = 'A'
                    AND s.status = 'A'
                    AND gs.status = 'A'
                    AND g.status = 'A'
                    AND t.status = 'A'
                    AND m.status = 'A'
                    AND f.status = 'A'

            ) FILTER_RESULT

        ORDER BY
            {string_ordenar_por}

        OFFSET
            :offset ROWS

        FETCH
            NEXT :limite ROWS ONLY
    """

    dict_query = {
        "query": desconto_query,
        "params": params,
    }

    desconto_query = dm.raw_sql_return(**dict_query)

    # Criando o JSON
    if desconto_query:
        dict_produtos = {
            "prod_dist_list": desconto_query,
            "id_cliente": id_cliente,
            "id_usuario": id_usuario,
            "image_replace": image_replace
        }

        full_data = json_products(**dict_produtos)

    else:
        full_data = []

    if full_data:
        counter = desconto_query[0].get("count__")
        
        return {
            "produtos": full_data,
            "counter": counter
        }

    return {}


def json_favoritos(id_usuario: int, id_cliente: int, image_replace: str = 'auto', **kwargs):
    """
    Json padrão de produtos favoritos

    Args:
        id_usuario (int): id do usuario.
        id_cliente (int): id do cliente.
        image_replace (str, optional): Qual deve ser a definição da imagem. Defaults to 'auto'.

    kwargs:
        id_distribuidor (int): id do distribuidor
        pagina (int): Qual a pagina de procura atual
        limite (int): Quantidade de produtos que devem ser mostrados por pagina
        ordernar_por (int): Qual deve ser a ordenação dos dados

            1 - Preco do maior para o menor

            2 - Preco do menor para o maior 

            3 - Ordem de rankeamento (1 para maior)

            4 - Não implementado (padroniza para o 3)

            5 - Ordenação por ordem alfabética (A à Z)

            6 - Ordenação por ordem alfabética (Z à A)

            7 - Mais novo registrado
 
            8 - Mais antigo registrado

        desconto (bool): Caso seja necessário retornar somente produtos com desconto
        estoque (bool): Caso seja necessário retornar somente produtos com estoque
        tipo_oferta (int): Qual o tipo de oferta que deve ser retornada

            1 - Ofertas de compre e ganhe

            2 - Ofertas de escalonamento

        marca (list): Lista contendos marcas dos produtos que devem retornar
        grupos (list): Lista contendos grupos dos produtos que devem retornar
        subgrupos (list): Lista contendos subgrupos dos produtos que devem retornar        

    Returns:
        dict: Objeto com os resultados e com a contagem
    """

    pagina = kwargs.get("pagina")
    limite = kwargs.get("limite")

    ordenar_por = int(kwargs.get("ordenar")) if kwargs.get("ordenar") else 7 # padrão mais novos
    if (ordenar_por > 8 or ordenar_por < 1):
        ordenar_por = 7

    desconto = True if kwargs.get("desconto") else False
    estoque = True if kwargs.get("estoque") else False

    tipo_oferta = int(kwargs.get("tipo_oferta")) if kwargs.get("tipo_oferta") else None

    marca = kwargs.get("marca")

    grupos = kwargs.get("grupos")
    subgrupos = kwargs.get("subgrupos")

    id_distribuidor = kwargs.get("id_distribuidor")

    # Pegando os distribuidores do cliente
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
            c.id_cliente = :id_cliente
            AND d.id_distribuidor = CASE
                                        WHEN ISNULL(:id_distribuidor, 0) = 0
                                            THEN d.id_distribuidor
                                        ELSE
                                            :id_distribuidor
                                    END
            AND c.status = 'A'
            AND c.status_receita = 'A'
            AND cd.status = 'A'
            AND cd.d_e_l_e_t_ = 0
            AND d.status = 'A'                
    """

    params = {
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor
    }

    distribuidores = dm.raw_sql_return(query, params = params, raw = True)
    if not distribuidores:
        return {}

    distribuidores = [id_distribuidor[0] for id_distribuidor in distribuidores]

    # Adicionando filtros do usuario
    params = {
        "id_usuario": id_usuario,
        "id_cliente": id_cliente,
        "id_distribuidor": id_distribuidor,
        "distribuidores": distribuidores,
        "offset": (pagina - 1) * limite,
        "limite": limite,
    }

    string_desconto = ""
    string_estoque = ""
    string_oferta = ""
    string_marca = ""
    string_grupos = ""
    string_subgrupos = ""

    # Ordenação
    dict_ordenacao = {
        1:"preco_tabela DESC, descricao_completa",
        2:"preco_tabela ASC, descricao_completa",
        3:"ordem_rankeamento ASC, descricao_completa",
        4:"ordem_rankeamento ASC, descricao_completa",
        5:"descricao_completa ASC",
        6:"descricao_completa DESC",
        7:"data_cadastro_favorito DESC, descricao_completa",
        8:"data_cadastro_favorito ASC, descricao_completa"
    }

    string_ordenar_por = dict_ordenacao.get(ordenar_por)

    # Procura por desconto
    if desconto:
        string_desconto = """
            INNER JOIN
            (
            
                SELECT
                    op.id_produto,
                    o.id_distribuidor

                FROM
                    (

                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

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
                    AND o.limite_ativacao_oferta > 0
                    AND od.desconto > 0
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND o.status = 'A'
                    AND op.status = 'A'
                    AND od.status = 'A'
            
            ) odesc ON
                pd.id_produto = odesc.id_produto
                AND pd.id_distribuidor = odesc.id_distribuidor
        """

    ## Procura por estoque
    if estoque:
        string_estoque = "AND pd.estoque > 0"

    # Procura por tipo_oferta
    if tipo_oferta:

        if tipo_oferta == 1:
            string_tipo_oferta = """
                AND (
                        o.tipo_oferta = 2 
                        AND o.id_oferta IN (

                                                SELECT
                                                    ob.id_oferta

                                                FROM
                                                    PRODUTO_DISTRIBUIDOR pd

                                                    INNER JOIN PRODUTO p ON
                                                        pd.id_produto = p.id_produto

                                                    INNER JOIN OFERTA o ON
                                                        pd.id_distribuidor = o.id_distribuidor

                                                    INNER JOIN
                                                    (
                                                    
                                                        SELECT
                                                            o.id_oferta

                                                        FROM
                                                            OFERTA o

                                                        WHERE
                                                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                                                        UNION

                                                        SELECT
                                                            oc.id_oferta

                                                        FROM
                                                            OFERTA_CLIENTE oc

                                                        WHERE
                                                            oc.id_cliente = :id_cliente
                                                            AND oc.status = 'A'
                                                            AND oc.d_e_l_e_t_ = 0

                                                    ) oc ON
                                                        o.id_oferta = oc.id_oferta

                                                    INNER JOIN OFERTA_BONIFICADO ob ON
                                                        p.id_produto = ob.id_produto
                                                        AND o.id_oferta = ob.id_oferta

                                                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                                                        pd.id_produto = tpp.id_produto
                                                        AND pd.id_distribuidor = tpp.id_distribuidor

                                                    INNER JOIN TABELA_PRECO tp ON
                                                        tpp.id_tabela_preco = tp.id_tabela_preco
                                                        AND tpp.id_distribuidor = tp.id_distribuidor

                                                WHERE
                                                    pd.estoque > 0
                                                    AND o.id_distribuidor IN :distribuidores
                                                    AND o.tipo_oferta = 2
                                                    AND tp.dt_inicio <= GETDATE()
                                                    AND tp.dt_fim >= GETDATE()
                                                    AND tp.tabela_padrao = 'S'
                                                    AND tp.status = 'A'
                                                    AND ob.status = 'A'
                                                    AND p.status = 'A'
                                                    AND pd.status = 'A'

                                            )
                    )
            """
        
        elif tipo_oferta == 2:
            string_tipo_oferta = """
                AND (
                                    
                        o.tipo_oferta = 3
                        AND o.id_oferta IN (
                                            
                                                SELECT
                                                    id_oferta

                                                FROM
                                                    OFERTA_ESCALONADO_FAIXA

                                                WHERE
                                                    desconto > 0
                                                    AND status = 'A'

                                            )
                            
                    )
            """

        string_oferta = f"""
            INNER JOIN
            (

                SELECT
                    o.id_oferta,
                    o.id_distribuidor,
                    o.tipo_oferta,
                    op.id_produto

                FROM
                    PRODUTO_DISTRIBUIDOR pd

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN OFERTA o ON
                        pd.id_distribuidor = o.id_distribuidor

                    INNER JOIN
                    (
                    
                        SELECT
                            o.id_oferta

                        FROM
                            OFERTA o

                        WHERE
                            id_oferta NOT IN (SELECT DISTINCT id_oferta FROM OFERTA_CLIENTE)

                        UNION

                        SELECT
                            oc.id_oferta

                        FROM
                            OFERTA_CLIENTE oc

                        WHERE
                            oc.id_cliente = 1
                            AND oc.status = 'A'
                            AND oc.d_e_l_e_t_ = 0

                    ) oc ON
                        o.id_oferta = oc.id_oferta

                    INNER JOIN OFERTA_PRODUTO op ON
                        p.id_produto = op.id_produto
                        AND o.id_oferta = op.id_oferta

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        pd.id_produto = tpp.id_produto
                        AND pd.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN TABELA_PRECO tp ON
                        tpp.id_tabela_preco = tp.id_tabela_preco
                        AND tpp.id_distribuidor = tp.id_distribuidor

                WHERE
                    pd.estoque > 0
                    AND o.id_distribuidor IN :distribuidores
                    {string_tipo_oferta}
                    AND o.limite_ativacao_oferta > 0
                    AND o.data_inicio <= GETDATE()
                    AND o.data_final >= GETDATE()
                    AND tp.tabela_padrao = 'S'
                    AND tp.dt_inicio <= GETDATE()
                    AND tp.dt_fim >= GETDATE()
                    AND op.status = 'A'
                    AND tp.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND o.status = 'A'
            
            ) op ON
                pd.id_produto = op.id_produto
                AND pd.id_distribuidor = op.id_distribuidor
        """

    ## Procura por marca
    if marca:

        if not isinstance(marca, (list, tuple, set)):
            marca = [marca]

        else:
            marca = list(marca)

        params.update({
            "marca": marca
        })

        string_marca = "AND m.id_marca IN :marca"

    # Procura por grupos
    if grupos:

        if not isinstance(grupos, (list, tuple, set)):
            grupos = [grupos]

        else:
            grupos = list(grupos)

        params.update({
            "grupos": grupos
        })

        string_grupos = "AND g.id_grupo IN :grupos"

    # Procura por subgrupos
    if subgrupos:

        if not isinstance(subgrupos, (list, tuple, set)):
            subgrupos = [subgrupos]

        else:
            subgrupos = list(subgrupos)

        params.update({
            "subgrupos": subgrupos
        })

        string_subgrupos = "AND s.id_subgrupo IN :subgrupos"

    # Realizando a query
    favorito_query = f"""
        SELECT
            id_produto,
            id_distribuidor,
            data_cadastro_favorito,
            COUNT(*) OVER() count__

        FROM
            (
                SELECT
                    DISTINCT
                        p.id_produto,
                        pd.id_distribuidor,
                        pd.ranking,
                        p.descricao_completa,
                        MIN(tpp.preco_tabela) OVER(PARTITION BY tpp.id_produto) as preco_tabela,
                        uf.data_cadastro as data_cadastro_favorito,
                        CASE WHEN pd.ranking > 0 THEN pd.ranking ELSE 999999 END ordem_rankeamento

                FROM
                    USUARIO_FAVORITO uf
                    
                    INNER JOIN PRODUTO_DISTRIBUIDOR pd ON
                        uf.id_usuario = :id_usuario
                        AND uf.id_produto = pd.id_produto
                        AND uf.id_distribuidor = pd.id_distribuidor

                    INNER JOIN PRODUTO p ON
                        pd.id_produto = p.id_produto

                    INNER JOIN TABELA_PRECO_PRODUTO tpp ON
                        pd.id_produto = tpp.id_produto
                        AND pd.id_distribuidor = tpp.id_distribuidor

                    INNER JOIN TABELA_PRECO tp ON
                        tpp.id_tabela_preco = tp.id_tabela_preco
                        AND tpp.id_distribuidor = tp.id_distribuidor

                    INNER JOIN PRODUTO_SUBGRUPO ps ON
                        pd.id_produto = ps.codigo_produto

                    INNER JOIN SUBGRUPO s ON
                        ps.id_subgrupo = s.id_subgrupo
                        AND ps.id_distribuidor = s.id_distribuidor

                    INNER JOIN GRUPO_SUBGRUPO gs ON
                        s.id_subgrupo = gs.id_subgrupo
                        AND s.id_distribuidor = gs.id_distribuidor

                    INNER JOIN GRUPO g ON
                        gs.id_grupo = g.id_grupo

                    INNER JOIN TIPO t ON
                        g.tipo_pai = t.id_tipo

                    INNER JOIN MARCA m ON
                        pd.id_marca = m.id_marca
                        AND pd.id_distribuidor = m.id_distribuidor

                    INNER JOIN FORNECEDOR f ON
                        pd.id_fornecedor = f.id_fornecedor

                    {string_desconto}

                    {string_oferta}
                        
                WHERE
                    pd.id_distribuidor IN :distribuidores
                    {string_estoque}
                    {string_marca}
                    {string_grupos}
                    {string_subgrupos}
                    AND (
                            (
                                tp.tabela_padrao = 'S'
                            )
                                OR
                            (
                                tp.id_tabela_preco IN (
                                                            SELECT
                                                                id_tabela_preco

                                                            FROM
                                                                TABELA_PRECO_CLIENTE

                                                            WHERE
                                                                id_distribuidor IN :distribuidores
                                                                AND id_cliente = :id_cliente
                                                        )
                            )
                        )
                    AND tp.dt_inicio <= GETDATE()
                    AND tp.dt_fim >= GETDATE()
                    AND tp.status = 'A'
                    AND p.status = 'A'
                    AND pd.status = 'A'
                    AND ps.status = 'A'
                    AND s.status = 'A'
                    AND gs.status = 'A'
                    AND g.status = 'A'
                    AND t.status = 'A'
                    AND m.status = 'A'
                    AND f.status = 'A'

            ) FILTER_RESULT

        ORDER BY
            {string_ordenar_por}

        OFFSET
            :offset ROWS

        FETCH
            NEXT :limite ROWS ONLY
    """

    dict_query = {
        "query": favorito_query,
        "params": params,
    }

    produtos = dm.raw_sql_return(**dict_query)

    if not produtos:
        return {}
    
    # Criando o JSON
    counter = int(produtos[0].get("count__"))
        
    dict_produtos = {
        "prod_dist_list": produtos,
        "id_cliente": id_cliente,
        "id_usuario": id_usuario,
        "image_replace": image_replace,
    }

    data = json_products(**dict_produtos)

    if not data:
        return {}

    return {
        "produtos": data,
        "counter": counter
    }