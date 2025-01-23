#=== Importações de módulos externos ===#
import flask, os, gzip

#=== Importações de módulos internos ===#

# Importações das rotas

######## Rotas Jmanager

## Rotas do Usuario
from app.resources.jmanager.usuarios.alterar_senha_logado_jmanager import AlterarSenhaLogadoJmanager
from app.resources.jmanager.usuarios.registro_usuario_jmanager import RegistroUsuarioJmanager
from app.resources.jmanager.usuarios.listar_usuarios_jmanager import ListarUsuariosJmanager
from app.resources.jmanager.usuarios.alterar_senha_jmanager import AlterarSenhaJmanager
from app.resources.jmanager.usuarios.cadastro_jmanager import CadastroJManager
from app.resources.jmanager.usuarios.logout_jmanager import LogoutJmanager
from app.resources.jmanager.usuarios.login_jmanager import LoginJManager

## Rotas de Ferramentas
from app.resources.jmanager.ferramentas.Listar_logs_erro import ListarLogsErro

## Rotas dos contribuidores
from app.resources.jmanager.colaboradores.alteracao_status_relacao_usuario_cliente import AlteracaoStatusRelacaoUsuarioCliente
from app.resources.jmanager.colaboradores.aprovacao_reprovacao_usuario_cliente import AprovacaoReprovacaoUsuarioCliente
from app.resources.jmanager.colaboradores.upload_imagem_distribuidor import UploadImagemDistribuidor
from app.resources.jmanager.colaboradores.delete_imagem_distribuidor import DeleteImagemDistribuidor
from app.resources.jmanager.colaboradores.alteracao_status_cliente import AlteracaoStatusCliente
from app.resources.jmanager.colaboradores.alteracao_status_usuario import AlteracaoStatusUsuario
from app.resources.jmanager.colaboradores.listar_usuarios_clientes import ListarUsuariosClientes
from app.resources.jmanager.colaboradores.listar_clientes_usuarios import ListarClientesUsuarios
from app.resources.jmanager.colaboradores.registro_cargo_jmanager import RegistroCargoJManager
from app.resources.jmanager.colaboradores.listar_perfil_jmanager import ListarPerfilJManager
from app.resources.jmanager.colaboradores.registro_distribuidor import RegistroDistribuidor
from app.resources.jmanager.colaboradores.envio_notificacao import EnvioNotificacao
from app.resources.jmanager.colaboradores.listar_subgrupo import ListarSubgrupo
from app.resources.jmanager.colaboradores.listar_grupo import ListarGrupo

## Rotas de produtos
from app.resources.jmanager.produtos.aprovacao_produtos_pendentes_jmanager import RegistroProdutosPendentesJmanager
from app.resources.jmanager.produtos.listar_tipo_grupo_subgrupo_geral import ListarTipoGrupoSubgrupoGeral
from app.resources.jmanager.produtos.registro_tipo_grupo_subgrupo import RegistroTipoGrupoSubgrupoProduto
from app.resources.jmanager.produtos.listar_produtos_pendentes import ListarProdutosPendentesJmanager
from app.resources.jmanager.produtos.produtos_distribuidor_jmanager import ProdutosDistribuidor
from app.resources.jmanager.produtos.listar_tipo_grupo_subgrupo import ListarTipoGrupoSubgrupo
from app.resources.jmanager.produtos.remocao_produto_subgrupo import RemocaoProdutoSubgrupo
from app.resources.jmanager.produtos.delete_imagens_produtos import DeleteImagensProdutos
from app.resources.jmanager.produtos.upload_imagens_produtos import UploadImagensProdutos
from app.resources.jmanager.produtos.registro_lista_produtos import RegistroListaProdutos
from app.resources.jmanager.produtos.listar_produtos_listas import ListarProdutosListas
from app.resources.jmanager.produtos.listar_lista_produtos import ListarListasProdutos
from app.resources.jmanager.produtos.listar_marcas import ListarMarcasJManager
from app.resources.jmanager.produtos.listar_fornecedor import ListarFornecedor

######## Rotas Marketplace

## Rotas do Usuario
from app.resources.marketplace.usuarios.registro_orcamento_usuario_marketplace import RegistroOrcamentoUsuarioMarketplace
from app.resources.marketplace.usuarios.registro_ultimos_vistos_marketplace import RegistroUltimosVistosMarketplace
from app.resources.marketplace.usuarios.listar_ultimos_vistos_marketplace import ListarUltimosVistosMarketplace
from app.resources.marketplace.usuarios.dados_cadastrais_usuario_marketplace import RegistroUsuarioMarketplace
from app.resources.marketplace.usuarios.registro_favoritos_marketplace import RegistroFavoritosMarketplace
from app.resources.marketplace.usuarios.remocao_cartao_credito_usuario import RemocaoCartaoUsuarioMarketplace
from app.resources.marketplace.usuarios.cartao_credito_usuario import RegistroCartaoUsuarioMarketplace
from app.resources.marketplace.usuarios.clientes_usuario_marketplace import ClientesUsuarioMarketplace
from app.resources.marketplace.usuarios.listar_favoritos_marketplace import ListarFavoritosMarketplace
from app.resources.marketplace.usuarios.envio_notas_fiscais_pedidos import EnvioNotasFiscaisPedidos
from app.resources.marketplace.usuarios.remocao_usuario_favorito import RemocaoFavoritosMarketplace
from app.resources.marketplace.usuarios.esquecer_senha_marketplace import EsquecerSenhaMarketplace
from app.resources.marketplace.usuarios.alterar_senha_marketplace import AlterarSenhaMarketplace
from app.resources.marketplace.usuarios.alinhamento_login_social import AlinhamentoLoginSocial
from app.resources.marketplace.usuarios.sessao_out_marketplace import SessaoOutMarketplace
from app.resources.marketplace.usuarios.checar_status_pix import ChecarStatusPagamentoPix
from app.resources.marketplace.usuarios.listar_pedidos_financeiro import ListarFinanceiro
from app.resources.marketplace.usuarios.sessao_in_marketplace import SessaoInMarketplace
from app.resources.marketplace.usuarios.upload_imagem_usuario import UploadImagemUsuario
from app.resources.marketplace.usuarios.deletar_dados_usuario import DeletarDadosUsuario
from app.resources.marketplace.usuarios.envio_boletos_pedidos import EnvioBoletosPedidos
from app.resources.marketplace.usuarios.logout_marketplace import LogoutMarketplace
from app.resources.marketplace.clientes.formas_pagamento import FormasPagamento
from app.resources.marketplace.usuarios.cadastro_usuario import CadastroUsuario
from app.resources.marketplace.usuarios.registro_pedido import RegistroPedidos
from app.resources.marketplace.usuarios.login_marketplace import LoginUsuario
from app.resources.marketplace.usuarios.listar_pedidos import ListarPedidos

## Rota dos cliente
from app.resources.marketplace.clientes.limite_credito_cliente import LimiteCreditoCliente
from app.resources.marketplace.clientes.limites import Limites

## Rotas dos colaboradores
from app.resources.marketplace.colaboradores.listar_distribuidor_marketplace import ListarDistribuidorMarketplace
from app.resources.marketplace.colaboradores.listar_fretes_distribuidores import ListarFretesDistribuidores

## Rotas de produtos
from app.resources.marketplace.produtos.listar_tipo_grupo_subgrupo_marketplace import ListarTipoGrupoSubgrupoMarketplace
from app.resources.marketplace.produtos.listar_descontos_marketplace import ListarDescontosMarketplace
from app.resources.marketplace.produtos.listar_produtos_destaques import ListarProdutosDestaquesGrupo
from app.resources.marketplace.produtos.listar_produtos_marketplace import ListarProdutosMarketplace
from app.resources.marketplace.produtos.listar_ofertas_marketplace import ListarOfertasMarketplace
from app.resources.marketplace.produtos.validar_cupons_marketplace import ValidarCuponsMarketplace
from app.resources.marketplace.produtos.listar_marcas_marketplace import ListarMarcasMarketplace
from app.resources.marketplace.produtos.listar_cupons_marketplace import ListarCuponsMarketplace
from app.resources.marketplace.produtos.listar_descricao_produto import ListarDescricaoProduto

## Rotas de ferramentas
from app.resources.marketplace.ferramentas.listar_notificacoes_nao_lidas import ListarNotificacoesNaoLidas
from app.resources.marketplace.ferramentas.listar_notificacoes_lidas import ListarNotificacoesLidas
from app.resources.marketplace.ferramentas.log_error_marketplace import LogErrorMarketplace
from app.resources.marketplace.ferramentas.marcar_notificacao import MarcarNotificacoe
from app.resources.marketplace.ferramentas.aviso_estoque import AvisoEstoque
from app.resources.marketplace.ferramentas.noticias import Noticias
from app.resources.marketplace.ferramentas.home import Home

######## Rotas API
from app.resources.api.integracao_protheus_cliente import IntegracaoProtheusClientes
from app.resources.api.integracao_protheus_pedido import IntegracaoProtheusPedidos
from app.resources.api.registro_estoque_produto import RegistroEstoqueProduto
from app.resources.api.registro_preco_item import RegistroProdutoPrecoItem
from app.resources.api.registro_pre_produto import RegistroPreProduto
from app.resources.api.registro_ofertas import RegistroOfertas
from app.resources.api.login_integracao import LoginIntegracao

######## Rota de teste
from app.resources.test_view import Test

######## Importações da documentação
import documentation.handlers as document 

######## Importação do app
from app.server import app, api, logger, scheduler, global_info
from app.config.sqlalchemy_config import db
import functions.job_management as jobs

#=== Criação dos endpoints ===#

######## Endpoints do Serviço Jmanager

## Usuarios JMANAGER
api.add_resource(AlterarSenhaJmanager, "/api/v1/jmanager/controle-usuarios/mudanca-senha")
api.add_resource(AlterarSenhaLogadoJmanager, "/api/v1/jmanager/controle-usuarios/eu/alterar-senha")
api.add_resource(CadastroJManager, "/api/v1/jmanager/controle-usuarios/cadastro")
api.add_resource(ListarUsuariosJmanager, "/api/v1/jmanager/controle-usuarios/usuarios")
api.add_resource(LoginJManager, "/api/v1/jmanager/controle-usuarios/login")
api.add_resource(LogoutJmanager, "/api/v1/jmanager/controle-usuarios/logout")
api.add_resource(RegistroUsuarioJmanager, "/api/v1/jmanager/controle-usuarios/eu")

## Ferramentas
api.add_resource(ListarLogsErro, "/api/v1/jmanager/logs-marketplace/erro")

## Colaboradores
api.add_resource(AlteracaoStatusCliente, "/api/v1/jmanager/colaboradores/clientes-marketplace/status")
api.add_resource(AlteracaoStatusUsuario, "/api/v1/jmanager/colaboradores/usuarios-marketplace/status")
api.add_resource(AlteracaoStatusRelacaoUsuarioCliente, "/api/v1/jmanager/colaboradores/usuarios-clientes-marketplace/status")
api.add_resource(AprovacaoReprovacaoUsuarioCliente, "/api/v1/jmanager/colaboradores/clientes-marketplace/registro")
api.add_resource(DeleteImagemDistribuidor, "/api/v1/jmanager/colaboradores/distribuidores/imagem/delete")
api.add_resource(EnvioNotificacao, "/api/v1/jmanager/colaboradores/envio-notificacao")
api.add_resource(ListarClientesUsuarios, "/api/v1/jmanager/colaboradores/clientes-marketplace/usuarios")
api.add_resource(ListarGrupo, "/api/v1/jmanager/colaboradores/grupos")
api.add_resource(ListarPerfilJManager, "/api/v1/jmanager/colaboradores/perfis")
api.add_resource(ListarSubgrupo, "/api/v1/jmanager/colaboradores/subgrupos")
api.add_resource(ListarUsuariosClientes, "/api/v1/jmanager/colaboradores/usuarios-marketplace/clientes")
api.add_resource(RegistroCargoJManager, "/api/v1/jmanager/colaboradores/cargos")
api.add_resource(RegistroDistribuidor, "/api/v1/jmanager/colaboradores/distribuidores")
api.add_resource(UploadImagemDistribuidor, "/api/v1/jmanager/colaboradores/distribuidores/imagem/upload")

## Produtos
api.add_resource(DeleteImagensProdutos, "/api/v1/jmanager/produtos/imagem/remocao")
api.add_resource(ListarFornecedor, "/api/v1/jmanager/produtos/fornecedores")
api.add_resource(ListarListasProdutos, "/api/v1/jmanager/produtos/listas")
api.add_resource(ListarMarcasJManager, "/api/v1/jmanager/produtos/marcas", "/api/v1/jmanager/produtos/marcas/<int:id_distribuidor>")
api.add_resource(ListarProdutosListas, "/api/v1/jmanager/produtos/listas/registros")
api.add_resource(ListarProdutosPendentesJmanager, "/api/v1/jmanager/produtos/pendentes")
api.add_resource(ListarTipoGrupoSubgrupo, "/api/v1/jmanager/produtos/tipo-grupo-subgrupo")
api.add_resource(ListarTipoGrupoSubgrupoGeral, "/api/v1/jmanager/produtos/tipo-grupo-subgrupo/geral")
api.add_resource(ProdutosDistribuidor, "/api/v1/jmanager/produtos/distribuidor")
api.add_resource(RegistroListaProdutos, "/api/v1/jmanager/produtos/listas/cadastro")
api.add_resource(RegistroProdutosPendentesJmanager, "/api/v1/jmanager/produtos/pendentes/status")
api.add_resource(RegistroTipoGrupoSubgrupoProduto, "/api/v1/jmanager/produtos/tipo-grupo-subgrupo/registro")
api.add_resource(RemocaoProdutoSubgrupo, "/api/v1/jmanager/produtos/subgrupos/remocao")
api.add_resource(UploadImagensProdutos, "/api/v1/jmanager/produtos/imagem/upload")

######## Endpoints do Serviço Marketplace

## Endpoints do usuario 
api.add_resource(AlinhamentoLoginSocial, "/api/v1/marketplace/usuarios/login-social")
api.add_resource(AlterarSenhaMarketplace, "/api/v1/marketplace/usuarios/mudanca-senha")
api.add_resource(CadastroUsuario, "/api/v1/marketplace/usuarios/cadastro")
api.add_resource(ChecarStatusPagamentoPix, "/api/v1/marketplace/usuarios/eu/pedidos/pix")
api.add_resource(ClientesUsuarioMarketplace, "/api/v1/marketplace/usuarios/eu/clientes")
api.add_resource(DeletarDadosUsuario, "/api/v1/marketplace/usuarios/eu/exclusao")
api.add_resource(EnvioBoletosPedidos, "/api/v1/marketplace/usuarios/eu/pedidos/boletos")
api.add_resource(EnvioNotasFiscaisPedidos, "/api/v1/marketplace/usuarios/eu/pedidos/notas-fiscais")
api.add_resource(EsquecerSenhaMarketplace, "/api/v1/marketplace/usuarios/senha-temporaria")
api.add_resource(ListarFavoritosMarketplace, "/api/v1/marketplace/usuarios/eu/favoritos")
api.add_resource(ListarFinanceiro, "/api/v1/marketplace/usuarios/eu/pedidos/financeiro")
api.add_resource(ListarPedidos, "/api/v1/marketplace/usuarios/eu/pedidos")
api.add_resource(ListarUltimosVistosMarketplace, "/api/v1/marketplace/usuarios/eu/ultimos-vistos")
api.add_resource(LoginUsuario, "/api/v1/marketplace/usuarios/login")
api.add_resource(LogoutMarketplace, "/api/v1/marketplace/usuarios/logout")
api.add_resource(RegistroCartaoUsuarioMarketplace, "/api/v1/marketplace/usuarios/eu/cartoes")
api.add_resource(RegistroFavoritosMarketplace, "/api/v1/marketplace/usuarios/eu/favoritos/registro")
api.add_resource(RegistroOrcamentoUsuarioMarketplace, "/api/v1/marketplace/usuarios/eu/orcamento", "/api/v1/marketplace/usuarios/eu/orcamento/<int:id_cliente>")
api.add_resource(RegistroPedidos, "/api/v1/marketplace/usuarios/eu/pedidos/registro")
api.add_resource(RegistroUltimosVistosMarketplace, "/api/v1/marketplace/usuarios/eu/ultimos-vistos/registro")
api.add_resource(RegistroUsuarioMarketplace, "/api/v1/marketplace/usuarios/dados-cadastrais")
api.add_resource(RemocaoCartaoUsuarioMarketplace, "/api/v1/marketplace/usuarios/eu/cartoes/remocao")
api.add_resource(RemocaoFavoritosMarketplace, "/api/v1/marketplace/usuarios/eu/favoritos/remocao")
api.add_resource(SessaoInMarketplace, "/api/v1/marketplace/usuarios/sessoes/inicializacao")
api.add_resource(SessaoOutMarketplace, "/api/v1/marketplace/usuarios/sessoes/finalizacao")
api.add_resource(UploadImagemUsuario, "/api/v1/marketplace/usuarios/eu/imagem/upload")

## Endpoints dos clientes
api.add_resource(FormasPagamento, "/api/v1/marketplace/clientes/formas-pagamento")
api.add_resource(Limites, "/api/v1/marketplace/clientes/limites")
api.add_resource(LimiteCreditoCliente, "/api/v1/marketplace/clientes/limite-credito")

## Endpoints dos colaboradores
api.add_resource(ListarDistribuidorMarketplace, "/api/v1/marketplace/colaboradores/distribuidores", "/api/v1/marketplace/colaboradores/distribuidores/<int:id_cliente>")
api.add_resource(ListarFretesDistribuidores, "/api/v1/marketplace/colaboradores/distribuidores/fretes/<int:id_cliente>")

## Endpoints dos produtos
api.add_resource(ListarCuponsMarketplace, '/api/v1/marketplace/produtos/cupons')
api.add_resource(ListarDescontosMarketplace, "/api/v1/marketplace/produtos/ofertas/descontos")
api.add_resource(ListarDescricaoProduto, "/api/v1/marketplace/produtos/descricao")
api.add_resource(ListarMarcasMarketplace, "/api/v1/marketplace/produtos/marcas")
api.add_resource(ListarOfertasMarketplace, "/api/v1/marketplace/produtos/ofertas")
api.add_resource(ListarProdutosDestaquesGrupo, "/api/v1/marketplace/produtos/tipo-grupo-subgrupo/grupo/produtos-destaques")
api.add_resource(ListarProdutosMarketplace, "/api/v1/marketplace/produtos")
api.add_resource(ListarTipoGrupoSubgrupoMarketplace, "/api/v1/marketplace/produtos/tipo-grupo-subgrupo")
api.add_resource(ValidarCuponsMarketplace, '/api/v1/marketplace/produtos/cupons/validacao')

## Endpoint das ferramentas
api.add_resource(AvisoEstoque, '/api/v1/marketplace/notificacoes/estoque-produto')
api.add_resource(Home, "/api/v1/marketplace/home")
api.add_resource(ListarNotificacoesLidas, '/api/v1/marketplace/notificacoes/lidas')
api.add_resource(ListarNotificacoesNaoLidas, '/api/v1/marketplace/notificacoes/nao-lidas')
api.add_resource(LogErrorMarketplace, "/api/v1/marketplace/log-erro")
api.add_resource(MarcarNotificacoe, "/api/v1/marketplace/notificacoes/checagem")
api.add_resource(Noticias, "/api/v1/marketplace/noticias")

######## Endpoints do Serviço API
api.add_resource(IntegracaoProtheusClientes, "/api/v1/api-registro/usuarios-clientes")
api.add_resource(IntegracaoProtheusPedidos, "/api/v1/api-registro/pedidos")
api.add_resource(LoginIntegracao, "/api/v1/api-registro/login")
api.add_resource(RegistroEstoqueProduto, "/api/v1/api-registro/produtos/estoque")
api.add_resource(RegistroOfertas, "/api/v1/api-registro/ofertas")
api.add_resource(RegistroPreProduto, "/api/v1/api-registro/produtos")
api.add_resource(RegistroProdutoPrecoItem, "/api/v1/api-registro/produtos/tabela-preco")

######## Endpoint de Teste
api.add_resource(Test, "/api/v1/test")

######## Configurações antes da request
@app.before_request
@logger
def before_request():

    global_info.clean_info_thread()

    endpoint = str(flask.request.path).lower()

    if "/marketplace/" in endpoint:
        api = "marketplace"

    elif "/jmanager/" in endpoint:
        api = "jmanager"

    elif "/api-registro/" in endpoint:
        api = "api-registro"

    else:
        api = "main"

    full_headers = flask.request.headers

    headers = {
        "Content-Type": full_headers.get("Content-Type"),
        "token": full_headers.get("token"),
    }

    token_social = full_headers.get("auth-token-social")
    plataforma = full_headers.get("social-platform")

    if token_social and plataforma:
        headers.update({
            "auth-token-social": token_social,
            "social-platform": plataforma,
        })

    global_info.save_info_thread(**{
        "endpoint": endpoint,
        "api": api,
        "method": flask.request.method,
        "data": flask.request.data,
        "headers": headers,
    })

######## Configurações depois da request
@app.after_request
@logger
def after_request(request_response):

    db.remove()

    global_info.clean_info_thread()

    if isinstance(request_response,  flask.wrappers.Response):

        if request_response.mimetype == "application/json":
            content = gzip.compress(flask.json.dumps(request_response.json).encode('utf8'))
            response = flask.make_response(content)

            response.headers = {
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
                "Content-length": len(content),
            }

            response.status = request_response.status_code

            return response

    return request_response


# Endpoints da Documentação
@app.route("/")
@app.route("/<string:path>")
def home_page(path = None):

    if path == None:  path = ""

    home_url = ["", "home", "documentation"]
    return document.home_page(path, home_url)


@app.route("/documentation/<string:api>")
def subhome_page(api):

    return document.subhome_page(api)


@app.route("/documentation/archives/<string:api>/<path:path_total>")
def documentation_routes(api, path_total):

    return document.doc_render(api, path_total)


@app.route("/page_not_found")
def page_not_found():
    
    return document.not_found()


# Jobs

## Notificação
key_job_notification = str(os.environ.get("NOTIFICATION_JOB_LIBERATION_KEY_PS")).upper() == 'S'
tempo_espera_notificacao = os.environ.get("NOTIFICATION_SEND_MINUTES_WAIT_PS")

if not str(tempo_espera_notificacao).isdecimal(): tempo_espera_notificacao = 30
else: tempo_espera_notificacao = int(tempo_espera_notificacao)

@logger
def enviar_notificacao():

    scheduler.pause_job("notification_job")

    global_info.save_info_thread(**{
        "api": "job"    
    })

    logger.info("Iniciando job de envio de notificações.")

    response = jobs.notification_job()

    answer_1 = response.get("notificacoes_enviadas")
    answer_2 = response.get("notificacoes_falhas")

    logger.info(f"Job de envio de notificações finalizado com {answer_1} notificacoes enviadas e {answer_2} notificacoes falhas.", extra = response)

    scheduler.resume_job("notification_job")

    return after_request({"mensagem": "Job finalizado"})

    
## Lembrete de estoque
key_job_notification_stock = str(os.environ.get("STOCK_NOTIFICATION_JOB_LIBERATION_KEY_PS")).upper() == 'S'
tempo_espera_estoque = os.environ.get("STOCK_NOTIFICATION_SEND_MINUTES_WAIT_PS")

if not str(tempo_espera_estoque).isdecimal(): tempo_espera_estoque = 30
else: tempo_espera_estoque = int(tempo_espera_estoque)

@logger
def enviar_notificacao_estoque():

    scheduler.pause_job("stock_notification_job")

    global_info.save_info_thread(**{
        "api": "job"    
    })

    logger.info("Iniciando job de envio de notificações de lembrete de estoque.")

    response = jobs.stock_notification_job()

    answer_1 = response.get("notificacoes_enviadas")
    answer_2 = response.get("notificacoes_falhas")

    logger.info(f"Job de envio de notificações de lembrete de estoque finalizado com {answer_1} notificacoes enviadas e {answer_2} notificacoes falhas.", extra = response)

    scheduler.resume_job("stock_notification_job")

    return after_request({"mensagem": "Job finalizado"})


## Envio de email
key_job_email = str(os.environ.get("EMAIL_JOB_LIBERATION_KEY_PS")).upper() == 'S'
tempo_espera_email = os.environ.get("EMAIL_SEND_MINUTES_WAIT_PS")

if not str(tempo_espera_email).isdecimal(): tempo_espera_email = 30
else: tempo_espera_email = int(tempo_espera_email)

@logger
def enviar_email():

    scheduler.pause_job("email_job")

    global_info.save_info_thread(**{
        "api": "job"    
    })

    logger.info("Iniciando job de envio de email.")

    response = jobs.email_job()

    answer_1 = response.get("emails_enviados")
    answer_2 = response.get("emails_falhos")

    logger.info(f"Job de envio de emails finalizado com {answer_1} emails enviados e {answer_2} emails falhos.", extra = response)

    scheduler.resume_job("email_job")

    return after_request({"mensagem": "Job finalizado"})


## Checagem do pix
key_job_pix = str(os.environ.get("PIX_JOB_LIBERATION_KEY_PS")).upper() == 'S'
tempo_espera_pix = os.environ.get("PIX_CHECK_LIMIT_JOB_PS")

if not str(tempo_espera_pix).isdecimal(): tempo_espera_pix = 30
else: tempo_espera_pix = int(tempo_espera_pix)

@logger
@app.route("/teste-pix")
def checar_pix():

    scheduler.pause_job("pix_job")

    global_info.save_info_thread(**{
        "api": "job"    
    })

    logger.info("Iniciando job de checagem de pix.")

    response = jobs.pix_job()

    answer_1 = response.get("pix")
    answer_2 = response.get("pix_falhos")

    logger.info(f"Job de checagem de pix finalizado com {answer_1} pixs checados e {answer_2} pixs com falhas apresentadas.", extra = response)

    scheduler.resume_job("pix_job")

    return after_request({"mensagem": "Job finalizado"})


## Inicialização dos jobs
if key_job_notification:
    scheduler.add_job(enviar_notificacao, "interval", id='notification_job', minutes = tempo_espera_notificacao)

if key_job_notification_stock:
    scheduler.add_job(enviar_notificacao_estoque, "interval", id='stock_notification_job', minutes = tempo_espera_estoque)

if key_job_email:
    scheduler.add_job(enviar_email, "interval", id='email_job', minutes = tempo_espera_email)

if key_job_pix:
    scheduler.add_job(checar_pix, "interval", id='pix_job', minutes = tempo_espera_pix)