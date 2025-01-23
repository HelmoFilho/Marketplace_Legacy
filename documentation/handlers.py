#=== Importações de módulos externos ===#
from flask import render_template, make_response, redirect, url_for
import os, json

#=== Setando variaveis Globais
headers = {'Content-Type': 'text/html'}

server = str(os.environ.get("PSERVER_PS")).lower()

if server == "production":
    route = str(os.environ.get("SERVER_GUARANY_PS")).lower()
    
elif server == "development":
    route = f"http://{os.environ.get('MAIN_IP_PS')}:{os.environ.get('MAIN_PORT_PS')}"

else:
    route = f"http://localhost:{os.environ.get('MAIN_PORT_PS')}"

    

def home_page(path: str = "", valid: list = []):
    """
    Home Page da documentação

    Args:
        path (str, optional): Caminho pertencente a documentação. Defaults to "".
        valid (list, optional): Caminhos validos que apontam para a home page. Defaults to [].

    Returns:
        [flask.wrappers.Response]: Página HTML
    """

    if os.environ.get('SHOW_DOCUMENTATION_PS') != 'S':
        return redirect(url_for("page_not_found"))

    # Se o path não existir destro da lista de validos, retorna a pagina de 404
    if path not in valid:
        return redirect(url_for("page_not_found"))

    try:
        with open("app\\templates\\static\\json\\doc.json", "r", encoding = "utf-8") as f:
            doc_json = json.dumps(json.loads(f.read()))

    except:
        doc_json = ""

    response =  make_response(render_template("home.html", doc_json = doc_json, 
                    url = f"{route}"), 200, headers)
    return response


def subhome_page(api: str = ""):
    """
    SubHome Page da documentação

    Args:
        path (str, optional): Caminho pertencente a documentação. Defaults to "".
        valid (list, optional): Caminhos validos que apontam para a home page. Defaults to [].

    Returns:
        [flask.wrappers.Response]: Página HTML
    """

    if os.environ.get('SHOW_DOCUMENTATION_PS') != 'S':
        return redirect(url_for("page_not_found"))

    try:
        with open("app\\templates\\static\\json\\doc.json", "r", encoding = "utf-8") as f:
            doc_json = json.loads(f.read())

    except:
        doc_json = {}

    valid = list(doc_json.keys())

    # Se o path não existir destro da lista de validos, retorna a pagina de 404
    if str(api).upper() not in valid:
        return redirect(url_for("page_not_found"))

    response =  make_response(render_template("subhome.html", doc_json = json.dumps(doc_json), 
                    url = f"{route}", api = api), 200, headers)
    return response


def doc_render(chosen_api: str, path: str):
    """
    Retorna a pagina HTML referente a uma documentação especifica

    Args:
        path (str): Caminho da documentação especifica

    Returns:
        [flask.wrappers.Response]: Página HTML
    """

    # Cria uma lista com o nome de todos os templates e verifica se o 
    # caminho digitado existe entre os templates
    if os.environ.get('SHOW_DOCUMENTATION_PS') != 'S':
        return redirect(url_for("page_not_found"))

    templates = "app/templates/static"
    html_route = f"archives/{chosen_api}"
    file = str(path)

    try:
        paths, redoc_filters = file.split("#")

    except:
        paths = file
        redoc_filters = ""

    paths = paths.split("/")

    if not paths:
        return redirect(url_for("page_not_found"))

    templates = f"{templates}/{html_route}"
       
    for path in paths:
        
        templates = f"{templates}/{path}"

        if not os.path.exists(templates):
            break
            
        html_route = f"{html_route}/{path}"

    else:
        if redoc_filters:
            html_route = f"{html_route}#{redoc_filters}"

        return make_response(render_template("redoc.html", filename = html_route), 200, headers)

    return redirect(url_for("page_not_found"))


def not_found():
    """
    Retorna a pagina de "não-encontrado"

    Returns:
        [flask.wrappers.Response]: Página HTML
    """

    return render_template("not_found.html"), 404