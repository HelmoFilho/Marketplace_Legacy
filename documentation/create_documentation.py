#=== Importações de módulos externos ===#
import os, json, yaml, shutil

def create_documentation():
    """
    Cria a documentação, não roda pelo programa principal
    """

    archives_url = "documentation\\archives"
    main_object = {}

    apis = [file  for file in os.listdir(archives_url) if ".yaml" not in file]

    for api in apis:

        holder = []

        subdir = os.listdir(f"{archives_url}\\{api}")
        
        subfiles = [file  for file in subdir  if ".yaml" in file and api in file]

        # Salvando o arquivo para o template conseguir enxergar
        if not os.path.exists("app/templates/static/archives"):
            os.mkdir("app/templates/static/archives")

        if not os.path.exists(f"app/templates/static/archives/{api}"):
            os.mkdir(f"app/templates/static/archives/{api}")

        for subfile in subfiles:

            doc_dir = f"{archives_url}\\{api}\\{subfile}"

            up_api = str(api).upper()

            section_object = {
                "full_doc": doc_dir.replace("\\", "/"),
                "security": False,
                "paths": []
            }

            with open(doc_dir, 'rt', encoding = 'utf8') as f:
                os_list = yaml.safe_load(f)
                data_json = json.loads(json.dumps(os_list, indent = 4))

            # Copiando o arquivo para o template ver
            shutil.copy(doc_dir, f"app/templates/static/archives/{api}/{subfile}")

            # Estruturando o JSON
            if data_json:
                for key, value in data_json.items():

                    if key == "components":

                        if value.get("securitySchemes"):
                            section_object.update({
                                "security": True
                            })

                    if key == "tags":

                        for tag_obj in value:

                            section_object["paths"].append({
                                "service": tag_obj.get("name"),
                                "description": tag_obj.get("description"),
                                "route": None,
                                "methods": []
                            })

                    if key == "paths":

                        for route, route_obj in value.items():
                            place = None
                            summary = None
                            for method, method_obj in route_obj.items():
                                
                                for inside_m, inside_m_obj in method_obj.items():
                                    if inside_m == "tags":
                                        for section_list in section_object["paths"]:
                                            if inside_m_obj[0] == section_list.get("service"):
                                                place = inside_m_obj[0]
                                                break
                                    
                                    if inside_m == "summary" and place is not None:
                                        summary = inside_m_obj
                                        break

                                for section_list in section_object["paths"]:
                                    if section_list.get("service") == place:
                                        section_list.update({
                                            "route": str(route)
                                        })
                                        section_list["methods"].append({
                                            "method": str(method),
                                            "summary": str(summary)
                                        })
                                        break
            
            holder.append(section_object)

        main_object.update({
            up_api: holder
        })              

    if not os.path.exists("app/templates/static/json"):
        os.mkdir("app/templates/static/json")

    if not os.path.exists("app/templates/static/json/doc.json"):
        with open("app/templates/static/json/doc.json", "x"): pass

    with open("app/templates/static/json/doc.json", "w+", encoding = "utf-8") as f:
        json.dump(main_object, f, ensure_ascii=False)


if __name__ == "__main__":
    create_documentation()