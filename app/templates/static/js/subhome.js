function capitalizeFirstLetter(string) {   
    return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase(); 
}

function unidecode(string) {
    return string.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
}

function subHome(mainJson, chosenPath, appRoute) {
    
    if (typeof(mainJson) === "string") {
        mainJson = JSON.parse(mainJson);
    }

    let lowerPath = chosenPath.toLowerCase();
    let upperPath = chosenPath.toUpperCase();

    let main = document.createElement("main");
    let section = document.createElement("section");

    let mainKeys = Object.keys(mainJson);
    
    // Criando o full-block
    let mainBlock = document.createElement("div");
    mainBlock.className = 'main-block';

    if (mainKeys.includes(upperPath)) {

        let tableSectionHeader = document.createElement("table");
        tableSectionHeader.className = 'table-section-header';

        let theadSectionHeader = tableSectionHeader.createTHead();
        let row = theadSectionHeader.insertRow();
        
        let th = document.createElement("th");
        
        let h2 = document.createElement('h2');
        h2.textContent = upperPath;
        
        th.appendChild(h2);
        row.appendChild(th);

        mainBlock.appendChild(tableSectionHeader);

        let docArray = [];

        for (let i = 0; i < mainJson[upperPath].length; i++) {
            docArray.push(mainJson[upperPath][i]["full_doc"]);
        }

        for (let i = 0; i < mainJson[upperPath].length; i++) {
            
            let docInfo = mainJson[upperPath][i];
            
            let fullDoc = docInfo["full_doc"];
            let checkSecurity = docInfo["security"];
            let paths = docInfo["paths"];

            let sectionName = fullDoc.split('/');
            let subSectionName = '';
            sectionName = sectionName[sectionName.length - 1];
            
            sectionName = sectionName.split('.')[0];
            
            if (sectionName.toUpperCase() !== upperPath) {
                sectionName = sectionName.split("_");
                subSectionName = sectionName[sectionName.length - 1];

                let h3 = document.createElement('h3');
                h3.textContent = `Seção - ${capitalizeFirstLetter(subSectionName)}`;

                mainBlock.appendChild(h3);
            }
            
            // Criando o full-doc-table
            let fullDocTable = document.createElement("table");
            fullDocTable.className = "full-doc-table";

            let fullDocBody = fullDocTable.createTBody();
            let fullDocRow = fullDocBody.insertRow(0);
            
            //-- Parte 1 url completa
            let fullDocTh = document.createElement("th");
            fullDocTh.textContent = 'Documentação completa da seção'
            fullDocTh.colSpan = "1";

            let fullDocTd = document.createElement("td");
            fullDocTd.colSpan = "2"

            let fullDocTdA = document.createElement("a");
            fullDocTdA.href = `${appRoute}/${fullDoc}`;
            fullDocTdA.textContent = `${appRoute}/${fullDoc}`;
            fullDocTd.appendChild(fullDocTdA);
            
            fullDocRow.appendChild(fullDocTh);
            fullDocRow.appendChild(fullDocTd);

            if (checkSecurity) {

                //-- Parte 2 nada
                fullDocRow = fullDocBody.insertRow(1);
                
                let tableSeparator = document.createElement("td");
                tableSeparator.className = 'table-separator';
                tableSeparator.colSpan = '3';
                fullDocRow.appendChild(tableSeparator);

                //-- Parte 3 security I
                fullDocRow = fullDocBody.insertRow(2);

                fullDocTh = document.createElement("th");
                fullDocTh.textContent = 'Atalho para seção de autenticação'
                fullDocTh.rowSpan = "2";

                fullDocRow.appendChild(fullDocTh);

                fullDocTd = document.createElement("td");
                fullDocTd.textContent = `Informações de segurança da api de CLIENTES - Seção segurança`
                fullDocTd.colSpan = '2';

                fullDocRow.appendChild(fullDocTd);

                //-- Parte 4 security II
                fullDocRow = fullDocBody.insertRow(3);

                fullDocTd = document.createElement("td");
                fullDocTd.textContent = `URL`
                fullDocTd.colSpan = '1';

                fullDocRow.appendChild(fullDocTd);

                fullDocTd = document.createElement("td");
                fullDocTd.colSpan = '1';
                let fullDocTdA = document.createElement("a");
                fullDocTdA.href = `${appRoute}/${fullDoc}#section/Authentication`;
                fullDocTdA.textContent = "Documentação do token";
                
                fullDocTd.append(fullDocTdA);
                fullDocRow.appendChild(fullDocTd);               

            }
            
            mainBlock.appendChild(fullDocTable);

            // Criando a tabela de endpoints

            if (paths.length > 0) {
                
                let endpointsTable = document.createElement("table");
                endpointsTable.className = "endpoints-table";

                let endpointsTableHead = endpointsTable.createTHead();
                let endpointsTableRow = endpointsTableHead.insertRow();

                let enpointsTableTh = document.createElement("th");
                enpointsTableTh.colSpan = '4';
                enpointsTableTh.textContent = 'ENDPOINTS';
                enpointsTableTh.className = "endpoints-Table-header";

                endpointsTableRow.appendChild(enpointsTableTh);

                let endpointsTableBody = endpointsTable.createTBody();

                for (let j = 0; j < paths.length; j++) {

                    let service = paths[j]["service"];
                    let description = paths[j]["description"];
                    let route = paths[j]["route"];
                    let methods = paths[j]["methods"];

                    let endpointA = document.createElement("a");

                    let hRef = `${appRoute}/${fullDoc}#tag/${unidecode(service).replaceAll(" ", "-")}`;
                    
                    endpointA.href = hRef;
                    endpointA.textContent = `${appRoute}/api/v1${route}`;

                    let endpointsTableRow = endpointsTableBody.insertRow();

                    let methodLength = methods.length;

                    // Parte 1
                    tableSeparator = document.createElement("td");
                    tableSeparator.className = 'table-separator';
                    tableSeparator.colSpan = `4`;
                    endpointsTableRow.appendChild(tableSeparator);

                    // Parte 2
                    let endpointsTableTh = document.createElement("th");
                    endpointsTableTh.textContent = `${capitalizeFirstLetter(service)}`;
                    endpointsTableTh.rowSpan = `${4 + (methodLength * 2)}`;
                    endpointsTableTh.colSpan = "1";

                    endpointsTableRow = endpointsTableBody.insertRow();
                    endpointsTableRow.appendChild(endpointsTableTh);

                    let endpointsTableTd = document.createElement("td");
                    endpointsTableTd.textContent = `${capitalizeFirstLetter(description)}`;
                    endpointsTableTd.colSpan = `3`;

                    endpointsTableRow = endpointsTableBody.insertRow();
                    endpointsTableRow.appendChild(endpointsTableTd);

                    // Parte 3
                    endpointsTableTd = document.createElement("td");
                    endpointsTableTd.textContent = `endpoint`;
                    endpointsTableTd.colSpan = "1";

                    endpointsTableRow = endpointsTableBody.insertRow();
                    endpointsTableRow.appendChild(endpointsTableTd);

                    endpointsTableTd = document.createElement("td");
                    endpointsTableTd.appendChild(endpointA);
                    endpointsTableTd.colSpan = `2`;

                    endpointsTableRow.appendChild(endpointsTableTd);

                    // Parte 4
                    if (methodLength > 0) {
                        endpointsTableTd = document.createElement("td");
                        endpointsTableTd.className = "method-separator";
                        endpointsTableTd.textContent = "Métodos";
                        endpointsTableTd.colSpan = '3';

                        endpointsTableRow = endpointsTableBody.insertRow();
                        endpointsTableRow.appendChild(endpointsTableTd);

                        // Parte 5
                        for (let k = 0; k < methodLength; k++) {

                            let method = methods[k]["method"];
                            let summary = methods[k]["summary"];

                            endpointsTableRow = endpointsTableBody.insertRow();

                            endpointsTableTd = document.createElement("td");
                            endpointsTableTd.textContent = method.toUpperCase();
                            endpointsTableTd.rowSpan = "2";
                            endpointsTableTd.colSpan = '1';

                            endpointsTableRow.appendChild(endpointsTableTd);

                            endpointsTableTd = document.createElement("td");
                            endpointsTableTd.textContent = capitalizeFirstLetter(summary);
                            endpointsTableTd.colSpan = '2';

                            endpointsTableRow.appendChild(endpointsTableTd);

                            // Parte 6
                            endpointsTableRow = endpointsTableBody.insertRow();

                            endpointsTableTd = document.createElement("td");
                            endpointsTableTd.textContent = 'URL';

                            endpointsTableRow.appendChild(endpointsTableTd);

                            endpointsTableTd = document.createElement("td");

                            let specificA = document.createElement("a");
                            specificA.textContent = 'Documentação do método especifico';
                            specificA.href = `${hRef}/paths/${route.replaceAll("/", "~1")}/${method.toLowerCase()}`

                            endpointsTableTd.appendChild(specificA);
                            endpointsTableRow.appendChild(endpointsTableTd);
                        }
                    }
                }

                mainBlock.appendChild(endpointsTable);

            }

            // Table separator
            if (fullDoc !== docArray[docArray.length - 1]) {
                
                let sectionSeparator = document.createElement("hr");
                sectionSeparator.className = 'section-separator';

                mainBlock.appendChild(sectionSeparator);
            }

        }

        // Salvando os dados
        section.appendChild(mainBlock);
        main.appendChild(section);
        document.body.appendChild(main);
    }
}

subHome(docJson, path, urlRoute);