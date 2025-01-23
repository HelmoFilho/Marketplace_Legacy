function mainHome(mainJson) {
    
    if (mainJson) {

        if (typeof(mainJson) === "string") {
            mainJson = JSON.parse(mainJson);
        }
    
        let main = document.createElement("main");
        let section = document.createElement("section");
    
        let mainKeys = Object.keys(mainJson);
    
        // Criando o explaining block 
        let expBlock = document.createElement("div");
        expBlock.className = 'exp-block';
    
        let h2Exp = document.createElement("h2");
        h2Exp.textContent = "Bem-vindo a documentação oficial do B2B";
    
        let pExp = document.createElement("p");
        pExp.textContent = "Aqui ficam armazenadas as documentações das apis utilizadas no projeto B2B. Estas api estão separadas para cada seção do projeto que são:";
    
        expBlock.appendChild(h2Exp);
        expBlock.appendChild(document.createElement("br"));
        expBlock.appendChild(pExp);
        expBlock.appendChild(document.createElement("br"));
    
        let ulExp = document.createElement("ul");

        let explaining = {
            "api": "Api utilizada para inserção de produtos, tabelas de preços e valores e estoques.",
            "jmanager": "Api utilizada pelo portal jmanager.",
            "marketplace": "Api utilizada pelo aplicativo JSMarket."
        };
        
        // Criando o full-block
        let fullBlock = document.createElement("div");
        fullBlock.className = 'full-block';
    
        for (let key in mainJson) {

            let upperKey = key.toUpperCase();
            let lowerKey = key.toLowerCase();
            
            if (lowerKey in explaining) {
                let liExp = document.createElement("li");
                liExp.textContent = `${upperKey}: ${explaining[lowerKey]}`;

                ulExp.appendChild(liExp);
            }
    
            let mainBlockButton = document.createElement("button");
            mainBlockButton.textContent = String(key).toUpperCase();

            mainBlockButton.onclick = function () {
                window.location.href = `${urlRoute}/documentation/${String(key).toLowerCase()}`.toLowerCase();
            };
            
            fullBlock.appendChild(mainBlockButton);
    
            if (key !== mainKeys[mainKeys.length - 1]) {
                fullSectionSeparator = document.createElement("hr");
                fullSectionSeparator.className = "full-section-separator";
    
                fullBlock.appendChild(fullSectionSeparator);
            }
    
        }

        expBlock.appendChild(ulExp);
    
        // Criando a vertical Line
        let verticalLine = document.createElement("div");
        verticalLine.className = 'vertical-line';
    
        // Salvando os dados
        section.appendChild(expBlock);
        section.appendChild(verticalLine);
        section.appendChild(fullBlock);
    
        main.appendChild(section);
        document.body.appendChild(main);
    }
}

mainHome(docJson);