# Projeto de Modelagem Dimensional com Spark SQL

Este repositório contém a entrega do desafio técnico baseado em modelagem dimensional com PySpark (utilizando SQL), realizado no ambiente do Google Colab.

## 📁 Estrutura dos Arquivos

```
.
├── data/
│   ├── dim_cliente.csv
│   ├── dim_produto.csv
│   ├── dim_data.csv
│   └── fato_vendas.csv
└── notebook/
    └── gaudium_test.ipynb
```

## 🧱 Tecnologias Utilizadas

- Apache Spark (3.x)
- Google Colab
- PySpark (com interface SQL)
- Python 3.8+

Observações: Google Colab foi escolhido pela facilidade de, na sua versão gratuita, integrar com GitHUb e organizar arquivos diretamente nos diretórios.

## 🧠 Descrição das Tabelas

### `dim_cliente`
Contém informações de clientes e suas localidades.

| Coluna         | Tipo    | Descrição                          |
|----------------|---------|------------------------------------|
| id_cliente     | inteiro | Chave substituta do cliente        |
| nome_cliente   | texto   | Nome do cliente                    |
| cidade         | texto   | Cidade do cliente                  |
| estado         | texto   | Estado do cliente                  |

### `dim_produto`
Contém produtos, categorias e fabricantes.

| Coluna         | Tipo    | Descrição                          |
|----------------|---------|------------------------------------|
| id_produto     | inteiro | Chave substituta do produto        |
| nome_produto   | texto   | Nome do produto                    |
| categoria      | texto   | Categoria de produto               |
| fabricante     | texto   | Fabricante                         |

### `dim_data`
Calendário simplificado com datas únicas.

| Coluna         | Tipo    | Descrição                          |
|----------------|---------|------------------------------------|
| id_data        | inteiro | Chave substituta da data           |
| data           | data    | Data da venda                      |
| ano            | inteiro | Ano                                |
| mes            | inteiro | Mês                                |
| dia            | inteiro | Dia do mês                         |
| dia_semana     | texto   | Dia da semana (em português)       |

### `fato_vendas`
Tabela fato com métricas de vendas.

| Coluna         | Tipo          | Descrição                              |
|----------------|---------------|----------------------------------------|
| id_fato        | inteiro       | Chave técnica da linha de venda        |
| qtd_vendida    | inteiro       | Quantidade vendida                     |
| valor_total    | decimal(10,2) | Valor total da venda                   |
| id_cliente     | inteiro       | Chave Estrangeira para `dim_cliente`   |
| id_produto     | inteiro       | Chave Estrangeira para `dim_produto`   |
| id_data        | inteiro       | Chave Estrangeira para `dim_data`      |

## 🧠 Explicação sobre as Decisões Tomadas

### Premissas
- As manipulações de dados foram todas feitas em Spark SQL, como solicitado, com exceção de uma mudança inicial de tipos de dados que será explicada nos itens seguintes.
- Como solicitado também, as tabelas foram dimensionadas no esquema estrela, menos granulado que o modelo floco de neve, mas julgando pelo volume de dados e quantidade de categorias em cada dimensão, o esquema estrela era sim o mais correto a ser usado, pelo menos em um primeiro momento, por não haver tanta cardinalidade nas dimensões e porque uma menor quantidade de tabelas dimensões é sempre que possível desejável para facilitar a manutenção e diminuir a quantidade de joins, gerando queries mais rápidas. Caso o volume de dados e categorias mudasse, seria necessário o uso do esquema floco de neve.
- Optou-se por cada tabela gerada ser separada em uma célula própria para visualização de output e facilidade de debugging.
- A visualização foi feita através de 'display(HTML())' para uma tabulação melhor que um simples '.show()'. A preferência deste candidato é por usar conversão a Pandas dentro de display, quando em ambiente diferente do Databricks, devido ao seu visual mais agradável e interativo, mas achei importante para o teste me ater o máximo possível as ferramentas solicitadas.

### Célula 1
- Setup do ambiente Spark dentro do Colab.

### Célula 2
- Importação de bibliotecas, dos dados brutos e mudança dos tipos das colunas data, qtd_vendida e valor_total.
- Foi decidida realizar a mudança conjunta com a importação dos dados brutos com o objetivo de economizar linhas de código que seriam repetidas mais de uma vez posteriormente, principalmente em relação a coluna data, que seria usada como coordenada de JOIN.
- A coluna data foi transformada em date type para permitir o uso de funções como YEAR() que só funcionam com date type.
- A coluna qtd_vendida foi transformada em inteiro para permitir que eventualmente possam ser feitos.
- A coluna valor_total foi transformada em decimal pelo mesmo motivo de cálculo, mas escolhido um formato mais representativo de valores monetários.
- Foi feita anteriormente uma devida exploração dos dados e foi verificada a inexistência de nulos e duplicados e investigado o volume dos dados.

### Célula 3
- Construção da tabela dim_cliente.
- Cidade e estado foram colocados juntos na tabela dimensão de clientes para garantir a unicidade destes, já que eventualmente poderia haver clientes com o mesmo nome, mas que poderiam ser distinguidos por suas localidades.

### Célula 4
- Construção da tabela dim_produto.
- Categoria e fabricante também foram colocados juntos com nome_produto também para garantir sua unicidade, pois se a tabela crescesse poderia ser possível ter um mesmo nome_produto, mas de marca diferente.

### Célula 5
- Construção da tabela dim_data.
- O padrão de criação de colunas seguiu o modelo clássico separando dia, mês e ano, abrindo a possibilidade de filtragem mais acessível posterior.
- O uso da CTE se deve, não só a sua agilidade, mas ao fato de que identifiquei a necessidade de fazer uma tradução dos dias da semana, já que o Spark SQL não possui função ou parâmetro que permita a direta conversão para os dias da semana em português, e é necessário manter tais tabelas mais legíveis não só aos engenheiros, mas outros setores que possam utilizá-la.

### Célula 6
- Construção da tabela fato_vendas.
- O uso da CTE se deve a necessidade de usar DISTINCT antes de se criar a chave primária, evitando ids eliminados por terem sido atribuídos a linhas duplicadas.
- Não há linhas duplicadas nos dados brutos fornecidos, principalmente quando se leva em conta a quantidade de itens vendidos e o preço final. É nítida que a intenção da tabela é que todas as vendas de um certo produto, para um certo cliente, em uma certa data sejam todas somadas e colocadas em apenas uma linha. Todavia, se porventura houvessem linhas com valores iguais, poderíamos seguramente interpretar, dada a natureza da coluna qte_vendida, que tal linha a mais não deveria ser considerada como um registro separado genuíno a ser posteriormente envolvido em cálculos, mas sim um registro errôneo realmente duplicado. Pensando nisso que se decidiu por criar uma query que fosse a prova de posteriores erros como este. Assim se faz a necessidade da CTE e do DISTINCT antes da query principal.

### Célula 7
- Exportação de cada uma das tabelas para arquivos .csv.

## 🚀 Como Reproduzir

1. Acesse o notebook [`star_spark_sql.ipynb`](notebook/star_spark_sql.ipynb)
2. Execute o notebook no Google Colab
3. Faça upload de `dados_brutos.csv` ao ser solicitado
4. Exporte os arquivos `.csv` ou `.zip` no final do processo

---

## ✍️ Autor

Higor Moretti Pereira
