# Catálogo Capes

Ferramenta para download de catálogos dos dados abertos da CAPES. 

## Pré-requisitos

- Python 3.11 ou superior
- Bibliotecas: `pandas`, `requests`, `tqdm`, `fire` e `tenacity`

Recomenda-se a instalação do [Anaconda](https://www.anaconda.com/download) para gerenciamento de ambientes virtuais.

## Dados

Os dados são obtidos do portal de dados abertos da CAPES. A lista de catálogos pode ser obtida [aqui](https://dadosabertos.capes.gov.br/dataset/).

## Instalação

```
pip install git+https://github.com/AcademicAI/catalogos-capes.git

# ou

git clone https://github.com/AcademicAI/catalogos-capes.git
cd catalogos-capes
pip install .
```

## Utilização

Usando a ferramenta de linha de comando:
```bash
# obtém todos os catálogos do portal de dados abertos da CAPES
python -m catalogos_capes --dest_dir ./data
```

Usando a biblioteca:

```python
from catalogos_capes import datasets

# obtém todos os catálogos do portal de dados abertos da CAPES
df = datasets.get_all_datasets_with_resources() 

# Obtém apenas dados de catálogo de teses e dissertações que
# estejam no formato CSV.
df = datasets.get_dataset_by_name(
    df, 'Catálogo de Teses e Dissertações', 'CSV'
)
# fazer download de um catálogo na pasta informada
datasets.download_file(df['url'].iloc[-1], "./data")

# fazer download de múltiplos catálogos de teses e dissertações (processo demorado)
# datasets.run("./data")
```
