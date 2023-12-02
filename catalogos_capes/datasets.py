import os
import re
import pandas as pd
import functools
import requests
import math
from tqdm.auto import tqdm
import fire
from tenacity import retry, stop_after_attempt, wait_random

import unicodedata

# API Dados Abertos CAPES
dados_abertos_api = "https://dadosabertos.capes.gov.br/api/3/action"


def slugify(text: str) -> str:
    """Converte um texto para um slug.

    Args:
        text (str): texto a ser convertido

    Returns:
        str: texto convertido
    """
    # Converte para minúsculas
    text = text.lower()

    # Remove acentos
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

    # Remove caracteres especiais e espaços
    text = re.sub(r"[^a-z0-9]+", "-", text)
    
    # Remove hífens no início e no final
    text = text.strip("-")
    
    return text

@functools.lru_cache(maxsize=128)
def get_all_datasets_with_resources(
    q: str = "catalogo-de-teses-e-dissertacoes",
    rows: int = 10,
) -> pd.DataFrame:
    """Obtém todos os conjuntos de dados com recursos.

    Args:
        q (str, optional): grupo de conjuntos de dados. Defaults to "catalogo-de-teses-e-dissertacoes".
        rows (int, optional): quantidade de registros. Defaults to 10.

    Returns:
        pd.DataFrame: retorna um DataFrame com os conjuntos de dados
    """

    # Obter todos os conjuntos de dados com recursos
    response = requests.get(
        f"{dados_abertos_api}/package_search", params={"q": q, "rows": rows}
    )
    r_json = response.json()
    datasets = r_json["result"]["results"]

    # Obter todos os recursos
    resources = []
    for dataset in datasets:
        for resource in dataset["resources"]:
            resource["dataset_name"] = dataset["title"]
            resources.append(resource)

    # Converter para DataFrame
    df = pd.DataFrame(resources)
    return df


@retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5))
@functools.lru_cache(maxsize=128)
def download_file(url: str, dest_dir: str = "./") -> str:
    """Realiza o download de um arquivo.

    Args:
        url (str): url do arquivo
        dest_dir (str, optional): diretório de destino. Defaults to "./".

    Returns:
        str: caminho do arquivo baixado
    """
    filename = url.split("/")[-1]
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    dest_path = os.path.join(dest_dir, filename)
    with requests.get(url, stream=True) as r:
        n_bytes = 10_000_000
        total = math.ceil(int(r.headers.get("content-length", 0)) // n_bytes)
        with open(dest_path, "wb") as f:
            for chunk in tqdm(
                r.iter_content(chunk_size=n_bytes),
                total=total,
                desc=filename,
                leave=False,
            ):
                f.write(chunk)
    return dest_path


def get_dataset_by_name(
    df_in: pd.DataFrame,
    name: str = "Catálogo de Teses e Dissertações",
    format: str = "CSV",
) -> pd.DataFrame:
    """Obtém um conjunto de dados pelo nome.

    Args:
        df_in (pd.DataFrame): DataFrame com os conjuntos de dados
        name (str, optional): nome do conjunto de dados. Defaults to "Catálogo de Teses e Dissertações".
        format (str, optional): formato do recurso. Defaults to "CSV".

    Returns:
        pd.DataFrame: DataFrame com os conjuntos de dados filtrados
    """
    return df_in.loc[
        (df_in["dataset_name"].str.contains(name))
        & (df_in["format"] == format)
    ]


def download_dataset(
    df_in: pd.DataFrame,
    name: str = "Catálogo de Teses e Dissertações",
    format: str = "CSV",
    dest_dir: str = "./",
):
    """
    Realiza o download de um conjunto de dados.

    Args:
        df_in (pd.DataFrame): DataFrame com os conjuntos de dados
        name (str, optional): nome do conjunto de dados. Defaults to "Catálogo de Teses e Dissertações".
        format (str, optional): formato do recurso. Defaults to "CSV".
        dest_dir (str, optional): diretório de destino. Defaults to "./".
    """
    df = get_dataset_by_name(df_in, name, format)
    for _, row in tqdm(
        df.iterrows(), total=df.shape[0], desc="Download datasets"
    ):
        download_file(row["url"], dest_dir)


def download_files(df: pd.DataFrame, dest_dir: str):
    """Realiza o download de todos os arquivos de um DataFrame

    Args:
        df (pd.DataFrame): DataFrame com os conjuntos de dados
        dest_dir (str): diretório de destino

    Returns:
        list: lista com os caminhos dos arquivos baixados
    """
    files = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Baixando..."):
        url = row["url"]
        identifier = slugify(row["dataset_name"])
        data_dest_dir = os.path.join(dest_dir, identifier)
        files.append(download_file(url, data_dest_dir))  
    return files

def run(dest_dir: str = "./", format: str = "CSV"):
    """ Realiza o download de todos os arquivos de um formato específico

    Args:
        dest_dir (str, optional): diretório de destino. Defaults to "./".
        format (str, optional): formato do recurso. Defaults to "CSV".

    Returns:
        list: lista com os caminhos dos arquivos baixados
    """
    df = get_all_datasets_with_resources(q="", rows=1000)
    df = get_dataset_by_name(df, name="", format=format)
    return download_files(df, dest_dir)

if __name__ == "__main__":
    fire.Fire(run)
