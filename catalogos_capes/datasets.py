import os
import pandas as pd
import functools
import requests
import math
from tqdm.auto import tqdm
import fire
from tenacity import retry, stop_after_attempt, wait_random

# API Dados Abertos CAPES
dados_abertos_api = 'https://dadosabertos.capes.gov.br/api/3/action'


@functools.lru_cache(maxsize=128)
def get_all_datasets_with_resources(
    q: str = 'catalogo-de-teses-e-dissertacoes',
    rows: int = 10,
) -> pd.DataFrame:
    """Obtém todos os conjuntos de dados com recursos.

    Args:
        q (str, optional): grupo de conjuntos de dados. Defaults to 'catalogo-de-teses-e-dissertacoes'.
        rows (int, optional): quantidade de registros. Defaults to 10.

    Returns:
        pd.DataFrame: retorna um DataFrame com os conjuntos de dados
    """

    # Obter todos os conjuntos de dados com recursos
    response = requests.get(
        f'{dados_abertos_api}/package_search', params={'q': q, 'rows': rows}
    )
    r_json = response.json()
    datasets = r_json['result']['results']

    # Obter todos os recursos
    resources = []
    for dataset in datasets:
        for resource in dataset['resources']:
            resource['dataset_name'] = dataset['title']
            resources.append(resource)

    # Converter para DataFrame
    df = pd.DataFrame(resources)
    return df


@retry(stop=stop_after_attempt(5), wait=wait_random(min=1, max=5))
@functools.lru_cache(maxsize=128)
def download_file(url: str, dest_dir: str = './') -> str:
    """Realiza o download de um arquivo.

    Args:
        url (str): url do arquivo
        dest_dir (str, optional): diretório de destino. Defaults to './'.

    Returns:
        str: caminho do arquivo baixado
    """
    filename = url.split('/')[-1]
    dest_path = os.path.join(dest_dir, filename)
    with requests.get(url, stream=True) as r:
        n_bytes = 1000000
        total = math.ceil(int(r.headers.get('content-length', 0)) // n_bytes)
        with open(dest_path, 'wb') as f:
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
    name: str = 'Catálogo de Teses e Dissertações',
    format: str = 'CSV',
) -> pd.DataFrame:
    """Obtém um conjunto de dados pelo nome.

    Args:
        df_in (pd.DataFrame): DataFrame com os conjuntos de dados
        name (str, optional): nome do conjunto de dados. Defaults to 'Catálogo de Teses e Dissertações'.
        format (str, optional): formato do recurso. Defaults to 'CSV'.

    Returns:
        pd.DataFrame: DataFrame com os conjuntos de dados filtrados
    """
    return df_in.loc[
        (df_in['dataset_name'].str.contains(name))
        & (df_in['format'] == format)
    ]


def download_dataset(
    df_in: pd.DataFrame,
    name: str = 'Catálogo de Teses e Dissertações',
    format: str = 'CSV',
    dest_dir: str = './',
):
    """
    Realiza o download de um conjunto de dados.

    Args:
        df_in (pd.DataFrame): DataFrame com os conjuntos de dados
        name (str, optional): nome do conjunto de dados. Defaults to 'Catálogo de Teses e Dissertações'.
        format (str, optional): formato do recurso. Defaults to 'CSV'.
        dest_dir (str, optional): diretório de destino. Defaults to './'.
    """
    df = get_dataset_by_name(df_in, name, format)
    for _, row in tqdm(
        df.iterrows(), total=df.shape[0], desc='Download datasets'
    ):
        download_file(row['url'], dest_dir)


def run(dest_dir: str = './'):
    """Realiza o download de todos os conjuntos de dados.

    Args:
        dest_dir (str, optional): diretório de destino. Defaults to './'.
    """
    df = get_all_datasets_with_resources()
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    download_dataset(df, dest_dir=dest_dir)


if __name__ == '__main__':
    fire.Fire(run)
