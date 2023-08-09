# Catálogo de Teses

1. Instale a biblioteca
    ```sh
    pip install git+https://github.com/AcademicAI/catalogos-capes.git
    ```
2. Obtendo lista de catálogos
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