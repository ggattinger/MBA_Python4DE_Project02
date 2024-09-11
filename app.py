import pandas as pd
import numpy as np
from assets.utils import *

if __name__ == "__main__":
    log_info(f"Início da execução!")
    log_info(f"Carregando arquivo CSV do repositório GH...")
    df = pd.read_csv(
        "https://raw.githubusercontent.com/JackyP/testing/master/datasets/nycflights.csv",
        index_col=0
        )

    log_info(f"Definindo as colunas que serão utilizadas na variável [usecols]...")
    usecols=["dep_time","arr_time","carrier","flight", "tailnum","air_time","distance", "origin", "dest"]

    log_info(f"Criando um novo DF [df_raw] preenchendo as colunas vazias com informações True ou False de acordo com seu conteúdo...")
    df_raw = df.loc[
        (~df["arr_time"].isna()) \
        & (~df["dep_time"].isna()) \
        & (~df["carrier"].isna()) \
        & (~df["flight"].isna())
    ].loc[:, usecols]

    log_info(f"Preenchendo com 0 os dados vazios da coluna air_time...")
    df_raw["air_time"] = df_raw["air_time"].fillna(0)

    log_info(f"Deletando as linhas duplicadas do DF [df_raw]...")
    df_raw.drop_duplicates(inplace=True)

    log_info(f"Definindo o DF [df_raw] como objeto...")
    df_raw = df_raw.astype("object")

    log_info(f"Criando um DF temporário [tmp] com o DF original [df], e aplicando lógica binária de vazios...")
    tmp = df.copy()
    for col in ["arr_time", "dep_time", "carrier", "flight"]:
        tmp_df = tmp.loc[~df[col].isna()]
        tmp = tmp_df.copy()

    log_info(f"Criando lista [new_columns] com as novas colunas a serem utilizadas...")
    new_columns = ["datetime_partida", "datetime_chegada", "companhia", "id_voo", "id_aeronave","tempo_voo", "distancia", "origem", "destino"]

    log_info(f"Criando uma variável [columns_map] com um loop utilizando a lista das novas colunas [new_columns]...")
    columns_map = {usecols[i]: new_columns[i] for i in range(len(usecols))}

    log_info(f"Criando um novo DF [df_work] com uma cópia do DF [df_raw]...")
    df_work = df_raw.copy()

    log_info(f"Renomeando as colunas do DF [df_work] de acordo com a variável criada [columns_map]...")
    df_work.rename(columns=columns_map, inplace=True)

    log_info(f"Definindo os tipos de dados das colunas do DF [df_work]...")
    df_work["tempo_voo"] = df_work.loc[:,"tempo_voo"].astype(float)
    df_work["distancia"] = df_work.loc[:,"distancia"].astype(float)
    df_work["companhia"] = df_work.loc[:,"companhia"].astype(str)
    df_work["id_voo"] = df_work.loc[:,"id_voo"].astype(str)
    df_work["id_aeronave"] = df_work.loc[:,"id_aeronave"].astype(str)
    df_work["datetime_partida"] = df_work.loc[:,"datetime_partida"].astype(str)
    df_work["datetime_chegada"] = df_work.loc[:,"datetime_chegada"].astype(str)
    df_work["origem"] = df_work.loc[:,"origem"].astype(str)
    df_work["destino"] = df_work.loc[:,"destino"].astype(str)

    log_info(f"Aplicando função lambda para padronização das strings no DF [df_work]...")
    df_work["companhia_formatted"] = df_work.loc[:,"companhia"].apply(lambda x: padroniza_str(x))
    df_work["id_voo_formatted"] = df_work.loc[:,"id_voo"].apply(lambda x: padroniza_str(x))
    df_work["id_aeronave_formatted"] = df_work.loc[:,"id_aeronave"].apply(lambda x: padroniza_str(x))
    df_work["origem_formatted"] = df_work.loc[:,"origem"].apply(lambda x: padroniza_str(x))
    df_work["destino_formatted"] = df_work.loc[:,"destino"].apply(lambda x: padroniza_str(x))

    log_info(f"Aplicando transformação para arredondamento do tempos de vôos no DF [df_work]...")
    df_work.loc[:,"datetime_partida"] = df_work.loc[:,"datetime_partida"].str.replace('.0', '')
    df_work.loc[:,"datetime_chegada"] = df_work.loc[:,"datetime_chegada"].str.replace('.0', '')

    log_info(f"Aplicando transformação para formatação do campo de data do vôo no DF [df_work]...")
    df_work["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 

    df_work["datetime_partida"].apply(lambda x: len(x)).unique()

    log_info(f"Aplicando transformação para formatação do campo de data do vôo...")
    datetime_partida2 = df_work.loc[:,"datetime_partida"].apply(lambda x: corrige_hora(x))
    datetime_chegada2 = df_work.loc[:,"datetime_chegada"].apply(lambda x: corrige_hora(x))

    pd.to_datetime(df_work.loc[:,'data_voo'].astype(str) + " " + datetime_partida2)

    log_info(f"Cria campo com os dados corretos de acordo com as transformações de data/hora dos vôos no DF [df_work]...")
    df_work['datetime_partida_formatted'] = pd.to_datetime(df_work.loc[:,'data_voo'].astype(str) + " " + datetime_partida2)
    df_work['datetime_chegada_formatted'] = pd.to_datetime(df_work.loc[:,'data_voo'].astype(str) + " " + datetime_chegada2)

    df_work["datetime_chegada_formatted"] = np.where(
        df_work["datetime_partida_formatted"] > df_work["datetime_chegada_formatted"],
        df_work["datetime_chegada_formatted"] + pd.Timedelta(days=1),
        df_work["datetime_chegada_formatted"]
        )

    df_work.loc[df_work["datetime_partida_formatted"] > df_work["datetime_chegada_formatted"]]

    log_info(f"Cria um novo DF [df_dw] com os campos tratados do DF [df_work]...")
    df_dw = df_work[["data_voo",
            "companhia_formatted",
            "id_voo_formatted",
            "id_aeronave_formatted",
            "datetime_partida_formatted",
            "datetime_chegada_formatted",
            "origem_formatted",
            "destino_formatted",
            "tempo_voo",
            "distancia"]].copy()
    
    log_info(f"Executa a função feat_eng que adicionará novos campos ao DF [df_dw]...")
    feat_eng(df_dw)

    df_work.loc[:,"datetime_partida_formatted"].dt.hour.apply(lambda x: classifica_hora(x))

    log_info(f"Cria um DF [df_filtrada] filtrando somente com os vôos válidos...")
    df_filtrada = df_dw[df_dw["atraso"]>-1].copy()

    log_info(f"Salva em um arquivo CSV [data/nycflights_tratada.csv] todos os vôos atrasados do DF [df_filtrada]...")
    df_filtrada.to_csv("data/nycflights_tratada.csv", index=False)

    key_columns = ['companhia_formatted', 'datetime_partida_formatted', 'id_voo', 'datetime_chegada_formatted']
    keys_check(df_work,key_columns)

    print(df_filtrada.head())

    log_info(f"Execução do código concluída com sucesso!")