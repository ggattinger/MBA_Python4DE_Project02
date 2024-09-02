import pandas as pd
import numpy as np
from assets.utils import keys_check, padroniza_str, corrige_hora, classifica_hora, flg_status, log_info, log_error, log_warning
# try:
#     from assets.utils import keys_check, padroniza_str, corrige_hora, classifica_hora
# except Exception as error:
#     print("keys_check not imported with the following error: " + error)
#     exit
# else:
#     print("keys_check imported sucessfully!")

df = pd.read_csv(
    "https://raw.githubusercontent.com/JackyP/testing/master/datasets/nycflights.csv",
    index_col=0
    )

usecols=["dep_time","arr_time","carrier","flight", "tailnum","air_time","distance", "origin", "dest"]

df_raw = df.loc[
    (~df["arr_time"].isna()) \
    & (~df["dep_time"].isna()) \
    & (~df["carrier"].isna()) \
    & (~df["flight"].isna())
].loc[:, usecols]

df_raw["air_time"] = df_raw["air_time"].fillna(0)

df_raw.drop_duplicates(inplace=True)

df_raw = df_raw.astype("object")

tmp = df.copy()
for col in ["arr_time", "dep_time", "carrier", "flight"]:
    tmp_df = tmp.loc[~df[col].isna()]
    tmp = tmp_df.copy()

tmp.shape[0] == df_raw.shape[0]

new_columns = ["datetime_partida", "datetime_chegada", "companhia", "id_voo", "id_aeronave","tempo_voo", "distancia", "origem", "destino"]

columns_map = {usecols[i]: new_columns[i] for i in range(len(usecols))}
columns_map

df_work = df_raw.copy()
df_work.rename(columns=columns_map, inplace=True)

df_work["tempo_voo"] = df_work.loc[:,"tempo_voo"].astype(float)
df_work["distancia"] = df_work.loc[:,"distancia"].astype(float)
df_work["companhia"] = df_work.loc[:,"companhia"].astype(str)
df_work["id_voo"] = df_work.loc[:,"id_voo"].astype(str)
df_work["id_aeronave"] = df_work.loc[:,"id_aeronave"].astype(str)
df_work["datetime_partida"] = df_work.loc[:,"datetime_partida"].astype(str)
df_work["datetime_chegada"] = df_work.loc[:,"datetime_chegada"].astype(str)
df_work["origem"] = df_work.loc[:,"origem"].astype(str)
df_work["destino"] = df_work.loc[:,"destino"].astype(str)

padroniza_str("ahsuw! @ ++  ~ ç 3n!!rrr")

df_work["companhia_formatted"] = df_work.loc[:,"companhia"].apply(lambda x: padroniza_str(x))
df_work["id_voo_formatted"] = df_work.loc[:,"id_voo"].apply(lambda x: padroniza_str(x))
df_work["id_aeronave_formatted"] = df_work.loc[:,"id_aeronave"].apply(lambda x: padroniza_str(x))
df_work["origem_formatted"] = df_work.loc[:,"origem"].apply(lambda x: padroniza_str(x))
df_work["destino_formatted"] = df_work.loc[:,"destino"].apply(lambda x: padroniza_str(x))

df_work.loc[:,"datetime_partida"] = df_work.loc[:,"datetime_partida"].str.replace('.0', '')
df_work.loc[:,"datetime_chegada"] = df_work.loc[:,"datetime_chegada"].str.replace('.0', '')
df_work["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 

df_work["datetime_partida"].apply(lambda x: len(x)).unique()

datetime_partida2 = df_work.loc[:,"datetime_partida"].apply(lambda x: corrige_hora(x))
datetime_chegada2 = df_work.loc[:,"datetime_chegada"].apply(lambda x: corrige_hora(x))

datetime_partida2

pd.to_datetime(df_work.loc[:,'data_voo'].astype(str) + " " + datetime_partida2)

df_work['datetime_partida_formatted'] = pd.to_datetime(df_work.loc[:,'data_voo'].astype(str) + " " + datetime_partida2)
df_work['datetime_chegada_formatted'] = pd.to_datetime(df_work.loc[:,'data_voo'].astype(str) + " " + datetime_chegada2)

df_work.head()

df_work["datetime_chegada_formatted"] = np.where(
    df_work["datetime_partida_formatted"] > df_work["datetime_chegada_formatted"],
    df_work["datetime_chegada_formatted"] + pd.Timedelta(days=1),
    df_work["datetime_chegada_formatted"]
    )

df_work.loc[df_work["datetime_partida_formatted"] > df_work["datetime_chegada_formatted"]]

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

df_dw["tempo_voo_esperado"] = (df_dw["datetime_chegada_formatted"] - df_dw["datetime_partida_formatted"]) / pd.Timedelta(hours=1)
df_dw["tempo_voo_hr"] = df_dw["tempo_voo"] /60
df_dw["atraso"] = df_dw["tempo_voo_hr"] - df_dw["tempo_voo_esperado"]
df_dw["dia_semana"] = df_dw["data_voo"].dt.day_of_week #0=segunda

df_dw["horario"] = df_work.loc[:,"datetime_partida_formatted"].dt.hour.apply(lambda x: classifica_hora(x))

df_work.loc[:,"datetime_partida_formatted"].dt.hour.apply(lambda x: classifica_hora(x))

df_dw.head()

df_dw[df_dw["atraso"]>-1].describe()

df_filtrada = df_dw[df_dw["atraso"]>-1].copy()

df_filtrada["flg_status"] = df_filtrada.loc[:,"atraso"].apply(lambda x: flg_status(x))

df_filtrada["flg_status"].value_counts()

df_filtrada.to_csv("nycflights_tratada.csv", index=False)

len(df.loc[df["tailnum"].isnull()])/len(df)

key_columns = ['companhia_formatted','datetime_partida_formatted', 'id_voo','datetime_chegada_formatted']

print(keys_check(df,key_columns))

log_info("Início da execução")

if len(df.loc[df["tailnum"].isnull()])/len(df) > 0.00001:
    log_warning("Coluna tailnum possui mais nulos do que o esperado.")

#Gera uma falha no processo
if len(df.loc[df["tailnum"].isnull()])/len(df) > 0.00001:
    log_error("Coluna tailnum possui mais nulos do que o esperado.")
    raise Exception("Coluna tailnum possui mais nulos do que o esperado.")