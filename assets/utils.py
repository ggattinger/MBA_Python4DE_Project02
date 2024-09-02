import pandas as pd
import re
import logging

def read_metadado(meta_path):
    meta = pd.read_excel(meta_path)
    metadados = {
        "tabela": meta["tabela"].unique(),
        "cols_originais" : list(meta["cols_originais"]), 
        "cols_renamed" : list(meta["cols_renamed"]),
        "tipos_originais" : dict(zip(list(meta["cols_originais"]),list(meta["tipo_original"]))),
        "tipos_formatted" : dict(zip(list(meta["cols_renamed"]),list(meta["tipo_formatted"]))),
        "cols_chaves" : list(meta.loc[meta["key"] == 1]["cols_originais"]),
        "null_tolerance" : dict(zip(list(meta["cols_renamed"]), list(meta["raw_null_tolerance"]))),
        "std_str" : list(meta.loc[meta["std_str"] == 1]["cols_renamed"]),
        "corrige_hr" : list(meta.loc[meta["corrige_hr"] == 1]["cols_renamed"])
        }
    return metadados

# Funções de Saneamento ----------------------------------------------------------------

def null_exclude(df, cols_chaves):
    '''
    Função de exclusao das observações nulas
    INPUT: Pandas DataFrame, lista de colunas que são chaves
    OUTPUT: Pandas DataFrame com as observações nulas excluidas
    '''
    tmp = df.copy()
    for col in cols_chaves:
        tmp_df = tmp.loc[~df[col].isna()]
        tmp = tmp_df.copy()
    return tmp_df

def select_rename(df, cols_originais, cols_renamed):
    '''
    Função de validação de nulos
    INPUT: Pandas DataFrame, lista dos nomes das colunas e lista dos novos nomes 
    OUTPUT: Pandas DataFrame com novos nomes
    '''
    df_work = df.loc[:, cols_originais].copy()
    columns_map = dict(zip(cols_originais,cols_renamed))
    df_work.rename(columns=columns_map, inplace = True)
    return df_work

def convert_data_type(df, tipos_map):
    '''
    Função de validação de nulos
    INPUT: Pandas DataFrame, dicionário de colunas como chave e seus tipos como valores
    OUTPUT: Pandas DataFrame com novos nomes
    '''
    data = df.copy()
    for col in tipos_map.keys():
        tipo = tipos_map[col]
        if tipo == "int":
            tipo = data[col].astype(int)
        elif tipo == "float":
            data[col] = data[col].astype(float)
        elif tipo == "datetime":
            data[col] = pd.to_datetime(data[col])
        elif tipo == "string":
            data[col] = data[col].astype(str)
    return data

def string_std(df, std_str):
    '''
    Função de validação de nulos
    INPUT: Pandas DataFrame, lista das colunas que devem receber a padronização de strings
    OUTPUT: Pandas DataFrame com as colunas padronizadas
    '''
    df_work = df.copy()
    for col in std_str:
        new_col = f'{col}_formatted'
        df_work[new_col] = df_work.loc[:,col].apply(lambda x: padroniza_str(x))
    return df_work

# Funções de validação ----------------------------------------------------------------
def null_check(df, null_tolerance):
    '''
    Função de validação de nulos
    INPUT: Pandas DataFrame, dicionário de colunas como chave e critério de nulo como valores
    OUTPUT: Pandas DataFrame
    '''
    for col in null_tolerance.keys():
        if  len(df.loc[df[col].isnull()])/len(df)> null_tolerance[col]:
            # logger.error(f"{datetime.datetime.now()} | {col} possui mais nulos do que o esperado.")
            log_error(f"{col} possui mais nulos do que o esperado.")
        else:
            #  logger.info(f"{datetime.datetime.now()} | {col} possui nulos dentro do esperado.")
            log_info(f"{col} possui nulos dentro do esperado.")
            
def keys_check(df, cols_chaves):
    '''
    Função Para validar as chaves basta saber se o campo(s) chave(s) possuem a mesma quantidade de observações únicas em relação a quantidade de observações da base.
    \nINPUT: data_frame and key_columns
    \nOUTPUT: ???????????????????????????
    '''
    #colocar log info
    # len(df_work[['companhia_formatted','datetime_partida_formatted', 'id_voo','datetime_chegada_formatted']].drop_duplicates()) == len(df_work)
    if len(df) == len(cols_chaves):
        # logger.info(f"{datetime.datetime.now()} | Os campos possuem a mesma quantidade significativa de informações.") 
        log_info("Os campos possuem a mesma quantidade significativa de informações.")
    else:
        # logger.error(f"{datetime.datetime.now()} | Os campos não possuem a mesma quantidade significativa de informações.")
        log_error("Os campos não possuem a mesma quantidade significativa de informações.")
    pass

# Funções auxiliares ----------------------------------------------------------------

def padroniza_str(obs):
    return re.sub('[^A-Za-z0-9]+', '', obs.upper())

def corrige_hora(hr_str, dct_hora = {1:"000?",2:"00?",3:"0?",4:"?"}):
    if hr_str == "2400":
        return "00:00"
    elif (len(hr_str) == 2) & (int(hr_str) <= 12):
        return f"0{hr_str[0]}:{hr_str[1]}0"
    else:
        hora = dct_hora[len(hr_str)].replace("?", hr_str)
        return f"{hora[:2]}:{hora[2:]}"
    
def classifica_hora(hra):
    if 0 <= hra < 6: return "MADRUGADA"
    elif 6 <= hra < 12: return "MANHA"
    elif 12 <= hra < 18: return "TARDE"
    else: return "NOITE"

def flg_status(atraso):
    if atraso > 0.5 : return "ATRASO"
    else: return "ONTIME"

# Função de log

def log_info(msg):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename='data/flights_pipe.log', encoding='utf-8', 
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', 
        datefmt='%Y-%m-%d %I:%M:%S'
    )
    logger.info(msg)

def log_error(msg):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename='data/flights_pipe.log', encoding='utf-8', 
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', 
        datefmt='%Y-%m-%d %I:%M:%S'
    )
    logger.error(msg)
    
def log_warning(msg):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        filename='data/flights_pipe.log', encoding='utf-8', 
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', 
        datefmt='%Y-%m-%d %I:%M:%S'
    )
    logger.warning(msg)