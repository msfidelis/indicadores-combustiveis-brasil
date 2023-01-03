
import wget
import ssl
import pandas as pd
import numpy as np
import json
import boto3

from datetime import datetime

def UploadS3(filepath, key):
    s3 = boto3.client("s3")
    bucket_name = "economia-popular-delivery-content-indices"
    s3.upload_file(
        Filename=filepath,
        Bucket=bucket_name,
        Key=key,
        ExtraArgs={
            "ACL": "public-read",
            "ServerSideEncryption": "AES256"
        }
    )


    return True

def CombustivelEstados():
    return True

def CombustivelBrasil():
    # Mensal - Brasil
    df_mensal_2001_2012 = pd.read_excel("data/raw/mensal-brasil-2001-a-2012.xlsx", skiprows=11, header=1)
    df_mensal_2001_2012 = df_mensal_2001_2012.drop([
        "NÚMERO DE POSTOS PESQUISADOS", 
        "UNIDADE DE MEDIDA", 
        "DESVIO PADRÃO REVENDA",
        "COEF DE VARIAÇÃO REVENDA",
        "DESVIO PADRÃO DISTRIBUIÇÃO",
        "COEF DE VARIAÇÃO DISTRIBUIÇÃO"
    ], axis=1)
    df_mensal_2001_2012 = df_mensal_2001_2012.rename({
        "MÊS": "referencia",
        "PRODUTO": "produto",
        "PRECO MÉDIO REVENDA": "preco_medio_revenda",
        "PRECO MÍNIMO REVENDA": "preco_minimo_revenda",
        "PRECO MÁXIMO REVENDA": "preco_maximo_revenda",
        "PRECO MÉDIO DISTRIBUIÇÃO": "preco_medio_distribuicao",
        "PRECO MÍNIMO DISTRIBUIÇÃO": "preco_minimo_distribuicao",
        "PRECO MÁXIMO DISTRIBUIÇÃO": "preco_maximo_distribuicao"
    }, axis='columns')

    df_mensal_2013_atual = pd.read_excel("data/raw/mensal-brasil-desde-jan2013.xlsx", skiprows = 15, header=1)
    df_mensal_2013_atual = df_mensal_2013_atual.drop([
        "NÚMERO DE POSTOS PESQUISADOS", 
        "UNIDADE DE MEDIDA", 
        "DESVIO PADRÃO REVENDA",
        "COEF DE VARIAÇÃO REVENDA",
        "DESVIO PADRÃO DISTRIBUIÇÃO",
        "COEF DE VARIAÇÃO DISTRIBUIÇÃO"
    ], axis=1)
    df_mensal_2013_atual = df_mensal_2013_atual.rename({
        "MÊS": "referencia",
        "PRODUTO": "produto",
        "PREÇO MÉDIO REVENDA": "preco_medio_revenda",
        "PREÇO MÍNIMO REVENDA": "preco_minimo_revenda",
        "PREÇO MÁXIMO REVENDA": "preco_maximo_revenda",
        "PREÇO MÉDIO DISTRIBUIÇÃO": "preco_medio_distribuicao",
        "PREÇO MÍNIMO DISTRIBUIÇÃO": "preco_minimo_distribuicao",
        "PREÇO MÁXIMO DISTRIBUIÇÃO": "preco_maximo_distribuicao"
    }, axis='columns')

    df_brasil = pd.concat([df_mensal_2001_2012, df_mensal_2013_atual], ignore_index=True)
    # print(df_brasil)

    df_brasil_columns = [
        'referencia',
        'ano',
        'mes',
        'gasolina_comum_preco_revenda_avg',
        'gasolina_comum_preco_revenda_min',
        'gasolina_comum_preco_revenda_max',
        'gasolina_aditivada_preco_revenda_avg',
        'gasolina_aditivada_preco_revenda_min',
        'gasolina_aditivada_preco_revenda_max',   
        'etanol_hidratado_preco_revenda_avg',
        'etanol_hidratado_preco_revenda_min',
        'etanol_hidratado_preco_revenda_max',
        'oleo_diesel_preco_revenda_avg',
        'oleo_diesel_preco_revenda_min',
        'oleo_diesel_preco_revenda_max',
        'oleo_diesel_s10_preco_revenda_avg',
        'oleo_diesel_s10_preco_revenda_min',
        'oleo_diesel_s10_preco_revenda_max',
        'gas_cozinha_glp_preco_revenda_avg',
        'gas_cozinha_glp_preco_revenda_min',
        'gas_cozinha_glp_preco_revenda_max',
        'gas_natural_veicular_gnv_preco_revenda_avg',
        'gas_natural_veicular_gnv_preco_revenda_min',
        'gas_natural_veicular_gnv_preco_revenda_max',        
    ]

    df_brasil_consolidado = pd.DataFrame(columns=df_brasil_columns)

    # Create Indexes
    for index, row in df_brasil.iterrows():
        if str(row['referencia']) not in df_brasil_consolidado.index:
            s = pd.Series(['consolidated'],index=[row["referencia"]])
            df_brasil_consolidado = pd.concat([df_brasil_consolidado, s])

    # Drop Consolidated Index
    df_brasil_consolidado = df_brasil_consolidado.iloc[: , :-1]

    print("Tabela de Frequencias dos Tipos de Combustíveis - Brasil")
    print(df_brasil.produto.value_counts())

    for index, row in df_brasil.iterrows():
        if row['produto'] == "ETANOL HIDRATADO":

            # Datetime Referencias
            df_brasil_consolidado.loc[str(row['referencia'])]['referencia'] = row["referencia"].strftime("%Y-%m")
            df_brasil_consolidado.loc[str(row['referencia'])]['ano'] = row["referencia"].strftime("%Y")
            df_brasil_consolidado.loc[str(row['referencia'])]['mes'] = row["referencia"].strftime("%m")

            df_brasil_consolidado.loc[str(row['referencia'])]['etanol_hidratado_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['etanol_hidratado_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['etanol_hidratado_preco_revenda_max'] = row["preco_maximo_revenda"]

        if row['produto'] == "GASOLINA COMUM":
            df_brasil_consolidado.loc[str(row['referencia'])]['gasolina_comum_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gasolina_comum_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gasolina_comum_preco_revenda_max'] = row["preco_maximo_revenda"]

        if row['produto'] == "GASOLINA ADITIVADA":
            df_brasil_consolidado.loc[str(row['referencia'])]['gasolina_aditivada_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gasolina_aditivada_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gasolina_aditivada_preco_revenda_max'] = row["preco_maximo_revenda"]

        if row['produto'] == "ÓLEO DIESEL" or row['produto'] == "OLEO DIESEL":
            df_brasil_consolidado.loc[str(row['referencia'])]['oleo_diesel_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['oleo_diesel_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['oleo_diesel_preco_revenda_max'] = row["preco_maximo_revenda"]

        if row['produto'] == "ÓLEO DIESEL S10" or row['produto'] == "OLEO DIESEL S10":
            df_brasil_consolidado.loc[str(row['referencia'])]['oleo_diesel_s10_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['oleo_diesel_s10_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['oleo_diesel_s10_preco_revenda_max'] = row["preco_maximo_revenda"]

        if row['produto'] == "GLP":
            df_brasil_consolidado.loc[str(row['referencia'])]['gas_cozinha_glp_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gas_cozinha_glp_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gas_cozinha_glp_preco_revenda_max'] = row["preco_maximo_revenda"]

        if row['produto'] == "GNV":
            df_brasil_consolidado.loc[str(row['referencia'])]['gas_natural_veicular_gnv_preco_revenda_avg'] = row["preco_medio_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gas_natural_veicular_gnv_preco_revenda_min'] = row["preco_minimo_revenda"]
            df_brasil_consolidado.loc[str(row['referencia'])]['gas_natural_veicular_gnv_preco_revenda_max'] = row["preco_maximo_revenda"]


    df_brasil_consolidado.to_csv('data/combustiveis-brasil.csv', index=False)
    print("Arquivo salvo em data/combustiveis-brasil.csv")


    UploadS3('data/combustiveis-brasil.csv', 'combustiveis/combustiveis-brasil.csv')


    now = datetime.now()

    result_json = df_brasil_consolidado.to_json(index=False, orient="table")
    parsed = json.loads(result_json)

    json_data = {
        "fonte": "https://www.gov.br/anp/", 
        "unidade_medida": "R$",
        "data_atualizacao": now.strftime("%m/%d/%Y, %H:%M:%S"),
        "data": parsed['data']
    }

    with open("data/combustiveis-brasil.json", "w") as outfile:
        json.dump(json_data, outfile)

    print("Arquivo salvo em data/combustiveis-brasil.json")

    UploadS3('data/combustiveis-brasil.json', 'combustiveis/combustiveis-brasil.json')

def Consolidate():
    CombustivelBrasil()



    # # Mensal - Estados
    # df_mensal_estados_2001_2012 = pd.read_excel("data/raw/mensal-estados-2001-a-2012.xlsx", skiprows=11, header=1)
    # df_mensal_estados_2001_2012 = df_mensal_estados_2001_2012.drop([
    #     "NÚMERO DE POSTOS PESQUISADOS", 
    #     "UNIDADE DE MEDIDA", 
    #     "DESVIO PADRÃO REVENDA",
    #     "COEF DE VARIAÇÃO REVENDA",
    #     "DESVIO PADRÃO DISTRIBUIÇÃO",
    #     "COEF DE VARIAÇÃO DISTRIBUIÇÃO"
    # ], axis=1)
    # df_mensal_estados_2001_2012 = df_mensal_estados_2001_2012.rename({
    #     "MÊS": "referencia",
    #     "PRODUTO": "produto",
    #     "REGIÃO": "regiao",
    #     "ESTADO": "estado",
    #     "PRECO MÉDIO REVENDA": "preco_medio_revenda",
    #     "PRECO MÍNIMO REVENDA": "preco_minimo_revenda",
    #     "PRECO MÁXIMO REVENDA": "preco_maximo_revenda",
    #     "PRECO MÉDIO DISTRIBUIÇÃO": "preco_medio_distribuicao",
    #     "PRECO MÍNIMO DISTRIBUIÇÃO": "preco_minimo_distribuicao",
    #     "PRECO MÁXIMO DISTRIBUIÇÃO": "preco_maximo_distribuicao"
    # }, axis='columns')
    
    # df_mensal_estados_2013_atual = pd.read_excel("data/raw/mensal-estados-desde-jan2013.xlsx",  skiprows = 15, header=1)
    # df_mensal_estados_2013_atual = df_mensal_estados_2013_atual.drop([
    #     "NÚMERO DE POSTOS PESQUISADOS", 
    #     "UNIDADE DE MEDIDA", 
    #     "DESVIO PADRÃO REVENDA",
    #     "COEF DE VARIAÇÃO REVENDA",
    #     "DESVIO PADRÃO DISTRIBUIÇÃO",
    #     "COEF DE VARIAÇÃO DISTRIBUIÇÃO"
    # ], axis=1)
    # df_mensal_estados_2013_atual = df_mensal_estados_2013_atual.rename({
    #     "MÊS": "referencia",
    #     "PRODUTO": "produto",
    #     "REGIÃO": "regiao",
    #     "ESTADO": "estado",
    #     "PREÇO MÉDIO REVENDA": "preco_medio_revenda",
    #     "PREÇO MÍNIMO REVENDA": "preco_minimo_revenda",
    #     "PREÇO MÁXIMO REVENDA": "preco_maximo_revenda",
    #     "PREÇO MÉDIO DISTRIBUIÇÃO": "preco_medio_distribuicao",
    #     "PREÇO MÍNIMO DISTRIBUIÇÃO": "preco_minimo_distribuicao",
    #     "PREÇO MÁXIMO DISTRIBUIÇÃO": "preco_maximo_distribuicao"
    # }, axis='columns')

    # df_estados = pd.concat([df_mensal_estados_2001_2012, df_mensal_estados_2013_atual], ignore_index=True)
    # print(df_estados)



    # # # Mensal - Regiões
    # df_mensal_regioes_2001_2012 = pd.read_excel("data/raw/mensal-regioes-2001-a-2012.xlsx", skiprows=11, header = 1)
    # df_mensal_regioes_2001_2012 = df_mensal_regioes_2001_2012.drop([
    #     "NÚMERO DE POSTOS PESQUISADOS", 
    #     "UNIDADE DE MEDIDA", 
    #     "DESVIO PADRÃO REVENDA",
    #     "COEF DE VARIAÇÃO REVENDA",
    #     "DESVIO PADRÃO DISTRIBUIÇÃO",
    #     "COEF DE VARIAÇÃO DISTRIBUIÇÃO"
    # ], axis=1)
    # df_mensal_regioes_2001_2012 = df_mensal_regioes_2001_2012.rename({
    #     "MÊS": "referencia",
    #     "PRODUTO": "produto",
    #     "REGIÃO": "regiao",
    #     "PRECO MÉDIO REVENDA": "preco_medio_revenda",
    #     "PRECO MÍNIMO REVENDA": "preco_minimo_revenda",
    #     "PRECO MÁXIMO REVENDA": "preco_maximo_revenda",
    #     "PRECO MÉDIO DISTRIBUIÇÃO": "preco_medio_distribuicao",
    #     "PRECO MÍNIMO DISTRIBUIÇÃO": "preco_minimo_distribuicao",
    #     "PRECO MÁXIMO DISTRIBUIÇÃO": "preco_maximo_distribuicao"
    # }, axis='columns')
    # print(df_mensal_regioes_2001_2012)


    # df_mensal_regioes_2013_atual = pd.read_excel("data/raw/mensal-regioes-desde-jan2013.xlsx", skiprows=15, header = 1)
    # df_mensal_regioes_2013_atual = df_mensal_regioes_2013_atual.drop([
    #     "NÚMERO DE POSTOS PESQUISADOS", 
    #     "UNIDADE DE MEDIDA", 
    #     "DESVIO PADRÃO REVENDA",
    #     "COEF DE VARIAÇÃO REVENDA",
    #     "DESVIO PADRÃO DISTRIBUIÇÃO",
    #     "COEF DE VARIAÇÃO DISTRIBUIÇÃO"
    # ], axis=1)
    # df_mensal_regioes_2013_atual = df_mensal_regioes_2013_atual.rename({
    #     "MÊS": "referencia",
    #     "PRODUTO": "produto",
    #     "REGIÃO": "regiao",
    #     "PREÇO MÉDIO REVENDA": "preco_medio_revenda",
    #     "PREÇO MÍNIMO REVENDA": "preco_minimo_revenda",
    #     "PREÇO MÁXIMO REVENDA": "preco_maximo_revenda",
    #     "PREÇO MÉDIO DISTRIBUIÇÃO": "preco_medio_distribuicao",
    #     "PREÇO MÍNIMO DISTRIBUIÇÃO": "preco_minimo_distribuicao",
    #     "PREÇO MÁXIMO DISTRIBUIÇÃO": "preco_maximo_distribuicao"
    # }, axis='columns')
    # print(df_mensal_regioes_2013_atual)

    # df_regioes = pd.concat([df_mensal_regioes_2001_2012, df_mensal_regioes_2013_atual], ignore_index=True)
    # print(df_regioes)



def DownloadFile(url, output):
    print(url)
    print(output)
    response = wget.download(url, output)
    print(response)

def DownloadSources():
    ssl._create_default_https_context = ssl._create_unverified_context
    urls = [
		# "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/2001-2012/mensal-brasil-2001-a-2012.xlsx",
		# "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/2001-2012/mensal-regioes-2001-a-2012.xlsx",
		# "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/2001-2012/mensal-estados-2001-a-2012.xlsx",
		# "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/2001-2012/mensal-municipios-2001-a-2012.xlsb",
		"https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-brasil-desde-jan2013.xlsx",
		"https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-regioes-desde-jan2013.xlsx",
		"https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/mensal/mensal-estados-desde-jan2013.xlsx",
    ]

    for n in urls:
        url_split = n.split("/")
        output = str(url_split[len(url_split) - 1])
        output = "./data/raw/" + output
        DownloadFile(n, output)

def main():
    #DownloadSources()
    Consolidate()

main()