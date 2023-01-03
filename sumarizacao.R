pacotes <- c("readxl") 

if(sum(as.numeric(!pacotes %in% installed.packages())) != 0){
  instalador <- pacotes[!pacotes %in% installed.packages()]
  for(i in 1:length(instalador)) {
    install.packages(instalador, dependencies = T)
    break()}
  sapply(pacotes, require, character = T)
} else {
  sapply(pacotes, require, character = T)
}

# Mensal - Brasil
df_mensal_2001_2012 = read_excel("data/mensal-brasil-2001-a-2012.xlsx", sheet = 1, skip=11)
df_mensal_2013_atual = read_excel("data/mensal-brasil-desde-jan2013.xlsx", sheet = 1, skip=16)

# Mensal - Estados
df_mensal_estados_2001_2012 = read_excel("data/mensal-estados-2001-a-2012.xlsx", sheet = 1, skip=11)
df_mensal_estados_2013_atual = read_excel("data/mensal-estados-desde-jan2013.xlsx", sheet = 1, skip=16)

# Mensal - Regiões
df_mensal_estados_2001_2012 = read_excel("data/mensal-regioes-2001-a-2012.xlsx", sheet = 1, skip=11)
df_mensal_estados_2013_atual = read_excel("data/mensal-regioes-desde-jan2013.xlsx", sheet = 1, skip=15)



# Mensal - Brasil - CONSOLIDACAO
colnames(df_mensal_2001_2012)

names(df_mensal_2001_2012)[1] = "mes"
names(df_mensal_2001_2012)[2] = "produto"
names(df_mensal_2001_2012)[3] = "postos_pequisados"
names(df_mensal_2001_2012)[4] = "unidade_de_medida"
names(df_mensal_2001_2012)[5] = "preco_medio_revenda"
names(df_mensal_2001_2012)[6] = "desvio_padrao_revenda"
names(df_mensal_2001_2012)[7] = "preco_minimo_revenda"
names(df_mensal_2001_2012)[8] = "preco_maximo_revenda"
names(df_mensal_2001_2012)[9] = "margem_media_revenda"
names(df_mensal_2001_2012)[10] = "coef_variacao_revenda"
names(df_mensal_2001_2012)[11] = "preco_medio_distribuicao"
names(df_mensal_2001_2012)[12] = "desvio_padrao_distribuicao"
names(df_mensal_2001_2012)[13] = "preco_minimo_distribuicao"
names(df_mensal_2001_2012)[14] = "preco_maximo_distribuicao"
names(df_mensal_2001_2012)[15] = "coef_variacao_distribuicao"

colnames(df_mensal_2001_2012)

colnames(df_mensal_2013_atual)

names(df_mensal_2013_atual)[1] = "mes"
names(df_mensal_2013_atual)[2] = "produto"
names(df_mensal_2013_atual)[3] = "postos_pequisados"
names(df_mensal_2013_atual)[4] = "unidade_de_medida"
names(df_mensal_2013_atual)[5] = "preco_medio_revenda"
names(df_mensal_2013_atual)[6] = "desvio_padrao_revenda"
names(df_mensal_2013_atual)[7] = "preco_minimo_revenda"
names(df_mensal_2013_atual)[8] = "preco_maximo_revenda"
names(df_mensal_2013_atual)[9] = "margem_media_revenda"
names(df_mensal_2013_atual)[10] = "coef_variacao_revenda"
names(df_mensal_2013_atual)[11] = "preco_medio_distribuicao"
names(df_mensal_2013_atual)[12] = "desvio_padrao_distribuicao"
names(df_mensal_2013_atual)[13] = "preco_minimo_distribuicao"
names(df_mensal_2013_atual)[14] = "preco_maximo_distribuicao"
names(df_mensal_2013_atual)[15] = "coef_variacao_distribuicao"

colnames(df_mensal_2013_atual)

df_brasil <- rbind(df_mensal_2001_2012, df_mensal_2013_atual)

head(df_brasil)
tail(df_brasil)

View(tail(df_brasil, n=40))


colnames(df_mensal_estados_2001_2012)

names(df_mensal_estados_2001_2012)[1] = "mes"
names(df_mensal_estados_2001_2012)[2] = "produto"
names(df_mensal_estados_2001_2012)[3] = "regiao"
names(df_mensal_estados_2001_2012)[4] = "postos_pequisados"
names(df_mensal_estados_2001_2012)[5] = "unidade_de_medida"
names(df_mensal_estados_2001_2012)[6] = "preco_medio_revenda"
names(df_mensal_estados_2001_2012)[7] = "desvio_padrao_revenda"
names(df_mensal_estados_2001_2012)[8] = "preco_minimo_revenda"
names(df_mensal_estados_2001_2012)[9] = "preco_maximo_revenda"
names(df_mensal_estados_2001_2012)[10] = "coef_variacao_revenda"
names(df_mensal_estados_2001_2012)[11] = "margem_media_revenda"
names(df_mensal_estados_2001_2012)[12] = "preco_medio_distribuicao"
names(df_mensal_estados_2001_2012)[13] = "desvio_padrao_distribuicao"
names(df_mensal_estados_2001_2012)[14] = "preco_minimo_distribuicao"
names(df_mensal_estados_2001_2012)[15] = "preco_maximo_distribuicao"
names(df_mensal_estados_2001_2012)[16] = "coef_variacao_distribuicao"


colnames(df_mensal_estados_2013_atual)


colnames(df_mensal_estados_2013_atual)

names(df_mensal_estados_2013_atual)[1] = "mes"
names(df_mensal_estados_2013_atual)[2] = "produto"
names(df_mensal_estados_2013_atual)[3] = "regiao"
names(df_mensal_estados_2013_atual)[4] = "postos_pequisados"
names(df_mensal_estados_2013_atual)[5] = "unidade_de_medida"
names(df_mensal_estados_2013_atual)[6] = "preco_medio_revenda"
names(df_mensal_estados_2013_atual)[7] = "desvio_padrao_revenda"
names(df_mensal_estados_2013_atual)[8] = "preco_minimo_revenda"
names(df_mensal_estados_2013_atual)[9] = "preco_maximo_revenda"
names(df_mensal_estados_2013_atual)[10] = "margem_media_revenda" 
names(df_mensal_estados_2013_atual)[11] = "coef_variacao_revenda" 
names(df_mensal_estados_2013_atual)[12] = "preco_medio_distribuicao"
names(df_mensal_estados_2013_atual)[13] = "desvio_padrao_distribuicao"
names(df_mensal_estados_2013_atual)[14] = "preco_minimo_distribuicao"
names(df_mensal_estados_2013_atual)[15] = "preco_maximo_distribuicao"
names(df_mensal_estados_2013_atual)[16] = "coef_variacao_distribuicao"

colnames(df_mensal_estados_2013_atual)


df_estados <- rbind(df_mensal_estados_2001_2012, df_mensal_estados_2013_atual)

head(df_estados)
tail(df_estados)

