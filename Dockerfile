FROM python:3.12.0a4-buster

WORKDIR /app

RUN mkdir -p data/raw

# Dados já consolidados
COPY data/raw/mensal-brasil-2001-a-2012.xlsx ./data/raw
COPY data/raw/mensal-estados-2001-a-2012.xlsx ./data/raw
COPY data/raw/mensal-regioes-2001-a-2012.xlsx ./data/raw

COPY requeriments.txt ./
COPY main.py ./

RUN pip install -r requeriments.txt

CMD ["python", "main.py"]