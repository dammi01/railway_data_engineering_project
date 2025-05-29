# Use a imagem oficial do Python como base
FROM python:3.12-slim-bookworm

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Instala uv (instalador de pacotes ultra-rápido) e outras dependências do sistema
# bookworm é o nome da distribuição Debian subjacente, o que 'slim-bookworm' significa.
RUN pip install --no-cache-dir uv && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        # Adicione quaisquer outras libs de sistema necessárias aqui, se houver
        # Ex: libpq-dev se você usar Postgres no futuro, etc.
    && rm -rf /var/lib/apt/lists/*

# Copia o arquivo de requisitos para aproveitar o cache do Docker
COPY requirements.txt .

# Instala as dependências Python usando uv
RUN uv pip install -r requirements.txt --system

# Copia o restante do código da sua aplicação para o contêiner
COPY . .

# Comando padrão para rodar quando o contêiner inicia (pode ser sobrescrito)
# Usaremos `python` como entrada, mas o Airflow e dbt terão seus próprios comandos.
CMD ["python"]
