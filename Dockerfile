# Utiliser l'image Python 3.11 slim
FROM python:3.11-slim

# Installer wget, unzip, git et dépendances système
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Mettre à jour pip
RUN pip install --upgrade pip

# Copier requirements et installer les packages Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le projet
COPY . /app
WORKDIR /app

# Commande par défaut (adapter selon ton bot)
CMD ["python", "signal_bot.py"]
