# Utiliser l'image Python officielle
FROM python:3.11-slim

# Installer git et build-essential pour compiler certains packages
RUN apt-get update && apt-get install -y git build-essential && rm -rf /var/lib/apt/lists/*

# Définir le dossier de travail
WORKDIR /app

# Copier uniquement requirements.txt d'abord pour installer les dépendances
COPY requirements.txt .

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libjpeg-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Installer les packages Python
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le projet ensuite
COPY . .

# Définir variable d'environnement pour logs
ENV PYTHONUNBUFFERED=1

# Commande par défaut pour lancer le bot
CMD ["python", "signal_bot.py"]
