FROM python:3.11-slim

# Installer git et autres utilitaires nécessaires
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le projet
COPY . /app
WORKDIR /app

# Commande par défaut
CMD ["python", "signal_bot.py"]
