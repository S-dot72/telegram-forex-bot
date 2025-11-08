# Image de base (Python 3.11 stable)
FROM python:3.11-slim

# Installer utilitaires nécessaires
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      wget unzip build-essential gcc g++ curl ca-certificates libatlas3-base && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN python -m pip install --upgrade pip

# Télécharger pandas-ta depuis GitHub (archive ZIP)
RUN wget https://github.com/twopirllc/pandas-ta/archive/refs/heads/master.zip -O /tmp/pandas-ta.zip && \
    unzip /tmp/pandas-ta.zip -d /tmp && \
    mv /tmp/pandas-ta-master /tmp/pandas-ta && \
    rm /tmp/pandas-ta.zip

# Copier requirements
WORKDIR /app
COPY requirements.txt /app/requirements.txt

# Installer d'abord les dépendances de base
RUN pip install --no-cache-dir -r /app/requirements.txt

# Installer pandas-ta depuis le dossier local
RUN pip install --no-cache-dir /tmp/pandas-ta

# Nettoyer
RUN rm -rf /tmp/pandas-ta

# Copier le projet
COPY . /app

ENV PYTHONUNBUFFERED=1

# Commande par défaut
CMD ["python", "signal_bot.py"]
