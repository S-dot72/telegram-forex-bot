# Image de base (Python 3.11 stable)
FROM python:3.11-slim

# Upgrade pip
RUN python -m pip install --upgrade pip

# Copier requirements et installer dépendances
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copier le projet
COPY . /app
ENV PYTHONUNBUFFERED=1

# Commande par défaut
CMD ["python", "signal_bot.py"]
