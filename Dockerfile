# Image Python 3.11 slim
FROM python:3.11-slim

# Installer dépendances système : wget, unzip, git
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Mettre à jour pip
RUN pip install --upgrade pip

# Copier le requirements.txt (sans pandas-ta)
COPY requirements.txt .

# Installer les dépendances Python
RUN sed -i '/pandas-ta/d' requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le projet
COPY . /app
WORKDIR /app

# Installer pandas-ta depuis GitHub
RUN git clone https://github.com/twopirllc/pandas-ta.git /tmp/pandas-ta \
    && pip install /tmp/pandas-ta \
    && rm -rf /tmp/pandas-ta

# Commande de lancement
CMD ["python", "signal_bot.py"]
