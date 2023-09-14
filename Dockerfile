# Utilisez une image OpenJDK comme point de départ
FROM openjdk:8


# Installez Python et pip
RUN apt-get update && apt-get install -y python3 python3-pip


# Installez Spark
RUN wget -q https://downloads.apache.org/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz && \
    tar -xzf spark-3.3.3-bin-hadoop3.tgz -C /opt --no-same-owner && \
    rm spark-3.3.3-bin-hadoop3.tgz


# Configurez l'environnement Spark
ENV SPARK_HOME /opt/spark-3.3.3-bin-hadoop3
ENV PATH $SPARK_HOME/bin:$PATH


# Installez Flask et les dépendances Python
RUN pip3 install Flask


# Copiez votre application Flask dans l'image
COPY ./app /app


# Exposez le port 5000 (ou tout autre port que votre application Flask utilise)
EXPOSE 5000


# Définissez la commande d'entrée
CMD ["python", "/app/app.py"]