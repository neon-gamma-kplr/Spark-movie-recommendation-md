# Utilisez une image OpenJDK comme point de départ
FROM openjdk:8

# Installez Spark
RUN wget -q https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.1.2-bin-hadoop3.2.tgz -C /opt && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

# Configurez l'environnement Spark
ENV SPARK_HOME /opt/spark-3.1.2-bin-hadoop3.2
ENV PATH $SPARK_HOME/bin:$PATH

# Installez Flask et les dépendances Python
RUN pip install Flask

# Copiez votre application Flask dans l'image
COPY ./app /app

# Exposez le port 5000 (ou tout autre port que votre application Flask utilise)
EXPOSE 5000

# Définissez la commande d'entrée
CMD ["python", "/app/your_flask_app.py"]