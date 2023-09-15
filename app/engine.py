from pyspark.sql.types import *
from pyspark.sql.functions import explode, col
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SQLContext
# from pyspark.sql import SparkSession


class RecommendationEngine:

    faT = spark.sql("SELECT userId FROM Fat_parquet")

    max_user_id = faT.agg(max("userId")).collect()[0][0]


    def create_user(self, user_id=None):
        # Méthode pour créer un nouvel utilisateur

        if user_id is None:
            user_id = self.max_user_identifier + 1
        else:
            user_id = int(user_id)

        if user_id > self.max_user_identifier:
            self.max_user_identifier = user_id

        return user_id


    def is_user_known(self, user_id):
        # Méthode pour vérifier si un utilisateur est connu

        return user_id is not None and user_id <= self.max_user_identifier


    def get_movie(self, movie_id=None):
        # Méthode pour obtenir un film
        
        if movie_id is None:
            # Obtenez le nombre total de lignes dans le DataFrame
            total_rows = best_movies_df.count()
            # Générez un échantillon aléatoire avec une fraction d'échantillonnage de 1/N
            random_row = best_movies_df.sample(withReplacement=False, fraction=1.0/total_rows).first()
            return self.movies_df.filter(col("movieId") == random_row.movieId)
        else:
            return self.movies_df.filter(col("movieId") == movie_id)         


    def get_ratings_for_user(self, user_id):
        # Méthode pour obtenir les évaluations d'un utilisateur
        ...
        # nombre_de_films_a_proposer = 5
        # echantillon_de_films = best_movies.orderBy(rand()).limit(nombre_de_films_a_proposer)

        # # Collectez les films échantillonnés et proposez-les à l'utilisateur pour notation
        # films_proposes = echantillon_de_films.collect()

        # print("Voici quelques-uns des meilleurs films à noter :")
        # for i, film in enumerate(films_proposes, 1):
        #     print(f"{i}. {film['title']} ") #(Note moyenne : {film['avg_rating']}, Nombre de notes : {film['num_ratings']})
        #     note_utilisateur = input(f"Donnez une note de 1 à 5 pour ce film (ou laissez vide si vous ne voulez pas noter) : ")
        #     new_row = spark.createDataFrame([(user_id, film[movieId], note_utilisateur)], ("userId","movieId","rating"))
        #     fat_boy = fat_boy.union(new_row)
        # # Vous pouvez enregistrer la note de l'utilisateur dans une base de données ou un autre emplacement.
        # # Ici, nous imprimons simplement la note pour l'exemple.
        # if note_utilisateur:
        #     print(f"Vous avez donné au film une note de {note_utilisateur}/5.")
        #     self.add_ratings(user_id, note_utilisateur)
        # else:
        #     print("Vous n'avez pas donné de note pour ce film.")

        return self.ratings_df.filter(col("userId") == user_id)


    def add_ratings(self, user_id, ratings):
        # Méthode pour ajouter de nouvelles évaluations et re-entraîner le modèle
        ...
        # new_row = spark.createDataFrame([(user_id, film[movieId], note_utilisateur)], ("userId","movieId","rating"))
        # fat_boy = fat_boy.union(new_row)

        new_ratings_df = self.spark.createDataFrame([Row(userId=user_id, movieId=rating["movieId"], rating=rating["rating"]) for rating in ratings])
        self.ratings_df = self.ratings_df.union(new_ratings_df)
        self.__train_model()

        
    def predict_rating(self, user_id, movie_id):
        # Méthode pour prédire une évaluation pour un utilisateur et un film donnés
        
        rating_df = self.spark.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
        predictions = self.model.transform(rating_df)
        prediction = predictions.select("prediction").first()
        if prediction:
            return prediction["prediction"]
        else:
            return -1


    def recommend_for_user(self, user_id, nb_movies):
        # Méthode pour obtenir les meilleures recommandations pour un utilisateur donné
        ...
        # user_df = fat_boy.select("*").where(fat_boy["userId"] == user_id)

        # recommendations = best_model.recommendForUserSubset(user_df, nb_movies) 
    
    
        # return recommendations

        user_df = self.spark.createDataFrame([(user_id,)], ["userId"])
        recommendations = self.model.recommendForUserSubset(user_df, nb_movies)
        recommended_movie_ids = [row.movieId for row in recommendations.select("recommendations.movieId").first()]
        recommended_movies = self.movies_df.filter(col("movieId").isin(recommended_movie_ids))
        return recommended_movies


    def __train_model(self):
        # Méthode privée pour entraîner le modèle avec ALS
        ...
        # # Créez un objet ALS
        # als = ALS(rank=10, maxIter=10, userCol='userId', itemCol='movieId', ratingCol='rating',
        #   coldStartStrategy="drop")  # Vous devriez spécifier 'ratingCol' si ce n'est pas déjà fait
        # model = als.fit(fat_boy)

        als = ALS(maxIter=self.maxIter, regParam=self.regParam, userCol="userId", itemCol="movieId", ratingCol="rating")
        model = als.fit(self.ratings_df)
        self.model = model
        # Vous pouvez appeler d'autres méthodes d'évaluation ici, comme __evaluate().


    def __evaluate(self):
        # Méthode privée pour évaluer le modèle en calculant l'erreur quadratique moyenne
        predictions = self.model.transform(self.ratings_df)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        print("Root Mean Squared Error (RMSE):", rmse)
        
        
    def __init__(self, sc, movies_set_path, ratings_set_path):
        # Méthode d'initialisation pour charger les ensembles de données et entraîner le modèle
        ...
    def __init__(self, sc, movies_set_path, ratings_set_path, maxIter=10, regParam=0.1):
        self.spark = SparkSession.builder.appName("RecommendationEngine").getOrCreate()
        self.sc = sc
        self.maxIter = maxIter
        self.regParam = regParam
        self.movies_set_path = movies_set_path
        self.ratings_set_path = ratings_set_path
        self.__train_model()