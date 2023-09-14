class RecommendationEngine:
    def create_user(self, user_id):
        # Méthode pour créer un nouvel utilisateur
        ...
        faT = spark.sql("SELECT userId FROM Fat_parquet")

        max_user_id = faT.agg(max("userId")).collect()[0][0]

        print(max_user_id)

        return max_user_id+1

    def is_user_known(self, user_id):
        # Méthode pour vérifier si un utilisateur est connu
        ...
        faT = spark.sql("SELECT userId FROM Fat_parquet")

        max_user_id = faT.agg(max("userId")).collect()[0][0]

        return user_id is not None and user_id <= self.max_user_identifier
    def get_movie(self, movie_id):
        # Méthode pour obtenir un film
        ...

    def get_ratings_for_user(self, user_id):
        # Méthode pour obtenir les évaluations d'un utilisateur
        ...
        nombre_de_films_a_proposer = 5
        echantillon_de_films = best_movies.orderBy(rand()).limit(nombre_de_films_a_proposer)

        # Collectez les films échantillonnés et proposez-les à l'utilisateur pour notation
        films_proposes = echantillon_de_films.collect()

        print("Voici quelques-uns des meilleurs films à noter :")
        for i, film in enumerate(films_proposes, 1):
            print(f"{i}. {film['title']} ") #(Note moyenne : {film['avg_rating']}, Nombre de notes : {film['num_ratings']})
            note_utilisateur = input(f"Donnez une note de 1 à 5 pour ce film (ou laissez vide si vous ne voulez pas noter) : ")
            new_row = spark.createDataFrame([(user_id, film[movieId], note_utilisateur)], ("userId","movieId","rating"))
            fat_boy = fat_boy.union(new_row)
        # Vous pouvez enregistrer la note de l'utilisateur dans une base de données ou un autre emplacement.
        # Ici, nous imprimons simplement la note pour l'exemple.
        if note_utilisateur:
            print(f"Vous avez donné au film une note de {note_utilisateur}/5.")
            self.add_ratings(user_id, note_utilisateur)
        else:
            print("Vous n'avez pas donné de note pour ce film.")


    def add_ratings(self, user_id, ratings):
        # Méthode pour ajouter de nouvelles évaluations et re-entraîner le modèle
        ...
        new_row = spark.createDataFrame([(user_id, film[movieId], note_utilisateur)], ("userId","movieId","rating"))
        fat_boy = fat_boy.union(new_row)

        
    def predict_rating(self, user_id, movie_id):
        # Méthode pour prédire une évaluation pour un utilisateur et un film donnés
        ...

    def recommend_for_user(self, user_id, nb_movies):
        # Méthode pour obtenir les meilleures recommandations pour un utilisateur donné
        ...
        user_df = fat_boy.select("*").where(fat_boy["userId"] == user_id)

        recommendations = best_model.recommendForUserSubset(user_df, nb_movies) 
    
    
        return recommendations


    def __train_model(self):
        # Méthode privée pour entraîner le modèle avec ALS
        ...
        # Créez un objet ALS
        als = ALS(rank=10, maxIter=10, userCol='userId', itemCol='movieId', ratingCol='rating',
          coldStartStrategy="drop")  # Vous devriez spécifier 'ratingCol' si ce n'est pas déjà fait
        model = als.fit(fat_boy)


    def __evaluate(self):
        # Méthode privée pour évaluer le modèle en calculant l'erreur quadratique moyenne
        ...
        
        
    def __init__(self, sc, movies_set_path, ratings_set_path):
        # Méthode d'initialisation pour charger les ensembles de données et entraîner le modèle
        ...
