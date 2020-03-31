from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)


def load_movie():
    movie_names = dict()
    with open("../data/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


lines = sc.textFile("../data/ml-100k/u.data")
name_dict = sc.broadcast(load_movie())

movies = lines.map(lambda x: (int(x.split()[1]), 1))
movie_counts = movies.reduceByKey(lambda x, y: x + y)

flipped = movie_counts.map(lambda x: (x[1], x[0]))
sorted_movies = flipped.sortByKey()
sorted_movies_with_name = sorted_movies.map(lambda x: (name_dict.value[x[1]], x[0]))

results = sorted_movies_with_name.collect()

for result in results:
    print(result)
