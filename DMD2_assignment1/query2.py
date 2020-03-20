from pymongo import *
from models import *
import csv

client = MongoClient("mongodb://localhost")  # connects to client locally
db = client['dvdrental']  # get the database
connect('dvdrental')

db.drop_collection("my_view1")
db.drop_collection("my_view2")
db.drop_collection("my_view5")

# project on the actor_id
db.create_collection(
    'my_view1',
    viewOn='actor',
    pipeline=[
        {
            "$project": {"_id": 1}
        }
    ]
)

# returns for each film, all the actors who participated in it
db.create_collection(
    'my_view2',
    viewOn='film__actor',
    pipeline=[
        {
            "$group": {"_id": '$_id.film_id', "actors": {"$addToSet": '$_id.actor_id'}}
        },
        {
            "$sort": {"_id": 1}
        }
    ]
)

# returns a 200x200 table where each cell tells the number of
# movies that a pair of actors have co-starred
db.create_collection(
    'my_view5',
    viewOn='my_view1',
    pipeline=[
        {
            "$lookup":
                {
                    "from": "my_view2",
                    "localField": "_id",
                    "foreignField": "actors",
                    "as": "relation"
                }
        },
        {
            "$project":
                {
                    "_id": 1,
                    "relation.actors": 1
                }
        },
        {
            "$unwind": "$relation"
        },
        {
            "$unwind": "$relation.actors"
        },
        {
            "$project": {
                "_id": 0,
                "actor1": "$_id",
                "co-actor": "$relation.actors",
            }
        },
        {
            "$sort":
                {
                    "actor1": 1,
                    "co-actor": 1
                }
        }
    ]
)

w, h = 201, 201
output = [[0 for x in range(w)] for y in range(h)]

query = db.my_view5.find({})

i = 0
for x in query:
    ii = x["actor1"]
    jj = x["co-actor"]
    output[ii][jj] = output[ii][jj] + 1
    i = i + 1

for i in range(1, 201):
    output[0][i] = output[i][0] = ("actor" + str(i))

# export the query result to csv
with open("query2.csv", "w+") as my_csv:
    csvWriter = csv.writer(my_csv, delimiter=',')
    csvWriter.writerows(output)