from pymongo import *
from models import *
import csv

client = MongoClient("mongodb://localhost")  # connects to client locally
db = client['dvdrental']  # get the database
connect('dvdrental')

db.drop_collection("my_view2")
db.drop_collection("my_view5")

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

# return for each actor, a list of actors who co-starred a film with him
db.create_collection(
    'my_view5',
    viewOn='actor',
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
            "$group": {"_id": '$_id', "actors": {"$addToSet": '$relation.actors'}}
        },
        {
            "$sort": {"_id": 1}
        }
    ]
)

# returns the degree of separation of each actor from Penelope Guiness
query = db.my_view5.aggregate([
    {
        "$graphLookup": {
            "from": "my_view5",
            "startWith": 1,
            "connectFromField": "actors",
            "connectToField": "_id",
            "maxDepth": 6,
            "depthField": "Depth",
            "as": "GraphOutPut",
        }
    },
    {
        "$match": {"_id": 1}
    },
    {
        "$unwind": "$GraphOutPut"
    },
    {
        "$project":
            {
                "_id": 0,
                "actor_id": "$GraphOutPut._id",
                "degree_of_separation": "$GraphOutPut.Depth"
            }
    },
    {
        "$sort": {"actor_id": 1}
    }
])

w, h = 3, 201
output = [[0 for x in range(w)] for y in range(h)]

output[0][0] = "actor_id"
output[0][1] = "actor_name"
output[0][2] = "degree of separation from Penelope Guiness"

i = 1
for x in query:
    actor = db.actor.find_one({"_id": x["actor_id"]})
    output[i][0] = actor["_id"]
    output[i][1] = actor["first_name"] + " " + actor["last_name"]
    output[i][2] = x["degree_of_separation"]
    i = i + 1

for i in range(0, h):
    for j in range(0, w):
        print(output[i][j], end=' ')
    print()

# export the query result to csv
with open("query5.csv", "w+") as my_csv:
    csvWriter = csv.writer(my_csv, delimiter=',')
    csvWriter.writerows(output)
