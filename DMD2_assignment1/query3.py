from pymongo import *
from models import *
import csv

client = MongoClient("mongodb://localhost")  # connects to client locally
db = client['dvdrental']  # get the database
connect('dvdrental')

db.drop_collection("my_view")
db.drop_collection("my_view1")

# project _id, film_id and sort by film_id
db.create_collection(
    'my_view1',
    viewOn='inventory',
    pipeline=[
        {
            "$project":
                {
                    "_id": 1,
                    "film": "$film_id"
                }
        },
        {
            "$sort": {"film": 1}
        }
    ]
)

# returns for each film, its category and the number of times it
# has been rented by a customer
db.create_collection(
    'my_view',
    viewOn='rental',
    pipeline=[
        {
            "$project":
                {
                    "_id": 0,
                    "inventory_id": "$inventory_id"
                }
        },
        {
            "$sort": {"inventory_id": 1}
        },
        {
            "$lookup":
                {
                    "from": "my_view1",
                    "localField": "inventory_id",
                    "foreignField": "_id",
                    "as": "inventory_docs"
                }
        },
        {
            "$unwind": "$inventory_docs"
        },
        {
            "$project":
                {
                    "inventory": "$inventory_id",
                    "film": "$inventory_docs.film"
                }
        },
        {
            "$group":
                {
                    "_id": "$film",
                    "number_of_times_rented": {"$sum": 1}
                }
        },
        {
            "$lookup":
                {
                    "from": "film_category",
                    "localField": "_id",
                    "foreignField": "_id.film_id",
                    "as": "relation"
                }
        },
        {
            "$unwind": "$relation"
        },
        {
            "$project":
                {
                    "_id": 0,
                    "film": "$_id",
                    "category": "$relation._id.category_id",
                    "number_of_times_rented": "$number_of_times_rented"
                }
        }
    ]
)

query = db['my_view'].find({})

w, h = 4, db['my_view'].count_documents({}) + 1
output = [[0 for x in range(w)] for y in range(h)]

output[0][0] = "film_id"
output[0][1] = "title"
output[0][2] = "category"
output[0][3] = "number_of_times_rented"

i = 1
for x in query:
    film = db.film.find_one({"_id": x["film"]})
    category = db.category.find_one({"_id": x["category"]})
    output[i][0] = film["_id"]
    output[i][1] = film["title"]
    output[i][2] = category["name"]
    output[i][3] = x["number_of_times_rented"]
    i = i + 1

for i in range(0, h):
    for j in range(0, w):
        print(output[i][j], end=' ')
    print()

# export the query result to csv
with open("query3.csv", "w+") as my_csv:
    csvWriter = csv.writer(my_csv, delimiter=',')
    csvWriter.writerows(output)
