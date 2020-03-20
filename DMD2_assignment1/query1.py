from pymongo import *
from models import *
import csv

client = MongoClient("mongodb://localhost")  # connects to client locally
db = client['dvdrental']  # get the database
connect('dvdrental')



db.drop_collection("my_view")
db.drop_collection("my_view1")
db.drop_collection("my_view2")

db.create_collection(
    'my_view2',
    viewOn='rental',
    pipeline=[
        {
            "$project" :
                {
                    "_id" : 0,
                    "year" : {"$year" : "$rental_date"}
                }
        },
        {
            "$sort" : {"year" : -1}
        }
]
)

current_year = db.my_view2.find_one({})["year"]

db.create_collection(
    'my_view1',
    viewOn='film_category',
    pipeline=[
        {
            "$project" :
                {
                    "_id" : 0,
                    "film" : "$_id.film_id",
                    "category" : "$_id.category_id"
                }
        },
        {
            "$sort" : {"film" : 1}
        }
]
)



db.create_collection(
    'my_view',
    viewOn='rental',
    pipeline=[
    {
        "$project" :
            {
                "_id" : 0,
                "inventory_id" : 1,
                "customer_id" : 1,
                "year" : {"$year" : "$rental_date"}
            },
    },
    {
        "$match" :
            {
                "year" : current_year
            }
    },
    {
       "$lookup":
       {
         "from": "inventory",
         "localField": "inventory_id",
         "foreignField": "_id",
         "as": "inventory_docs"
       }
    },
        {
          "$unwind" : "$inventory_docs"
        },
    {
        "$project" :
            {
                "customer_id" : 1,
                "film_id" : "$inventory_docs.film_id",
            }
    },
    {
        "$lookup":
           {
             "from": "film_category",
             "localField": "film_id",
             "foreignField": "_id.film_id",
             "as": "relation"
           }
    },
    {
        "$unwind" : "$relation"
    },
    {
        "$project" :
            {
                "customer_id" : 1,
                "categories" : "$relation._id.category_id",
            }
    },
    {
        "$group" :
            {
                "_id" : "$customer_id",
                "num_of_categories" : {"$sum" : 1}
            }
    },
    {
        "$match" :
            {
                "num_of_categories" : {"$gte" : 2}
            }
    }
]
)


w, h = 4, db['my_view'].count_documents({})+1
output = [[0 for x in range(w)] for y in range(h)]

output[0][0] = "customer_id"
output[0][1] = "first_name"
output[0][2] = "last_name"
output[0][3] = "number_of_categories"

print(db['my_view'].find({}).explain())

query = db['my_view'].find({})


i = 1
for x in query:
    customer = db.customer.find_one({"_id": x["_id"]})
    output[i][0] = customer["_id"]
    output[i][1] = customer["first_name"]
    output[i][2] = customer["last_name"]
    output[i][3] = x["num_of_categories"]
    i = i+1


for i in range(0, h):
    for j in range(0, w):
        print(output[i][j], end=' ')
    print()



with open("query1.csv", "w+") as my_csv:
    csvWriter = csv.writer(my_csv, delimiter=',')
    csvWriter.writerows(output)
