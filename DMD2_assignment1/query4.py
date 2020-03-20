from pymongo import *
from models import *
import csv

client = MongoClient("mongodb://localhost")  # connects to client locally
db = client['dvdrental']  # get the database
connect('dvdrental')


customer = int(input("please enter the customer id "))

db.drop_collection("my_view")

db.create_collection(
    'my_view',
    viewOn='rental',
    pipeline=[
    {
        "$match" :
        {
            "customer_id" : customer
        }
    },
    {
    "$group": { "_id": '$customer_id' , "films": { "$push": '$inventory_id'}}
    },
    {
       "$lookup":
       {
         "from": "rental",
         "localField": "films",
         "foreignField": "inventory_id",
         "as": "inventory_docs"
       }
    },
        {
          "$unwind" : "$inventory_docs"
        },
    {
        "$match" :
        {
            "inventory_docs.customer_id" : {"$not" : {"$eq" : customer}}
        }
    },
    {
       "$lookup":
       {
         "from": "rental",
         "localField": "inventory_docs.customer_id",
         "foreignField": "customer_id",
         "as": "customer_films"
       }
    },
    {
        "$unwind" : "$customer_films"
    },
    {
        "$group": {"_id" : "$_id", "film_c" : {"$push" : {"customer" : '$customer_films.customer_id', "inv" : "$customer_films.inventory_id"}}}
    },
    {
        "$unwind" : "$film_c"
    },
    {
       "$lookup":
       {
         "from": "inventory",
         "localField": "film_c.inv",
         "foreignField": "_id",
         "as": "inventory_films"
       }
    },
    {
        "$unwind" : "$inventory_films"
    },
    {
        "$group": {"_id": '$inventory_films.film_id', "recommendation_degree" : {"$sum" : 1}}
    },
    {
        "$sort" : {"recommendation_degree" : -1}
    }
]
)

print(db['my_view'].find({}).explain())

query = db['my_view'].find({})

w, h = 3, db['my_view'].count_documents({})+1
output = [[0 for x in range(w)] for y in range(h)]

output[0][0] = "film_id"
output[0][1] = "title"
output[0][2] = "recommendation degree for customer " + str(customer)



i = 1
for x in query:
    film = db.film.find_one({"_id": x["_id"]})
    output[i][0] = film["_id"]
    output[i][1] = film["title"]
    output[i][2] = x["recommendation_degree"]
    i = i+1


#
# for i in range(0, h):
#     for j in range(0, w):
#         print(output[i][j], end=' ')
#     print()



with open("query4.csv", "w+") as my_csv:
    csvWriter = csv.writer(my_csv, delimiter=',')
    csvWriter.writerows(output)