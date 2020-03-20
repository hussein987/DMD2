from models import *
from pymongo import *
import psycopg2

conn = psycopg2.connect(host="localhost", database="dvdrental", user="postgres", password="my name123")

client = MongoClient("mongodb://localhost")  # connects to client locally
db = client['dvdrental']  # get the database

connect('dvdrental')


def get_actors():
    cur = conn.cursor()
    cur.execute("select * from actor")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Actor(actor_id=row[0], first_name=row[1], last_name=row[2], last_update=row[3]).save()
        row = cur.fetchone()


def get_countries():
    cur = conn.cursor()
    cur.execute("select * from country")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Country(country_id=row[0], country=row[1], last_update=row[2]).save()
        row = cur.fetchone()


def get_cities():
    cur = conn.cursor()
    cur.execute("select * from city")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        City(city_id=row[0], city=row[1], country_id=row[2], last_update=row[3]).save()
        row = cur.fetchone()


def get_addresses():
    cur = conn.cursor()
    cur.execute("select * from address")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Address(address_id=row[0],
                address=row[1],
                address2=row[2],
                district=row[3],
                cit_id=row[4],
                postal_code=row[5],
                phone=row[6],
                last_update=row[7]).save()
        row = cur.fetchone()


def get_categories():
    cur = conn.cursor()
    cur.execute("select * from category")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Category(category_id=row[0],
                 name=row[1],
                 last_update=row[2]).save()
        row = cur.fetchone()


def get_stores():
    cur = conn.cursor()
    cur.execute("select * from store")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Store(store_id=row[0],
              manager_staff_id=row[1],
              address_id=row[2],
              last_update=row[3]).save()
        row = cur.fetchone()


def get_films():
    cur = conn.cursor()
    cur.execute("select * from film")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Film(film_id=row[0],
             title=row[1],
             description=row[2],
             release_year=row[3],
             language_id=row[4],
             rental_duration=row[5],
             rental_rate=row[6],
             length=row[7],
             replacement_cost=row[8],
             rating=row[9],
             last_update=row[10],
             special_features=row[11],
             full_text=str(row[12])).save()
        row = cur.fetchone()


def get_customers():
    cur = conn.cursor()
    cur.execute("select * from customer")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Customer(customer_id=row[0],
                 store_id=row[1],
                 first_name=row[2],
                 last_name=row[3],
                 email=row[4],
                 address_id=row[5],
                 activebool=row[6],
                 create_date=row[7],
                 last_update=row[8],
                 active=row[9]).save()
        row = cur.fetchone()


def get_languages():
    cur = conn.cursor()
    cur.execute("select * from language")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Language(language_id=row[0],
                 name=row[1],
                 last_update=row[2]).save()
        row = cur.fetchone()


def get_film_actor():
    cur = conn.cursor()
    cur.execute("select * from film_actor")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Film_Actor(key=Film_Actor_key(actor_id=row[0], film_id=row[1]), last_update=row[2]).save()
        row = cur.fetchone()


def get_staff():
    cur = conn.cursor()
    cur.execute("select * from staff")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        if row[10] is not None:
            pic = str(row[10].tobytes())
        else:
            pic = None
        print(pic)
        Staff(staff_id=row[0],
              first_name=row[1],
              last_name=row[2],
              address_id=row[3],
              email=row[4],
              store_id=row[5],
              active=row[6],
              user_name=row[7],
              password=row[8],
              last_update=row[9],
              picture=pic).save()
        row = cur.fetchone()


def get_inventory():
    cur = conn.cursor()
    cur.execute("select * from inventory")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Inventory(inventory_id=row[0],
                  film_id=row[1],
                  store_id=row[2],
                  last_update=row[3], ).save()
        row = cur.fetchone()


def get_rental():
    cur = conn.cursor()
    cur.execute("select * from rental")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Rental(rental_id=row[0],
               rental_date=row[1],
               inventory_id=row[2],
               customer_id=row[3],
               return_date=row[4],
               staff_id=row[5],
               last_update=row[6], ).save()
        row = cur.fetchone()


def get_payment():
    cur = conn.cursor()
    cur.execute("select * from payment")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Payment(payment_id=row[0],
                customer_id=row[1],
                staff_id=row[2],
                rental_id=row[3],
                anmount=row[4],
                payment_date=row[5]).save()
        row = cur.fetchone()


def get_film_category():
    cur = conn.cursor()
    cur.execute("select * from film_category")
    print(cur.rowcount)
    row = cur.fetchone()
    while row is not None:
        Film_category(key=Film_Category_key(film_id=row[0], category_id=row[1]), last_update=row[2]).save()
        row = cur.fetchone()


def migrate_database():
    get_actors()
    get_countries()
    get_cities()
    get_addresses()
    get_categories()
    get_stores()
    get_customers()
    get_languages()
    get_film_actor()
    get_rental()
    get_payment()
    get_inventory()
    get_staff()
    get_film_category()
    get_films()


migrate_database()

