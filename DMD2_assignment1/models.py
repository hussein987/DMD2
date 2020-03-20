import mongoengine
from mongoengine import *
from extras_mongoengine.fields import *
from enum import Enum


class MpaaRating(Enum):
    a = 'G'
    b = 'PG'
    c = 'PG-13'
    d = 'R'
    e = 'NC-17'


class Actor(mongoengine.Document):
    actor_id = mongoengine.IntField(primary_key=True)
    first_name = mongoengine.StringField()
    last_name = mongoengine.StringField()
    last_update = mongoengine.ComplexDateTimeField()


class Country(mongoengine.Document):
    country_id = mongoengine.IntField(primary_key=True)
    country = mongoengine.StringField()
    last_update = mongoengine.ComplexDateTimeField()


class City(mongoengine.Document):
    city_id = mongoengine.IntField(primary_key=True)
    city = mongoengine.StringField()
    country_id = ReferenceField(Country)
    last_update = mongoengine.ComplexDateTimeField()


class Address(mongoengine.Document):
    address_id = mongoengine.IntField(primary_key=True)
    address = mongoengine.StringField()
    address2 = mongoengine.StringField()
    district = mongoengine.StringField()
    cit_id = mongoengine.ReferenceField(City, reverse_delete_rule=CASCADE)
    postal_code = mongoengine.StringField()
    phone = mongoengine.StringField()
    last_update = mongoengine.ComplexDateTimeField()


class Category(mongoengine.Document):
    category_id = mongoengine.IntField(primary_key=True)
    name = mongoengine.StringField()
    last_update = mongoengine.ComplexDateTimeField()


class Store(mongoengine.Document):
    store_id = mongoengine.IntField(primary_key=True)
    manager_staff_id = mongoengine.IntField()
    address_id = mongoengine.ReferenceField(Address, reverse_delete_rule=CASCADE)
    last_update = mongoengine.ComplexDateTimeField()


class Customer(mongoengine.Document):
    customer_id = mongoengine.IntField(primary_key=True)
    store_id = mongoengine.ReferenceField(Store)
    first_name = mongoengine.StringField()
    last_name = mongoengine.StringField()
    email = mongoengine.StringField()
    address_id = mongoengine.ReferenceField(Address)
    activebool = mongoengine.BooleanField()
    create_date = mongoengine.DateField()
    last_update = mongoengine.ComplexDateTimeField()
    active = mongoengine.IntField()


class Language(mongoengine.Document):
    language_id = mongoengine.IntField(primary_key=True)
    name = mongoengine.StringField()
    last_update = mongoengine.ComplexDateTimeField()


class Film(mongoengine.Document):
    film_id = mongoengine.IntField(primary_key=True)
    title = mongoengine.StringField()
    description = mongoengine.StringField()
    release_year = mongoengine.IntField()
    language_id = mongoengine.ReferenceField(Language)
    rental_duration = mongoengine.IntField()
    rental_rate = mongoengine.DecimalField()
    length = mongoengine.IntField()
    replacement_cost = mongoengine.DecimalField()
    rating = StringEnumField(MpaaRating)
    last_update = mongoengine.ComplexDateTimeField()
    special_features = mongoengine.ListField(StringField())
    full_text = mongoengine.StringField()


class Film_Actor_key(EmbeddedDocument):
    actor_id = mongoengine.ReferenceField(Actor, required=True)
    film_id = mongoengine.ReferenceField(Film, required=True)


class Film_Actor(mongoengine.Document):
    key = mongoengine.EmbeddedDocumentField(Film_Actor_key, primary_key=True)
    last_update = mongoengine.ComplexDateTimeField()


class Staff(mongoengine.Document):
    staff_id = mongoengine.IntField(primary_key=True)
    first_name = mongoengine.StringField()
    last_name = mongoengine.StringField()
    address_id = mongoengine.ReferenceField(Address)
    email = mongoengine.EmailField()
    store_id = mongoengine.ReferenceField(Store)
    active = mongoengine.BooleanField()
    user_name = mongoengine.StringField()
    password = mongoengine.StringField()
    last_update = mongoengine.ComplexDateTimeField()
    picture = mongoengine.StringField()


class Inventory(mongoengine.Document):
    inventory_id = mongoengine.IntField(primary_key=True)
    film_id = mongoengine.ReferenceField(Film)
    store_id = mongoengine.ReferenceField(Store)
    last_update = mongoengine.ComplexDateTimeField()


class Rental(mongoengine.Document):
    rental_id = mongoengine.IntField(primary_key=True)
    rental_date = mongoengine.DateTimeField()
    inventory_id = mongoengine.ReferenceField(Inventory)
    customer_id = mongoengine.ReferenceField(Customer)
    return_date = mongoengine.ComplexDateTimeField()
    staff_id = mongoengine.ReferenceField(Staff)
    last_update = mongoengine.ComplexDateTimeField()


class Payment(mongoengine.Document):
    payment_id = mongoengine.IntField(primary_key=True)
    customer_id = mongoengine.ReferenceField(Customer)
    staff_id = mongoengine.ReferenceField(Staff)
    rental_id = mongoengine.ReferenceField(Rental)
    anmount = mongoengine.DecimalField()
    payment_date = mongoengine.ComplexDateTimeField()


class Film_Category_key(EmbeddedDocument):
    film_id = mongoengine.ReferenceField(Film, required=True)
    category_id = mongoengine.ReferenceField(Category, required=True)


class Film_category(mongoengine.Document):
    key = mongoengine.EmbeddedDocumentField(Film_Category_key, primary_key=True)
    last_update = mongoengine.ComplexDateTimeField()