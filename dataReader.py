import os

import pandas as pd

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table

def createRecipeTable(session):
    session.execute("""CREATE TABLE recipe (
                       id bigint PRIMARY KEY,
                       contributor_id bigint,
                       minutes int,
                       mean_rating double,
                       name text,
                       submitted date,
                       tags set<text>,
                       nutrition set<double>,
                       n_steps int,
                       steps set<text>,
                       description text,
                       ingredients set<text>,
                       n_ingredients int
                       )""")

# create csv data if it doesnt exist
if os.path.exists(os.getcwd()+"/processed_recipes.csv"):
    pass
else:
    print("Creating CSV")
    recipesRaw = pd.read_csv("RAW_recipes.csv")
    interactionsRaw = pd.read_csv("RAW_interactions.csv")
    recipeId = recipesRaw["id"].values.tolist()
    # get mean rating
    recipeMeanRating = []
    for id in recipeId :
        recipeMeanRating.append(interactionsRaw[interactionsRaw["recipe_id"]==id]["rating"].mean())
    # add mean rating to dataframe
    recipesRaw["mean rating"] = recipeMeanRating
    recipesRaw.to_csv("processed_recipes.csv")
# read csv data
print("Reading CSV")
editedRecipes = pd.read_csv("processed_recipes.csv")
# connect to cassandra
print("Connecting to database")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
# use the correct keyspace
session.execute("use recipesharing;")
##
## Test
row = session.execute("SELECT release_version FROM system.local")
if row:
    print(row[0])
##Test
##
# try get table data
try :
    rows = session.execute("SELECT * FROM test_table WHERE id=%s", [2])
    if not rows:
        print("Does not exist")
except:
    createRecipeTable(session)

'''
ingridientMapping = pd.read_pickle("ingr_map.pkl")
print(str(ingridientMapping.keys()))

ppUsers = pd.read_csv("PP_users.csv")
print(str(ppUsers.keys()))

ppRecipes = pd.read_csv("PP_recipes.csv")
print(str(ppRecipes.keys()))

interactionsValidation = pd.read_csv("interactions_validation.csv")
print(str(interactionsValidation.keys()))

interactionsTrain = pd.read_csv("interactions_train.csv")
print(str(interactionsTrain.keys()))

interactionsTest = pd.read_csv("interactions_test.csv")
print(str(interactionsTest.keys()))
'''
