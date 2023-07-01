import os

import pandas as pd

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


from cassandra.cluster import Cluster

print("Connecting to database")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

row = session.execute("SELECT release_version FROM system.local").one()
if row:
    print(row[0])

rows = session.execute("SELECT * FROM test_table WHERE id=%s", 2)
if not rows:
    print "Does not exist"
#ingridientMapping = pd.read_pickle("ingr_map.pkl")
#print(str(ingridientMapping.keys()))

if os.path.exists(os.getcwd()+"\\processed_recipes.csv"):
    pass
else:
    print("Creating CSV")
    recipesRaw = pd.read_csv("RAW_recipes.csv")
    #print(str(recipesRaw.keys()))
    #print(str(recipesRaw["submitted"]))
    interactionsRaw = pd.read_csv("RAW_interactions.csv")
    #print(str(interactionsRaw.keys()))

    recipeId = recipesRaw["id"].values.tolist()
    recipeMeanRating = []
    for id in recipeId :
        recipeMeanRating.append(interactionsRaw[interactionsRaw["recipe_id"]==id]["rating"].mean())

    recipesRaw["mean rating"] = recipeMeanRating
    recipesRaw.to_csv("processed_recipes.csv")
print("Reading CSV")


'''

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
