import os

import pandas as pd
import numpy as np
import ast

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel

def createRecipeTable(session):
    session.execute("""CREATE TABLE recipe (
                       id bigint PRIMARY KEY,
                       contributor_id bigint,
                       minutes float,
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

def recipeBulkInsert(recipes,session):
    #recipes.fillna(0,inplace=True)
    tableOrder = ["id","contributor_id","minutes","mean rating","name","submitted",
                  "tags","nutrition","n_steps","steps","description","ingredients","n_ingredients"]
    insertStatement = "insert into recipe (id,contributor_id,minutes,mean_rating,name,submitted,"+"tags,nutrition,n_steps,steps,description,ingredients,n_ingredients) VALUES ("+"?,"*12+"?"+")"
    insertRecipes = session.prepare(insertStatement)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    counter = 0
    for recipe in recipes[tableOrder].values.tolist():
        counter+=1
        try :
            if np.isnan(recipe[4]):
                recipe[4]="noName"
        except:
            pass
        recipe[6] = ast.literal_eval(recipe[6])
        recipe[7] = ast.literal_eval(recipe[7])
        recipe[9] = ast.literal_eval(recipe[9])
        try :
            if np.isnan(recipe[10]):
                recipe[10]=""
        except:
            pass
        recipe[11] = ast.literal_eval(recipe[11])
        print(str(recipe[10]))
        batch.add(insertRecipes, tuple(recipe))
        if counter > 10:
            session.execute(batch)
            counter = 0
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        else:
            pass
    session.execute(batch)
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
# try get table data
try :
    rows = session.execute("SELECT * FROM recipe", [])
    if not rows:
        print("Does not exist")
except:
    createRecipeTable(session)

recipeBulkInsert(editedRecipes,session)
rows = session.execute("SELECT * FROM recipe", [])
if not rows:
    print("Does not exist")
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
