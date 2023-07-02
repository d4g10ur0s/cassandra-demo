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
    session.execute("""CREATE TABLE IF NOT EXISTS recipe (
                       id bigint ,
                       contributor_id bigint,
                       minutes float,
                       mean_rating double,
                       name text,
                       submitted date,
                       nutrition set<double>,
                       n_steps int,
                       steps set<text>,
                       description text,
                       ingredients set<text>,
                       n_ingredients int,
                       PRIMARY KEY (difficulty) );""")
    session.execute("""CREATE TABLE IF NOT EXISTS recipe_tags (
                       id bigint ,
                       tag_name text,
                       PRIMARY KEY(tag_name,id) );""")

def recipeBulkInsert(recipes,session):
    #recipes.fillna(0,inplace=True)
    tableOrder = ["id","contributor_id","minutes","mean rating","name","submitted",
                  "nutrition","n_steps","steps","description","ingredients","n_ingredients"]
    insertStatement = "insert into recipe (id,contributor_id,minutes,mean_rating,name,submitted,"+"nutrition,n_steps,steps,description,ingredients,n_ingredients) VALUES ("+"?,"*11+"?"+")"
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
        #recipe[7] = ast.literal_eval(recipe[7])
        recipe[8] = ast.literal_eval(recipe[8])
        try :
            if np.isnan(recipe[9]):
                recipe[9]=""
        except:
            pass
        recipe[10] = ast.literal_eval(recipe[10])
        print(str(recipe[9]))
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
    #create difficulty
    recipesRaw = pd.read_csv("RAW_recipes.csv")
    recipesRaw[["minutes","n_steps"]]=recipesRaw[["minutes","n_steps"]].fillna(0,inplace=False)
    maxDifficulty = recipesRaw["minutes"].max() * recipesRaw["n_steps"].max()
    recipesRaw["difficulty"] = recipesRaw["minutes"]*recipesRaw["n_steps"]/maxDifficulty
    dist = recipesRaw["difficulty"].max()-recipesRaw["difficulty"].min()
    diffRange = [i/4 * dist for i in range(1,4) ] + [recipesRaw["difficulty"].max(),]
    zeroSkill = recipesRaw[recipesRaw["difficulty"]<=diffRange[0]]
    easy = recipesRaw[recipesRaw["difficulty"]<=diffRange[1]]
    intermediate = recipesRaw[recipesRaw["difficulty"]<=diffRange[2]]
    professional = recipesRaw[recipesRaw["difficulty"]>diffRange[2]]
    #############################################################################
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
        print("Data doesn\'t exist")
        print("Inserting data")
        recipeBulkInsert(editedRecipes,session)
except:
    createRecipeTable(session)

# SELECT * FROM recipe WHERE submitted >= '2012-01-01' AND  submitted <= '2012-05-31' ORDER BY mean_value ASC LIMIT 100 ALLOW FILTERING;

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
