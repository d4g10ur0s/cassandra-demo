import os
import datetime as dt

import pandas as pd
import numpy as np
import ast
# cassandra imports
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel
# custom imports
from tableCreation import createRecipeTable
from insertData import recipeTagsBulkInsert,recipeBulkInsert

p = "F:\\5oEtos\\EarinoEksamhno\\BigData"

def createCSV():
    global p
    tags = {}
    print("Creating CSV")
    # create difficulty
    # Objective : 3
    recipesRaw = pd.read_csv(p+"\\RAW_recipes.csv")
    recipesRaw[["minutes","n_steps"]]=recipesRaw[["minutes","n_steps"]].fillna(0,inplace=False)
    maxDifficulty = recipesRaw["minutes"].max() * recipesRaw["n_steps"].max()
    recipesRaw["difficulty"] = recipesRaw["minutes"]*recipesRaw["n_steps"]/maxDifficulty
    dist = recipesRaw["difficulty"].max()-recipesRaw["difficulty"].min()
    diffRange = [i/4 * dist for i in range(1,4) ] + [recipesRaw["difficulty"].max(),]
    zeroSkill = recipesRaw.loc[recipesRaw["difficulty"]<=diffRange[0]]
    zeroSkill["difficulty"] = "zeroSkill"
    easy = recipesRaw.loc[recipesRaw["difficulty"]<=diffRange[1]]
    easy["difficulty"] = "easy"
    intermediate = recipesRaw.loc[recipesRaw["difficulty"]<=diffRange[2]]
    intermediate["difficulty"] = "intermediate"
    professional = recipesRaw.loc[recipesRaw["difficulty"]>diffRange[2]]
    professional["difficulty"] = "professional"
    recipesRaw = pd.concat([zeroSkill,easy,intermediate,professional])
    #############################################################################
    interactionsRaw = pd.read_csv(p+"\\RAW_interactions.csv")
    recipeId = recipesRaw["id"].values.tolist()
    # get mean rating
    recipeMeanRating = []
    goGo = len(recipeId)
    for id in recipeId :
        goGo-=1
        print(str(goGo))
        recipeMeanRating.append(interactionsRaw[interactionsRaw["recipe_id"]==id]["rating"].mean())
    # add mean rating to dataframe
    recipesRaw["mean rating"] = recipeMeanRating
    recipesRaw.to_csv("processed_recipes.csv")

# create csv data if it doesnt exist
if os.path.exists(p+"\\processed_recipes.csv"):
    pass
else:
    createCSV()
# connect to cassandra
# '127.0.0.1:9042
print("Connecting to database")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
# create keyspace if not exist
keyspace_name = 'recipesharing'
replication_options = {
    'class': 'SimpleStrategy',
    'replication_factor': 1  # Adjust replication factor as needed
}
create_keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {str(replication_options)}"
session.execute(create_keyspace_query)
# use the correct keyspace
session.execute("use recipesharing;")
# try get table data
try :
    # for recipes
    rows = session.execute("SELECT * FROM recipe limit 10", [])
    if not rows:
        print("Data doesn\'t exist")
        print("Inserting data")
        # read csv data
        print("Reading CSV")
        editedRecipes = pd.read_csv(p+"\\processed_recipes.csv")
        recipeBulkInsert(editedRecipes,session)
    # for recipe tags
    rows = session.execute("SELECT * FROM recipe_tags limit 10", [])
    if not rows:
        print("Data doesn\'t exist")
        print("Inserting data")
        print("Reading CSV")
        editedRecipes = pd.read_csv(p+"\\processed_recipes.csv")
        recipeTagsBulkInsert(editedRecipes,session)
    # make the queries
    while 1 :
        try :
            choice = int(input("Choose an integer between 1 to 5\n"))
            if choice==1:
                recipes = query_1(session)
                for r in recipes :
                    print(str("*"*10+"\n"+"Name : " + r[0] + "\nMean Rating : " + str(r[1])))
                    print(str("Submitted : " + str(r[2]) + "\nDifficulty : " + str(r[3]) +"\n"+"*"*10))
            elif choice==2:
                recipes = query_2(session)
                if len(recipes)>0:
                    for r in recipes :
                        print(str("*"*10+"\n"+"Difficulty : " + r[0] + "\nMean Rating : " + str(r[1])))
                        print(str("*"*10+"\n"+"Name : " + r[10] + "\nMinutes : " + str(r[9])))
                        print("Steps")
                        for i in range(len(r[12])):
                            print(str(i+1) + ") " + str(r[12][i]))
                else:
                    print("There is no recipe with this name.")
            elif choice==3:
                recipes = query_3(session)
                for r in recipes :
                    print(str("*"*10+"\n"+"Name : " + r[0] + "\nMean Rating : " + str(r[1])))
                    print(str("Difficulty : " + str(r[2]) +"\n"+"*"*10))
            elif choice==4:
                recipes = query_4(session)
                for r in recipes :
                    print(str("*"*10+"\n"+"Name : " + r[0] + "\nMean Rating : " + str(r[1])+"\n"+"*"*10))
            elif choice==5:
                recipes = query_5(session)
                if len(recipes)>0:
                    for r in recipes :
                        print(str("*"*10+"\n"+"Difficulty : " + r[0] + "\nMean Rating : " + str(r[1])))
                        print(str("*"*10+"\n"+"Name : " + r[10] + "\nMinutes : " + str(r[9])))
                        print("Steps")
                        for i in range(len(r[12])):
                            print(str(i+1) + ") " + str(r[12][i]))
                else:
                    print("There is no recipe with this name.")
        except:
            if (input("Do you want to exit?\n(y\\n)\n")) == "y":
                break
except:
    # create tables
    createRecipeTable(session)
finally :
    # it's just a select all statement , commonly used to validate data insertion
    rows = session.execute("SELECT * FROM recipe", [])
    if not rows:
        print("Does not exist")
