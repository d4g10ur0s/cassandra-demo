import os
import datetime as dt

import uuid
import pandas as pd
import numpy as np
import ast
# cassandra imports
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel,InvalidRequest
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
    # for recipes cassandra.InvalidRequest
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
                # get the ids
                query_1 = """select * from query_1 where submitted>='2012-01-01' and submitted<='2012-05-31' ALLOW FILTERING;"""
                recipes = session.execute(query_1)
                idl = []
                for r in recipes :
                    idl.append(r[1])
                # select by id
                query_1 = f"SELECT * FROM recipe WHERE id IN ({', '.join(['%s']*len(idl))}) ALLOW FILTERING;"
                recipes = session.execute(query_1, idl)
                #sorting
                rows = list(recipes)
                sorted_rows = sorted(rows, key=lambda x: x.mean_rating, reverse=True)
                # Select the top 30 rows
                top_30_rows = sorted_rows[:30]
                # Process the top 30 rows
                for r in top_30_rows[:5] :
                    #print(str(r))
                    print(str("*"*10+"\n"+"Name : " + str(r.name) + "\nMean Rating : " + str(r.mean_rating)))
                    print(str("Submitted : " + str(r.submitted) + "\nDifficulty : " + str(r.difficulty) +"\n"+"*"*10))
            elif choice==2:
                rName = str(input('Provide recipe\'s name : '))
                query_2 = "select * from query_2 where name = \'" + rName+"\';"
                recipes = session.execute(query_2)
                idl = []
                for r in recipes :
                    idl.append(r[1])
                # select by id
                query_2 = f"SELECT * FROM recipe WHERE id IN ({', '.join(['%s']*len(idl))}) ALLOW FILTERING;"
                recipes = session.execute(query_2, idl)
                #sorting
                rows = list(recipes)
                if len(rows) > 0:
                    for r in rows :
                        print(str("*"*10+"\n"+"Name : " + str(r.name) + "\nMean Rating : " + str(r.mean_rating)))
                        print(str("Submitted : " + str(r.submitted) + "\nDifficulty : " + str(r.difficulty) +"\n"+"*"*10))
                else:
                    print("There is no recipe named : " + str(rName))
            elif choice==3:
                categoryNum = int(input('Provide recipe\'s category number \nzeroSkill : 1\neasy : 2 \nintermediate : 3 \nprofessional : 4 \n'))
                if categoryNum in range(1,5):
                    category = ["zeroSkill", "easy" , "intermediate", "professional"]
                    query_3 = "select * from recipe where difficulty=\'"+category[categoryNum-1]+"\' ORDER BY mean_rating DESC;"
                    recipes = session.execute(query_3)
                    rows = list(recipes)
                    for r in rows[:5] :
                        print(str("*"*10+"\n"+"Name : " + str(r.name) + "\nMean Rating : " + str(r.mean_rating)))
                        print(str("Submitted : " + str(r.submitted) + "\nDifficulty : " + str(r.difficulty) +"\n"+"*"*10))
                else:
                    print("Provide a correct category number .")
            elif choice==4:
                rTag = str(input('Provide recipe\'s tag : '))
                query_4 = "select * from recipe_tags where tag_name = \'" + rTag+"\';"
                recipes = session.execute(query_4)
                idl = []
                for r in recipes :
                    idl.append(r[1])
                # select by id
                query_4 = f"SELECT * FROM recipe WHERE id IN ({', '.join(['%s']*len(idl))}) ALLOW FILTERING;"
                recipes = session.execute(query_4, idl)
                #sorting
                rows = list(recipes)
                if len(rows) > 0:
                    sorted_rows = sorted(rows, key=lambda x: x.submitted, reverse=True)
                    for r in sorted_rows[:5] :
                        print(str("*"*10+"\n"+"Name : " + str(r.name) + "\nMean Rating : " + str(r.mean_rating)))
                        print(str("Submitted : " + str(r.submitted) + "\nDifficulty : " + str(r.difficulty) +"\n"+"*"*10))
                else:
                    print("There is no recipe named : " + str(rName))
            elif choice==5:
                rTag = str(input('Provide recipe\'s tag : '))
                query_4 = "select * from recipe_tags where tag_name = \'" + rTag+"\';"
                recipes = session.execute(query_4)
                idl = []
                for r in recipes :
                    idl.append(r[1])
                # select by id
                query_4 = f"SELECT * FROM recipe WHERE id IN ({', '.join(['%s']*len(idl))}) LIMIT 20 ALLOW FILTERING;"
                recipes = session.execute(query_4, idl)
                #sorting
                rows = list(recipes)
                if len(rows) > 0:
                    for r in rows[:5] :
                        print(str("*"*10+"\n"+"Name : " + str(r.name) + "\nMean Rating : " + str(r.mean_rating)))
                        print(str("Submitted : " + str(r.submitted) + "\nDifficulty : " + str(r.difficulty) +"\n"+"*"*10))
                else:
                    print("There is no recipe named : " + str(rName))
        except ValueError as err:
            print(str(err))
            if (input("Do you want to exit?\n(y\\n)\n")) == "y":
                break
except InvalidRequest as err :
    print(str(err))
    # create tables
    createRecipeTable(session)
finally :
    # it's just a select all statement
    # used to validate data insertion
    rows = session.execute("SELECT * FROM recipe", [])
    if not rows:
        print("Does not exist")
