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

def createCSV():
    tags = {}
    print("Creating CSV")
    # create difficulty
    # Objective : 3
    recipesRaw = pd.read_csv("RAW_recipes.csv")
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
    interactionsRaw = pd.read_csv("RAW_interactions.csv")
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

def query_1(session):
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    chosen = []
    for i in typeOfRecipe:
        statement = "SELECT name,mean_rating,submitted,difficulty FROM recipe WHERE difficulty=\'"+ str(i)+"\' and submitted >= '2012-01-01' AND  submitted <= '2012-05-31' ORDER BY mean_rating DESC LIMIT 30 ALLOW FILTERING;"
        result = session.execute(statement)
        print(str(i))
        lastIndex = 0
        for res in result:
            # sort results
            if len(chosen)<30:
                chosen.append( tuple(res) )
            else:
                for indx in range(lastIndex,30):
                    if chosen[indx][1] < res[1]:
                        chosen[indx] = tuple(res)
                        lastIndex+=1
                        break
                if lastIndex==29:
                    lastIndex=0
                    break
    return chosen

def query_2(session):
    recipeInput=str(input("Give name of recipe : "))
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    chosen = []
    for i in typeOfRecipe:
        statement = "SELECT * FROM recipe WHERE difficulty=\'"+ str(i) +"\' and name=\'"+ recipeInput +"\' ALLOW FILTERING;"
        result = session.execute(statement)
        for res in result :
            chosen.append(tuple(res))
    return chosen

def query_3(session):
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    print("Choose number of difficulty : \nSelect between 1 to 4\n")
    for i in range(len(typeOfRecipe)):
        print(str(i+1)+") "+str(typeOfRecipe[i]))
    choice = int(input())-1
    print("Selected : " + str(typeOfRecipe[choice]))
    statement = "SELECT name,mean_rating,difficulty FROM recipe WHERE difficulty=\'"+ str(typeOfRecipe[choice])+"\' ORDER BY mean_rating DESC LIMIT 100 ALLOW FILTERING;"
    result = session.execute(statement)
    chosen = []
    for res in result :
        chosen.append(tuple(res))
    return chosen

def getIdList(session):
    tagName=input("Type tag name : ")
    statement = "SELECT id FROM recipe_tags WHERE tag_name=\'"+ str(tagName)+"\' ALLOW FILTERING;"
    result = session.execute(statement)
    if not result.one()==None:
        return [res.id for res in result]
    else:
        return []

def query_4(session):
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    tagName=input("Type tag name : ")
    statement = "SELECT id FROM recipe_tags WHERE tag_name=\'"+ str(tagName)+"\' ALLOW FILTERING;"
    result = session.execute(statement)
    idList = []
    if not result.one()==None:
        idList=[res.id for res in result]
    else:
        idList = []
    # get ids
    if len(idList)>0:
        chosen = []
        for t in typeOfRecipe:
            statement = "SELECT name,submitted FROM recipe WHERE difficulty = \'"+str(t)+"\'and id IN "+str(tuple(idList))+" LIMIT 20 ALLOW FILTERING ;"
            result = session.execute(statement)
            for res in result:
                if len(chosen)==0:
                    chosen.append(tuple(res))
                else:
                    indx = 0
                    for e in chosen :
                        if e[1] >= res[1]:
                            indx+=1
                        else:
                            chosen.insert(indx,tuple(res))
                            indx = 0
                            break
                    if not(indx < len(chosen)):
                        chosen.append(tuple(res))
        return chosen
    else:
        return []

def query_5(session):
    chosen = []
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    idList = getIdList(session)
    for i in typeOfRecipe:
        statement = "SELECT * FROM recipe WHERE difficulty=\'"+str(i)+"\' and id in "+str(tuple(idList))+" ORDER BY mean_rating DESC LIMIT 20 ALLOW FILTERING;"
        result = session.execute(statement)
        lastIndex = 0
        for res in result:
            # sort results
            if len(chosen)<20:
                chosen.append( tuple(res) )
            else:
                for indx in range(lastIndex,20):
                    if chosen[indx][2] < res[2]:
                        chosen[indx] = tuple(res)
                        lastIndex+=1
                        break
                if lastIndex==19:
                    lastIndex=0
                    break
    return chosen
# create csv data if it doesnt exist
if os.path.exists(os.getcwd()+"/processed_recipes.csv"):
    pass
else:
    createCSV()
# connect to cassandra
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

session.execute("use recipesharing;")# use the correct keyspace

try :# try get table data
    # for recipes
    rows = session.execute("SELECT * FROM recipe limit 10", [])
    if not rows:
        print("Data doesn\'t exist")
        print("Inserting data")
        # read csv data
        print("Reading CSV")
        editedRecipes = pd.read_csv("processed_recipes.csv")
        recipeBulkInsert(editedRecipes,session)
    # for recipe tags
    rows = session.execute("SELECT * FROM recipe_tags limit 10", [])
    if not rows:
        print("Data doesn\'t exist")
        print("Inserting data")
        print("Reading CSV")
        editedRecipes = pd.read_csv("processed_recipes.csv")
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
    createRecipeTable(session)

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
