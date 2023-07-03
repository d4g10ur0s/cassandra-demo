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
    zeroSkill = recipesRaw[recipesRaw["difficulty"]<=diffRange[0]]
    zeroSkill["difficulty"] = "zeroSkill"
    easy = recipesRaw[recipesRaw["difficulty"]<=diffRange[1]]
    easy["difficulty"] = "easy"
    intermediate = recipesRaw[recipesRaw["difficulty"]<=diffRange[2]]
    intermediate["difficulty"] = "intermediate"
    professional = recipesRaw[recipesRaw["difficulty"]>diffRange[2]]
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
        statement = "SELECT name,mean_rating FROM recipe WHERE difficulty=\'"+ str(i)+"\' and submitted >= '2012-01-01' AND  submitted <= '2012-05-31' ORDER BY mean_rating DESC LIMIT 30 ALLOW FILTERING;"
        result = session.execute(statement)
        print(str(i))
        lastIndex = 0
        for res in result:
            # sort results
            if len(chosen)<30:
                chosen.append( (res[0], res[1]) )
            else:
                for indx in range(lastIndex,30):
                    if chosen[indx][1] < res[1]:
                        chosen[indx] = (res[0], res[1])
                        lastIndex+=1
                        break
                if lastIndex==29:
                    lastIndex=0
                    break
    return chosen

def query_2(session):
    statement = "SELECT name,mean_rating FROM recipe WHERE name=\'"+ str(input("Give name of recipe : "))+"\' ALLOW FILTERING;"
    result = session.execute(statement)
    return result

def query_3(session):
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    print("Choose number of difficulty : \nSelect between 1 to 4\n")
    for i in range(len(typeOfRecipe)):
        print(str(i+1)+") "+str(typeOfRecipe[i]))
    choice = int(input())-1
    print("Selected : " + str(typeOfRecipe[choice]))
    statement = "SELECT name,mean_rating FROM recipe WHERE difficulty=\'"+ str(typeOfRecipe[choice])+"\' ORDER BY mean_rating DESC LIMIT 100 ALLOW FILTERING;"
    result = session.execute(statement)
    return result

def getIdList(session):
    tagName=input("Type tag name : ")
    statement = "SELECT id FROM recipe_tags WHERE tag_name=\'"+ str(tagName)+"\' ALLOW FILTERING;"
    result = session.execute(statement)
    if not result.one()==None:
        return [res.id for res in result]
    else:
        return []

def query_4(session):
    tagName=input("Type tag name : ")
    statement = "SELECT id FROM recipe_tags WHERE tag_name=\'"+ str(tagName)+"\' ORDER BY submitted DESC ALLOW FILTERING;"
    result = session.execute(statement)
    idList = []
    if not result.one()==None:
        idList=[res.id for res in result]
    else:
        idList = []
    # get ids
    #idList = getIdList(session)
    if len(idList)>0:
        statement = "SELECT name,date FROM recipe WHERE id IN "+str(tuple(idList))+"ALLOW FILTERING ;"
        result = session.execute(statement)
        return result
    else:
        return []

def query_5(session):
    typeOfRecipe=["zeroSkill","easy","intermediate","professional"]
    idList = getIdList(session)
    result = []
    for i in typeOfRecipe:
        statement = "SELECT * FROM recipe WHERE difficulty=\'"+str(i)+"\' and id in "+str(idList)+" ORDER BY mean_rating DESC LIMIT 20 ALLOW FILTERING;"
        tempResult = session.execute(statement)
        result+=tempResult
    return result
#
# CREATE TABLES
#
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
                       difficulty text,
                       PRIMARY KEY ((difficulty) , mean_rating , submitted ,n_steps) )
                       WITH CLUSTERING ORDER BY (mean_rating DESC, submitted DESC);""")
    session.execute("""CREATE TABLE IF NOT EXISTS recipe_tags (
                       id bigint ,
                       tag_name text,
                       submitted date,
                       PRIMARY KEY((tag_name,id) ,submitted) )
                       WITH CLUSTERING ORDER BY (submitted DESC);""")
#
# UPLOAD RECIPE TAGS
#
def recipeTagsBulkInsert(recipes,session):
    recipe = recipes[["id","submitted"]].values.tolist()
    insertStatement="insert into recipe_tags (id,tag_name,submitted) values (? , ?);"
    insertRecipeTags = session.prepare(insertStatement)
    goGo = len(recipeId)
    for r in recipe :
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        goGo-=1
        print(str(goGo))
        # get the tags
        rTags = recipes[recipes["id"]==r[0]]["tags"].values.tolist()
        rTags=ast.literal_eval(rTags[0])
        for t in rTags:
            if len(t)>0:
                batch.add(insertRecipeTags, (r[0],t,r[1]) )
        session.execute(batch)
#
# UPLOAD RECIPES
#
def recipeBulkInsert(recipes,session):
    tableOrder = ["id","contributor_id","minutes","mean rating","name","submitted",
                  "nutrition","n_steps","steps","description","ingredients","n_ingredients","difficulty"]
    insertStatement = "insert into recipe (id,contributor_id,minutes,mean_rating,name,submitted,"+"nutrition,n_steps,steps,description,ingredients,n_ingredients,difficulty) VALUES ("+"?,"*12+"?"+")"
    insertRecipes = session.prepare(insertStatement)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    counter = 0
    acounter = 0
    for recipe in recipes[tableOrder].values.tolist():
        counter+=1
        acounter+=1
        print(str(acounter))
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
    createCSV()
# connect to cassandra
print("Connecting to database")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("use recipesharing;")# use the correct keyspace

#print("Reading CSV")
#editedRecipes = pd.read_csv("processed_recipes.csv")
#recipeTagsBulkInsert(editedRecipes,session)

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
                    print(str("*"*10+"\n"+"Name : " + r[0] + "\nMean Rating : " + str(r[1]) +"\n"+"*"*10+"\n"))
            elif choice==2:
                recipes = query_2(session)
                for r in recipes :
                    print(str("*"*10+"\n"+"Name : " + r[0] + "\nMean Rating : " + str(r[1]) +"\n"+"*"*10+"\n"))
            elif choice==3:
                recipes = query_3(session)
                for r in recipes :
                    print(str("*"*10+"\n"+"Name : " + r[0] + "\nMean Rating : " + str(r[1]) +"\n"+"*"*10+"\n"))
            elif choice==4:
                recipes = query_4(session)
            elif choice==5:
                recipes = query_5(session)
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
