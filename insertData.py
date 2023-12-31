from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import uuid

import numpy as np
import ast
#
# UPLOAD RECIPE TAGS
#
def recipeTagsBulkInsert(id,tags,session):
    insertStatement="insert into recipe_tags (id,tag_name) values (? , ?);"
    insertRecipeTags = session.prepare(insertStatement)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for tname in tags :
        batch.add(insertRecipeTags, (id,tname) )
    session.execute(batch)
#
# UPLOAD RECIPE TAGS
#
#
# UPLOAD RECIPES
#
def recipeBulkInsert(recipes,session):
    tableOrder = ["id","name","submitted","mean rating","description","difficulty","ingredients","minutes",
                 "n_ingredients","n_steps","nutrition","steps","tags"]
    insertStatement = "insert into recipe (id,name,submitted,mean_rating,description,difficulty,ingredients,minutes,n_ingredients,n_steps,nutrition,steps,tags) VALUES ("+"?,"*12+"?"+")"
    insertRecipes = session.prepare(insertStatement)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    insertStatement_1 = "insert into query_1 (submitted,id) VALUES (?,?)"
    insertRecipes_1 = session.prepare(insertStatement_1)
    batch_1 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    insertStatement_2 = "insert into query_2 (name,id) VALUES (?,?)"
    batch_2 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insertRecipes_2 = session.prepare(insertStatement_2)

    insertStatement_3 = "insert into query_3 (difficulty,mean_rating,id) VALUES (?,?,?)"
    batch_3 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insertRecipes_3 = session.prepare(insertStatement_3)

    counter = 0
    acounter = 0

    for recipe in recipes[tableOrder].values.tolist():
        acounter+=1
        recipe[0] = uuid.uuid4()
        recipe[6] = ast.literal_eval(recipe[6])
        recipe[10] = ast.literal_eval(recipe[10])
        recipe[11] = ast.literal_eval(recipe[11])
        recipe[12] = ast.literal_eval(recipe[12])
        # no recipe tags or name or submitted or any other value
        #print(str(len(recipe[12])))
        if len(recipe[12])<=1:
            print("+mvainei")
            continue

        recipeTagsBulkInsert(recipe[0],recipe[12],session)
        try :
            batch.add(insertRecipes, tuple(recipe))
            batch_1.add(insertRecipes_1, (recipe[2],recipe[0]))
            batch_2.add(insertRecipes_2, (recipe[1],recipe[0]))
            batch_3.add(insertRecipes_3, (recipe[5],recipe[3],recipe[0]))
            counter+=1
        except:
            #input(str(recipe))
            pass

        if counter > 15:
            print(str(acounter))
            session.execute(batch)
            session.execute(batch_1)
            session.execute(batch_2)
            session.execute(batch_3)
            counter = 0
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_1 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_2 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_3 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        else:
            pass
    session.execute(batch)
#
# UPLOAD RECIPES
#
