from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import uuid

import ast
#
# UPLOAD RECIPE TAGS
#
def recipeTagsBulkInsert(recipes,session):
    recipeId = recipes["id"].values.tolist()
    recipeDate = recipes["submitted"].values.tolist()
    insertStatement="insert into recipe_tags (id,tag_name) values (? , ?);"
    insertRecipeTags = session.prepare(insertStatement)
    for id in recipeId :
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        # get the tags
        rTags = recipes[recipes["id"]==id]["tags"].values.tolist()
        rTags=ast.literal_eval(rTags[0])
        for t in rTags:
            if len(t)>0:
                batch.add(insertRecipeTags, (id,t) )
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
    counter = 0
    acounter = 0

    for recipe in recipes[tableOrder].values.tolist():
        counter+=1
        acounter+=1
        print(str(acounter))
        recipe[0] = uuid.uuid1()
        input(str(recipe[6]))
        recipe[6] = ast.literal_eval(recipe[6])
        #try :
        #    if np.isnan(recipe[9]):
        #        recipe[9]=""
        #except:
        #    pass
        batch.add(insertRecipes, tuple(recipe))
        if counter > 10:
            session.execute(batch)
            counter = 0
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        else:
            pass
    session.execute(batch)
#
# UPLOAD RECIPES
#
