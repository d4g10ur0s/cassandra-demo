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
#
# UPLOAD RECIPES
#
