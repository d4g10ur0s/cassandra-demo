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
