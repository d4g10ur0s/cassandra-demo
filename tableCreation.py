#
# CREATE TABLES
#
def createRecipeTable(session):
    # basic table
    session.execute("""CREATE TABLE IF NOT EXISTS recipe (
                       id uuid ,
                       name text,
                       submitted date,
                       mean_rating double,
                       steps set<text>,
                       n_steps int,
                       ingredients set<text>,
                       n_ingredients int,
                       nutrition set<double>,
                       minutes float,
                       description text,
                       difficulty text,
                       PRIMARY KEY ((id) , name , submitted ,mean_rating) )
                       WITH CLUSTERING ORDER BY (name ASC, submitted ASC,mean_rating ASC);""")
    # Q1 table
    session.execute("""CREATE TABLE IF NOT EXISTS query_1 (
                       submitted date ,
                       id uid,
                       PRIMARY KEY((submitted),id ) )
                       WITH CLUSTERING ORDER BY (id ASC);""")
    # Q2 table
    session.execute("""CREATE TABLE IF NOT EXISTS query_2 (
                       name text ,
                       id uuid,
                       PRIMARY KEY((name),id ) )
                       WITH CLUSTERING ORDER BY (id ASC);""")
    # Q3 table
    session.execute("""CREATE TABLE IF NOT EXISTS query_3 (
                       difficulty text,
                       id uuid,
                       PRIMARY KEY((difficulty),id ) )
                       WITH CLUSTERING ORDER BY (id ASC);""")
    # Q.4-5 table
    session.execute("""CREATE TABLE IF NOT EXISTS recipe_tags (
                       tag_name text,
                       id uuid ,
                       PRIMARY KEY((tag_name),id ) )
                       WITH CLUSTERING ORDER BY (id ASC);""")
