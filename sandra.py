from cassandra.cluster import Cluster

KEYSPACE = "sandra"
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()


rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
if KEYSPACE in [row[0] for row in rows]:
    print("Keyspace by the name \"" + KEYSPACE + "\" already exists, dropping.")
    session.execute("DROP KEYSPACE " + KEYSPACE)

print ("Creating keyspace \"" + KEYSPACE + "\".")
session.execute("""
    CREATE KEYSPACE %s
    WITH REPLICATION =
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""" % KEYSPACE)

print("Setting keyspace \"" + KEYSPACE + "\":")
session.execute('USE %s' % KEYSPACE)


session.execute("""
    CREATE TABLE director (
        id_director int,
        surname text,
        name text,
        imdb int,
        PRIMARY KEY (id_director, imdb)
)
""")


session.execute("""
    CREATE TABLE film (
        id_film int,
        title text,
        genre text,
        budget int,
        iid_directord_d int,
        PRIMARY KEY (id_director, id_film, budget)
)
""")


session.execute("""
    CREATE TABLE movie_screening (
        cinema_id int,
        time text,
        cost int,
        id_film int,
        PRIMARY KEY ((cinema_id, id_film), time)
)
""")


session.execute("""
    CREATE TABLE cinema (
        cinema_id int,
        name text,
        address text,
        city varchar,
        zipcode varchar,
        PRIMARY KEY (cinema_id, address)
)
""")


session.execute("INSERT INTO cinema (cinema_id, name, address, city, zipcode) VALUES (111, 'Cinema 1', 'Address 1', 'City 1', '1111')") 
session.execute("INSERT INTO cinema (cinema_id, name, address, city, zipcode) VALUES (112, 'Cinema 2', 'Address 2', 'City 2', '2222')")
session.execute("INSERT INTO cinema (cinema_id, name, address, city, zipcode) VALUES (113, 'Cinema 3', 'Address 3', 'City 3', '3333')")


session.execute("INSERT INTO director (id_director, surname, name, imdb) VALUES (1, 'Bird', 'Brad', 100)")
session.execute("INSERT INTO director (id_director, surname, name, imdb) VALUES (2, 'Adamson', 'Andrew', 101)") 
session.execute("INSERT INTO director (id_director, surname, name, imdb) VALUES (3, 'Sharpsteen', 'Ben', 102)")  
session.execute("INSERT INTO director (id_director, surname, name, imdb) VALUES (4, 'Unkrich', 'Lee', 103)") 


session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (1, 'Ratatouille', 'Fantasy', 150, 1)")
session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (2, 'Shrek', 'Comedy', 125, 2)")  
session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (3, 'Dumbo', 'Adventure', 13, 3)")  
session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (4, 'The Incredibles', 'Adventure', 92, 1)")  
session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (5, 'Coco', 'Adventure', 175, 4)") 
session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (6, 'The Incredibles 2', 'Adventure', 200, 1)")   
session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (7, 'Toy Story', 'Adventure', 30, 4)") 


session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 111, '12:45', 10, 7)") 
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 112, '17:00', 12, 2)") 
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 111, '23:25', 15, 3)") 
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 113, '9:30', 6, 2)") 
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 112, '14:35', 6, 4)")
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 113, '21:17', 29, 1)")  
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 113, '7:00', 9, 5)") 
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 111, '20:50', 11, 6)") 
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 112, '9:30', 19, 7)")     
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 112, '19:30', 12, 7)")
session.execute("INSERT INTO movie_screening ( cinema_id, time, cost, id_film) VALUES ( 111, '19:30', 22, 7)")  


def menu():
    print (" ")
    print ("  Cassandra queries ")
    print ("-----------------------------")
    print ("1. Retrieve all directors ")
    print ("2. Retrieve all films ")
    print ("3. Retrieve one director and all directors films ")
    print ("4. Retrieve all directors and all their films ")
    print ("5. Retrieve specific film shown in specific cinema ")
    print ("6. Retrieve films and their screenings ")
    print ("7. Retrieve cinemas and their screenings ")
    print ("8. New film addition ")
    print ("9. Directors by imdb ")
    print ("10. Films by budget ")
    print ("0. Exit ")
    print (" ")
    return int(input ("Choose your option: "))


def all_directors_querie():
    print(" ")
    print("All directos: ")

    rows = session.execute('SELECT name, surname FROM director')
    for row in rows:
        print("name:    ", row.name,  "         surname:  ", row.surname)
    print(" ")


def all_films_querie():
    print(" ")
    print("All films: ")

    rows = session.execute('SELECT title FROM film')
    for row in rows:
        print("title:    ", row.title)
    print(" ")


def director_and_all_films_querie():
    print(" ")
    print("Director and all directors films: ")

    print(" ")
    print("Director: ")

    directors = session.execute('SELECT name, surname FROM director WHERE id_director=1')
    for director in directors:
        print("name:  ", director.name,"     surname:  ", director.surname)

    print(" ")
    print("Films: ")

    rows = session.execute('SELECT title FROM film WHERE id_director=1')
    for row in rows:
        print("title:  ", row.title)
    print(" ")
    

def directors_imdb():
    print(" ")
    print("Directors: ")

    rows = session.execute('SELECT name, surname, imdb FROM director WHERE id_director in (1, 2, 3, 4) AND imdb > 101')
    for row in rows:
        print("imdb:  ", row.imdb, "     name:    ", row.name,  "         surname:  ", row.surname)
    print(" ")


def all_director_and_all_films_querie():
    print(" ")
    print("All director and all their films: ")

    result = session.execute('SELECT COUNT(*) FROM director')
    count = 0
    for row in result:
        count += row.count
    
    for i in range(count):
        print("------------------------------------------------------------")
        print("Director: ")
        directors = session.execute('SELECT name, surname FROM director WHERE id_director = ' + str(i + 1))
        for director in directors:
            print("name:  ", director.name,"     surname:  ", director.surname)
 
        print(" ")
        print("Films: ")
 
        films = session.execute('SELECT title FROM film WHERE id_director = ' + str(i + 1))
        for film in films:
            print("title:  ", film.title)

        print(" ")
        

def film_in_cinema_querie():
    print(" ")
    print("Film: ")

    rows = session.execute('SELECT id_film, title FROM film WHERE id_film=7 AND id_director=4')
    for row in rows:
        print("id:    ", row.id_film, "     title:    ", row.title)  

    print(" ")
    print("Cinema: ")

    rows = session.execute("SELECT cinema_id, name, address, city, zipcode FROM cinema WHERE  address = 'Address 1' AND cinema_id=111 ")
    for row in rows:
        print("id:    ", row.cinema_id, "     name:    ", row.name, "     address:    ", row.address, "     city:    ", row.city, "     zipcode:    ", row.zipcode)

    print(" ")
    print("Movie screenings in specific cinema: ")

    rows = session.execute("SELECT cinema_id, time, cost, id_film FROM movie_screening WHERE cinema_id=111 AND id_film=7 AND time in ('19:30','12:45') ")
    for row in rows:
        print("cinema id :  ", row.cinema_id, "     screening time:  ", row.time, "     cost:  ", row.cost, "     film id:  ", row.id_film)

    print(" ")


def all_films_and_all_film_screenings_querie():
    print(" ")
    print("All films and all their screenings: ")
    
    result = session.execute('SELECT COUNT(*) FROM film')
    count = 0
    for row in result:
        count += row.count
    
    for i in range(count):
        print("------------------------------------------------------------")
        print("Film: ")
        films = session.execute('SELECT title FROM film WHERE id_director in (1, 2, 3, 4) AND id_film = ' + str(i + 1))
        for film in films:
            print("title:  ", film.title)
 
        print(" ")
        print("Film screenings: ")
 
        rows = session.execute('SELECT cinema_id, time, cost, id_film FROM movie_screening WHERE cinema_id in (111, 112, 113) AND id_film = ' + str(i + 1))
        for row in rows:
            print("cinema id :  ", row.cinema_id, "     screening time:  ", row.time, "     cost:  ", row.cost, "     film id:  ", row.id_film)

        print(" ")


def all_cinemas_and_all_film_screenings_querie():
    print(" ")
    print("All films and all their screenings: ")
    
    result = session.execute('SELECT COUNT(*) FROM cinema')
    count = 0
    for row in result:
        count += row.count
    
    for i in range(count):
        print("------------------------------------------------------------")
        print("Cinema: ")
        rows1 = session.execute('SELECT cinema_id, name, address, city, zipcode FROM cinema WHERE cinema_id = ' + str(i + 111))
        for row1 in rows1:
            print("id:    ", row1.cinema_id, "     name:    ", row1.name, "     address:    ", row1.address, "     city:    ", row1.city, "     zipcode:    ", row1.zipcode)
 
        print(" ")
        print("Film screenings: ")
 
        rows = session.execute('SELECT cinema_id, time, cost, id_film FROM movie_screening WHERE id_film in (1, 2, 3, 4, 5, 6, 7) AND cinema_id = ' + str(i + 111))
        for row in rows:
            print("cinema id :  ", row.cinema_id, "     screening time:  ", row.time, "     cost:  ", row.cost, "     film id:  ", row.id_film)

        print(" ")


def data_insertion_querie():
    print(" ")
    print("All films: ")

    rows = session.execute('SELECT title FROM film')
    for row in rows:
        print("title:    ", row.title)

    print(" ")
    print("Inserting new films ")

    session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (8, 'Toy Story 2', 'Adventure', 230, 4)IF NOT EXISTS")
    session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (9, 'Toy Story 3', 'Adventure', 325, 4)IF NOT EXISTS")
    session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (10, 'Toy Story 4', 'Adventure', 520, 4)IF NOT EXISTS")
    session.execute("INSERT INTO film (id_film, title, genre, budget, id_director) VALUES (7, 'Toy Story', 'Fantasy', 30, 4)IF NOT EXISTS") # toks jau yra
    


    print(" ")
    print("All films after new film insertion: ")

    rows = session.execute('SELECT title FROM film')
    for row in rows:
        print("title:    ", row.title)


def films_by_budget():
    print(" ")
    print("Films: ")

    rows = session.execute('SELECT title, genre, budget FROM film WHERE id_director in (1, 2, 3, 4) AND id_film in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AND budget > 150')
    for row in rows:
        print("title:  ", row.title, "\t\t\t genre:    ", row.genre,  "\t\t\t budget:  ", row.budget)
    print(" ")


print ("")
loop = 1
choice = 0
while loop == 1:
    choice = menu()
    print (" ")
    if choice == 1:
        all_directors_querie()
    elif choice == 2:
        all_films_querie()
    elif choice == 3:
        director_and_all_films_querie()
    elif choice == 4:
        all_director_and_all_films_querie()
    elif choice == 5:
        film_in_cinema_querie()
    elif choice == 6:
        all_films_and_all_film_screenings_querie()
    elif choice == 7:
        all_cinemas_and_all_film_screenings_querie()
    elif choice == 8:
        data_insertion_querie()
    elif choice == 9:
        directors_imdb() 
    elif choice == 10:
        films_by_budget() 
    elif choice == 0:
        loop = 0
