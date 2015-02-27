#!usr/bin/python

__author__ = 'chukwuyem'

import psycopg2
# import sys

conn = psycopg2.connect(database='newdb', user='postgres', host='localhost')

cur = conn.cursor()

##########################################################################################
#Loading Person

#open person.csv
f = open('/home/chukwuyem/small/outputDir-1k/person.csv', 'r')

#create person table
cur.execute("CREATE TABLE Person (PersonId integer PRIMARY KEY, firstName text, lastName text,\
gender varchar(10), birthday date, creationDate timestamp, locationIP text, browserUsed varchar(30));")

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #removing the newline from browserUsed string
    b[7] = b[7][:-1]
    #formatting the creationDate input
    b[5] = b[5].split('T')
    b[5] = b[5][0] + ' ' + b[5][1][:-1]
    #creating the SQL insert query
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    #the full insert query
    insert_str = "INSERT INTO Person VALUES(" + ins + ");"
    #execute the insert query
    cur.execute(insert_str)
    #go to next line
    b = f.readline()

f.close()

###########################################################################################
#Loading forum

#open forum.csv
f = open('/home/chukwuyem/small/outputDir-1k/forum.csv', 'r')

#create forum table
cur.execute("CREATE TABLE Forum (ForumId integer PRIMARY KEY, title text, creationDate timestamp);")

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #fixing the timestamp input
    b[2] = b[2].split('T')
    b[2] = b[2][0] + ' ' + b[2][1][:-2]
    #creating SQL insert query
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    #the full insert query
    insert_str = "INSERT INTO Forum VALUES(" + ins + ");"
    #execute the insert query
    cur.execute(insert_str)
    #go to next line
    b = f.readline()

f.close()

############################################################################################
#Loading tag

#open tag.csv
f = open('/home/chukwuyem/small/outputDir-1k/tag.csv', 'r')

#creating tag table
cur.execute("CREATE TABLE Tag (TagId integer PRIMARY KEY, name text, url text);")
#length integer is missing because it's not in the csv

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #removing the newline character
    b[2] = b[2][:-1]
    #creating SQL insert query
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    #the full insert query
    insert_str = "INSERT INTO Tag VALUES(" + ins + ");"
    #execute the insert query
    cur.execute(insert_str)
    #go to next line
    b = f.readline()

f.close()

###############################################################################################
#Loading tagclass

#open tagclass.csv
f = open('/home/chukwuyem/small/outputDir-1k/tagclass.csv', 'r')

#creating tag table
cur.execute("CREATE TABLE TagClass (TagClassId integer PRIMARY KEY, name text, url text);")
#length integer is missing because it's not in the csv

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #removing the newline character
    b[2] = b[2][:-1]
    #creating SQL insert query
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    #the full insert query
    insert_str = "INSERT INTO TagClass VALUES(" + ins + ");"
    #execute the insert query
    cur.execute(insert_str)
    #go to next line
    b = f.readline()

f.close()

###############################################################################################
#Loading post

#open post.csv
f = open('/home/chukwuyem/small/outputDir-1k/post.csv', 'r')

#creating post table
cur.execute("CREATE TABLE Post (PostId integer PRIMARY KEY, imageFile text, creationDate timestamp, \
locationIP text, browserUsed text, language text, content text);")
#length integer is missing because it's not in the csv


a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #fixing the timestamp input
    b[2] = b[2].split('T')
    b[2] = b[2][0] + ' ' + b[2][1][:-1]
    ins = ''
    #fixing the apostrophes
    for elem in b:
        if(len(elem.split("'")) > 1):
            elem = elem.split("'")
            new_elem=""
            for string_ in elem:
                new_elem += string_
                new_elem += '\u0027'
            elem = new_elem[:-6]
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    insert_str = "INSERT INTO Post VALUES(" + ins + ");"
    #print insert_str
    cur.execute(insert_str)
    b = f.readline()

f.close()

###############################################################################################
f = open('/home/chukwuyem/small/outputDir-1k/comment.csv', 'r')


#creating comment table
cur.execute("CREATE TABLE Comment (CommentId integer PRIMARY KEY, creationDate timestamp, locationIP text, \
browserUsed text, content text);")
#length integer is missing because it's not in the csv

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #fixing the timestamp input
    b[1] = b[1].split('T')
    b[1] = b[1][0] + ' ' + b[1][1][:-1]
    #removing the newline character
    b[4] = b[4][:-1]
    ins = ''
    #fixing the apostrophes
    for elem in b:
        if(len(elem.split("'")) > 1):
            elem = elem.split("'")
            new_elem=""
            for string_ in elem:
                new_elem += string_
                new_elem += '\u0027'
            elem = new_elem[:-6]
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    insert_str = "INSERT INTO Comment VALUES(" + ins + ");"
    #print insert_str
    cur.execute(insert_str)
    b = f.readline()


f.close()
###############################################################################################
f = open('/home/chukwuyem/small/outputDir-1k/comment_replyOf_comment.csv', 'r')



cur.execute("CREATE TABLE Comment_replyOf_Comment (Comment1_id integer references Comment(CommentId),\
 Comment2_id integer references Comment(CommentId));")
#length integer is missing because it's not in the csv

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #removing the newline character
    b[1] = b[1][:-1]
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    insert_str = "INSERT INTO Comment_replyOf_Comment VALUES(" + ins + ");"
    #print insert_str
    cur.execute(insert_str)
    b = f.readline()

f.close()
###############################################################################################
f = open('/home/chukwuyem/small/outputDir-1k/comment_hasCreator_person.csv', 'r')



cur.execute("CREATE TABLE Comment_hasCreator_Person (Comment_id integer references Comment(CommentId),\
 Person_id integer references Person(PersonId));")
#length integer is missing because it's not in the csv

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #removing the newline character
    b[1] = b[1][:-1]
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    insert_str = "INSERT INTO Comment_hasCreator_Person VALUES(" + ins + ");"
    #print insert_str
    cur.execute(insert_str)
    b = f.readline()

f.close()
###############################################################################################
f = open('/home/chukwuyem/small/outputDir-1k/person_knows_person.csv', 'r')



cur.execute("CREATE TABLE Person_knows_Person (Person1_id integer references Person(PersonId),\
 Person2_id integer references Person(PersonId));")
#length integer is missing because it's not in the csv

a = f.readline()
b = f.readline()

cur.execute("SET CLIENT_ENCODING TO LATIN2;")

while(b):
    b = b.split('|')
    #removing the newline character
    b[1] = b[1][:-1]
    ins = ''
    for elem in b:
        elem = "'" + elem + "'"
        ins += elem
        ins += ','
        ins += ' '
    ins = ins[:-2]
    insert_str = "INSERT INTO Person_knows_Person VALUES(" + ins + ");"
    #print insert_str
    cur.execute(insert_str)
    b = f.readline()

f.close()
###############################################################################################
conn.commit()
