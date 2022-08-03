import psycopg2
import os
import sys

def getConnector(user='postgres', password='1234', dbname='dds_assignment'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(tablename, file_path, connector):
    #conn = getConnector()
    cursor = connector.cursor()
    cursor.execute("DROP TABLE IF EXISTS %s" % tablename)
    sql = 'CREATE TABLE %s (UserID int, MovieID int, Rating float)' % tablename
    cursor.execute(sql)
    connector.commit()
    
    ratingFile = open(file_path)
    lines = ratingFile.readlines()
    for line in lines:
        temp = line.strip().split("::")[:3]
        insert_query = 'INSERT INTO ratings VALUES (%d, %d, %f)' % (int(temp[0]), int(temp[1]), float(temp[2]))
        cursor.execute(insert_query)
        connector.commit()

def rangePartition(ratings_table, N, connector):
    #conn = getConnector()
    cursor = connector.cursor()
    drop_table_if_exist = 'DROP TABLE IF EXISTS range_part%d'
    create_fragment_start = 'CREATE TABLE range_part%d AS SELECT * FROM %s WHERE rating >= %f and rating <= %f'
    create_fragment = 'CREATE TABLE range_part%d AS SELECT * FROM %s WHERE rating > %f and rating <= %f'
    
    step = 5.0/N
    min_rate, max_rate = 0, step
    for i in range(N):
        cursor.execute(drop_table_if_exist % i)
        if i == 0:
            cursor.execute(create_fragment_start % (i, ratings_table, min_rate, max_rate))
        else:
            cursor.execute(create_fragment % (i, ratings_table, min_rate, max_rate))
        min_rate += step
        max_rate += step
    connector.commit()
    cursor.close()

def roundRobinPartition(ratings_table, N, connector):
    #conn = getConnector()
    cursor = connector.cursor()
    drop_table_if_exist = 'DROP TABLE IF EXISTS rrobin_part%d'
    
    create_rrobin = '''
    CREATE TABLE rrobin_part%d AS SELECT userid,movieid,rating 
    FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowid FROM %s) AS temp
    WHERE mod(temp.rowid-1,%d) = %d'''
    
    for i in range(N):
        cursor.execute(drop_table_if_exist % i)
        cursor.execute(create_rrobin % (i, ratings_table, N, i))
    connector.commit()
    cursor.close()

def roundrobininsert(ratings_table, user_id, movie_id, rating, connector):
    #conn = getConnector()
    cursor = connector.cursor()
    insert_q = 'INSERT INTO %s VALUES (%d, %d, %f)'
    cursor.execute(insert_q % (ratings_table, user_id, movie_id, rating))
    
    count_q = 'select count(*) from %s' % ratings_table
    results = []
    cursor.execute(count_q, results)
    results = cursor.fetchone()
    count = results[0]
    
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%'")
    partition_count = len(cursor.fetchall())
    
    fragment_index = (count - 1) % partition_count
    fragment_name = 'rrobin_part%d' % fragment_index
    cursor.execute(insert_q % (fragment_name, user_id, movie_id, rating))
    
    connector.commit()
    cursor.close()

def rangeinsert(ratings_table, user_id, movie_id, rating, connector):
    #conn = getConnector()
    cursor = connector.cursor()
    insert_q = 'INSERT INTO %s VALUES (%d, %d, %f)'
    cursor.execute(insert_q % (ratings_table, user_id, movie_id, rating))
    
    count_q = 'select count(*) from %s' % ratings_table
    results = []
    cursor.execute(count_q, results)
    results = cursor.fetchone()
    count = results[0]
    
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%'")
    partition_count = len(cursor.fetchall())
    step = 5.0 / partition_count;
    min_rate, max_rate = 0, step
    for i in range(partition_count):
        range_name = 'range_part%d' % i
        if i == 0:
            if rating >= min_rate and rating <= max_rate:
                
                cursor.execute(insert_q % (range_name, user_id, movie_id, rating))
        else:
            if rating > min_rate and rating <= max_rate:
                cursor.execute(insert_q % (range_name, user_id, movie_id, rating))   
        min_rate += step
        max_rate += step
    connector.commit()
    cursor.close()