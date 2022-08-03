#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def GeneralQuery(filename, tablePrefix, countTableQuery, getDataQuery, outPrefix, cursor):
    cursor.execute(countTableQuery)
    partition_count = len(cursor.fetchall())
    
    with open(filename, 'a') as f:
        for i in range(partition_count):
            cursor.execute(getDataQuery.format(i))
            current_dataset = cursor.fetchall()
            for row in current_dataset:
                f.write('%s%d,' % (outPrefix, i))
                f.write(','.join(str(attribute) for attribute in row))
                f.write('\n')
                
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()
    prefixPair = {'rangeratingspart':'rangeratingspart', 'roundrobinratingspart':'roundrobinratingspart'}
    
    for prefix, outPrefix in prefixPair.items():
        countTableQuery = "SELECT * FROM information_schema.tables WHERE table_name LIKE '{}%'".format(prefix)
        getDataQuery = "SELECT * FROM %s{} WHERE rating >= %f and rating <= %f" % (prefix, ratingMinValue, ratingMaxValue)
        GeneralQuery('RangeQueryOut.txt', prefix, countTableQuery, getDataQuery, outPrefix, cursor)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()
    prefixPair = {'rangeratingspart':'rangeratingspart', 'roundrobinratingspart':'roundrobinratingspart'}
    
    for prefix, outPrefix in prefixPair.items():
        countTableQuery = "SELECT * FROM information_schema.tables WHERE table_name LIKE '{}%'".format(prefix)
        getDataQuery = "SELECT * FROM %s{} WHERE rating = %f" % (prefix, ratingValue)
        GeneralQuery('PointQueryOut.txt', prefix, countTableQuery, getDataQuery, outPrefix, cursor)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()