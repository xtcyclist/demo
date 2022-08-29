#!/bin/python3

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import csv
import sys

insertVertexTemplate = "INSERT VERTEX player(name, age) VALUES \"%s\":(\"%s\", %s)"
rollbackTemplate = "DELETE VERTEX \"%s\""

def readCSV(filename):
  data = []
  with open(filename, mode = 'r') as f:
    csvFile = csv.reader(f, delimiter = ',')
    for line in csvFile:
      data.append([line[0], line[1]])
  print(data)
  return data

def genBatch(data):
  todo = []
  undo = []
  for player in data:
    insert = insertVertexTemplate % (player[0], player[0], player[1])
    rollback = rollbackTemplate % (player[0])
    todo.append(insert)
    undo.append(rollback)
  # undo[0] = insertVertexTemplate
  # todo[1] = insertVertexTemplate
  return todo, undo

def exeBatch(space, batch, session):
  counter = 0
  session.execute("use " + space)
  for query in batch:
    result = session.execute(query)
    if result.is_succeeded():
      counter = counter + 1
    else:
      return counter
  return counter

def rollback(undo, progress, session):
  count = 0
  while (count < progress):
    result = session.execute(undo[count])
    if result.is_succeeded():
      count = count + 1
    else:
      raise Exception("Rollback failed while executing the %d-th undo statement \"%s\"." % (count, undo[count]))
  if count == progress:
    return True
  else:
    return False

if __name__ == "__main__":
  csvfile = sys.argv[1]
  data = readCSV(csvfile)
  todo, undo = genBatch(data)
  config = Config()
  config.max_connection_pool_size = 10
  conn = ConnectionPool()
  status = conn.init([('127.0.0.1', 18588)], config)
  if status:
    with conn.session_context('root', 'nebula') as session:
      progress = exeBatch('nba', todo, session)
      if (progress != len(todo)):
        if rollback(undo, progress, session) == False:
          raise Exception("Rollback failed.")
        else:
          print ("Bacth insert failed, with all inserted vertices rolled back.")
      else:
        print ("Batch insert succeeded.")
  conn.close()
