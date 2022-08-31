#!/bin/python3

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import csv
import logging

# Query template for todo queries
insertVertexTemplate = "INSERT VERTEX player(name, age) VALUES \"%s\":(\"%s\", %s)"
# Query template for undo queries that delete the inserted vertices
rollbackTemplate = "DELETE VERTEX \"%s\""
# Retry each query execution for the following times
retryTimes = 10

logging.basicConfig(level=logging.INFO)

def readCSV(filename):
  data = []
  with open(filename, mode = 'r') as f:
    csvFile = csv.reader(f, delimiter = ',')
    for line in csvFile:
      data.append([line[0], line[1]])
  return data

# Generate two lists of queries: todo and undo.
# (1) Append each query waiting to be executed into the todo list.
# (2) For each such query, prepare an counterpart that undos its changes in the database.
#     Append this undo counterpart into the undo list.
# (3) The undo counterpart of the i-th todo query is the i-th query in the undo list.
def genBatch(data):
  todo = []
  undo = []
  for player in data:
    insert = insertVertexTemplate % (player[0], player[0], player[1])
    rollback = rollbackTemplate % (player[0])
    todo.append(insert)
    undo.append(rollback)
  # Ingest some errors for testing:
  # undo[0] = insertVertexTemplate
  # todo[4] = insertVertexTemplate
  return todo, undo

# Execute the given query multiple times.
# Return a None as the result, if all attempted executions failed.
# The error code and message of the last attempt is logged.
def exeQueryWithRetries(query, session):
  result = session.execute(query)
  if result.is_succeeded():
    return result
  i = 0
  while i < retryTimes:
    logging.info("Executing %s." % query)
    result = session.execute(query)
    if not result.is_succeeded():
      i = i + 1
    else:
      break
  if i == retryTimes:
    logging.error("Error %s (%d), while executing query %s." % (result.error_msg(), result.error_code(), query))
    return None
  elif result.is_succeeded():
    return result
  else:
    raise Exception("Failed at trying to execute query %s." % (query))

# Execute queries in a batch.
def exeBatch(space, batch, session):
  counter = 0
  session.execute("use " + space)
  for query in batch:
    result = exeQueryWithRetries(query, session)
    if result == None:
      return counter
    else:
      counter = counter + 1
  return counter

# Rollback the batch execution by executing the undo counterparts of all successfully executed queries.
def rollback(undo, progress, session):
  count = 0
  while (count < progress):
    result = exeQueryWithRetries(undo[count], session)
    if result == None:
      logging.error("Rollback failed while executing the %d-th undo statement \"%s\"." % (count, undo[count]))
      return False
    else:
      count = count + 1
  if count == progress:
    return True
  else:
    return False

# Main entry
if __name__ == "__main__":
  # Load data to fill query templates from a csv
  csvfile = "players.csv"
  data = readCSV(csvfile)
  todo, undo = genBatch(data)
  config = Config()
  config.max_connection_pool_size = 10
  conn = ConnectionPool()
  # IP and port of the nebula-graphd service
  addr = "127.0.0.1"
  port = 18588
  # The default login
  usr = "root"
  pwd = "nebula"
  # Initialize the connection pool
  status = conn.init([(addr, port)], config)
  if status:
    with conn.session_context(usr, pwd) as session:
      progress = exeBatch('nba', todo, session)
      if (progress != len(todo)):
        if rollback(undo, progress, session) == False:
          logging.error("Rollback failed.")
        else:
          logging.warning("Bacth insert failed, with all inserted vertices rolled back.")
      else:
        logging.info("Batch insert succeeded.")
  else:
    logging.error("Connection pool initialization failed.")
  conn.close()
