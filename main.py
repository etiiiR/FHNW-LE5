from pyArango.connection import *
    

if __name__ == "__main__":
  conn = Connection(arangoURL="https://arangodb.roulet.dev", username="root", password="9icKdiH@toH933Cdf*7ixdJADM&")
  db = conn["GDB"]
  aql = "FOR x IN Airports_LE5 RETURN x.Name"
  queryResult = db.AQLQuery(aql, rawResults=True, batchSize=100)
  for key in queryResult:
    print(key)