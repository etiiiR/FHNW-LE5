from pyArango.connection import *

conn = Connection(arangoURL="https://arangodb.roulet.dev", username="root", password="9icKdiH@toH933Cdf*7ixdJADM&")
db = conn["GDB"]


def import_flight_routes():
    with open("routes.csv") as routes:
        count = 0
        lines = routes.readlines()
        lines.pop(0)
        for line in lines:
            count += 1
            values = line.strip().split(",")

            # Get Airport Keys
            source_airport = values[2].replace('"', '')
            destination_airport = values[4].replace('"', '')

            aql = f"""
                for airport in Airports_LE5 filter airport.IATA == "{source_airport}" return airport._key
            """

            print(aql)
            source_airport_key = db.AQLQuery(aql, rawResults=True)

            aql = f"""
                for airport in Airports_LE5 filter airport.IATA == "{destination_airport}" return airport._key
            """

            print(aql)
            destination_airport_key = db.AQLQuery(aql, rawResults=True)

            if len(source_airport_key) == 1 & len(destination_airport_key) == 1:
                source_airport_key = source_airport_key[0]
                destination_airport_key = destination_airport_key[0]

                # Insert Graph for Flight Route
                airline_id = values[1]
                codeshare = values[6]
                stops = values[7]
                equipment = values[8]

                aql = f"""
                    insert {{_from: "Airports_LE5/{source_airport_key}", _to: "Airports_LE5/{destination_airport_key}",
                      "airline_id": "{airline_id}",
                      "codeshare": "{codeshare}",
                      "stops": {stops},
                      "equipment": "{equipment}",
                    }} into Flight_Connections_LE5
                """

                print(aql)
                db.AQLQuery(aql)
            else:
                print(f"Route Import failed for row {count}")


import_flight_routes()
