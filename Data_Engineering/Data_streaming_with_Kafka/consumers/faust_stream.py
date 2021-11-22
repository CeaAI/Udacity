"""Defines trends calculations for stations"""
import logging

import faust



logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format

class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station, partitions=1)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

table = app.Table (
   "transformed_stations",
   partitions=8,
   changelog_topic=out_topic
)

@app.agent(topic)
async def transform_data(stations):
    async for station in stations.group_by(Station.station_name):
        index= None
        try:
            index =  [station.red, station.blue, station.green, ""].index(True) 
        except ValueError:
            index = 3
        line = ["red", "blue", "green"][index] 
        table[station.station_name] = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = line,
        )  
        

if __name__ == "__main__":
    app.main()
