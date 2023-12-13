from dataclasses import dataclass

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("helo.mali.faust.table", broker="kafka://localhost:9092")
clickevents_topic = app.topic("helo.mali.stream.clicks", value_type=ClickEvent)

#
# Define a uri summary table
#   See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
#
uri_summary_table = app.Table("page.visit.count", default=int)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # Group By URI
    #       See: https://faust.readthedocs.io/en/latest/userguide/streams.html#group-by-repartition-the-stream
    #
    async for ce in clickevents.group_by(ClickEvent.uri):
        #
        # Use the URI as key, and add the number for each click event. Print the updated
        #       entry for each key so you can see how the table is changing.
        #       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
        #
        uri_summary_table[ce.uri] += ce.number
        print(f"{ce.uri}: {uri_summary_table[ce.uri]}")


if __name__ == "__main__":
    app.main()
