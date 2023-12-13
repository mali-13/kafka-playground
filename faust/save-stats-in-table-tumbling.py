from dataclasses import dataclass
from datetime import timedelta

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("helo.faust.table.tumbling", broker="kafka://localhost:9092")
clickevents_topic = app.topic("helo.mali.stream.clicks", value_type=ClickEvent)

#
# Define a tumbling window of 10 seconds
#       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#how-to
#
uri_summary_table = app.Table("uri_summary", default=int).tumbling(
    timedelta(10),
    expires=timedelta(minutes=1)
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents.group_by(ClickEvent.uri):
        uri_summary_table[ce.uri] += ce.number
        #
        # Play with printing value by: now(), current(), value()
        #       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#how-to
        #
        print(f"{ce.uri}: {uri_summary_table[ce.uri].current()}")


if __name__ == "__main__":
    app.main()
