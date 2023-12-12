from dataclasses import dataclass

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise4", broker="kafka://localhost:9092")

clickevents_topic = app.topic("helo.mali.stream.clicks", value_type=ClickEvent)

popular_uris_topic = app.topic(
    "helo.mali.stream.popular.uris.clicks",
    value_type=ClickEvent,
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # Filter clickevents to only those with a number higher than or
    #       equal to 100
    #       See: https://faust.readthedocs.io/en/latest/userguide/streams.html#filter-filter-values-to-omit-from-stream
    #
    async for click in clickevents.filter(lambda c: c.number >= 100):
        await popular_uris_topic.send(value=click)


if __name__ == "__main__":
    app.main()
