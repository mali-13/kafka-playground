import random
from dataclasses import dataclass

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int
    score: int = 0


#
# Define a scoring function for incoming ClickEvents.
#       It doens't matter _how_ you score the incoming records, just perform
#       some modification of the `ClickEvent.score` field and return the value
#
def add_score(click):
    click.score = random.random()
    return click


app = faust.App("faust.app.processors", broker="kafka://localhost:9092")
clickevents_topic = app.topic("helo.mali.stream.clicks", value_type=ClickEvent)
scored_topic = app.topic(
    "helo.mali.stream.scored.clicks",
    value_type=ClickEvent,
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # Add the `add_score` processor to the incoming clickevents
    #       See: https://faust.readthedocs.io/en/latest/reference/faust.streams.html?highlight=add_processor#faust.streams.Stream.add_processor
    #
    clickevents.add_processor(add_score)

    async for ce in clickevents:
        await scored_topic.send(value=ce)


if __name__ == "__main__":
    app.main()
