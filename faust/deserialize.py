import json
from dataclasses import asdict, dataclass

import faust


#
# Define a ClickEvent Record Class with an email (str), timestamp (str), uri(str),
#       and number (int)
#
#       See: https://docs.python.org/3/library/dataclasses.html
#       See: https://faust.readthedocs.io/en/latest/userguide/models.html#model-types
#
@dataclass
class ClickEvent(faust.Record):
    email: str = ""
    timestamp: str = ""
    uri: str = ""
    number: int = 0


app = faust.App("faust-deserialize", broker="kafka://localhost:9092")

#
# Provide the key (uri) and value type to the clickevent
#
clickevents_topic = app.topic(
    "helo.mali.stream.clicks",
    value_type=ClickEvent,
)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(json.dumps(asdict(ce), indent=2))


if __name__ == "__main__":
    app.main()
