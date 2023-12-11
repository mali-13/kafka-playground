import faust

#
# Create the faust app with a name and broker
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#application-parameters
#
app = faust.App("helofaust", broker="localhost:9092", )

#
# Connect Faust to com.udacity.streams.purchases
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-topic-create-a-topic-description
#
topic = app.topic("helo.mali.asyncvssync.purchase")


#
# Provide an app agent to execute this function on topic event retrieval
#       See: https://faust.readthedocs.io/en/latest/userguide/application.html#app-agent-define-a-new-stream-processor
#
@app.agent(topic)
async def purchase_event(purchases):
    # Define the async for loop that iterates over purchase events
    #       See: https://faust.readthedocs.io/en/latest/userguide/agents.html#the-stream
    async for purchase in purchases:
        print(purchase)


if __name__ == "__main__":
    app.main()
