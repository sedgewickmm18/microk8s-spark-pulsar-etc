import names
import faust

class Greeting(faust.Record):
    from_name: str
    to_name: str

app = faust.App(
    'hello-app',
    broker='kafka://10.152.183.177:9092',
    value_serializer='raw',
)

greetings_topic = app.topic('greetings', value_type=Greeting)
greeted = app.Table('greeted', default=int)

@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        greeted[greeting] += 1
        print(greeting, greeted[greeting])



