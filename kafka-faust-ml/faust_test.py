import names
import faust

class Greeting(faust.Record):
    from_name: str
    to_name: str

app = faust.App('send-app', broker='kafka://10.152.183.177:9092')
topic = app.topic('my-topic', value_type=Greeting)
print(names.get_full_name(gender='female'))

@app.agent(topic)
async def hello(greetings):
    print('hello', greetings)
    #async for greeting in greetings:
    #    print(f'Hello from {greeting.from_name} to {greeting.to_name}')

@app.timer(interval=1.0)
async def example_sender(app):
    name = names.get_full_name(gender='female')
    await hello.send(
        value=Greeting(from_name=name, to_name='you'),
    )

#if __name__ == '__main__':
#    app.main()
