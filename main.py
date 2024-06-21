import discord
import json

# Create the client connection
intents = discord.Intents()
intents.emojis_and_stickers = True
intents.guilds = True
intents.members = True
intents.message_content = True
intents.messages = True
intents.reactions = True
intents.typing = True
intents.webhooks = True
client = discord.Client(intents=intents)
credentials = json.load(open("credentials.json"))

is_ready = False  # whether the bot is ready and doesn't need to be readied again

@client.event
async def on_ready():
    global is_ready
    if is_ready:
        return

    print(f"{client.user} is connected to the following servers:\n")
    for server in client.guilds:
        print(f"{server.name}(id: {server.id})")

    is_ready = True


@client.event
async def on_message(message: discord.Message):
    print(message.content)


client.run(credentials["app_token"], reconnect=True)
