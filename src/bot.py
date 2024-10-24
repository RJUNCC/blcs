import os
import discord
from discord import Intents, Client, Message
from typing import Final
from discord.ext import commands
from dotenv import load_dotenv
from responses import get_response
import asyncio

load_dotenv("../.env")
# DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
TOKEN: Final[str] = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = 1259392687778431018

# STEP 1: BOT SETUP
intents: Intents = Intents.default()
intents.message_content = True  # NOQA
client: Client = Client(intents=intents)

last_image_message = None


# STEP 2: MESSAGE FUNCTIONALITY
async def send_message(message: Message, user_message: str) -> None:
    if not user_message:
        print('(Message was empty because intents were not enabled probably)')
        return

    if is_private := user_message[0] == '?':
        user_message = user_message[1:]

    try:
        response: str = get_response(user_message)
        await message.author.send(response) if is_private else await message.channel.send(response)
    except Exception as e:
        print(e)

# Function to remove the previous image message by fetching recent messages
async def remove_previous_image_message(channel):
    # Fetch the most recent 100 messages in the channel
    async for message in channel.history(limit=100):
        # Check if the message was sent by the bot
        if message.author == client.user:
            try:
                await message.delete()
                print("Previous message deleted.")
                return  # Once we've deleted the bot's message, we can stop
            except Exception as e:
                print(f"Failed to delete the message: {e}")
    print("No previous message found to delete.")

# Function to send a new image
async def send_new_image(channel, file_path):
    with open(file_path, 'rb') as f:
        await channel.send(file=discord.File(f))
        print("New image sent.")

# STEP 3: HANDLING THE STARTUP FOR OUR BOT
@client.event
async def on_ready() -> None:
    # print(f'{client.user} is now running!')
    # channel = client.get_channel(CHANNEL_ID)

    # if channel:
    #     # await channel.send("test message")
    #     # print(f"Message sent to {channel.name}")
    #     with open('../images/goals_for_vs_team.png', 'rb') as f:
    #         picture = discord.File(f)
    #         await channel.send(file=picture)
    #     print(f"Image sent to {channel.name}")
    # else:
    #     print("Couldn't find the channel")
    print(f'Logged in as {client.user}')
    channel = client.get_channel(CHANNEL_ID)

    if channel:
        # Example usage: send a new image and replace the previous one
        # await replace_last_image(channel, '../images/goals_for_vs_team.png')  # Replace with your image path
        # await replace_last_image(channel, '../images/goals_for_vs_team.png')
        await remove_previous_image_message(channel)
        await remove_previous_image_message(channel)
        await remove_previous_image_message(channel)
        await remove_previous_image_message(channel)

        await send_new_image(channel, "../images/goals_for_vs_team.png")
        await send_new_image(channel, "../images/goals_against_vs_team.png")
        await send_new_image(channel, "../images/demos_inflicted_vs_team.png")
        await send_new_image(channel, "../images/demos_taken_vs_team.png")


# STEP 4: HANDLING INCOMING MESSAGES
# @client.event
# async def on_message(message: Message) -> None:
#     if message.author == client.user:
#         return

#     username: str = str(message.author)
#     user_message: str = message.content
#     channel: str = str(message.channel)

#     print(f'[{channel}] {username}: "{user_message}"')
#     await send_message(message, user_message)


# STEP 5: MAIN ENTRY POINT
client.run(token=TOKEN)