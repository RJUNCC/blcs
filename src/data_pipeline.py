import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import os
import seaborn as sns 
import matplotlib.pyplot as plt
import re
from scipy.stats import zscore
from prefect import flow, task
from prefect.server.schemas.schedules import CronSchedule
import subprocess
import discord
from discord import Intents, Client, Message
from typing import Final
from discord.ext import commands
from responses import get_response
import asyncio
from datetime import timedelta, datetime
from prefect.client.schemas.schedules import IntervalSchedule

load_dotenv("../.env")

@task(retries=2)
def step1():
    TOKEN = os.getenv("TOKEN")
    acutal_replay_url = "https://ballchasing.com/api/replays"
    replay_headers = {
        "Authorization": TOKEN
    }

    replay_params = {
        'group': "all-blcs-2-games-12x79igbdo",
    }

    replay_response = requests.get(acutal_replay_url, headers=replay_headers, params=replay_params)

    replay_df = pd.json_normalize(
        replay_response.json(),
        record_path=["list"]
    )

    REPLAY_DETAIL_URL = "https://ballchasing.com/api/replays/{replay_id}"

    replay_df.to_parquet("../data/parquet/replay_df.parquet")

    important_features = ["id", "rocket_league_id", "map_name", "duration", "overtime_seconds", "date", "created", "groups", "blue.name", "blue.goals", "orange.name", "orange.goals"]

    # replay_df = replay_df[important_features]

    # Step 1: Explode the 'blue.players' column and create a new DataFrame with the 'id' column
    blue_expanded = replay_df[important_features + ['blue.players']].explode('blue.players').reset_index(drop=True)

    # Step 2: Flatten the 'blue.players' column and create a separate DataFrame
    blue_expanded['blue_platform'] = blue_expanded['blue.players'].apply(lambda x: x['id']['platform'])
    blue_expanded['blue_player_id'] = blue_expanded['blue.players'].apply(lambda x: x['id']['id'])
    blue_expanded['blue_name'] = blue_expanded['blue.players'].apply(lambda x: x['name'])
    blue_expanded['blue_score'] = blue_expanded['blue.players'].apply(lambda x: x['score'])
    blue_expanded['blue_mvp'] = blue_expanded['blue.players'].apply(lambda x: x.get('mvp', False))  # MVP may be absent for some players

    # Step 3: Explode the 'groups' column and filter out the unwanted IDs
    blue_expanded = blue_expanded.explode('groups').reset_index(drop=True)
    blue_expanded['group_id'] = blue_expanded['groups'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
    blue_expanded = blue_expanded[blue_expanded['group_id'] != 'all-blcs-2-games-12x79igbdo']

    # Step 4: Drop the original 'groups' and 'blue.players' columns
    blue_df = blue_expanded.drop(columns=['groups', 'blue.players'])

    # Step 5: Explode the 'orange.players' column and create a new DataFrame with the 'id' column
    orange_expanded = replay_df[important_features + ['orange.players']].explode('orange.players').reset_index(drop=True)

    # Step 6: Flatten the 'orange.players' column and create a separate DataFrame
    orange_expanded['orange_platform'] = orange_expanded['orange.players'].apply(lambda x: x['id']['platform'])
    orange_expanded['orange_player_id'] = orange_expanded['orange.players'].apply(lambda x: x['id']['id'])
    orange_expanded['orange_name'] = orange_expanded['orange.players'].apply(lambda x: x['name'])
    orange_expanded['orange_score'] = orange_expanded['orange.players'].apply(lambda x: x['score'])
    orange_expanded['orange_mvp'] = orange_expanded['orange.players'].apply(lambda x: x.get('mvp', False))

    # Step 7: Explode the 'groups' column and filter out the unwanted IDs
    orange_expanded = orange_expanded.explode('groups').reset_index(drop=True)
    orange_expanded['group_id'] = orange_expanded['groups'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
    orange_expanded = orange_expanded[orange_expanded['group_id'] != 'all-blcs-2-games-12x79igbdo']

    # Step 8: Drop the original 'groups' and 'orange.players' columns
    orange_df = orange_expanded.drop(columns=['groups', 'orange.players'])

    blue_df.to_parquet("../data/parquet/blue_df.parquet")
    orange_df.to_parquet("../data/parquet/orange_df.parquet")

    replay_url = "https://ballchasing.com/api/groups/all-blcs-2-games-12x79igbdo"
    headers = {
        'Authorization': TOKEN
    }

    response = requests.get(replay_url, headers=headers)
    response

    replay_url = "https://ballchasing.com/api/groups/all-blcs-2-games-12x79igbdo"
    headers = {
        'Authorization': TOKEN
    }

    response = requests.get(replay_url, headers=headers)

    url = 'https://ballchasing.com/group/all-blcs-2-games-12x79igbdo/teams-stats'

    response_bs = requests.get(url)

    html_content = response_bs.text

    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the table in the HTML
    table = soup.find('table', {'class': 'stats-table'})

    # Ensure the table was found
    if table:
        # Extract headers
        headers = []
        for th in table.find('thead').find_all('th'):
            header = th.text.strip()
            headers.append(header)

        # Extract rows
        rows = []
        for tr in table.find('tbody').find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            row = [cell.text.strip() for cell in cells]
            rows.append(row)

        # Create a DataFrame
        df_bs = pd.DataFrame(rows, columns=headers)

    json = response.json()
    df = pd.json_normalize(
        json,
        record_path=["players"],
    )

    df.loc[15, "cumulative.games"] = df.loc[15, "cumulative.games"]-1
    df.loc[15, "cumulative.wins"] = df.loc[15, "cumulative.wins"]-1
    df.loc[15, "cumulative.win_percentage"] = df.loc[6, "cumulative.win_percentage"]
    df.loc[15, "cumulative.play_duration"] = df.loc[6, "cumulative.play_duration"]
    df.loc[15, "cumulative.core.shots_against"] = df.loc[6, "cumulative.core.shots_against"]
    df.loc[15, "game_average.core.goals_against"] = df.loc[6, "game_average.core.goals_against"]
    df.loc[15, "game_average.core.shots_against"] = df.loc[6, "game_average.core.shots_against"]

    grouped = df.groupby("team")

    team_stats = {}

    for team_name, group in grouped:
        unique_counts = group.nunique()
        columns_with_single_val = unique_counts[unique_counts==1].index.tolist()

        team_stat_vals = group[columns_with_single_val].iloc[0]

        team_stats[team_name] = team_stat_vals

    team_stats_df = pd.DataFrame(team_stats).transpose()

    team_stats_df.reset_index(inplace=True)

    team_stats_df.rename(columns={"index":'team'}, inplace=True)

    team_stats_df = team_stats_df.iloc[:, :-2]

    team_stats_df.to_parquet("../data/parquet/team_stats_df.parquet")

    team_dicts = {
        "BECKYARDIGANS": "BECKYARDIGANS",
        "KILLER": "KILLER BS",
        "DIDDLERS":"DIDDLERS",
        "EXECUTIVE": "EXECUTIVE PROJEC",
        "MINORITIES":"MINORITIES",
        "PUSHIN":"PUSHIN PULLIS",
        "SCAVS":"SCAVS",
        "WONDER":"WONDER PETS"
    }

    df_bs["Team"] = df_bs["Team"].apply(lambda x: x.replace("\n", "")).str.extract(r'^(\S+)', expand=False)
    df_bs["Team"] = df_bs["Team"].map(team_dicts)

    df_bs["Win %"] = df_bs["Win %"].apply(lambda x: float(str(x).replace(" %", "")), 2)

    df_bs.loc[4, "Games"] = team_stats_df.loc[7, "cumulative.games"]
    df_bs.loc[4, "Win %"] = team_stats_df.loc[7, "cumulative.win_percentage"]
    df_bs.loc[4, "Score"] = team_stats_df.loc[7, "cumulative.games"]

    average_score = (df.loc[21, "cumulative.core.score"] + df.loc[18, "cumulative.core.score"] + df.loc[11, "cumulative.core.score"]) / df.loc[21, "cumulative.games"]
    df_bs.loc[4, "Score"] = average_score

    team_goals_df = df.groupby("team")["cumulative.core.goals"].sum().reset_index()
    team_goals_df

    teams_goals_against = dict(zip(df["team"], df["cumulative.core.goals_against"]))
    teams_goals = dict(zip(team_goals_df["team"], team_goals_df["cumulative.core.goals"]))

    df_bs["Goals Against"] = df_bs["Team"].map(teams_goals_against)
    df_bs["Goals For"] = df_bs["Team"].map(teams_goals)

    demos_inflicted_df = df.groupby("team")["cumulative.demo.inflicted"].sum().reset_index()
    demos_taken_df = df.groupby("team")["cumulative.demo.taken"].sum().reset_index()

    demos_inflicted = dict(zip(demos_inflicted_df["team"], demos_inflicted_df["cumulative.demo.inflicted"]))
    demos_taken = dict(zip(demos_taken_df["team"], demos_taken_df["cumulative.demo.taken"]))

    df_bs["Demos Inflicted"] = df_bs["Team"].map(demos_inflicted)
    df_bs["Demos Taken"] = df_bs["Team"].map(demos_taken)

    shots_for_df = df.groupby("team")["cumulative.core.shots"].sum().reset_index()
    shots_against_df = df.groupby('team')["cumulative.core.shots_against"].sum().reset_index()

    shots_for = dict(zip(shots_for_df["team"], shots_for_df["cumulative.core.shots"]))
    shots_against = dict(zip(df["team"], df["cumulative.core.shots_against"]))

    df_bs["Shots For"] = df_bs["Team"].map(shots_for)
    df_bs["Shots Against"] = df_bs["Team"].map(shots_against)

    df_bs = df_bs.drop(5, axis=0)

    df_bs["Games"] = df_bs["Games"].astype(int)

    df_bs.to_excel("../data/excel_files/team_stats.xlsx")
    df_bs[["Team", "Win %", "Games", "Goals Against", "Goals For", "Demos Inflicted", "Demos Taken", "Shots For", "Shots Against"]].to_parquet("../data/parquet/better_team_stats.parquet")

    team_stats_rankings = team_stats_df.sort_values(by="cumulative.win_percentage", ascending=False).reset_index(drop=True)

    os.makedirs("../data/excel_files", exist_ok=True)

    team_stats_rankings.to_excel("../data/excel_files/team_rankings_descending.xlsx")
    df.to_excel('../data/excel_files/players_data.xlsx')
    df_bs.to_excel("../data/excel_files/better_team_data.xlsx")

    # Function to highlight the row based on which team has more goals
    def highlight_winner(row):
        if row['blue_goals'] > row['orange_goals']:
            return ['background-color: lightblue'] * len(row)  # Blue team wins
        elif row['orange_goals'] > row['blue_goals']:
            return ['background-color: orange'] * len(row)  # Orange team wins
        else:
            return ['background-color: lightgreen'] * len(row)  # Tie
        
    def create_barplot(df, x_col, y_col, save_path=None):
        """
        Create a bar plot with a dark blue and light gold theme and a white background.

        Parameters:
        df (pd.DataFrame): The DataFrame containing the data.
        x_col (str): The column name for the x-axis.
        y_col (str): The column name for the y-axis.
        save_path (str, optional): The file path to save the plot. If None, the plot won't be saved.
        """

        # Set a clean white background and apply custom color palette
        sns.set_theme(style="whitegrid")
        custom_palette = ["#1f3d7a", "#f5c542"]  # Dark blue and light gold

        # Set font size and other rcParams for better appearance
        plt.rcParams.update({
            'axes.labelsize': 14,
            'xtick.labelsize': 12,
            'ytick.labelsize': 12,
            'axes.titlesize': 16,
            'figure.figsize': (15, 7),
            'axes.facecolor': '#ffffff',  # White background
            'axes.edgecolor': '#1f3d7a',  # Dark blue border
            'grid.color': '#dddddd',  # Light gray gridlines for subtlety
            'text.color': '#1f3d7a',  # Dark blue text
            'axes.labelcolor': '#1f3d7a',  # Dark blue labels
            'xtick.color': '#1f3d7a',  # X-ticks to dark blue
            'ytick.color': '#1f3d7a'   # Y-ticks to dark blue
        })

        # Create the bar plot
        plt.figure(figsize=(15, 7))
        sns.barplot(
            data=df,
            x=x_col,
            y=y_col,
            palette=custom_palette
        )

        # Remove the right and top spines for a clean look
        sns.despine(right=True, top=True)

        # Set the title and labels
        plt.title(f"{y_col} vs {x_col}", color="#1f3d7a")
        plt.xlabel(x_col)
        plt.ylabel(y_col)

        # Save the plot if a save path is provided
        if save_path:
            plt.savefig(save_path, bbox_inches="tight", dpi=300)

        # Show the plot
        # plt.show()

    create_barplot(df=df_bs, x_col="Team", y_col="Goals For", save_path="../images/goals_for_vs_team.png")
    create_barplot(df=df_bs, x_col="Team", y_col="Goals Against", save_path="../images/goals_against_vs_team.png")
    create_barplot(df=df_bs, x_col="Team", y_col="Demos Inflicted", save_path="../images/demos_inflicted_vs_team.png")
    create_barplot(df=df_bs, x_col="Team", y_col="Demos Taken", save_path="../images/demos_taken_vs_team.png")

    bs_df = pd.read_parquet("../data/parquet/better_team_stats.parquet")

    bs_df["Goal Diff"] = bs_df["Goals For"] - bs_df["Goals Against"]
    bs_df["Demo Diff"] = bs_df["Demos Inflicted"] - bs_df["Demos Taken"]
    bs_df["Shots Diff"] = bs_df["Shots For"] - bs_df["Shots Against"]

    team_stats_df = pd.read_parquet("../data/parquet/team_stats_df.parquet")

    team_wins = dict(zip(team_stats_df["team"], team_stats_df["cumulative.wins"]))

    bs_df["Wins"] = bs_df["Team"].map(team_wins)

    win_perc_weight  = 0.6
    goal_diff_weight = 0.22
    shot_diff_weight = 0.06
    demo_diff_weight = 0.02
    strength_of_schedule = 0.10

    blue_df = pd.read_parquet("../data/parquet/blue_df.parquet")
    orange_df = pd.read_parquet("../data/parquet/orange_df.parquet")
    replay_df = pd.read_parquet("../data/parquet/replay_df.parquet")

    team_opponents = {}

    for _, row in replay_df.iterrows():
        blue_team = row["blue.name"]
        orange_team = row["orange.name"]

        if blue_team not in team_opponents:
            team_opponents[blue_team] = []
        if orange_team not in team_opponents[blue_team]:
            team_opponents[blue_team].append(orange_team)

        if orange_team not in team_opponents:
            team_opponents[orange_team] = []
        if blue_team not in team_opponents[orange_team]:
            team_opponents[orange_team].append(blue_team)

    team_stats_dict = bs_df.set_index("Team").T.to_dict()
    team_opponents_stats = {}

    for team, opponents in team_opponents.items():
        total_wins = 0
        total_games = 0

        for opponent in opponents:
            opponent_stats = team_stats_dict.get(opponent, {"Wins": 0, "Games" : 0})
            total_wins += opponent_stats["Wins"]
            total_games += opponent_stats["Games"]

        team_opponents_stats[team] = {
            'Opponents': opponents,
            "Opponents Total Wins": int(total_wins),
            "Opponents Total Games": int(total_games),
            "Win %": round(float(total_wins / total_games), 2),
        }

    bs_df["Teams Played"] = bs_df["Team"].map(team_opponents_stats)

    teams_played_stats = pd.DataFrame(team_opponents_stats).T.reset_index().rename(columns={"index":"Team", "Win %": "Team Played Win %"})

    joint = bs_df.merge(teams_played_stats, on="Team")
    joint = joint.drop("Teams Played", axis=1)
    joint[["Opponents Total Wins", "Opponents Total Games"]] = joint[["Opponents Total Wins", "Opponents Total Games"]].astype(int)
    joint["Team Played Win %"] = joint["Team Played Win %"].astype(float)
    joint["Win % Zscore"] = zscore(joint["Win %"]) * win_perc_weight
    joint["Goal Diff Zscore"] = zscore(joint["Goal Diff"]) * goal_diff_weight
    joint["Demo Diff Zscore"] = zscore(joint["Demo Diff"]) * shot_diff_weight
    joint["Shots Diff Zscore"] = zscore(joint["Shots Diff"]) * demo_diff_weight
    joint["Team Played Win % Zscore"] = zscore(joint["Team Played Win %"]) * strength_of_schedule
    joint["Win % Rank"] = joint["Win %"].rank(ascending=False, method="min").astype(int)
    joint["Goals Diff Rank"] = joint["Goal Diff"].rank(ascending=False, method="min").astype(int)
    joint["Shots Diff Rank"] = joint["Demo Diff"].rank(ascending=False, method="min").astype(int)
    joint["Demo Diff Rank"] = joint["Shots Diff"].rank(ascending=False, method="min").astype(int)
    joint["Team Played Win % Rank"] = joint["Team Played Win %"].rank(ascending=False, method="min").astype(int)
    joint["EPI Score"] = joint[["Win % Zscore", "Goal Diff Zscore", "Demo Diff Zscore", "Shots Diff Zscore", "Team Played Win % Zscore"]].sum(axis=1)
    joint["EPI Rank"] = joint["EPI Score"].rank(ascending=False, method="min").astype(int)
    joint = joint.sort_values(by="EPI Rank", ascending=True)

    final = joint[["Team", "EPI Score", "Games", "Win %", "Goals For", "Goals Against", "Goal Diff", "Shots For", "Shots Against", "Shots Diff", "Team Played Win %"]]
    final = final.rename(columns={"Team Played Win %":"Strength of Schedule"})
    final["EPI Score"] = round(final["EPI Score"]*50, 2)
    final["Win %"] = final["Win %"].astype(int).apply(lambda x: str(f"{x}%") if x > 0 else str(x))

    final.set_index("Team").to_excel("../data/excel_files/final.xlsx")

    return


# Async function for running the Discord bot
async def run_discord_bot_async():
    TOKEN: Final[str] = os.getenv('DISCORD_TOKEN')
    CHANNEL_ID = 1259392687778431018

    intents: Intents = Intents.default()
    intents.message_content = True
    client: Client = Client(intents=intents)

    async def remove_previous_image_message(channel):
        async for message in channel.history(limit=100):
            if message.author == client.user:
                try:
                    await message.delete()
                    return
                except Exception as e:
                    print(f"Failed to delete the message: {e}")

    async def send_new_image(channel, file_path):
        with open(file_path, 'rb') as f:
            await channel.send(file=discord.File(f))

    @client.event
    async def on_ready():
        channel = client.get_channel(CHANNEL_ID)
        if channel:
            await remove_previous_image_message(channel)
            await send_new_image(channel, "../images/goals_for_vs_team.png")
            await send_new_image(channel, "../images/goals_against_vs_team.png")
            await send_new_image(channel, "../images/demos_inflicted_vs_team.png")
            await send_new_image(channel, "../images/demos_taken_vs_team.png")

    await client.start(TOKEN)

# Task to wrap the async bot logic
@task
async def start_discord_bot():
    await run_discord_bot_async()

# Flow definition
@flow(log_prints=True)
def run_prefect():
    step1()
    start_discord_bot()

    
run_prefect()