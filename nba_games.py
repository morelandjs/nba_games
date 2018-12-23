#!/usr/bin/env python3

from datetime import datetime
from pathlib import Path
import re
import time

import bs4
import pandas as pd
import requests
from xdg import XDG_DATA_HOME


cachedir = Path(XDG_DATA_HOME, 'nba')
cachedir.mkdir(parents=True, exist_ok=True)
cachefile = cachedir / 'games.pkl'


def pullTable(url, tableID, header=True):
    """
    Pulls a table (indicated by tableID) from the specified url.

    """
    res = requests.get(url)
    comm = re.compile("<!--|-->")
    soup = bs4.BeautifulSoup(comm.sub("", res.text), 'lxml')
    tables = soup.findAll('table', id=tableID)
    data_rows = tables[0].findAll('tr')

    game_data = [
        [td.getText() for td in data_rows[i].findAll(['th','td'])]
        for i in range(len(data_rows))
    ]

    data = pd.DataFrame(game_data)

    if header == True:
        data_header = tables[0].findAll('thead')
        data_header = data_header[0].findAll("tr")
        data_header = data_header[0].findAll("th")

        header = []
        for i in range(len(data.columns)):
            header.append(data_header[i].getText())

        data.columns = header
        data = data.loc[data[header[0]] != header[0]]

    data = data.reset_index(drop=True)

    return data


def games_gen(season_min, season_max):
    """
    Yield games for each month in the specified range

    """
    baseurl = 'https://www.basketball-reference.com/'

    nba_season = [
        'october',
        'november',
        'december',
        'january',
        'february',
        'march',
        'april',
        'may',
        'june',
    ]

    for season in range(season_min, season_max + 1):
        print(f'[INFO][NBA] updating {season} season')

        for month in nba_season:
            url = baseurl + f"leagues/NBA_{season}_games-{month}.html"
            try:
                table = pullTable(url, "schedule")
            except IndexError:
                pass
            else:
                yield table

            time.sleep(1)


def pull_games(season_min, season_max):
    """
    Pandas dataframe for all NBA games in the specified season range

    """
    table = pd.concat([games for games in games_gen(season_min, season_max)])

    drop_cols = [6, 7, 8, 9]
    table.drop(table.columns[drop_cols], axis=1, inplace=True)

    table.columns = [
        'date',
        'time',
        'away_team',
        'away_points',
        'home_team',
        'home_points',
    ]

    for loc in ['home', 'away']:
        city = f'{loc}_city'
        team = f'{loc}_team'

        # every name follows the pattern: city [city] team
        # except for the Portland Trail Blazers
        table[city], table[team] = table[team].str.rsplit(' ', 1).str
        table[city] = table[city].str.replace('Portland Trail', 'Portland')

    table.replace('', float('nan'), inplace=True)
    table.dropna(inplace=True)

    return table


def update_games(start=2018, stop=2018, rebuild=False, **kwargs):
    """
    Load games from the cache if available, otherwise
    download the games and cache a new instance

    """
    games = pull_games(start, stop)

    if not rebuild and cachefile.exists():
        cached_games = pd.read_pickle(cachefile)
        games = pd.concat(
            [cached_games, games]
        ).drop_duplicates().reset_index(drop=True)

    games['date'] = pd.to_datetime(games['date'])
    games.sort_values('date', inplace=True)
    games.drop_duplicates(inplace=True)
    print(games)

    games.to_pickle(cachefile)


if __name__ == '__main__':
    import argparse

    current_season = datetime.now().year + 1

    parser = argparse.ArgumentParser(
            description='pull NBA game scores from basketball-reference.com',
            argument_default=argparse.SUPPRESS
        )
    parser.add_argument(
            '--start', type=int, dest='start', default=current_season,
            help='season to start pulling data'
        )
    parser.add_argument(
            '--stop', type=int, dest='stop', default=current_season,
            help='season to end pulling data'
        )
    parser.add_argument(
            '--rebuild', action='store_true',
            help='update game data even if cached'
        )

    args = parser.parse_args()
    kwargs = vars(args)

    update_games(**kwargs)
else:
    games = pd.read_pickle(cachefile)
