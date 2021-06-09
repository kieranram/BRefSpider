# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from shotchart.items import *
import re
import sqlite3

class ShotProcessor:

    def __init__(self, pipe):
        self.pipeline = pipe
        self.insert_statement = ('INSERT INTO shots (game_id, quarter, time_remaining, home_score, away_score, player_id, make, player_home, x_loc, y_loc, distance, points) '
                                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);')
        self.rows = []

    def process_shot(self, shot):
        game = shot['game']
        quarter = int(shot['quarter'])
        m, s = re.match('(\d+):([\d.]+)', shot['remaining']).groups()
        remaining = 60 * float(m) + float(s)
        score = shot['score']
        team, opp = re.match('(\d+)-(\d+)', score).groups()
        make = shot['make'] == 'make'

        if shot['home']:
            home = int(team)
            away = int(opp)
        else:
            home = int(opp)
            away = int(team)

        player = shot['player_id']
        x = int(shot['x'])
        y = int(shot['y'])
        dist = int(shot['distance'])
        pts = int(shot['points'])

        self.rows.append((game, quarter, remaining, home, away, player, make, shot["home"], x, y, dist, pts))

class PlayerGameProcessor:

    def __init__(self, pipe):
        self.pipeline = pipe
        self.insert_statement = ('INSERT INTO player_games (game_id, player_id, starter, home, mp, fg, fga, fg3, fg3a, ft, fta, '
                                'orb, drb, trb, ast, stl, blk, tov, pf, pts, plus_minus, fg_pct, fg3_pct, ft_pct) '
                                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);')
        self.rows = []

    def process_game(self, pg):
        game = pg['game_id']
        player = pg['player_id']
        starter = pg['starter']
        home = pg['home']

        mins, secs = pg['mp'].split(':')
        time = 60 * int(mins) + int(secs)

        ints = ['fg', 'fga', 'fg3', 'fg3a', 'ft', 'fta', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus']
        floats = ['fg_pct', 'fg3_pct', 'ft_pct']

        new_row = [game, player, starter, home, time]
        for i in ints:
            new_row.append(pg[i])
        for f in floats:
            new_row.append(pg[f])

        self.rows.append(new_row)
        
class PlayProcessor:

    def __init__(self, pipe):
        self.pipeline = pipe
        self.insert_statement = ('INSERT INTO pbp_raw (game_id, quarter, time_remaining, away_play, away_pts, '
                                'score, home_pts, home_play) ' 
                                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)')

        self.rows = []

    def process_play(self, play):
        new_row = (play['game_id'], play['quarter'], play['time'], play['away'], play['away_pts'], play['score'], 
                    play['home_pts'], play['home'])
        self.rows.append(new_row)

class GameProcessor:

    def __init__(self, pipe):
        self.pipeline = pipe
        self.insert_statement = ('INSERT INTO games (game_id, home, away, date, home_score, away_score) '
                                    'VALUES (?, ?, ?, ?, ?, ?)')

        self.rows = []

    def process_game(self, game):
        new_row = (game['game_id'], game['home'], game['away'], game['date'].strftime('%D'), int(game['home_score']), int(game['away_score']))
        self.rows.append(new_row)

class DBConn:

    def __init__(self, pipe):
        self.pipeline = pipe
        self.conn = sqlite3.connect('shot.db')

    def create_games(self):
        drop  = 'DROP TABLE IF EXISTS games'
        sql = '''CREATE TABLE games(
            game_id TEXT,
            home TEXT, 
            away TEXT, 
            date TEXT, 
            home_score INT, 
            away_score INT
        )'''

        cursor = self.conn.cursor()
        cursor.execute(drop)
        cursor.execute(sql)

    def insert_games(self):
        games = self.pipeline.game_processor.rows
        sql = self.pipeline.game_processor.insert_statement

        cursor = self.conn.cursor()
        cursor.executemany(sql, games)

    def create_subs(self):
        drop = 'DROP TABLE IF EXISTS subs'
        sql = '''CREATE TABLE subs (
            game_id TEXT,
            home INT, 
            quarter INT, 
            time REAL, 
            entering TEXT, 
            exiting TEXT
        )'''

        cursor = self.conn.cursor()
        cursor.execute(drop)
        cursor.execute(sql)

    def insert_subs(self):
        subs = self.pipeline.sub_processor.rows
        sql = self.pipeline.sub_processor.insert_statement

        cursor = self.conn.cursor()
        cursor.executemany(sql, subs)

    def create_shots(self):
        drop = 'DROP TABLE IF EXISTS shots'
        sql = '''CREATE TABLE shots (
            game_id TEXT,
            quarter INT, 
            time_remaining REAL, 
            home_score INT, 
            away_score INT, 
            player_id TEXT, 
            make INT,
            player_home INT,
            x_loc INT, 
            y_loc INT, 
            distance INT, 
            points INT
        )'''

        cursor = self.conn.cursor()
        cursor.execute(drop)
        cursor.execute(sql)

    def insert_shots(self):
        shots = self.pipeline.shot_processor.rows
        sql = self.pipeline.shot_processor.insert_statement

        cursor = self.conn.cursor()
        cursor.executemany(sql, shots)

    def create_player_games(self):
        drop = 'DROP TABLE IF EXISTS player_games'
        sql = '''CREATE TABLE player_games(
            game_id TEXT,
            player_id TEXT,
            starter INT,
            home INT, 
            mp INT, 
            fg INT,
            fga INT, 
            fg_pct REAL, 
            fg3 INT,
            fg3a INT,
            fg3_pct REAL,
            ft INT, 
            fta INT, 
            ft_pct REAL, 
            orb INT,
            drb INT,
            trb INT, 
            ast INT, 
            stl INT,
            blk INT, 
            tov INT, 
            pf INT, 
            pts INT,
            plus_minus INT
        )'''

        cursor = self.conn.cursor()
        cursor.execute(drop)
        cursor.execute(sql)

    def insert_player_games(self):
        games = self.pipeline.pg_processor.rows
        sql = self.pipeline.pg_processor.insert_statement
        
        cursor = self.conn.cursor()

        cursor.executemany(sql, games)

    def create_pbp(self):
        drop = 'DROP TABLE IF EXISTS pbp_raw'
        sql = '''CREATE TABLE pbp_raw (
            game_id TEXT, 
            quarter TEXT, 
            time_remaining TEXT, 
            away_play TEXT, 
            away_pts TEXT, 
            score TEXT, 
            home_pts TEXT,
            home_play TEXT 
        )'''

        cursor = self.conn.cursor()
        cursor.execute(drop)
        cursor.execute(sql)

    def insert_raw_pbp(self):
        plays = self.pipeline.pbp_processor.rows
        sql = self.pipeline.pbp_processor.insert_statement

        cursor = self.conn.cursor()
        cursor.executemany(sql, plays)

    def commit_and_close(self):
        self.conn.commit()
        self.conn.close()

class ShotchartPipeline:

    def open_spider(self, spider):
        self.shot_processor = ShotProcessor(self)
        self.pbp_processor = PlayProcessor(self)
        self.pg_processor = PlayerGameProcessor(self)
        self.game_processor = GameProcessor(self)
        self.db_conn = DBConn(self)

        self.db_conn.create_player_games()
        self.db_conn.create_shots()
        self.db_conn.create_pbp()
        self.db_conn.create_games()
        
    def process_item(self, item, spider):
        if isinstance(item, Shot):
            self.shot_processor.process_shot(item)
        if isinstance(item, PBP):
            self.pbp_processor.process_play(item)
        elif isinstance(item, PlayerGame):
            self.pg_processor.process_game(item)
        elif isinstance(item, Game):
            self.game_processor.process_game(item)

    def close_spider(self, spider):
        self.db_conn.insert_shots()
        self.db_conn.insert_player_games()

        print('\n\n', len(self.pbp_processor.rows), '\n\n')
        self.db_conn.insert_raw_pbp()
        self.db_conn.insert_games()
        self.db_conn.commit_and_close()
