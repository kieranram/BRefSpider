import scrapy
from ..items import *
import pandas as pd

import re

class ShotSpider(scrapy.Spider):
    name = 'shotspider'

    base_url = 'https://www.basketball-reference.com'

    def start_requests(self):
        yield scrapy.Request(self.base_url + '/leagues', self.get_by_year)

    def get_by_year(self, response):
        years = response.css("#stats > tr >th > a::attr(href)") # > tr > th::attr(href)
        for year in years: 
            url = year.extract()[:-5] + '_games.html'
            yield scrapy.Request(self.base_url + url, self.parse_year)

            break
    
    def parse_year(self, response): 
        months = response.css('.filter > div > a::attr(href)')
        for month in months: 
            m = month.get()
            yield scrapy.Request(self.base_url + m, self.parse_month)

    def parse_month(self, response):
        rows = response.css('#schedule > tbody> tr')

        for row in rows:
            if 'class' in row.attrib:
                continue
            if row.css('td[data-stat="visitor_pts"]::text').get() is None:
                continue
            date = row.css('th[data-stat="date_game"] > a::text')
            time = row.css('td[data-stat="game_start_time"]::text')
            dt = pd.to_datetime(date.get() + ' ' + time.get())

            away = row.css('td[data-stat="visitor_team_name"]').attrib['csk'].split('.')[0]
            home = row.css('td[data-stat="home_team_name"]').attrib['csk'].split('.')[0]

            away_score = row.css('td[data-stat="visitor_pts"]::text').get().split('.')[0]
            home_score = row.css('td[data-stat="home_pts"]::text').get().split('.')[0]
            

            box = row.css('td.center[data-stat="box_score_text"] > a::attr(href)')
        
            game_id = box.get().split('/')[-1]

            yield Game(game_id = game_id, home = home, away = away, date = dt, home_score = home_score, away_score = away_score)

            shot_url = '/boxscores/shot-chart/' + game_id
            box_url = '/boxscores/' + game_id
            pbp_url = '/boxscores/pbp/' + game_id
            pm_url = '/boxscores/plus-minus/' + game_id
            
            yield scrapy.Request(self.base_url + box_url, self.parse_box)
            yield scrapy.Request(self.base_url + pbp_url, self.parse_pbp)
            yield scrapy.Request(self.base_url + shot_url, self.parse_chart)

    def parse_box(self, response):
        game = response.url.split('/')[-1][:-5]
        boxes = response.css('table[id^="box-"][id$="-game-basic"] > tbody')

        home_starters = []
        away_starters = []

        name_id_map = {}
        
        home = False
        for box in boxes: 
            players = box.css('tr')

            for i, player in enumerate(players):
                if i < 5:
                    starter = True
                elif i == 5:
                    continue
                else:
                    starter = False

                p_info = player.css('th')
                pid = p_info.attrib['data-append-csv']
                pname = p_info.css('::text').get()
                name_id_map[pname] = pid

                player_game = PlayerGame(game_id = game, player_id = pid, starter = starter, home = home)

                stats = player.css('td')
                if len(stats) == 1:
                    continue
                for stat in stats:
                    player_game[stat.attrib['data-stat']] = stat.css('::text').get()
                
                if starter:
                    if home:
                        home_starters.append(pid)
                    else:
                        away_starters.append(pid)
                yield player_game

            home = ~home

    def parse_pbp(self, response):

        game = response.url.split('/')[-1][:-5]

        pbp = response.css('#pbp')
        rows = pbp.css('tr')
        
        prev_rows = []

        for row in rows:
            if 'id' in row.attrib:
                id_ = row.attrib['id']
                if id_.startswith('q'):
                    q = int(id_[1]) - 1
                    for pr in prev_rows:
                        pr['quarter'] = q
                        yield pr
                    prev_rows = []
                else:
                    print(f'\n\n{id_}\n\n')
            datas = row.css('td')
            if len(datas) != 6:
                continue
            time = datas[0].get()
            away = datas[1].get()
            away_pts = datas[2].get()
            score = datas[3].get()
            home_pts = datas[4].get()
            home = datas[5].get()

            play = PBP(time = time, away = away, away_pts = away_pts, score = score, 
                        home_pts = home_pts, home = home, game_id = game)

            prev_rows.append(play)

        q += 1
        for row in prev_rows:
            row['quarter'] = q
            yield row


    def parse_pm(self, response):
        game = response.url.split('/')[-1][:-5]

        players = response.css('.plusminus > div > div > div.player')
        pms = response.css('.plusminus > div > div > div.player-plusminus')

        mapper = response.meta['ids']

        for player, pm in zip(players, pms):
            # Need to get player id at some point
            divs = pm.css('div > div')
            pname = player.css('::text').get()

            left = 0
            if pname in mapper:
                p_id = mapper[pname]
                print(p_id, pname)
            else:
                print('No ID: ', pname)
            for div in divs:
                if 'class' not in div.attrib:
                    div_type = 'offcourt'
                else:
                    div_type = 'oncourt'
                span = div.attrib['style'].split(':')[-1][:-3]
                span = int(span) + 1
                left += span
                
            print()

    def parse_chart(self, response):
        game = response.url.split('/')[-1][:-5]

        areas = response.css('.shot-area')
        home = False
        for area in areas: 
            shots = area.css('.shot-area > .tooltip')
            for single in shots: 
                classes = single.attrib['class']
                quarter, player, acc = re.findall('tooltip q-(\d) p-(\w+) (make|miss)', classes)[0]

                loc = single.attrib['style']
                top, left = re.findall('top:(-?\d+)px;left:(-?\d+)', loc)[0]

                text = single.attrib['tip']

                time, info, score = text.split('<br>')
                time = re.findall('[\d\:\.]+', time)[1]
                points = re.findall('(\d)-pointer', info)[0]
                dist = re.findall('(\d+) ft', info)[0]

                score = re.findall('\d+-\d+', score)[0]

                shot = Shot(x = left, y = top, 
                        game = game, quarter = quarter, remaining = time, 
                        home = home, player_id = player, make = acc,
                        distance = dist, score = score, points = points)

                yield shot
            home = ~home