# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field 


class Shot(Item):
    
    #location info
    x = Field()
    y = Field()
    #game id to join on
    game = Field()
    #time info
    quarter = Field()
    remaining = Field()
    #shooter
    player_id = Field()
    home = Field()
    #misc, can be used to check other logic? 
    make = Field()
    distance = Field()
    score = Field()
    points = Field()

class PBP(Item):

    quarter = Field()
    time = Field()
    away = Field()
    away_pts = Field()
    score = Field()
    home_pts = Field()
    home = Field()
    game_id = Field()

class Game(Item):

    game_id = Field()

    #known before game starts
    home = Field()
    away = Field()
    date = Field()

    #outcome fields
    home_score = Field()
    away_score = Field()

class PlayerGame(Item):

    game_id = Field()

    player_id = Field()
    starter = Field()
    home = Field()

    mp = Field()
    fg = Field()
    fga = Field()
    fg_pct = Field()
    fg3 = Field()
    fg3a = Field()
    fg3_pct = Field()
    ft = Field()
    fta = Field()
    ft_pct = Field()
    orb = Field()
    drb = Field()
    trb = Field()
    ast = Field()
    stl = Field()
    blk = Field()
    tov = Field()
    pf = Field()
    pts = Field()
    plus_minus = Field()