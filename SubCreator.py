import sqlite3
import pandas as pd
from tqdm import tqdm

tqdm.pandas()

def get_play(row):
    home = row['home_play']
    away = row['away_play']
    
    if home == '<td>\xa0</td>':
        return pd.Series({'home' : False, 'p_text' : away})
    elif away == '<td>\xa0</td>':
        return pd.Series({'home' : True, 'p_text' : home})

def get_sub_order(sub_inds, subs = pd.DataFrame()):
    '''expects a df of all a teams substitutions in a given quarter'''
    inds = sub_inds.index
    order = subs.loc[inds].sort_values('total_time', ascending = False)
    players = set(order['enters'].tolist() + order['leaves'].tolist())
    
    game = subs.loc[inds, 'game_id'].unique()[0]
    quarter = subs.loc[inds, 'quarter'].unique()[0]
    home = subs.loc[inds, 'home'].unique()[0]
    
    all_subs = pd.DataFrame()
    
    for player in players:
        entries = order.query('enters == @player')['total_time'].tolist()
        leaves = order.query('leaves == @player')['total_time'].tolist()
        
        if len(entries) > len(leaves):
            # add a row to leaves at end of quarter
            leaves.append(0)
        elif len(entries) < len(leaves):
            # add a row to entries at start of quarter
            entries.insert(0, 60 * 12)
        if min(entries) < min(leaves):
            # add a row to leaves at end of quarter
            leaves.append(0)
        if max(entries) < max(leaves):
            # add a row to entries at start of quarter
            entries.insert(0, 60 * 12)
        if len(entries) > len(leaves):
            # add a row to leaves at end of quarter
            leaves.append(0)
        elif len(entries) < len(leaves):
            # add a row to entries at start of quarter
            entries.insert(0, 60 * 12)
        
        
        assert len(entries) == len(leaves), (game, quarter, player, entries, leaves)
        assert all([x >= y for x, y in zip(entries, leaves)]), (game, quarter, player, entries, leaves)
        
        player_subs = pd.DataFrame({'Player_ID' : player, 'Home' : home, 'Game_ID' : game, 'Quarter' : int(quarter), 'Home' : home, 
                                   'Entry_Time' : entries, 'Exit_Time' : leaves})
        all_subs = all_subs.append(player_subs)
        
    return all_subs

def main():
    conn = sqlite3.connect('shotchart/shot.db')

    plays = pd.read_sql('SELECT * FROM pbp_raw', conn)
    p_texts = plays.progress_apply(get_play, axis = 1)

    subs = p_texts[p_texts['p_text'].str.contains(' enters ')]
    subs = pd.concat([plays.iloc[subs.index], subs], axis = 1)
    subs.loc[:, 'enters'] = subs['Players'].str[0]
    subs.loc[:, 'leaves'] = subs['Players'].str[1]

    sub_times = subs['time_remaining'].str.extract('(\d+):(\d{2}).(\d)')
    sub_times.columns = ['Minutes', 'Seconds', 'Decimal']
    sub_times = sub_times.astype(int)
    sub_times.loc[:, 'total_time'] = 60 * sub_times['Minutes'] + sub_times['Seconds'] + sub_times['Decimal']/10

    subs = pd.concat([subs, sub_times['total_time']], axis = 1)
    subs[['game_id', 'quarter', 'home', 'enters', 'leaves', 'total_time']]

    all_subs = subs.groupby(['game_id', 'quarter', 'home']).progress_apply(get_sub_order, subs = subs).reset_index(drop = True)

    cursor = conn.cursor()
    cursor.execute('DROP TABLE subs')
    conn.commit()

    all_subs.to_sql('subs', conn, index = False)
