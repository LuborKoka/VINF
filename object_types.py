from typing import TypedDict

class METADATA(TypedDict):
    download_date: str
    download_url: str
    file_path: str



class PLAYER_DATA(TypedDict):
    file_path: str
    download_url: str
    player_name: str
    position: str
    shoots: str
    height: int
    weight: int
    games_played: int
    goals: int
    assists: int
    points: int
    plus_minus: int
    point_shares: float
    penalty_minuts: int
    shots_on_goal: int
    game_winning_goals: int