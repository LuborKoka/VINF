from typing import TypedDict, Optional, Dict, Set

class METADATA(TypedDict):
    download_date: str
    download_url: str
    file_path: str



class PLAYER_DATA(TypedDict):
    file_path: str
    download_url: str
    player_name: str
    dob: str
    draft_team: Optional[str]
    position: str
    hand: str # shoots/catches
    height: Optional[int]
    weight: Optional[int]
    games_played: Optional[int]
    wins: Optional[int]
    losses: Optional[int]
    ties_ot_losses: Optional[int]
    minutes: Optional[int]
    shootouts: Optional[float]
    gaa: Optional[float]
    save_percentage: Optional[float]
    goals: Optional[int]
    assists: Optional[int]
    points: Optional[int]
    plus_minus: Optional[int]
    point_shares: Optional[float]
    penalty_minutes: Optional[int]
    shots_on_goal: Optional[int]
    game_winning_goals: Optional[int]

class INDEX(TypedDict):
    player_name: Dict[str, Set[int]]
    dob: Dict[str, Set[int]]
    draft_team: Dict[str, Set[int]]
    position: Dict[str, Set[int]]
    hand: Dict[str, Set[int]]
    height: Optional[int]
    weight: Optional[int]
    games_played: Optional[int]
    wins: Optional[int]
    losses: Optional[int]
    ties_ot_losses: Optional[int]
    minutes: Optional[int]
    shootouts: Optional[float]
    gaa: Optional[float]
    save_percentage: Optional[float]
    goals: Optional[int]
    assists: Optional[int]
    points: Optional[int]
    plus_minus: Optional[int]
    point_shares: Optional[float]
    penalty_minutes: Optional[int]
    shots_on_goal: Optional[int]
    game_winning_goals: Optional[int]

class CAREER_FRAME(TypedDict):
    start: Optional[int]
    end: Optional[int]


class DRAFT(TypedDict):
    draft: Optional[str]
    draft_year: Optional[int]
    draft_team: Optional[str]


class WIKI_PLAYER(TypedDict):
    full_name: str
    birthplace: str
    career_start: Optional[int]
    career_end: Optional[int]
    draft: Optional[str]
    draft_year: Optional[str]
    draft_team: Optional[str]
    current_league: Optional[str]
    national_team: Optional[str]
    current_team: Optional[str]
    nationality: Optional[str]
