from datetime import datetime
from datetime import timedelta


_DT_U2D_TRANS = {
    "yyyy": "%Y",
    "mm": "%m",
    "dd": "%d",
    "HH": "%H",  # Hour
    "MM": "%M",  # Minute
    "SS": "%S",  # Second
    "ww": "%u",  # Day of a week, 0 for Monday
    "WW": "%W",  # Week of a the, Monday as the first day for a week
}


def std_dt_tans(dt, days=0, dt_fmt="%Y%m%d", out_fmt="%Y%m%d"):
    """
    Stander date transformer
    """

    now_dt = datetime.strptime(dt, dt_fmt)

    return (now_dt + timedelta(days=days)).strftime(out_fmt)


def u2s_dt_fmt(user_dt_str):
    """
    Parser date template
    """

    std_dt_str = user_dt_str
    for s, d in _DT_U2D_TRANS.items():
        std_dt_str = std_dt_str.replace(s, d)

    return std_dt_str


def trans_date(dt, user_dt_str):
    """
    Transform date string to another format date string
    """

    std_dt_str = u2s_dt_fmt(user_dt_str)
    new_dt_str = std_dt_tans(dt, out_fmt=std_dt_str)

    return new_dt_str
