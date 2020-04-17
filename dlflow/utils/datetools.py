from datetime import datetime
from datetime import timedelta


_DT_U2D_TRANS = {
    "yyyy": "%Y",
    "mm": "%m",
    "dd": "%d",
}


def std_dt_tans(dt, days=0, dt_fmt="%Y%m%d", out_fmt="%Y%m%d"):
    now_dt = datetime.strptime(dt, dt_fmt)

    return (now_dt + timedelta(days=days)).strftime(out_fmt)


def u2s_dt_fmt(user_dt_str):
    std_dt_str = user_dt_str
    for s, d in _DT_U2D_TRANS.items():
        std_dt_str = std_dt_str.replace(s, d)

    return std_dt_str


def trans_date(dt, user_dt_str):
    std_dt_str = u2s_dt_fmt(user_dt_str)
    new_dt_str = std_dt_tans(dt, out_fmt=std_dt_str)

    return new_dt_str
