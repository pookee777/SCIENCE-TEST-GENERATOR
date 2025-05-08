import pytz
from datetime import datetime

def format_duration(seconds):
    """Format seconds into a HH:MM:SS string"""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

def calculate_grade(percentage):
    """Return a letter grade based on percentage"""
    if percentage >= 90:
        return "A+"
    elif percentage >= 80:
        return "A"
    elif percentage >= 70:
        return "B"
    elif percentage >= 60:
        return "C" 
    elif percentage >= 50:
        return "D"
    else:
        return "F"

def utc_to_local(utc_dt, timezone_str='Asia/Kolkata'):
    local_tz = pytz.timezone(timezone_str)
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
