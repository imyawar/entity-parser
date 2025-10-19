import logging
import re
from datetime import datetime, timedelta


class Utils:
    def __init__(self, event):
        if "version" in event:
            version_str = event["version"]
            if re.fullmatch(r"\d{7,8}", version_str):
                self.version = event["version"]
            else:
                logging.error('Invalid version: %s given using default version', version_str)

    # day 0=Monday, 1=Tuesday, 4=Friday, 5=Saturday, 6=Sunday
    def get_week_by_day(self, date, day):
        # Find the first Friday of the year
        year_start = datetime(date.year, 1, 1)
        first_friday = year_start + timedelta((day - year_start.weekday()) % 7)

        # Calculate the difference between the given date and the first Friday
        delta = (date - first_friday).days

        # Calculate the custom week number
        week_number = (delta // 7) + 1 if delta >= 0 else 0

        return week_number

    def get_directory_name(self):
        date = datetime.now()
        week = self.get_week_by_day(date, 4)
        if hasattr(self, 'version'):
            return self.version
        else:
            return f"{date.strftime('%Y%m')}{week}"