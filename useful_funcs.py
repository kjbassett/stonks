import datetime
import holidays
from dateutil.easter import easter
import keyring


def is_open(date):
    us_holidays = holidays.US()

    # If a holiday
    if date in us_holidays and us_holidays[date] not in ['Veterans Day', 'Columbus Day']:
        return False

    # Good Friday not included above, but stock market is closed
    if date == easter(date.year) - datetime.timedelta(days=2):
        return False

    # If it's a weekend
    if date.weekday() > 4:
        return False

    return True


def calc_date(date, business_days):
    if not business_days:
        return date

    end_date = date
    counter = 0
    direction = abs(business_days) / business_days
    while counter < abs(business_days):
        end_date += datetime.timedelta(days=direction)
        if is_open(end_date):
            counter += 1

    return end_date


def get_cred(service, item):
    cred = keyring.get_password(service, item)
    if not cred:
        cred = input(f'No value found for Service: {service}, Item: {item}. Please type it here: ')
        keyring.set_password(service, item, cred)
    return cred
