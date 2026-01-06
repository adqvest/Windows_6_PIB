#!/usr/bin/env python
# coding: utf-8

import datetime as datetime
from datetime import timedelta


def last_day_of_month(rdate):
    rdate = rdate + datetime.timedelta(1)
    next_month = rdate.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)