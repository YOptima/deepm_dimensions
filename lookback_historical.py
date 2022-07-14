from datetime import date, timedelta, datetime
import subprocess
import sys

name = sys.argv[1]

for i in range(1,225):
    yesterday = date.today() - timedelta(days = i)
    curr_date = yesterday.strftime("%Y%m%d")
    subprocess.call('sudo bq cp -f _deepm."{}"$"{}" deepm."{}"$"{}" '.format(name,curr_date,name,curr_date), shell = True)
