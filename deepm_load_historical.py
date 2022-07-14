import os
import sys
import pathlib
from datetime import date, timedelta, datetime
import subprocess


def main():
    advertiser_name = sys.argv[1]
    report_id_base = sys.argv[2]
    report_id_7d = sys.argv[3]
    report_id_3d = sys.argv[4]
    report_id_1d = sys.argv[5]
    report_id_1h = sys.argv[6]
    yesterday = date.today() - timedelta(days = 1)
    curr_date = yesterday.strftime("%Y/%m/%d")
    file = pathlib.Path("/home/yoptima/DeepVu/advertiser-deeplake/tableau/lock_" + advertiser_name)
    if curr_date == date_validator(report_id_base, "33") and curr_date == date_validator(report_id_7d, "33") and curr_date == date_validator(report_id_3d, "33") and curr_date == date_validator(report_id_1d, "33") and curr_date == date_validator(report_id_1h, "33") and not file.exists():
        timestamp = datetime.now()
        print("Data loading to bigquery Started at ", timestamp)
        subprocess.call("./deepm_transfer_historical.sh " + advertiser_name + "  " + report_id_base + " " + report_id_7d + " " + report_id_3d + " " + report_id_1d + " " + report_id_1h, shell = True)
        timestamp = datetime.now()
        print("Data loading to bigquery done at ", timestamp)


def date_validator(report_id, tail):
    curr_date = date.today().strftime("%d%m%Y")
    filename = report_id + "_" + curr_date + ".csv"
    cmd = 'tail -' + tail + " " + filename + ' | head -1 | tail -c 11'
    report_date = os.popen(cmd).read()
    return report_date.strip()


if __name__ == "__main__":
    main()
