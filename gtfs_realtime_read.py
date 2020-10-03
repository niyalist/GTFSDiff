"""
to install library
pip install --upgrade gtfs-realtime-bindings

https://github.com/MobilityData/gtfs-realtime-bindings/blob/master/python/README.md

"""
import gtfsrealtime
import datetime
from pathlib import Path

        

alert_path = Path("alert")
HOUR_STEP = 12
from_time = datetime.datetime(2020, 3, 1, 0, 0, 0)
to_time   = datetime.datetime(2020, 6, 7, 23, 59, 59)


def scan_gtfs_rt_files(path, from_time, to_time, step):
    next_time = from_time
    pre_alert = None
    while next_time <= to_time:
        file_name = next_time.strftime("%Y-%m-%dT%H:%M:*.pb")
        files = sorted(alert_path.glob(file_name))
        if len(files) > 0:
            file = files[0]
            f = open(file, "rb")
            content = f.read()
            f.close()
            alert = gtfsrealtime.read_gtfs_realtime_alert(content)


            if not gtfsrealtime.comp_alert(pre_alert, alert):
                print("==========={}===================".format(next_time))                
#                gtfsrealtime.print_alert(alert)
                print(gtfsrealtime.comp_alert(pre_alert, alert))
            pre_alert = alert

        next_time = next_time + datetime.timedelta(hours=HOUR_STEP)



def main():
    scan_gtfs_rt_files(alert_path, from_time, to_time, HOUR_STEP)


if __name__ == '__main__':
    main()


