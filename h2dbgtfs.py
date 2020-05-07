"""
本ライブラリ実行前に、h2dbを以下のオプションで起動する
java -cp h2-1.4.200.jar org.h2.tools.Server -webAllowOthers -tcpAllowOthers -pgAllowOthers -baseDir ../data/  -ifNotExists
-tcpAllowOthers 外部からの接続を許す
-pgAllowOthers postgresql互換形式
-baseDir データが保存される先のDB
DB名を mem:hoge とすることで、インメモリDBとして動くので何も保存されない
-ifNotExists 外部接続時にテーブル作成を許可する
"""
import psycopg2 
#import copy
import psycopg2.extras
from pathlib import Path
from datetime import datetime, date, timedelta


db_connection_info = {}

#GTFSデータが対応している期間を取得する
def get_data_duration(cursor):
    
    sql = """
    select
        min(start_date) as min_start_date,
        max(end_date) as max_end_date
    from
        calendar
    """    
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        min_start_date = row['min_start_date']
        max_end_date = row['max_end_date']

    sql = """
    select
        feed_start_date,
        feed_end_date
    from
        feed_info
    """
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results: 
        feed_start_date = row['feed_start_date']
        feed_end_date = row['feed_end_date']

#    print("{}, {}, {}, {}".format(min_start_date, max_end_date, feed_start_date, feed_end_date))        
    # feed_info の start_date と calendar の start_date の遅い方を start_date に
    # feed_info の end_date と calendar の end_date の早いを end_date に
    start_date = feed_start_date if feed_start_date >= min_start_date else min_start_date
    end_date   = feed_end_date if feed_end_date <= max_end_date else max_end_date
    return {
        "start_date": datetime.strptime(start_date, "%Y%m%d").date(), 
        "end_date"  : datetime.strptime(end_date,   "%Y%m%d").date()
    }
    
#順序あり辞書(dictionary)として、key: date, value 空のlist のオブジェクトを返す
def expand_date(from_date, to_date):
    next_date = from_date
    return_dict = {}
    while next_date <= to_date:
        return_dict[next_date] = set()
        next_date = next_date + timedelta(days=1) #1日加える

    return return_dict

# calendar.txt を読み、日付とservice_idのsetというデータ構造を作る
def expand_service_id_in_calendar(date_dict, cursor):
    sql = "select * from calendar"
    date_map = {0: "monday",1:"tuesday",2:"wednesday",3:"thursday",4:"friday",5:"saturday",6:"sunday"}
    cursor.execute(sql)
    results = cursor.fetchall()

    for row in results: 
        service_id = row["service_id"]
        start_date = datetime.strptime(row["start_date"], "%Y%m%d").date()
        end_date   = datetime.strptime(row["end_date"], "%Y%m%d").date()

        for date, service_set in date_dict.items():
            state = row[date_map[date.weekday()]]
            if state == "1" and date >= start_date and date <= end_date:
                service_set.add(service_id)

def process_exception_in_calendar_dates(date_dict, cursor):
    sql = "select * from calendar_dates order by date, exception_type"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results: 
        try:
            date_info = date_dict[datetime.strptime(row["date"], "%Y%m%d").date()]
            if row["exception_type"] == "1":
                date_info.add(row["service_id"])
            elif row["exception_type"] == "2":
                date_info.remove(row["service_id"])
        except KeyError:
            print("{} in calendar_date is out of duration.".format(row))

def create_universal_calendar(date_dict, cursor):
    sql = """
    create table universal_calendar(
        service_id char(255),
        date date
    )
    """
    cursor.execute(sql)
    insert_sql = "insert into universal_calendar (service_id, date) values (%(service_id)s, %(date)s)"
    for date, service_array in date_dict.items():
        for service_id in service_array:
    #        print("{}: {}".format(date, service_id))
            cursor.execute(insert_sql, {"service_id": service_id, "date":date})


def load_gtfs(dbname, base_dir):
    #postgreSQLに接続（接続情報は環境変数、PG_XXX）
    connection = psycopg2.connect("dbname=mem:{} user=sa password='sa' host=localhost port=5435".format(dbname))
    #クライアントプログラムのエンコードを設定（DBの文字コードから自動変換してくれる）
    connection.set_client_encoding('utf-8') 
    #select結果を辞書形式で取得するように設定 
    connection.cursor_factory=psycopg2.extras.DictCursor
    #カーソルの取得
    cursor = connection.cursor()

    gtfs_files = ['agency','calendar','calendar_dates','feed_info','routes','shapes','stop_times','stops','translations','trips']

    for file in gtfs_files:
        gtfs_file = Path(base_dir,file + ".txt")
        sql = "CREATE TABLE {} AS SELECT * FROM CSVREAD('{}')".format(file, str(gtfs_file))
        cursor.execute(sql)

    duration = get_data_duration(cursor)
    date_dict = expand_date(duration['start_date'], duration['end_date'])
    expand_service_id_in_calendar(date_dict, cursor)
    process_exception_in_calendar_dates(date_dict, cursor)

    create_universal_calendar(date_dict, cursor)
    db_connection_info[dbname] = {'connection': connection, 'cursor': cursor}
    return {'cursor':cursor, 'start': duration['start_date'], 'end':duration['end_date']}

#切断
def close_gtfs(dbname):
    info = db_connection_info.pop(dbname) #delete
    info['cursor'].close()
    info['connection'].close()
