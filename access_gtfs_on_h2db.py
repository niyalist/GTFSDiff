"""
本プログラム実行前に、h2dbを以下のオプションで起動する
java -cp h2-1.4.200.jar org.h2.tools.Server -webAllowOthers -tcpAllowOthers -pgAllowOthers -baseDir ../data/  -ifNotExists
-tcpAllowOthers 外部からの接続を許す
-pgAllowOthers postgresql互換形式
-baseDir データが保存される先のDB
DB名を mem:hoge とすることで、インメモリDBとして動くので何も保存されない
-ifNotExists 外部接続時にテーブル作成を許可する
"""

### http://gtfs-archives.t-brain.jp

from pathlib import Path
import psycopg2 
import copy
import psycopg2.extras
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
        
def select_trip_ids(cursor, start,end):
    sql="""
    select
        uc.date,
        uc.service_id,
        tr.trip_id,
        tr.route_id
    from
        universal_calendar as uc
        inner join trips as tr
        on uc.service_id = tr.service_id
    where
        date >= %(start)s and
        date <= %(end)s
    """
    return_list = []
    cursor.execute(sql, {'start': start, 'end':end})
    for row in cursor.fetchall():
        date = row['date']
        service_id = row['service_id'].strip()
        trip_id = row['trip_id'].strip()
        route_id = row['route_id'].strip()
        return_list.append({"date": date, "service_id": service_id, "trip_id": trip_id, "route_id": route_id})
    return return_list
        
        
def select_service_ids(cursor, start, end):
    sql="""
    select
        date,
        service_id
    from
        universal_calendar
    where
        date >= %(start)s and
        date <= %(end)s
    """
    return_list = []
    cursor.execute(sql, {'start': start, 'end':end})
    for row in cursor.fetchall():
        date = row['date']
        service_id = row['service_id'].strip()
        return_list.append({"date": date, "service_id": service_id})
    return return_list
        
        
def select_number_of_trips(cursor):
    sql = """
    select
        date as date,
        count(*) as count
    from(
        select
            uc.*,
            tr.trip_id
        from
            universal_calendar as uc
            inner join trips as tr
            on uc.service_id = tr.service_id
        where
            uc.date >= '2020-01-01' and
            uc.date <= '2020-06-30'
    )as src
    group by 
        date
    order by
        date
    """
    return_dict = {}
    cursor.execute(sql)
    for row in cursor.fetchall():
        date = row['date']
        count = row['count']
        return_dict[date] = {"count": count}
    return return_dict

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

# 渡された2つの dictionanry のkeyの値に関して、変更があった場合に古いものを保存しながら新しい値を保存する
def merge_and_logging_ordered_dictionary(old_dict, new_dict, keyword, version):
    for key, content in new_dict.items():
        if key in old_dict:
            #キー（日付）に関係するdictがすでに存在するので、書き換え（ロギング）が必要かどうかをチェック
#            print(key, old_dict[key])
#            print(old_dict[key][keyword])
            if keyword in old_dict[key]:
                old_value = old_dict[key][keyword][0] #形式はタプル 
                new_value = content[keyword]
#                print("    CHECK: from {} to {}".format(old_value, new_value))                
                if not old_value[0] == new_value: #値に変化があったときだけ追加する
#                    print("    CHANGE in {}, value[{}]: from {} to {}".format(key, keyword, old_value, new_value))
                    old_dict[key][keyword].insert(0, (new_value, version))
            else: #日付はあるがハッシュ内に値が無い場合
                old_dict[key][keyword] = [ (content[keyword], version) ]
        else:
            old_dict[key] = {keyword: [ (content[keyword], version) ]}

# キーと値のペアの集合を、同一キーでまとめてキーとsetにする
def reduce_to_key_set(input_list, aggregate_key):
    keys = list(input_list[0].keys())
    keys.remove(aggregate_key)
    return_val = {}
    for values in input_list:   # {'date': datetime.date(2020, 3, 6), 'service_id': '平日'}
        agg_key = values[aggregate_key] # datetime.date(2020, 3, 6)
        if agg_key in return_val:
            for key in keys:
                return_val[agg_key][key].add(values[key]) # add value to the set
        else:
            return_val[agg_key] = {}
            for key in keys:
                return_val[agg_key][key] = {values[key]} #create set

    return return_val

def main():
    base_dir = "uncompressed"
    gtfs_dir = [files for files in Path(base_dir).iterdir()]
    gtfs_dir.sort()

    gtfs_by_date = {}

    for d in gtfs_dir:
        print("======================================")
        print("LOAD: {}".format(d.absolute()))
        gtfs_name = d.name
        gtfs_info = load_gtfs(gtfs_name, d.absolute())

        info_by_date = {}
        date_t = gtfs_info['start']
        while date_t <= gtfs_info['end']:
            info_by_date[date_t] = {'gtfs_name': gtfs_name}
            date_t = date_t + timedelta(days=1)

        merge_and_logging_ordered_dictionary(gtfs_by_date, info_by_date, 'gtfs_name', gtfs_name)

#        service_ids = select_service_ids(gtfs_info['cursor'], gtfs_info['start'], gtfs_info['end'])
        service_ids = select_trip_ids(gtfs_info['cursor'], gtfs_info['start'], gtfs_info['end'])
        reduced = reduce_to_key_set(service_ids, 'date')

#        for k, r in reduced.items():
#            print(k, r)

        merge_and_logging_ordered_dictionary(gtfs_by_date, reduced, 'service_id', gtfs_name)
        merge_and_logging_ordered_dictionary(gtfs_by_date, reduced, 'trip_id', gtfs_name)



        close_gtfs(gtfs_name)

    print("============================")
    print("===========FINAL RESULT========")
#    for k, v in gtfs_by_date.items():
#        print("{}: {}".format(k, v['gtfs_name']))    
    for k, v in gtfs_by_date.items():
        print('[{}] GTFS: {} (Gen {}), Trip: {}'.format(k, v['gtfs_name'][0][0], str(len(v['gtfs_name'])), history_count(v['trip_id'])))
#        print("[{}] GTFS: {f}({}), service_id: {}: , trip_no: }".format(
#                k, 
#                v['gtfs_name'][0][0], #最新[0]の gtfs_name の値のみ
#                len(v['gtfs_name']),  #gtfs_nameの個数
#                history_string(v['service_id']) 
#                history_count(v['trip_id'])
#            )
#        )


def history_string(list_of_sets):
    values = list(map(lambda set_data: str(set_data), list_of_sets))
    return " <- ".join(values) +"(" + str(len(values))+")"

def history_count(list_of_sets):
    values = list(map(lambda set_data: str(len(set_data[0])) + " (" + set_data[1] + ")", list_of_sets))
    return " <- ".join(values)

if __name__ == '__main__':
    main()


