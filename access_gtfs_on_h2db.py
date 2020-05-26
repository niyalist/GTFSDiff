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
from datetime import datetime, date, timedelta
import h2dbgtfs
import csv
        
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
        
        
def select_number_of_trips(cursor, start, end):
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
            date >= %(start)s and
            date <= %(end)s
    )as src
    group by 
        date
    order by
        date
    """
    return_dict = {}
    cursor.execute(sql, {'start': start, 'end':end})
    for row in cursor.fetchall():
        date = row['date']
        count = row['count']
        return_dict[date] = {"count": count}
    return return_dict


def append_column(old_dict, new_dict, keyword, header, table):
    old_dict.setdefault('header', {})
    #最大のlistの値を求める
    max_list_length = 0
    for element in old_dict.values():
        if table in element and len(element[table]) > max_list_length:
            max_list_length = len(element[table])

    old_dict['header'].setdefault(table, [])
    old_dict['header'][table].append(header)

    #key = '2020-05-01', content= data
    for key, content in new_dict.items():
        new_value = content[keyword]
        if not key in old_dict:
            old_dict[key] = {table: []}
        if not table in old_dict[key]:
            old_dict[key][table] = []
        while len(old_dict[key][table]) < max_list_length:
            old_dict[key][table].append("")
        old_dict[key][table].append(new_value)

    for element in old_dict.values():
        while (table in element) and (len(element[table]) < max_list_length+1):
            element[table].append("")
    


# 渡された2つの dictionanry のkeyの値に関して、変更があった場合に古いものを保存しながら新しい値を保存する
def merge_and_logging_ordered_dictionary(old_dict, new_dict, keyword, version):
    for key, content in new_dict.items():
        if key in old_dict:
            #キー（日付）に関係するdictがすでに存在するので、書き換え（ロギング）が必要かどうかをチェック
            if keyword in old_dict[key]:
                old_value = old_dict[key][keyword][0] #形式はタプル 
                new_value = content[keyword]
                if not old_value[0] == new_value: #値に変化があったときだけ追加する
                    old_dict[key][keyword].insert(0, (new_value, version))
            else: #日付はあるがハッシュ内に値が無い場合
                old_dict[key][keyword] = [ (content[keyword], version) ]
        else:
            old_dict[key] = {keyword: [ (content[keyword], version) ]}

# キーと複数の値の集合を、同一キーでまとめてキーとsetにする setなので重複はない
# {'2020-05-01' {
#    service_id: {'平日', '平日特別'},
#    trip_id:{'trip_1', 'trip_2', 'trip_3', 'trip_4'},
#    route_id: {'路線1', '路線2'}
#}
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
#    base_dir = "uncompressed"
    base_dir = "/Users/niya/Documents/oguchi/2020/gtfs/h2db/"
    uncompress_dir = "uncompressed"
    gtfs_dir = [files for files in Path(base_dir + uncompress_dir).iterdir()]
    gtfs_dir.sort()

    gtfs_names = []
    gtfs_by_date = {}
    gtfs_by_route = {}

    for d in gtfs_dir:
        print("======================================")
        print("LOAD: {}".format(d.absolute()))
        gtfs_name = d.name
        gtfs_names.append(gtfs_name)
        gtfs_info = h2dbgtfs.load_gtfs(gtfs_name, d.absolute())

        info_by_date = {}
        date_t = gtfs_info['start']
        while date_t <= gtfs_info['end']:
            info_by_date[date_t] = {'gtfs_name': gtfs_name}
            date_t = date_t + timedelta(days=1)

        append_column(gtfs_by_date, info_by_date, 'gtfs_name', gtfs_name, 'gtfs_name')

        service_ids = select_trip_ids(gtfs_info['cursor'], gtfs_info['start'], gtfs_info['end'])
        reduced = reduce_to_key_set(service_ids, 'date')
        append_column(gtfs_by_date, reduced, 'service_id', gtfs_name, 'service_id')

        number_of_trips = select_number_of_trips(gtfs_info['cursor'], gtfs_info['start'], gtfs_info['end'])
        append_column(gtfs_by_date, number_of_trips, 'count', gtfs_name, 'number_of_trips')

#        merge_and_logging_ordered_dictionary(gtfs_by_date, reduced, 'service_id', gtfs_name)
#        merge_and_logging_ordered_dictionary(gtfs_by_date, reduced, 'trip_id', gtfs_name)
#        merge_and_logging_ordered_dictionary(gtfs_by_date, info_by_date, 'gtfs_name', gtfs_name)

        h2dbgtfs.close_gtfs(gtfs_name)


    print("===========FINAL RESULT========")
    for k, v in gtfs_by_date.items():
        print("{}: {}".format(k, v))

    logfile = Path(base_dir + "log.csv")
    with open(logfile, 'w') as f:
        writer = csv.writer(f)
        for k, v in gtfs_by_date.items():
            val = [k]
            for key, data in v.items():
                val += data
            writer.writerow(val)



    
def history_string(list_of_sets):
    values = list(map(lambda set_data: str(set_data), list_of_sets))
    return " <- ".join(values) +"(" + str(len(values))+")"

def history_count(list_of_sets):
    values = list(map(lambda set_data: str(len(set_data[0])) + " (" + set_data[1] + ")", list_of_sets))
    return " <- ".join(values)

if __name__ == '__main__':
    main()


