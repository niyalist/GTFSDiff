"""
to install library
pip install --upgrade gtfs-realtime-bindings

https://github.com/MobilityData/gtfs-realtime-bindings/blob/master/python/README.md

"""

from google.transit import gtfs_realtime_pb2
import datetime
from collections import namedtuple


cause_dict ={}
effect_dict = {}
for v in gtfs_realtime_pb2._ALERT_CAUSE.values:
    cause_dict[v.number] = v.name

for v in gtfs_realtime_pb2._ALERT_EFFECT.values:
    effect_dict[v.number] = v.name


def read_gtfs_realtime_alert(data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    gtfs_realtime_version = feed.header.gtfs_realtime_version
    timestamp             = datetime.datetime.fromtimestamp(feed.header.timestamp)
    alerts = []
    for entity in feed.entity:
        active_period = []
        informed_entity = []
        Period = namedtuple('Period', ['start', 'end'])
        InformedEntity = namedtuple('InformedEntity', ['agency_id', 'route_id', 'route_type', 'trip_id', 'stop_id'])
        Alert = namedtuple('Alert',['period', 'informed_entity', 'cause', 'effect', 'url', 'header_text', 'description_text'])

        for period in entity.alert.active_period:
            active_period.append(
                Period(
                    start = datetime.datetime.fromtimestamp(period.start), 
                    end = datetime.datetime.fromtimestamp(period.end)
                )
            )
        #Entitiy Selector
        for ientity in entity.alert.informed_entity:
            informed_entity.append(
                InformedEntity(
                    agency_id  = ientity.agency_id,
                    route_id   = ientity.route_id,
                    route_type = ientity.route_type,
                    trip_id    = ientity.trip.trip_id,
                    stop_id    = ientity.stop_id
                )
            )
        cause  = entity.alert.cause
        effect = entity.alert.effect

        url              = entity.alert.url.translation[0].text
        header_text      = entity.alert.header_text.translation[0].text
        description_text = entity.alert.description_text.translation[0].text
        alerts.append(
            Alert(
                period           = active_period,
                informed_entity  = informed_entity,
                cause            = cause_dict[cause],
                effect           = effect_dict[effect],
                url              = url,
                header_text      = header_text,
                description_text = description_text
            )
        )
    return {'version': gtfs_realtime_version, 'timestamp': timestamp, 'alert': alerts}

def print_alert(alert):
#    print(alert['version'])
    print("timestamp: {}".format(alert['timestamp']))
    print("Alert: {}".format(len(alert['alert'])))        
#    return
    for data in alert['alert']:
        print("------------------------------")
        for period in data.period:
            print(period)
        for entity in data.informed_entity:
            print(entity)
        print(data.cause, data.effect)
        print(data.url)
        print(data.header_text)
        print(data.description_text)

def comp_alert(a, b):
    if a is None or b is None:
        return False

    # version, timestamp, alertの3つ timestampは比較しない
    if not a['version']  == b['version']:
        return False
    
    alertA = a['alert'] #AlertA
    alertB = b['alert'] #AlertB

    if not len(alertA) == len(alertB):
        return False

    #本当はZIPするまえにsortして順序を揃えた方が良いかもしれないが、適切なsortキーがない
    for entityA, entityB in zip(alertA, alertB):
        if not len(entityA.period) == len(entityB.period):
            return False
        for periodA, periodB in zip(entityA.period, entityB.period):
            if(not periodA.start == periodB.start or not periodA.end == periodB.end ):
                return False
        
        if not len(entityA.informed_entity) == len(entityB.informed_entity):
            return False
        for informed_entityA, informed_entityB in zip(entityA.informed_entity, entityB.informed_entity):
            if(
                not informed_entityA.agency_id  == informed_entityB.agency_id  or
                not informed_entityA.route_id   == informed_entityB.route_id   or
                not informed_entityA.route_type == informed_entityB.route_type or
                not informed_entityA.trip_id    == informed_entityB.trip_id    or
                not informed_entityA.stop_id    == informed_entityB.stop_id
            ):
                return False
        
        if(
            not entityA.cause            == entityB.cause or 
            not entityA.effect           == entityB.effect or
            not entityA.url              == entityB.url or 
            not entityA.header_text      == entityB.header_text or
            not entityA.description_text == entityB.description_text
        ):
            return False                

    return True

