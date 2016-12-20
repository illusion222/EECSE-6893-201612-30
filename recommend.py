#usr/bin/python2.7
from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

conf = SparkConf().setAppName("expedia_hotel")
sc = SparkContext(conf=conf)


arr = sc.textFile("./traindata.csv")
print arr.take(2)
arr = arr.map(lambda x:x.split(","))


def get_best_hotels_od_ulc(arr):
    if arr[5] != '' and arr[6] != '':
        return ((arr[5], arr[6],arr[23]),1)
    else:
        return ((arr[5], arr[6],arr[23]),0)

def get_best_hotels_search_dest(arr):
    if arr[16] != '' and arr[21] != '' and arr[22] != '' and int(arr[0][:4]) == 2014:
        return ((arr[16], arr[21], arr[22], arr[23]), int(arr[18]) * 17 + 3)
    else:
        return ((arr[16], arr[21], arr[22], arr[23]), 0)

def get_best_hotels_search_dest1(arr):
    if arr[16] != '':
        return ((arr[16], arr[23]) ,int(arr[18]) * 17 + 3)
    else:
        return ((arr[16], arr[23]), 0)

def get_best_hotel_country(arr):
    if arr[21] != '':
        return ((arr[21], arr[23]), 1 + 5 * int(arr[18]))
    else:
        return ((arr[21], arr[23]), 0)

def get_popular_hotel_cluster(arr):
    return (arr[23],1)


best_hotels_od_ulc = arr.map(lambda x:get_best_hotels_od_ulc(x))
best_hotels_od_ulc = best_hotels_od_ulc.foldByKey(0, add).cache()

best_hotels_search_dest = arr.map(lambda x:get_best_hotels_search_dest(x))
best_hotels_search_dest = best_hotels_search_dest.foldByKey(0, add).cache()


best_hotels_search_dest1 = arr.map(lambda x:get_best_hotels_search_dest1(x))
best_hotels_search_dest1 = best_hotels_search_dest1.foldByKey(0, add).cache()

best_hotel_country = arr.map(lambda x:get_best_hotel_country(x))
best_hotel_country = best_hotel_country.foldByKey(0, add).cache()

popular_hotel_cluster = arr.map(lambda x:get_popular_hotel_cluster(x))
popular_hotel_cluster = popular_hotel_cluster.foldByKey(0, add).cache()


path = 'result.csv'
out = open(path, "w")
f = open("./testdata.csv", "r")
schema = f.readline()
total = 0
out.write("id,hotel_cluster\n")
topclasters = popular_hotel_cluster.sortBy(lambda x: -x[1]).map(lambda x:x[0]).take(5)
idnumber = 0
while 1:
    line = f.readline().strip()
    total += 1

    if total % 10 == 0:
        print('Write {} lines...'.format(total))

    if total % 999 == 0:
        break

    arr = line.split(",")
    id = idnumber
    idnumber = idnumber + 1
    print arr
    user_location_city = arr[5]
    orig_destination_distance = arr[6]
    srch_destination_id = arr[16]
    hotel_country = arr[21]
    hotel_market = arr[22]

    out.write(str(id) + ',')
    filled = []


    Topitems = best_hotels_od_ulc.filter(lambda x:(x[0][0] == user_location_city)&(x[0][1] == orig_destination_distance))
    Topitems = Topitems.sortBy(lambda x: -x[1]).map(lambda x:x[0][2])
    topitems = Topitems.take(5)
    for i in range(len(topitems)):
        if topitems[i] in filled:
            continue
        if len(filled) == 5:
            break
        out.write(' ' + topitems[i])
        filled.append(topitems[i])

    if len(filled) < 5:
        Topitems = best_hotels_search_dest.filter(lambda x: (x[0][0] == srch_destination_id) & (x[0][1] == hotel_country) & (x[0][2] == hotel_market))
        Topitems = Topitems.sortBy(lambda x: -x[1]).map(lambda x: x[0][3])
        topitems = Topitems.take(5)
        for i in range(len(topitems)):
            if topitems[i] in filled:
                continue
            if len(filled) == 5:
                break
            out.write(' ' + topitems[i])
            filled.append(topitems[i])

    if len(filled) < 5:
        if len(topitems) != 0:
            Topitems = best_hotels_search_dest1.filter(lambda x: (x[0][0] == srch_destination_id))
            Topitems = Topitems.sortBy(lambda x: -x[1]).map(lambda x: x[0][1])
            topitems = Topitems.take(5)
            for i in range(len(topitems)):
                if topitems[i] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i])
                filled.append(topitems[i])

    if len(filled) < 5:
        Topitems = best_hotel_country.filter(lambda x: (x[0][0] == hotel_country))
        Topitems = Topitems.sortBy(lambda x: -x[1]).map(lambda x: x[0][1])
        for i in range(len(topitems)):
            if topitems[i] in filled:
                continue
            if len(filled) == 5:
                break
            out.write(' ' + topitems[i])
            filled.append(topitems[i])

    if len(filled) < 5:
        for i in range(5):
            if topclasters[i] in filled:
                continue
            if len(filled) == 5:
                break
            out.write(' ' + topclasters[i])
            filled.append(topclasters[i])
    out.write("\n")
out.close()
print('Completed!')