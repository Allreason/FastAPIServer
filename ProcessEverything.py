import os
import re
from pymongo import MongoClient

from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
import sys
import subprocess

para = sys.argv[1]
print(para)

path_list = ['~/Downloads/everything2022-10-17.csv','F:/everything2022-10-20.csv']
ps = path_list[1]
if para:
    ps = para
pathd = os.path.expanduser(ps)




excluded_list = ['zaluan', 'softs archive']


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb://aireason2:maths3141@aireason.tpddns.cn:21483"
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)
    # Create the database for our example (we will use the same database throughout the tutorial
    return client['test']


mongo = get_database()


def process_and_split(s):
    stack = []
    dup = ''
    for c in s:
        if c == '"':
            if not stack:
                stack.append(c)
            else:
                stack.pop()
            continue
        if c == ',' and stack:
            c = '~'
        dup += c
    return [i.replace('~', ',').replace('\n', '') for i in dup.split(',')]


def get_drive_name(pathd):
    with open(pathd, 'r', encoding='utf-8') as f:
        line = f.readline()
        while line:
            if line.startswith("\"###!"):
                r = re.search(r'"###!(.*?)"', line)
                drive = r.group(1) if r else None
                return drive
            line=f.readline()
        return None


def loop_file(pathd, drop_before_insert=False, only_deal_with='', excluded_list=None):
    # how to take or fill this information ?
    # put a file at the root of the drive, this is the best choice.
    # the name of the file starts with '!!!'to ensure to be the first one of all files.
    drive = get_drive_name(pathd)
    # I had put the get drive operation in the main reading file loop. When the drive is found, I let the f seek to 0
    # then begin write each line to database.
    # But it is a bad design! It makes the code complex.
    if drive is None:
        return
    print(f'drive is {drive}')
    mongo[drive].drop()
    with open(pathd, 'r', encoding='utf-8') as f:
        line = f.readline()
        fields = ['filename', 'location', 'size', 'dm', 'dc', 'da', 'extension', 'attributes']
        n, cache = 1, []
        while line:
            if only_deal_with:
                if not re.search(only_deal_with, line):
                    line = f.readline()
                    continue
            if excluded_list:
                flag = False
                for i in excluded_list:
                    if i in line:
                        flag = True
                        break
                if flag is True:
                    line = f.readline()
                    continue

            data = process_and_split(line)
            data[1] = re.sub(r'\\', '/', data[1])
            di = dict(zip(fields, data))
            di['location'] = re.sub(r'^[A-Z]:', drive + ':', di['location'])
            _id = di['filename'] + '|' + di['location']
            di['filename:size'] = di['filename'] + ":" + di['size']
            cache.append(UpdateOne(filter={'_id': _id}, update={'$set': di, '$currentDate': {'cellUpdated': True}},
                                   upsert=True))
            if len(cache) > 300:
                mongo[drive].bulk_write(cache)
                cache = []

            if n % 10000 == 0:
                print(n)
            line = f.readline()
            n += 1

        mongo[drive].bulk_write(cache)


def search_by_filename(s, only_folder=False):
    sl = [re.compile(i, flags=re.IGNORECASE) for i in s.split(' ') if i]
    collections = mongo.list_collections()
    for c in collections:
        if only_folder is True:
            founded = mongo[c.get('name')].find({'location': {'$all': sl}, 'attributes': 'D'})
        else:
            founded = mongo[c.get('name')].find({'filename': {'$all': sl}})
        k = [i for i in founded]
        if k:
            print(c.get('name'))
            print(k)


def iteration_of_all_collections():
    search_by_filename('backupflag$')
    # 2022-10-11xart.backupflag


def stripe_all_path_layers_one_by_one(path):
    if path.count('/') == 1:
        return [path]
    striped = re.sub(r'/[^/]+$', '', path)
    # return stripe_all_path_layers_one_by_one(striped).append(path) will cause an error
    # because list.append('ddd') will modify the original list and return none.
    return stripe_all_path_layers_one_by_one(striped) + [path]


print(stripe_all_path_layers_one_by_one('D:/lenevo1t/S1/s2'))


def file_counting_in_folder():
    import redis
    r = redis.Redis(host='114.132.248.40', port=6379, db=7,
                    password='foo1bared', decode_responses=True)
    pipe = r.pipeline()
    with open(pathd, 'r') as f:
        for i in r.keys('N:*'):
            pipe.delete(i)
        pipe.execute()
        f.readline()
        line = f.readline()
        n = 1
        while line:
            data = [i.rstrip('\n').rstrip('"').lstrip('"') for i in line.split(',')]
            path = re.sub(r'\\', '/', data[1])
            if path.count('/') > 0:
                lis = stripe_all_path_layers_one_by_one(path)
                for p in lis:
                    if 0 < p.count('/') < 5:
                        pipe.sadd('set_for_all_in_DriveN', p)
                        pipe.incr(p)

            line = f.readline()
            n += 1
            if n % 1000 == 0:
                pipe.execute()
                print(n)
        pipe.execute()


# iteration_of_all_collections()
# search_by_filename('lenevo',only_folder=True)

# file_counting_in_folder()

# keys = r.execute_command("sort set_for_all_in_DriveN by * desc")
# vs = r.execute_command("sort set_for_all_in_DriveN by * get * desc")

# transposed = numpy.array([numpy.array(keys), numpy.array(vs)]).T
# numpy.printoptions(suppress=False)
# for t in transposed:
#     print(t)

# loop_file(drop_before_insert=True)


command = """
CREATE TABLE if not exists {drive}
(
`filename` String,
`location` String,
`size` Nullable(UInt128),
`dm` Nullable(DateTime),
`dc` Nullable(DateTime),
`da` Nullable(DateTime),
`extension` String,
`attributes` String,
`cellUpdated` Nullable(DateTime)
)
ENGINE=MergeTree
ORDER BY location
"""

import datetime
def use_ck():
    import clickhouse_connect
    
    client = clickhouse_connect.get_client(host='aireason.tpddns.cn', username='default', password='fighting')
    
    print(client.command('show tables'))
    #sys.exit()

    now = datetime.datetime.now()
    dates = str(now.year)+'-'+str(now.month)+'-'+str(now.day)
    # ds = '2022/04/19 00:50'
    # pdate = datetime.datetime.strptime(ds, '%Y/%m/%d %H:%M')
    # row1 = ['1.txt', 'D:/', 28123, pdate, pdate, pdate, 'txt', 'A', now]
    drive = get_drive_name(pathd)
    copycommand = f'copy {para} C:\\Users\\aireason\\Documents\\{drive}_{dates}.csv'
    print(copycommand)
    p=subprocess.Popen(copycommand,shell=True)
    p.wait()
    if drive is None:
        return
    print(f'drive is {drive}')
    client.command(f'drop table if exists {drive}')
    client.command(command.format(drive=drive))

    with open(pathd, 'r', encoding='utf-8') as f:
        f.readline()
        line = f.readline()
        n, cache = 1, []
        while line:
            row = process_and_split(line)
            # print(row)
            row[1] = re.sub(r'^[^:]*:/?','',re.sub(r'\\', '/', row[1]))
            row[2] = row[2] if row[2] else None
            row[3] = datetime.datetime.strptime(row[3], '%Y/%m/%d %H:%M') if row[3] and row[3]>'1971' else None
            row[4] = datetime.datetime.strptime(row[4], '%Y/%m/%d %H:%M') if row[4] and row[4]>'1971' else None
            row[5] = datetime.datetime.strptime(row[5], '%Y/%m/%d %H:%M') if row[5] and row[5]>'1971' else None
            row.append(now)
            cache.append(row)
            # client.insert('wd_4t', [row])

            if n % 2000 == 0:
                print(n)
                try:
                    client.insert(drive, cache)
                except:
                    print(cache)
                    sys.exit()
                cache = []
            line = f.readline()
            n += 1
        client.insert(drive, cache)

    # client.insert('wd_4t', data)


use_ck()

# t='1970/01/01 08:00'
# tt=datetime.datetime.strptime(t, '%Y/%m/%d %H:%M')
# print(tt.timestamp())
