import os
from pydoc import cli
import re

import sys
import subprocess

path_list = ['~/Downloads/everything2022-10-17.csv', 'F:/everything2022-10-20.csv']
ps = path_list[1]
if len(sys.argv) > 1:
    para = sys.argv[1]
    print(para)
    ps = para

pathd = os.path.expanduser(ps)

excluded_list = ['zaluan', 'softs archive']





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
            line = f.readline()
        return None




def stripe_all_path_layers_one_by_one(path):
    if path.count('/') == 1:
        return [path]
    striped = re.sub(r'/[^/]+$', '', path)
    # return stripe_all_path_layers_one_by_one(striped).append(path) will cause an error
    # because list.append('ddd') will modify the original list and return none.
    return stripe_all_path_layers_one_by_one(striped) + [path]


# print(stripe_all_path_layers_one_by_one('D:/lenevo1t/S1/s2'))

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


create_table_command = """
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


def folder_file_counting_command(table, layer):
    command = """
select bl,count(*) counting from
(
select w.location as wl,onelayer.location as bl from 
{table} as w
cross join
(
select distinct location as location from {table}
where countMatches(location,'/') ={layer}
) as onelayer
) c 
where position(wl,bl)!=0 
group by bl 
order by counting desc 
limit 100;
""".format(table=table, layer=layer)
    return command


def find_identical_file_command(table1, table2, default_size_of_list='10'):
    command = """
select location,sl,count(*),groupArray({default_size_of_list})(filename)
from 
(
select f.*,s.location as sl
from {table1} f
inner join
{table2} s
on f.filename=s.filename 
and f.size=s.size 
and f.attributes!='D' 
and f.extension!='DS_Store' 
and f.size>0 
)
group by location,sl
order by location
""".format(default_size_of_list=default_size_of_list, table1=table1, table2=table2)
    return command


def find_identical_itself(tablename):
    command = """
select location,sl,count(*) as counting,groupArray(10)(filename)
from 
(
select f.*,s.location as sl
from {table} f
inner join
{table} s
on f.filename=s.filename 
and f.size=s.size 
and f.attributes!='D' 
and f.extension!='DS_Store' 
and f.size>0 
)
where location>sl
group by location,sl
order by location desc    
""".format(table=tablename)
    return command


import clickhouse_connect

client = clickhouse_connect.get_client(host='aireason.tpddns.cn', username='default', password='fighting')


def list_all_drives(drive=None,path=None):
    all_drives = client.command("show tables").split('\n')
    
    if not drive or drive=='null':
        return all_drives
    o = client.query(f"select filename,location,attributes from {drive} where countMatches(location,'/') = 0").result_set
    fields = ['filename','location','attributes']
    zipped = dict(zip(fields, o))
    return o


def use_ck():
    import datetime

    now = datetime.datetime.now()
    dates = str(now.year) + '-' + str(now.month) + '-' + str(now.day)
    # ds = '2022/04/19 00:50'
    # pdate = datetime.datetime.strptime(ds, '%Y/%m/%d %H:%M')
    # row1 = ['1.txt', 'D:/', 28123, pdate, pdate, pdate, 'txt', 'A', now]
    drive = get_drive_name(pathd)
    copycommand = f'copy {para} C:\\Users\\aireason\\Documents\\{drive}_{dates}.csv'
    print(copycommand)
    p = subprocess.Popen(copycommand, shell=True)
    p.wait()
    if drive is None:
        return
    print(f'drive is {drive}')
    client.create_table_command(f'drop table if exists {drive}')
    client.create_table_command(create_table_command.format(drive=drive))

    with open(pathd, 'r', encoding='utf-8') as f:
        f.readline()
        line = f.readline()
        n, cache = 1, []
        while line:
            row = process_and_split(line)
            # print(row)
            row[1] = re.sub(r'\\', '/', row[1])
            row[2] = row[2] if row[2] else None
            row[3] = datetime.datetime.strptime(row[3], '%Y/%m/%d %H:%M') if row[3] and row[3] > '1971' else None
            row[4] = datetime.datetime.strptime(row[4], '%Y/%m/%d %H:%M') if row[4] and row[4] > '1971' else None
            row[5] = datetime.datetime.strptime(row[5], '%Y/%m/%d %H:%M') if row[5] and row[5] > '1971' else None
            row.append(now)
            cache.append(row)
            # client.insert('wd_4t', [row])

            if n % 2000 == 0:
                print(n)
                client.insert(drive, cache)
                cache = []
            line = f.readline()
            n += 1
        client.insert(drive, cache)

    # client.insert('wd_4t', data)


def find_in_all_ck_tables(content, only_folder=False):
    all_tables = client.command('show tables').split('\n')
    # sl = [re.compile(i, flags=re.IGNORECASE) for i in content.split(' ') if i]
    sl = '[' + ','.join([f"'{i}'" for i in content.split(' ') if i]) + ']'
    print(sl)

    for t in all_tables:
        accessory = ''
        if only_folder:
            accessory = "and attributes='D'"
        founded = client.query(f"select filename,location,dm from {t} where has(multiSearchAllPositionsCaseInsensitiveUTF8(filename,{sl}),0)=0 {accessory} limit 100")
        if founded.result_set:
            print(f'\n--------start--------\n【{t}】 table has what you want:')
            for i in founded.result_set:
                print(i)
            print('--------end--------')


# use_ck()
# u = folder_file_counting_command('HD004', '1')
# u = find_identical_file_command('hgst_4t', 'lenevo_1t')
# print(u)

# find_in_all_ck_tables('田真琴',only_folder=True)
