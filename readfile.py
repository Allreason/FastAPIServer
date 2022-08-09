import re
import datetime

def sql2list(filepath):
    ret = []
    with open(filepath,'r') as f:
        ts=f.readline()
        line = f.readline()
        while line:
            if re.match(r'# .*\n',line):
                ret.append(ts)
                ts=line
            else:
                ts = ts + line
            line=f.readline()
        ret.append(ts)
    # for r in ret:
    #     print(r)
    return ret

def sql2dict(filepath):
    mapping = {}
    ret = []
    for i in sql2list(filepath):
        if i:
            k = re.search(r'(?sm)^# ([^\n]*?)\n(.*)',i)
            if k:
                heading = k.group(1)
                print(heading)
                ret.append([heading,k.group(2)])
                mapping[heading]=k.group(2)
            else:
                ret.append(['notitle',i])
                mapping['notitle']=i
    return mapping

def directlyread(filepath):
    with open(filepath,'r') as f:
        l = f.read().split('\n')
        n = []
        for i in l:
            if re.match(r'# .*',i):
                i=f'<h2>{i}</h2>'
            n.append(i)
        # return ''.join(l)
        return l

def prepend_sql(filepath,str):
    ct = datetime.datetime.now().isoformat()
    with open(filepath,'r+') as f:
        f.seek(0)
        firstline = f.readline()
        all = f.read()
        if re.match(r'^# ',firstline):
            fl = ''
        else:
            fl = f'# before {ct}\n'
        concated = str+'\n'+fl+firstline+all
        f.seek(0)
        f.write(concated)
    return str


out = sql2dict('sql.md')
# print('not' in out)





