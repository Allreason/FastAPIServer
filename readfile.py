import re

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

out = sql2dict('sql.md')
# print('not' in out)





