from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI,UploadFile
import re
import os
import readfile


app = FastAPI()

app.mount("/static", StaticFiles(directory="/tmp"), name="static")
app.mount("/script", StaticFiles(directory="."), name="script")

@app.post("/uploadfile/")
async def receive_file(file: UploadFile):
    batepath="/home/lighthouse/pokemon/"
    filename = file.filename
    print(filename)
    tenant = re.search(r'(^\d+)_',filename)
    print(tenant)
    if tenant:
        tenant=tenant.group(1)
        print(tenant)
        if not os.path.isdir(f"{batepath}{tenant}"):
            os.mkdir(f"{batepath}{tenant}")
    else:
        tenant=''

    dirt = ''
    if tenant:
        dirt=tenant+'/'


    with open(f"{batepath}{dirt}{filename}",'wb') as f:
        content = await file.read()
        f.write(content)
    return f"http://114.132.248.40:8888/{dirt}{filename}"

@app.post("/uploadfile/archive")
async def receive_file2(file: UploadFile):
    batepath="/home/lighthouse/pokemon/archive/"
    filename = file.filename
    print(filename)

    with open(f"{batepath}{filename}",'wb') as f:
        content = await file.read()
        f.write(content)
    return f"http://114.132.248.40:8888/archive/{filename}"

@app.get("/getsql/")
async def get_sql(title:str=''):
    m = readfile.sql2dict('sql.md')
    if not title:
        title='notitle'
    print(m)
    if title in m:
        return m[title]
    else:
        return m
    # return {'sql':d,'title':title}
