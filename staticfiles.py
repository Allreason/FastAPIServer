from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI,UploadFile,Request,Form
import re
import os
import readfile
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

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

@app.post("/postform/")
async def postform(text: str = Form()):
    
    return readfile.prepend_sql('sql.md',text)

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

templates = Jinja2Templates(directory="templates")

@app.get("/sendsql/",response_class=HTMLResponse)
async def sendsql(request: Request):
    return templates.TemplateResponse("sql.html",{"request": request,"sql":readfile.directlyread('sql.md')})