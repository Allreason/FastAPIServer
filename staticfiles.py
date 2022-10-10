from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI,UploadFile,Request,Form
import re
import os
import readfile
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse,RedirectResponse

app = FastAPI()

app.mount("/static", StaticFiles(directory="/tmp"), name="static")
app.mount("/script", StaticFiles(directory="."), name="script")

@app.post("/uploadfile/")
async def receive_file(file: UploadFile):
    basepath="/home/lighthouse/pokemon/"
    filename = file.filename
    print(filename)
    tenant = re.search(r'(^\d+)_',filename)
    print(tenant)
    if tenant:
        tenant=tenant.group(1)
        print(tenant)
        if not os.path.isdir(f"{basepath}{tenant}"):
            os.mkdir(f"{basepath}{tenant}")
    else:
        tenant=''

    dirt = ''
    if tenant:
        dirt=tenant+'/'


    with open(f"{basepath}{dirt}{filename}",'wb') as f:
        content = await file.read()
        f.write(content)
    return f"http://114.132.248.40:8888/{dirt}{filename}"

@app.post("/uploadfile/archive")
async def receive_file2(file: UploadFile):
    basepath="/home/lighthouse/pokemon/archive/"
    filename = file.filename
    print(filename)
    foldername=''
    if '-createfolder' in filename:
        print('create a folder for it')
        se = re.search(r'(.*?)-createfolder',filename)
        foldername = se.group(1)
        try:
            os.mkdir(basepath+foldername)
        except:
            print('folder exists')

    with open(f"{basepath}{foldername}/{filename}",'wb') as f:
        content = await file.read()
        f.write(content)
    return f"http://114.132.248.40:8888/archive/{foldername}/{filename}"

@app.post("/postform/")
async def postform(request:Request,text: str = Form(),type:str=Form()):
    print(type)
    if type=='append':
        return readfile.prepend_sql('sql.md',text)
    elif type=='substitute':
        u = await readfile.substitute_sql('sql.md',text)
        return
    elif type=='snappend':
        return readfile.prepend_sql('snlist.txt',text)
    elif type=='snsubstitute':
        u = await readfile.substitute_sql('snlist.txt',text)
        return

    #response=await sendsql(request)
    #return RedirectResponse('/sendsql/')

@app.get("/getsql/")
async def get_sql(title:str='',isgettitle:int=0):
    print(str.encode(title),isgettitle)
    m = readfile.sql2dict('sql.md')
    if title=='notitle' or not title:
        # return the first one
        for i,j in m.items():
            if isgettitle == 0:
                return j
            else:
                if i=='notitle':
                    return ''
                return i
    elif title:
        title=title.strip('"') 
        if title in m:
            print(f'########{title}')
            if isgettitle == 0:
                return m[title]
            else:
                return title


templates = Jinja2Templates(directory="templates")

@app.get("/sendsql/",response_class=HTMLResponse)
async def sendsql(request: Request):
    return templates.TemplateResponse("sql2.html",{"request": request,"sql":readfile.directlyread('sql.md')})

@app.get("/sendsnlist/",response_class=HTMLResponse)
async def sendsql(request: Request):
    return templates.TemplateResponse("sn.html",{"request": request,"sql":readfile.directlyread('snlist.txt'),"filetype":"sn"})
