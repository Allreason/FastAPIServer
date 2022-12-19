SET mypath=%~dp0
echo %CD:~0,3%
echo %mypath:~0,3%
set mydate=%DATE:~0,10%

echo %mydate:/=-%
es.exe %mypath:~0,3%  -name -path-column -size -dm -dc -da -ext -attributes -export-csv %mypath:~0,3%everything%mydate:/=-%.csv
python C:\Users\aireason\Documents\code\ProcessEverything.py %mypath:~0,3%everything%mydate:/=-%.csv
pause
