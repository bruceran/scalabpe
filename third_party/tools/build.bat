@echo off
set pluginname=tools
set pluginversion=0.1.2

mkdir classes\scalabpe\  2>>nul 1>>nul
call scalac -deprecation -encoding "UTF-8" -classpath "lib\*;..\..\lib\*;.\*"  -d "./classes"  ./src/*.scala
copy /Y src\release_notes.txt .\classes\   2>>nul 1>>nul
jar cf ./lib/scalabpe-%pluginname%-%pluginversion%.jar -C ./classes/  ./scalabpe -C ./classes/ ./release_notes.txt
cp ./lib/scalabpe-%pluginname%-%pluginversion%.jar ../../lib/
