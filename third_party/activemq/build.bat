@echo off
set pluginname=activemq
set pluginversion=1.0.0

mkdir classes\scalabpe\plugin\  2>>nul 1>>nul
call scalac -deprecation -encoding "UTF-8" -classpath "lib\*;..\..\lib\*;.\*"  -d "./classes"  ./src/*.scala
copy /Y src\scalabpe.plugins.conf .\classes\   2>>nul 1>>nul
copy /Y src\release_notes.txt .\classes\   2>>nul 1>>nul
jar cf ./lib/scalabpe-plugin-%pluginname%-%pluginversion%.jar -C ./classes/  ./scalabpe/plugin -C ./classes/ ./scalabpe.plugins.conf -C ./classes/ ./release_notes.txt
