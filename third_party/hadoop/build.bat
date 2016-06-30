@echo off
set pluginname=hadoop
set pluginversion=1.0.0

mkdir classes\jvmdbbroker\plugin\  2>>nul 1>>nul
call scalac -deprecation -encoding "UTF-8" -classpath "lib\*;..\..\lib\*;.\*"  -d "./classes"  ./src/*.scala
copy /Y src\jvmdbbroker.plugins.conf .\classes\   2>>nul 1>>nul
copy /Y src\release_notes.txt .\classes\   2>>nul 1>>nul
jar cf ./lib/scalabpe-plugin-%pluginname%-%pluginversion%.jar -C ./classes/  ./jvmdbbroker/plugin -C ./classes/ ./jvmdbbroker.plugins.conf -C ./classes/ ./release_notes.txt
