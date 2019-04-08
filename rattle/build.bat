@echo off
setlocal
cd %~dp0\..
stack exec sh rattle/build.sh
