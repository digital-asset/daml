@echo off
setlocal
cd %~dp0\..
stack exec --stack-yaml=rattle/stack.yaml sh rattle/build.sh
