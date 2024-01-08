@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM    http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM ----------------------------------------------------------------------------

@REM ----------------------------------------------------------------------------
@REM based on sbt-pack launch script
@REM ----------------------------------------------------------------------------

@echo off

@REM set %HOME% to equivalent of $HOME
if "%HOME%" == "" (set HOME=%HOMEDRIVE%%HOMEPATH%)

set ERROR_CODE=0

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM ==== START VALIDATION ====
if not "%JAVA_HOME%" == "" goto OkJHome

for /f %%j in ("java.exe") do (
  set JAVA_EXE="%%~$PATH:j"
  goto init
)

:OkJHome
if exist "%JAVA_HOME%\bin\java.exe" (
 SET JAVA_EXE="%JAVA_HOME%\bin\java.exe"
 goto init
)

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory. 1>&2
echo JAVA_HOME = %JAVA_HOME% 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation 1>&2
echo. 1>&2
goto error

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM -- 4NT shell
if "%@eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto endInit

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto endInit

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of agruments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto endInit
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

@REM Reaching here means variables are defined and arguments have been captured
:endInit

SET PROG_HOME=%~dp0..
SET PSEP=;

@REM Start Java program
:runm2
SET CMDLINE=%JAVA_EXE% REPLACE_JVM_OPTS %JAVA_OPTS% -Dprog.home="%PROG_HOME%" -Dprog.version="REPLACE_VERSION" -Dprog.revision="REPLACE_REVISION" -cp %PROG_HOME%\REPLACE_JAR;%EXTRA_CLASSPATH% REPLACE_MAIN_CLASS %CMD_LINE_ARGS%
%CMDLINE%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set JAVA_EXE=
set CMD_LINE_ARGS=
set CMDLINE=
set PSEP=
goto postExec

:endNT
@endlocal

:postExec
exit /B %ERROR_CODE%
