We can create projects using the 'new' command:
  $ echo 1 | da list > /dev/null 2>&1
  $ da new my_project
  Creating a project called my_project
  in .*/my_project. (re)

In the new project we can check the 'status':

  $ cd my_project
  $ find . -type f | sort
  ./da.yaml
  ./daml/Main.daml
  $ da --term-width 800 status
  Welcome to the SDK Assistant.
  You are currently in the project 'my_project' \(.*\/my_project\)\. (re)
  No processes are running. To start them, type: da start

We can start and stop services:

  $ da --term-width 800 start sandbox
  \[Info\] Starting: Sandbox ledger server .*/my_project/daml/Main\.daml with scenario Main\.example and binding to port 7... (re)
  Created .*\/03-new-bt.t\/my_project\/target\/my_project.dar. (re)
  Waiting for Sandbox(\.*)ok (re)

  $ da --term-width 800 start
  \[Info\] Starting: Navigator web app using localhost \(....\) and binding to port .... (re)
  Waiting for Navigator\.*ok (re)
  \[Info\] Starting: web browser on URL http://localhost:7... (re)

  $ da --term-width 800 stop navigator
  stopping... Navigator web app using localhost \(7...\) and binding to port 7... (re)


  $ da --term-width 800 status
  Welcome to the SDK Assistant.
  You are currently in the project 'my_project' \(.*/my_project\)\. (re)
  The following processes are running:
  
  Sandbox ledger server .*/03-new-bt\.t/my_project/daml/Main\.daml with scenario Main\.example and binding to port 7... (re)
  

  $ da --term-width 800 start navigator
  \[Info\] Starting: Navigator web app using localhost \(....\) and binding to port .... (re)
  Waiting for Navigator\.*ok (re)
  \[Info\] Starting: web browser on URL http://localhost:7... (re)

  $ da --term-width 800 stop
  stopping\.\.\. Navigator web app using localhost \(7...\) and binding to port 7... (re)
  stopping\.\.\. Sandbox ledger server .*/03-new-bt\.t/my_project/daml/Main\.daml with scenario Main\.example and binding to port 7... (re)

  $ da --term-width 800 stop
  No process is running.