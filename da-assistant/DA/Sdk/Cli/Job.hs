-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | This module defines a function that will run subcommands in the background.

module DA.Sdk.Cli.Job
  (
    getProcesses
  , isProjectProcess
  , runJob
  , runSandbox
  , runNavigator
  , runCompile
  , start
  , stop
  , restart
  ,copyDetailsFromOldConf

  , isSDKVersionEligibleForNewApiUsage
  , module DA.Sdk.Cli.Job.Types
  , JobException
  ) where

import           Control.Exception         (SomeException, IOException, catch, bracket, throwIO)
import           "cryptohash" Crypto.Hash               (Digest, MD5 (..), hash)
import           DA.Sdk.Cli.Navigator.Conf as NavConf
import qualified Data.ByteString           as BS
import           Data.Maybe                (catMaybes)
import qualified Data.Text.Encoding        as T
import qualified Data.Text.Extended        as T
import qualified Data.Text.IO              as T
import qualified Data.Text                 as DT
import qualified Data.Yaml                 as Y
import qualified Network.HTTP.Client       as HTTP
import qualified Network.HTTP.Types        as HTTP
import qualified Network.Socket            as NS
import qualified Network.Socket.ByteString as NSB
import           DA.Sdk.Cli.System         (exitFailure, runAsDaemonProcess, startNewProcess)
import           System.Directory          (createDirectoryIfMissing)
import           System.Directory.Extra    (listFilesRecursive)
import           System.FilePath           (takeExtension)
import           Turtle                    (sleep, testfile, (<.>))
import qualified Turtle

import qualified DA.Sdk.Cli.Command.Types  as Command.Types
import           DA.Sdk.Cli.Conf           (Project (..), Conf(confOutputPath))
import qualified DA.Sdk.Cli.Env            as Env
import           DA.Sdk.Cli.Job.Types
import qualified DA.Sdk.Cli.Monad.Locations as Loc
import qualified DA.Sdk.Cli.Locations      as L
import qualified DA.Sdk.Cli.Locations.Turtle as LT
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Cli.OS             (OS (..), currentOS)
import           DA.Sdk.Cli.Paths          (findExecutable, findPath, PackageExecutable(PEJar))
import           DA.Sdk.Cli.Project        (requireProject)
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty             as P
import qualified DA.Sdk.Version            as V
import qualified DA.Sdk.Cli.SdkVersion     as V
import           System.Timeout
import           Network.Socket
import           Control.Exception.Safe    (Exception, Typeable, try, throwString)
import           DA.Sdk.Cli.Monad.UserInteraction
import           Control.Monad.Extra
import           DA.Sdk.Data.Either.Extra
import qualified Control.Foldl             as Fold
import           Control.Monad.Except
import           DA.Sdk.Cli.Monad.FileSystem (MkTreeFailed)
import           Data.Either.Combinators 

data JobException = NoFreePortFound Int Int
    deriving (Typeable, Show)

instance Exception JobException

hexMD5 :: BS.ByteString -> Text
hexMD5 bs = T.pack $ show (hash bs :: Digest MD5)

isProjectProcess :: Project -> Process -> Bool
isProjectProcess proj proc = projectDir == processDir
  where
    projectDir = projectPath proj
    processDir = fromProjectPath (processProjectPath proc)

processes :: (FilePath -> Process -> a) ->  CliM [a]
processes fn = do
    procPath <- Loc.getProcPath
    exists <- LT.testdir procPath
    if exists
      then do
        inPath <- LT.ls procPath
        files <- filterM isProcessFile inPath
        fmap catMaybes $ liftIO $ forM (sort files) $ \f -> do
          mbProcess <- either (const Nothing) Just <$> Y.decodeFileEither (pathToString f)
          return $ fmap (fn f) mbProcess
      else
        return []
  where
     isProcessFile file = do
         isFile <- testfile file
         return $ isFile && extension file == Just "yaml"

getProcesses :: CliM [(FilePath, Process)]
getProcesses = processes $ \f p -> (f, p)


serviceToJob :: Command.Types.Service -> CliM [Job]
serviceToJob Command.Types.SandboxService = projectSandbox
serviceToJob Command.Types.NavigatorServices = allJobs
serviceToJob Command.Types.All = allJobs

allJobs :: CliM [Job]
allJobs = do
  proj <- asks envProject
  case proj of
    Nothing -> return []
    Just p  -> projectJobs p

isRunning :: Job -> CliM Bool
isRunning j = do
  procs <- processes $ curry snd
  return $ j `elem` fmap processJob procs

restart :: Command.Types.Service -> CliM ()
restart svc = do
    mbSvcsStopped <- stop' svc
    sleep 2
    case mbSvcsStopped of
        Nothing ->
            throwM $ UserError "You have to run restart in a project."
        Just svcsStopped -> do
            -- we need to restart all the stopped services
            forM_ (nubOrd $ svc:svcsStopped) start

-- | Handle start request
start :: Command.Types.Service -> CliM ()
start svc = do
  mbProject <- asks envProject
  case mbProject of
    Nothing -> display "You have to run services from a project"
    Just _project -> do
      jobs <- serviceToJob svc
      runnables <- filterM (fmap not . isRunning) jobs
      forM_ runnables runJob

-- | Handle stop request
stop :: Command.Types.Service -> CliM ()
stop svc = do
    mbStopped <- stop' svc
    case mbStopped of
      Nothing -> throwM $ UserError "You have to run stop in a project."
      Just [] -> throwM $ UserInfo "No process is running."
      Just _s -> return ()

-- | Handle stop request returning the list of services stopped
-- svc =/= return in some cases because this function also stops
-- dependency services, see @selectProcAndDepsForSvc
stop' :: Command.Types.Service -> CliM (Maybe [Command.Types.Service])
stop' svc = do
  mbProject <- asks envProject
  case mbProject of
    Nothing -> return Nothing
    Just p  -> do
      procs <- fmap (filter (isProjectProcess p . snd)) getProcesses
      Just <$>
       (forM (selectProcAndDepsForSvc procs) $
            \(path, proc, svcStoped) -> do
                t <- renderPretty $ describeJob $ processJob proc
                display $ "stopping... " <> t
                stopProcess (path, proc)
                return svcStoped)
  where
    selectProcAndDepsForSvc = mapMaybe
      (\(path, proc) ->
        case svc of
          Command.Types.All ->
            Just (path, proc, Command.Types.All)
          Command.Types.SandboxService ->
            case processJob proc of
              JobSandbox _ ->
                Just (path, proc, Command.Types.SandboxService)
              -- Stopping Sandbox also stops Navigator:
              JobNavigator _ ->
                Just (path, proc, Command.Types.NavigatorServices)
              _ ->
                Nothing
          Command.Types.NavigatorServices ->
            case processJob proc of
              JobNavigator _ ->
                Just (path, proc, Command.Types.NavigatorServices)
              _ ->
                Nothing)

stopProcess :: (FilePath, Process) -> CliM ()
stopProcess (fp, process) = do
    errOrOk :: Either SomeException ()
        <- liftIO $ try $ Turtle.sh $ Turtle.inprocWithErr "/bin/kill" [procPid] empty
    whenJust (getLeft errOrOk) $
       \err -> whenM checkIfStillRunningWithPs $ do
                    logError $ "Unable to kill process: " <> (jobName $ processJob process)
                    liftIO $ throwIO err
    Turtle.rm fp
  where
    procPid = T.pack $ show $ processPid process
    checkIfStillRunningWithPs = do
      errOrOutput :: Either SomeException Turtle.Line
          <- liftIO $ try $ Turtle.fold
                  (Turtle.inproc "ps" ["-o", "pid=", "-p", procPid] Turtle.empty)
                  Fold.mconcat
      case errOrOutput of
        Right output ->
          return $ procPid `T.isInfixOf` Turtle.lineToText output
        _err ->
          return False

getDAMLEntryFile :: Project -> CliM FilePath
getDAMLEntryFile proj = do
    rootPath <- Turtle.realpath (projectPath proj)
    let damlSource = rootPath </> textToPath (projectDAMLSource proj)
    isPath <- Turtle.testpath damlSource
    isFile <- Turtle.testfile damlSource
    case (isPath, isFile) of
      (False, _) ->
          -- TODO: Better error handling (custom Exception type)
          error $ "Could not find the specified DAML entry point " <> show damlSource
      (True, True) -> return damlSource
      -- TODO: Probably deprecate the use of DAML source as just a folder. Add a
      -- warning here for 30 days or so before removing/simplifying.
      (True, False) -> return $ damlSource </> textToPath "Main.daml"

isSDKVersionEligibleForNewApiUsage :: Project -> Bool
isSDKVersionEligibleForNewApiUsage proj =
       projSdkVsn >= V.SemVersion 0 9 0 Nothing
    || projSdkVsn == V.SemVersion 0 0 0 Nothing
  where
    projSdkVsn = projectSDKVersion proj

projectJobs :: Project -> CliM [Job]
projectJobs proj = do
  damlEntryFile <- getDAMLEntryFile proj
  runningSandboxPort <- getProjectSandboxPort proj
  runningNavPort     <- getProjectNavigatorPort proj
  let
        configText =
            P.renderPlain $ NavConf.navigatorConfig (fmap NavConf.partyToUser (projectParties proj))
        confFile = projectPath proj </> ".navigator.conf"
  mbNewNavPort  <- liftIO $ findFreePort 20 7500
  mbSandboxPort <- liftIO $ findFreePort 20 6865
  sandboxPort   <- liftIO $ maybe (throwIO $ NoFreePortFound 6865 20) return (runningSandboxPort <|> mbSandboxPort)
  navPort       <- liftIO $ maybe (throwIO $ NoFreePortFound 7500 20) return (runningNavPort <|> mbNewNavPort)
  let useNewApiComponents = isSDKVersionEligibleForNewApiUsage proj
      navigatorApiUrl     = if useNewApiComponents then NewApiUrlAndPort "localhost" sandboxPort else OldApiUrlWithPort ("http://localhost:" <> (T.pack $ show sandboxPort) <> "/v0")
  liftIO $ T.writeFile (pathToString confFile) configText
  return
    [ JobSandbox $ Sandbox useNewApiComponents (projectProjectName proj) (pathToText $ projectPath proj)
                    (pathToText damlEntryFile) (map pathToText $ projectDarDependencies proj) sandboxPort (projectDAMLScenario proj)
    , JobNavigator $ Navigator navPort navigatorApiUrl (pathToText confFile)
    , JobWebsite $ "http://localhost:" <> T.pack (show navPort)
    ]

runNavigator :: CliM ()
runNavigator = start Command.Types.NavigatorServices

projectSandbox :: CliM [Job]
projectSandbox = do
  mbProj <- asks envProject
  case mbProj of
    Nothing -> return []
    Just p -> do
       mbPort <- getProjectSandboxPort p
       damlEntryFile <- getDAMLEntryFile p
       mbNewSandboxPort <- liftIO $ findFreePort 20 6865
       sandboxPort <- liftIO $ maybe (throwIO $ NoFreePortFound 6865 20) return (mbPort <|> mbNewSandboxPort)
       let useJavaSandbox = isSDKVersionEligibleForNewApiUsage p
       return [JobSandbox $ Sandbox useJavaSandbox (projectProjectName p) (pathToText $ projectPath p)
                                (pathToText damlEntryFile) (map pathToText $ projectDarDependencies p)
                                sandboxPort (projectDAMLScenario p)]

getProjectSandboxPort :: Project -> CliM (Maybe Int)
getProjectSandboxPort p =
  listToMaybe . mapMaybe (getSandboxPort .  processJob) . filter (isProjectProcess p) <$>
  (processes $  \_ pr -> pr)
  where getSandboxPort = \case
          (JobSandbox s) -> Just (sandboxPort s)
          _ -> Nothing

getProjectNavigatorPort :: Project -> CliM (Maybe Int)
getProjectNavigatorPort p =
  listToMaybe . mapMaybe (getNavPort .  processJob) . filter (isProjectProcess p) <$>
  (processes $  \_ pr -> pr)
  where getNavPort = \case
          (JobNavigator n) -> Just (navigatorPort n)
          _ -> Nothing

runSandbox :: CliM ()
runSandbox = start Command.Types.SandboxService
-- | Start a job. This should be improved to throw a JobException or have a
-- return type for different kinds of failures. The caller should then be
-- responsible for printing help, ignoring, or propagating exceptions as
-- applicable.
runJob :: Job -> CliM ()
runJob job = do
    running <- isRunning job
    unless running $ renderPretty (P.label "Starting" $ describeJob job) >>= logInfo
    case job of
        JobNavigator Navigator {..} -> do
            Env.errorOnMissingBinaries
                [ Env.Javac Env.requiredMinJdkVersion
                ]
            navigatorJarPath <- findPath "navigator" Command.Types.PPQExecutable
            -- java -jar navigator server localhost 6865
            let args  =
                    [ "java", "-jar", pathToText navigatorJarPath
                    , "server"
                    , "--config-file", navigatorConfigFile
                    , "--port", T.show navigatorPort
                    ]
                args' = case navigatorUrl of
                        NewApiUrlAndPort url apiPort ->
                            args <> [ url, T.show apiPort ]
                        OldApiUrlWithPort urlWApiPort ->
                            args <> [ urlWApiPort ]
            void $ runAsDaemon job "/usr/bin/env" args'
        JobSandbox Sandbox {..} | sandboxUseJavaVersion -> do
            (exitCode, darRoot) <- runCompileFromRoot (textToPath sandboxProjectRoot)
                                        (textToPath sandboxDAMLFile) sandboxProjectName
            allFiles <- liftIO $ listFilesRecursive $ pathToString darRoot
            case exitCode of
                Turtle.ExitSuccess -> do
                    mbSandboxJar <- findJavaSandbox
                    case mbSandboxJar of
                      Nothing       -> display "Java sandbox not found."
                      Just newSbJar -> let command = "/usr/bin/env"
                                           scenarioArgs = maybe [] (\s -> ["--scenario", s]) sandboxScenario
                                           args0   = [ "java"
                                                     , "-jar"
                                                     , newSbJar
                                                     , "--no-parity"
                                                     , "--port"
                                                     , T.show sandboxPort
                                                     ]
                                           darArg =
                                             [T.pack file | file <- allFiles, takeExtension file `elem` [".dar", ".dalf"]]
                                           args   = args0 <> scenarioArgs <> darArg <> sandboxDarDeps
                                        in unless running $ void $ runAsDaemon job command args
                Turtle.ExitFailure e -> do
                    display ("Error at daml LF compilation, exit code: " <> (T.pack $ show e))
                    liftIO exitFailure
        JobSandbox Sandbox {..} -> do
            damlcPath <- findPath "damlc" Command.Types.PPQExecutable
            let scenarioArgs = maybe [] (\s -> ["--scenario", s]) sandboxScenario
                args =
                    [ "sandbox"
                    , sandboxDAMLFile
                    , "--port"
                    , T.show sandboxPort
                    ] <> scenarioArgs
            unless running $ void $ runAsDaemon job damlcPath args
        JobStudio path -> Turtle.realpath (textToPath path) >>= runDAMLStudio
        JobWebsite url -> openResource url
        JobResource name -> openResource name

runCompile :: CliM ()
runCompile = do
  proj <- requireProject
  void $
    runCompileFromRoot
      (projectPath proj)
      (textToPath $ projectDAMLSource proj)
      (projectProjectName proj)

runCompileFromRoot :: FilePath -> FilePath -> Text -> CliM (Turtle.ExitCode, FilePath)
runCompileFromRoot projectRoot damlFile name = do
    confOutPath <- asks (confOutputPath . envConf)
    damlcPath <- findPath "damlc" Command.Types.PPQExecutable
    -- Compilation of Daml LF, Sandbox only accepts that.
    let damlcPath' = pathToText damlcPath
        defaultTarget = projectRoot </> "target"
        darRoot = fromMaybe defaultTarget confOutPath
        darName = textToPath name <.> "dar"
        darPath = pathToText (darRoot </> darName)
    when (darRoot == defaultTarget) $
        liftIO $ createDirectoryIfMissing False $ pathToString defaultTarget
    mbActiveVersion <- V.getActiveSdkVersion
    let lfVsnFlags = if
            | mbActiveVersion == sdkVHead -> []
            | mbActiveVersion < sdkV010 -> ["--force-lf-v1"]
            | mbActiveVersion < sdkV011 -> ["-f", "lf1"]
            | otherwise -> []
    -- Example call: da-hs-damlc-app package A.daml modname -o /tmp/modname.dar
    exitCode <- Turtle.proc damlcPath' ([ "package"
                                        , pathToText damlFile
                                        , name
                                        ] ++
                                        lfVsnFlags ++
                                        [ "-o"
                                        , darPath
                                        ]) empty
    return (exitCode, darRoot)
  where
    sdkVHead = Just $ V.SemVersion 0 0 0 Nothing
    sdkV010 = Just $ V.SemVersion 0 10 0 Nothing
    sdkV011 = Just $ V.SemVersion 0 11 0 Nothing

findJavaSandbox :: CliM (Maybe Text)
findJavaSandbox = do
    executable <- findExecutable "sandbox"
    case executable of
        PEJar packagePath execPath ->
                   return $ Just $ pathToText (packagePath </> execPath)
        _  -> return Nothing

runDAMLStudio :: FilePath -> CliM ()
runDAMLStudio path = do
    let (cmd, args) = case currentOS of
          MacOS   -> ("open", ["-a", "Visual Studio Code", pathToText path])
          Linux   -> ("code", [pathToText path])
          Windows -> ("cmd", ["/C code " <> pathToText path])
          _       -> error "Unsupported OS"
    exitCode :: Either SomeException Turtle.ExitCode
        <- liftIO $ try $ Turtle.proc cmd args empty
    case either (const $ Turtle.ExitFailure 1) id exitCode of
        Turtle.ExitSuccess -> pure ()
        Turtle.ExitFailure _ -> do
            display "DAML Studio could not be started."
            display "Please check if you have VS Code installed."
            displayNewLine
            display "You can install VS code from here:"
            display "https://code.visualstudio.com/"

getLogPath :: Project -> Job -> CliM (Either MkTreeFailed FilePath)
getLogPath proj job = runExceptT $ do
    logDir <- ExceptT $ Loc.getProjectLogPath proj
    return $ logDir </> (textToPath $ jobName job <> ".log")

runAsDaemon :: Job -> FilePath -> [Text] -> CliM Process
runAsDaemon job command args = do
    proj <- requireProject
    let path = ProjectPath $ projectPath proj
    procPath <- Loc.getProcPath
    errOrLogPath <- getLogPath proj job
    logPath <- either (throwString . show) return errOrLogPath
    let projectHash = hexMD5 $ T.encodeUtf8 $ pathToText $ projectPath proj
        pidPath = L.unwrapFilePathOf procPath
             </> (textToPath $ T.take 8 projectHash <> "-" <> jobName job <> ".yaml")
             -- TODO introduce a pid path type

    logDebug $ "Executing: " <> pathToText command <> " " <> (T.pack $ show args)
    let writePidFile = \pid -> Y.encodeFile (pathToString pidPath) $ Process (fromIntegral pid) path job
    liftIO $ runAsDaemonProcess procPath logPath command (fmap T.unpack args) writePidFile
    -- Wait for the job to be properly running
    liftIO $ waitUntilRunning job `catch`
        (\(_ex :: SomeException) -> do
          display $ "Failed to start " <> jobName job
          displayStr $ displayException _ex
          logTail <- unlines . reverse . take 20 . reverse . lines <$> readFile (pathToString logPath)
          display "------ last 20 lines from log ------"
          displayStr logTail
          displayStr "------------------------------------"

          -- Make sure the process is not running and remove the pid file
          mbProcess <- liftIO $ either (const Nothing) Just <$> Y.decodeFileEither (pathToString pidPath)
          case mbProcess of
            Just process ->
                Turtle.sh $ Turtle.inprocWithErr "/bin/kill" [T.pack $ show $ processPid process] empty
            Nothing -> pure ()
          Turtle.rm pidPath
          exitFailure
        )

    -- Verify that we can read the process file
    waitTillFileIsThere pidPath (6 :: Int) ("Command's (" <> pathToString command <> ") " <>
                                            "pid file was not created: " <> pathToString pidPath)
    liftIO (Y.decodeFileEither (pathToString pidPath)) >>= \case
      Left er -> do
        logDebug $ T.pack $ Y.prettyPrintParseException er
        error "Could not read process file. Run with '--log-level debug' for more info"
      Right p  -> return p
  where
    waitTillFileIsThere fPath secondsLeft err = do
      if secondsLeft == 0
        then error err
        else do
            pidFileExists <- testfile fPath
            when (not pidFileExists) $
                 sleep 1 >> waitTillFileIsThere fPath (secondsLeft - 1) err

waitUntilRunning :: Job -> IO ()
waitUntilRunning (JobSandbox sandbox)
  | sandboxUseJavaVersion sandbox =
    waitForConnectionOnPort "Sandbox" 15 (sandboxPort sandbox)
waitUntilRunning (JobSandbox sandbox) =
    waitForHttpServer "Sandbox" $ "http://localhost:" <> show (sandboxPort sandbox) <> "/v0/templates"
waitUntilRunning (JobNavigator nav)  =
    waitForHttpServer "Navigator" $ "http://localhost:" <> show (navigatorPort nav)
waitUntilRunning _ = pure ()

waitForHttpServer :: String -> String -> IO ()
waitForHttpServer desc url = do
    putStr $ "Waiting for " <> desc <> "."
    manager <- HTTP.newManager HTTP.defaultManagerSettings
    request <- HTTP.parseRequest $ "HEAD " <> url
    tryRequest manager request (30 :: Int)
    displayStr "ok"
  where
    tryRequest _ _ 0 = do
        displayStr $ "Timed out while waiting for " <> desc <> " to start"
        error "timed out"
    tryRequest manager request n = do
        putStr "."
        exOrResponse <- try $ HTTP.httpNoBody request manager :: IO (Either SomeException (HTTP.Response ()))
        case exOrResponse of
          Right response
              | HTTP.statusCode (HTTP.responseStatus response) == 200 -> return ()
          _oterwise -> do
              sleep 1
              tryRequest manager request (n-1)

waitForConnectionOnPort :: String -> Int -> Int -> IO ()
waitForConnectionOnPort desc seconds port = do
    putStr $ "Waiting for " <> desc <> "."
    let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
    addrInfos <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just $ show port)
    addr <- maybe (connErr "cannot get address info") return $ headMay addrInfos
    result <- timeout (seconds * 1000000) $ checkConnection addr
    case result of
      Just () ->
        displayStr "ok"
      Nothing ->
        connErr ("timeout after " ++ show seconds ++ " seconds")
  where
    errMsg = "Unable to connect to Sandbox on port " <> show port <> ": "
    connErr cause = throwIO $ userError (errMsg <> cause)
    checkConnection addr = do
        putStr "."
        result <- try $ checkConnectionOnce addr
        case result of
          Right () ->
            return ()
          Left (_err :: IOException) -> do
            sleep 1
            checkConnection addr
    checkConnectionOnce addr = do
      bracket (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
              close (\sock -> connect sock (addrAddress addr))
         
-- | Attempt to open the given resource using the OS's general resource
-- launcher. Returns Nothing if there is no known resource launcher and Just
-- ExitCode if there is one.
openResource :: Text -> CliM ()
openResource resourceName = do
    logDebug $ "Attempting to open resource '" <> resourceName <> "'."
    let strRes = T.unpack resourceName
    let mbOpenCommandAndArgs = case currentOS of
          MacOS   -> Just ("open", [strRes])
          Linux   -> Just ("xdg-open", [strRes])
          Windows -> Just ("cmd", ["/C start " <> strRes])
          _       -> Nothing
    case mbOpenCommandAndArgs of
      Nothing -> do
        display $ DT.pack ("Failed to open the resource, try pointing the browser to " ++ strRes)
      Just (openCommand, args) -> do 
        result <-  liftIO $ startNewProcess openCommand args
        whenLeft result $ \err ->
          display $ DT.pack ("Failed with " ++ show err ++  
          " \n To access the resource, try pointing the browser to " ++ strRes)

-- copy details from da.yml to daml-project.daml
copyDetailsFromOldConf :: Maybe Project -> IO (Either IOException ())
copyDetailsFromOldConf mbProject = do
  case mbProject of
    Just proj -> do
      let sdkVersion = V.showSemVersionCompatible $ projectSDKVersion proj
      let versionObject = Y.object [("sdk-version", Y.String sdkVersion)]
      try $ liftIO (Y.encodeFileWith Y.defaultEncodeOptions "daml.yaml" versionObject)
    Nothing -> return $ Left (userError "Command must be run from within a project")

-- Find the next free TCP port
findFreePort :: Int -> Int -> IO (Maybe Int)
findFreePort remainingRetries actualPort = do
    inUse <- isPortInUse actualPort
    if inUse
      then if remainingRetries > 0
              then findFreePort (remainingRetries-1) (actualPort+1)
              else return Nothing
      else return $ Just actualPort
  where
    -- Check if the given port is in use by connecting to it.
    isPortInUse testPort = do
        bracket
          (NS.socket NS.AF_INET NS.Stream NS.defaultProtocol)
          NS.close
          (\sock -> do
              (ainfo:_) <- NS.getAddrInfo
                Nothing (Just "127.0.0.1") (Just $ show testPort)
              NS.connect sock $ NS.addrAddress ainfo
              void $ NSB.send sock "test"
              return True)
          `catch`
          (\(_e :: IOException) -> return False)

instance P.Pretty JobException where
  pretty = \case
    NoFreePortFound port range ->
        P.t $ "No free port found in the range "
                <> (T.pack $ show port)
                <> "-"
                <> (T.pack $ show $ port + range)
