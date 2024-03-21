-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
module DA.Daml.LF.ScenarioServiceClient.LowLevel
  ( Options(..)
  , TimeoutSeconds
  , findServerJar
  , Handle
  , BackendError(..)
  , Error(..)
  , withScenarioService
  , ContextId
  , newCtx
  , cloneCtx
  , deleteCtx
  , gcCtxs
  , ContextUpdate(..)
  , SkipValidation(..)
  , updateCtx
  , runScenario
  , runScript
  , SS.ScenarioResult(..)
  , SS.WarningMessage(..)
  , SS.Location(..)
  , encodeScenarioModule
  , ScenarioServiceException(..)
  ) where

import Conduit (runConduit, (.|), MonadUnliftIO(..))
import Data.Either
import Data.Functor
import Data.Maybe
import Data.IORef
import GHC.Generics
import Text.Read
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import DA.Daml.LF.Mangling
import DA.Daml.Options.Types (EnableScenarios (..))
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Conduit as C
import Data.Conduit.Process
import qualified Data.Conduit.Text as C.T
import Data.Int (Int64)
import Data.List.Split (splitOn)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V
import Network.GRPC.HighLevel.Client (ClientError, ClientRequest(..), ClientResult(..), GRPCMethodType(..))
import Network.GRPC.HighLevel.Generated (withGRPCClient)
import Network.GRPC.LowLevel (ClientConfig(..), Host(..), Port(..), StatusCode(..), Arg(MaxReceiveMessageLength))
import qualified Proto3.Suite as Proto
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import qualified System.IO

import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import qualified ScenarioService as SS

data Options = Options
  { optServerJar :: FilePath
  , optJvmOptions :: [String]
  , optRequestTimeout :: TimeoutSeconds
  , optGrpcMaxMessageSize :: Maybe Int
  , optLogDebug :: String -> IO ()
  , optLogInfo :: String -> IO ()
  , optLogError :: String -> IO ()
  , optDamlLfVersion :: LF.Version
  , optEnableScenarios :: EnableScenarios
  }

type TimeoutSeconds = Int

data Handle = Handle
  { hClient :: SS.ScenarioService ClientRequest ClientResult
  , hOptions :: Options
  }

newtype ContextId = ContextId { getContextId :: Int64 }
  deriving (NFData, Eq, Show)

-- | If true, the scenario service server do not run package validations.
newtype SkipValidation = SkipValidation { getSkipValidation :: Bool }
  deriving Show

data ContextUpdate = ContextUpdate
  { updLoadModules :: ![(LF.ModuleName, BS.ByteString)]
  , updUnloadModules :: ![LF.ModuleName]
  , updLoadPackages :: ![(LF.PackageId, BS.ByteString)]
  , updUnloadPackages :: ![LF.PackageId]
  , updSkipValidation :: SkipValidation
  }

encodeScenarioModule :: LF.Version -> LF.Module -> BS.ByteString
encodeScenarioModule version m = case version of
    LF.V1{} -> BSL.toStrict (Proto.toLazyByteString (EncodeV1.encodeScenarioModule version m))

data BackendError
  = BErrorClient ClientError
  | BErrorFail StatusCode
  deriving Show

data Error
  = ScenarioError SS.ScenarioError
  | BackendError BackendError
  | ExceptionError SomeException
  deriving (Generic, Show)

instance NFData Error where
    rnf = rwhnf

findServerJar :: IO FilePath
findServerJar = do
  runfilesDir <- locateRunfiles (mainWorkspace </> "compiler/scenario-service/server")
  pure (runfilesDir </> "scenario-service.jar")

-- | Return the 'CreateProcess' for running java.
-- Uses 'java' from JAVA_HOME if set, otherwise calls java via
-- /usr/bin/env. This is needed when running under "bazel run" where
-- JAVA_HOME is correctly set, but 'java' is not in PATH.
javaProc :: [String] -> IO CreateProcess
javaProc args =
  lookupEnv "JAVA_HOME" <&> \case
    Nothing ->
      proc "java" args
    Just javaHome ->
      let javaExe = javaHome </> "bin" </> "java"
      in proc javaExe args

data ScenarioServiceException = ScenarioServiceException String deriving Show

instance Exception ScenarioServiceException

validateJava :: IO ()
validateJava = do
    getJavaVersion <- liftIO $ javaProc ["-version"]
    -- We could validate the Java version here but Java version strings are annoyingly
    -- inconsistent, e.g. you might get
    -- java version "11.0.2" 2019-01-15 LTS
    -- or
    -- openjdk version "1.8.0_181"
    -- so for now we only verify that "java -version" runs successfully.
    (exitCode, _stdout, stderr) <- readCreateProcessWithExitCode getJavaVersion "" `catch`
      (\(e :: IOException) -> throwIO (ScenarioServiceException ("Failed to run java: " <> show e)))
    case exitCode of
        ExitFailure _ -> throwIO (ScenarioServiceException ("Failed to start `java -version`: " <> stderr))
        ExitSuccess -> pure ()

-- | This is sadly not exposed by Data.Conduit.Process.
terminateStreamingProcess :: MonadIO m => StreamingProcessHandle -> m ()
terminateStreamingProcess = liftIO . terminateProcess . streamingProcessHandleRaw

-- | Variant of withCheckedProcessCleanup that gives access to the
-- StreamingProcessHandle.
withCheckedProcessCleanup'
    :: ( InputSource stdin
       , OutputSink stderr
       , OutputSink stdout
       , MonadUnliftIO m
       )
    => CreateProcess
    -> (StreamingProcessHandle -> stdin -> stdout -> stderr -> m b)
    -> m b
withCheckedProcessCleanup' cp f = withRunInIO $ \run -> bracket
    (streamingProcess cp)
    (\(_, _, _, sph) -> closeStreamingProcessHandle sph)
    $ \(x, y, z, sph) -> do
        res <- run (f sph x y z) `onException` terminateStreamingProcess sph
        ec <- waitForStreamingProcess sph
        if ec == ExitSuccess
            then return res
            else throwIO $ ProcessExitedUnsuccessfully cp ec

handleCrashingScenarioService :: IORef Bool -> StreamingProcessHandle -> IO a -> IO a
handleCrashingScenarioService exitExpected h act =
    -- `race` doesn’t quite work here since we might end up
    -- declaring an expected exit at the very end as a failure.
    -- In particular, once we close stdin of the scenario service
    -- `waitForStreamingProcess` can return before `act` returns.
    -- See https://github.com/digital-asset/daml/pull/1974.
    withAsync (waitForStreamingProcess h) $ \scenarioProcess ->
    withAsync act $ \act' -> do
        r <- waitEither scenarioProcess act'
        case r of
            Right a -> pure a
            Left _ -> do
                expected <- readIORef exitExpected
                if expected
                   then wait act'
                   else fail "Scenario service exited unexpectedly"

withScenarioService :: Options -> (Handle -> IO a) -> IO a
withScenarioService opts@Options{..} f = do
  optLogDebug "Starting scenario service..."
  serverJarExists <- doesFileExist optServerJar
  unless serverJarExists $
      throwIO (ScenarioServiceException (optServerJar <> " does not exist."))
  validateJava
  cp <- javaProc $ concat
    [ optJvmOptions
    , ["-jar" , optServerJar]
    , ["--max-inbound-message-size=" <> show size | Just size <- [optGrpcMaxMessageSize]]
    , ["--enable-scenarios=" <> show b | EnableScenarios b <- [optEnableScenarios]]
    ]

  exitExpected <- newIORef False
  let closeStdin hdl = do
          atomicWriteIORef exitExpected True
          System.IO.hClose hdl
  withCheckedProcessCleanup' cp $ \processHdl (stdinHdl :: System.IO.Handle) stdoutSrc stderrSrc ->
          flip finally (closeStdin stdinHdl) $ handleCrashingScenarioService exitExpected processHdl $ do
    let splitOutput = C.T.decode C.T.utf8 .| C.T.lines
    let printStderr line
            -- The last line should not be treated as an error.
            | T.strip line == "ScenarioService: stdin closed, terminating server." =
              liftIO (optLogDebug (T.unpack ("SCENARIO SERVICE STDERR: " <> line)))
            | otherwise =
              liftIO (optLogError (T.unpack ("SCENARIO SERVICE STDERR: " <> line)))
    let printStdout line = liftIO (optLogDebug (T.unpack ("SCENARIO SERVICE STDOUT: " <> line)))
    -- stick the error in the mvar so that we know we won't get an BlockedIndefinitedlyOnMvar exception
    portMVar <- newEmptyMVar
    let handleStdout = do
          mbLine <- C.await
          case mbLine of
            Nothing ->
              liftIO (putMVar portMVar (Left "Stdout of scenario service terminated before we got the PORT=<port> message"))
            Just (T.unpack -> line) ->
              case splitOn "=" line of
                ["PORT", ps] | Just p <- readMaybe ps ->
                  liftIO (putMVar portMVar (Right p)) >> C.awaitForever printStdout
                _ -> do
                  liftIO (optLogError ("Expected PORT=<port> from scenario service, but got '" <> line <> "'. Ignoring it."))
                  handleStdout
    withAsync (runConduit (stderrSrc .| splitOutput .| C.awaitForever printStderr)) $ \_ ->
        withAsync (runConduit (stdoutSrc .| splitOutput .| handleStdout)) $ \_ ->
        -- The scenario service will shut down cleanly when stdin is closed so we do this at the end of
        -- the callback. Note that on Windows, killThread will not be able to kill the conduits
        -- if they are blocked in hGetNonBlocking so it is crucial that we close stdin in the
        -- callback or withAsync will block forever.
        flip finally (closeStdin stdinHdl) $ do
            port <- either fail pure =<< takeMVar portMVar
            liftIO $ optLogDebug $ "Scenario service backend running on port " <> show port
            -- Using 127.0.0.1 instead of localhost helps when our packaging logic falls over
            -- and DNS lookups break, e.g., on Alpine linux.
            let grpcConfig = ClientConfig
                  { clientServerHost = Host "127.0.0.1"
                  , clientServerPort = Port port
                  , clientArgs = MaxReceiveMessageLength . fromIntegral <$> maybeToList optGrpcMaxMessageSize
                  , clientSSLConfig = Nothing
                  , clientAuthority = Nothing
                  }
            withGRPCClient grpcConfig $ \client -> do
                ssClient <- SS.scenarioServiceClient client
                f Handle
                    { hClient = ssClient
                    , hOptions = opts
                    }

newCtx :: Handle -> IO (Either BackendError ContextId)
newCtx Handle{..} = do
  res <- performRequest
      (SS.scenarioServiceNewContext hClient)
      (optRequestTimeout hOptions)
      (SS.NewContextRequest $ TL.pack $ LF.renderMinorVersion $ LF.versionMinor $ optDamlLfVersion hOptions)
  pure (ContextId . SS.newContextResponseContextId <$> res)

cloneCtx :: Handle -> ContextId -> IO (Either BackendError ContextId)
cloneCtx Handle{..} (ContextId ctxId) = do
  res <-
    performRequest
      (SS.scenarioServiceCloneContext hClient)
      (optRequestTimeout hOptions)
      (SS.CloneContextRequest ctxId)
  pure (ContextId . SS.cloneContextResponseContextId <$> res)

deleteCtx :: Handle -> ContextId -> IO (Either BackendError ())
deleteCtx Handle{..} (ContextId ctxId) = do
  res <-
    performRequest
      (SS.scenarioServiceDeleteContext hClient)
      (optRequestTimeout hOptions)
      (SS.DeleteContextRequest ctxId)
  pure (void res)

gcCtxs :: Handle -> [ContextId] -> IO (Either BackendError ())
gcCtxs Handle{..} ctxIds = do
    res <-
        performRequest
            (SS.scenarioServiceGCContexts hClient)
            (optRequestTimeout hOptions)
            (SS.GCContextsRequest (V.fromList (map getContextId ctxIds)))
    pure (void res)

updateCtx :: Handle -> ContextId -> ContextUpdate -> IO (Either BackendError ())
updateCtx Handle{..} (ContextId ctxId) ContextUpdate{..} = do
  res <-
    performRequest
      (SS.scenarioServiceUpdateContext hClient)
      (optRequestTimeout hOptions) $
      SS.UpdateContextRequest
          ctxId
          (Just updModules)
          (Just updPackages)
          (getSkipValidation updSkipValidation)
  pure (void res)
  where
    updModules =
      SS.UpdateContextRequest_UpdateModules
        (V.fromList (map convModule updLoadModules))
        (V.fromList (map encodeName updUnloadModules))
    updPackages =
      SS.UpdateContextRequest_UpdatePackages
        (V.fromList (map snd updLoadPackages))
        (V.fromList (map (TL.fromStrict . LF.unPackageId) updUnloadPackages))
    encodeName = TL.fromStrict . mangleModuleName
    convModule :: (LF.ModuleName, BS.ByteString) -> SS.ScenarioModule
    convModule (_, bytes) = SS.ScenarioModule bytes

mangleModuleName :: LF.ModuleName -> T.Text
mangleModuleName (LF.ModuleName modName) =
    T.intercalate "." $
    map (fromRight (error "Failed to mangle scenario module name") . mangleIdentifier) modName

runScenario :: Handle -> ContextId -> LF.ValueRef -> IO (Either Error SS.ScenarioResult)
runScenario Handle{..} (ContextId ctxId) name = do
  res <-
    performRequest
      (SS.scenarioServiceRunScenario hClient)
      (optRequestTimeout hOptions)
      (SS.RunScenarioRequest ctxId (Just (toIdentifier name)))
  pure $ case res of
    Left err -> Left (BackendError err)
    Right (SS.RunScenarioResponse (Just (SS.RunScenarioResponseResponseError err))) -> Left (ScenarioError err)
    Right (SS.RunScenarioResponse (Just (SS.RunScenarioResponseResponseResult r))) -> Right r
    Right _ -> error "IMPOSSIBLE: missing payload in RunScenarioResponse"

toIdentifier :: LF.ValueRef -> SS.Identifier
toIdentifier (LF.Qualified pkgId modName defn) =
  let ssPkgId = SS.PackageIdentifier $ Just $ case pkgId of
        LF.PRSelf     -> SS.PackageIdentifierSumSelf SS.Empty
        LF.PRImport x -> SS.PackageIdentifierSumPackageId (TL.fromStrict $ LF.unPackageId x)
      mangledDefn =
          fromRight (error "Failed to mangle scenario name") $
          mangleIdentifier (LF.unExprValName defn)
      mangledModName = mangleModuleName modName
  in
    SS.Identifier
      (Just ssPkgId)
      (TL.fromStrict $ mangledModName <> ":" <> mangledDefn)

runScript :: Handle -> ContextId -> LF.ValueRef -> IO (Either Error SS.ScenarioResult)
runScript Handle{..} (ContextId ctxId) name = do
  res <-
    performRequest
      (SS.scenarioServiceRunScript hClient)
      (optRequestTimeout hOptions)
      (SS.RunScenarioRequest ctxId (Just (toIdentifier name)))
  pure $ case res of
    Left err -> Left (BackendError err)
    Right (SS.RunScenarioResponse (Just (SS.RunScenarioResponseResponseError err))) -> Left (ScenarioError err)
    Right (SS.RunScenarioResponse (Just (SS.RunScenarioResponseResponseResult r))) -> Right r
    Right _ -> error "IMPOSSIBLE: missing payload in RunScriptResponse"

performRequest
  :: (ClientRequest 'Normal payload response -> IO (ClientResult 'Normal response))
  -> TimeoutSeconds
  -> payload
  -> IO (Either BackendError response)
performRequest method timeoutSeconds payload = do
  method (ClientNormalRequest payload timeoutSeconds mempty) >>= \case
    ClientNormalResponse resp _ _ StatusOk _ -> return (Right resp)
    ClientNormalResponse _ _ _ status _ -> return (Left $ BErrorFail status)
    ClientErrorResponse err -> return (Left $ BErrorClient err)
