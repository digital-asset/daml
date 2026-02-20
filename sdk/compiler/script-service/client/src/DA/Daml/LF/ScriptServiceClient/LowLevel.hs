-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
module DA.Daml.LF.ScriptServiceClient.LowLevel
  ( Options(..)
  , TimeoutSeconds
  , findServerJar
  , Handle
  , BackendError(..)
  , Error(..)
  , withScriptService
  , ContextId
  , newCtx
  , cloneCtx
  , deleteCtx
  , gcCtxs
  , ContextUpdate(..)
  , SkipValidation(..)
  , updateCtx
  , runLiveScript
  , SS.ScriptResult(..)
  , SS.ScriptStatus(..)
  , SS.WarningMessage(..)
  , SS.Location(..)
  , encodeSinglePackageModule
  , encodeSinglePackageModuleWithImports
  , ScriptServiceException(..)
  ) where

import System.Random (randomIO)
import Conduit (runConduit, (.|), MonadUnliftIO(..))
import Data.Either
import Data.Functor
import Data.Maybe
import Data.IORef
import GHC.Generics
import Text.Read
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import DA.Daml.LF.Mangling
import qualified DA.Daml.LF.Proto3.Encode as Encode
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
import Network.GRPC.HighLevel.Client (ClientError(..), ClientRequest(..), ClientResult(..), GRPCMethodType(..))
import Network.GRPC.HighLevel.Generated (withGRPCClient, GRPCIOError)
import Network.GRPC.LowLevel (ClientConfig(..), Host(..), Port(..), StatusCode(..), Arg(MaxReceiveMessageLength))
import Network.GRPC.LowLevel.Call (endpoint)
import qualified Proto3.Suite as Proto
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import qualified System.IO

import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import qualified ScriptService as SS

import Development.IDE.Types.Logger (Logger)
import qualified Development.IDE.Types.Logger as Logger

data Options = Options
  { optServerJar :: FilePath
  , optJvmOptions :: [String]
  , optEvaluationTimeout :: TimeoutSeconds
  , optGrpcTimeout :: TimeoutSeconds
  , optGrpcMaxMessageSize :: Maybe Int
  , optLogDebug :: String -> IO ()
  , optLogInfo :: String -> IO ()
  , optLogError :: String -> IO ()
  , optDamlLfVersion :: LF.Version
  }

type TimeoutSeconds = Int64

data Handle = Handle
  { hClient :: SS.ScriptService ClientRequest ClientResult
  , hOptions :: Options
  }

newtype ContextId = ContextId { getContextId :: Int64 }
  deriving (NFData, Eq, Show)

-- | If true, the script service server do not run package validations.
newtype SkipValidation = SkipValidation { getSkipValidation :: Bool }
  deriving Show

data ContextUpdate = ContextUpdate
  { updLoadModules :: ![(LF.ModuleName, BS.ByteString)]
  , updUnloadModules :: ![LF.ModuleName]
  , updLoadPackages :: ![(LF.PackageId, BS.ByteString)]
  , updUnloadPackages :: ![LF.PackageId]
  , updSkipValidation :: SkipValidation
  , updPackageMetadata :: LF.PackageMetadata
  }

encodeSinglePackageModule :: LF.Version -> LF.Module -> BS.ByteString
encodeSinglePackageModule version m =
    BSL.toStrict (Proto.toLazyByteString (Encode.encodeSinglePackageModule version (m, Left $ LF.noPkgImportsReasonTrace "DA.Daml.LF.ScriptServiceClient.LowLevel:encodeSinglePackageModule")))

encodeSinglePackageModuleWithImports :: LF.Version -> LF.ModuleWithImports -> BS.ByteString
encodeSinglePackageModuleWithImports version m =
    BSL.toStrict (Proto.toLazyByteString (Encode.encodeSinglePackageModule version m))

data BackendError
  = BErrorClient ClientError
  | BErrorFail StatusCode
  deriving Show

data Error
  = ScriptError SS.ScriptError
  | BackendError BackendError
  | ExceptionError SomeException
  deriving (Generic, Show)

instance NFData Error where
    rnf = rwhnf

findServerJar :: IO FilePath
findServerJar = locateResource Resource
  -- //compiler/script-service/server:scenario_service_jar
  { resourcesPath = "script-service.jar"
    -- In a packaged application, this is stored directly underneath the
    -- resources directory because it's the target's only output.
    -- See @bazel_tools/packaging/packaging.bzl@.
  , runfilesPathPrefix = mainWorkspace </> "compiler" </> "script-service" </> "server"
  }

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

data ScriptServiceException = ScriptServiceException String deriving Show

instance Exception ScriptServiceException

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
      (\(e :: IOException) -> throwIO (ScriptServiceException ("Failed to run java: " <> show e)))
    case exitCode of
        ExitFailure _ -> throwIO (ScriptServiceException ("Failed to start `java -version`: " <> stderr))
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

handleCrashingScriptService :: IORef Bool -> StreamingProcessHandle -> IO a -> IO a
handleCrashingScriptService exitExpected h act =
    -- `race` doesn’t quite work here since we might end up
    -- declaring an expected exit at the very end as a failure.
    -- In particular, once we close stdin of the script service
    -- `waitForStreamingProcess` can return before `act` returns.
    -- See https://github.com/digital-asset/daml/pull/1974.
    withAsync (waitForStreamingProcess h) $ \scriptProcess ->
    withAsync act $ \act' -> do
        r <- waitEither scriptProcess act'
        case r of
            Right a -> pure a
            Left _ -> do
                expected <- readIORef exitExpected
                if expected
                   then wait act'
                   else fail "Script service exited unexpectedly"

withScriptService :: Options -> (Handle -> IO a) -> IO a
withScriptService opts@Options{..} f = do
  optLogDebug "Starting script service..."
  serverJarExists <- doesFileExist optServerJar
  unless serverJarExists $
      throwIO (ScriptServiceException (optServerJar <> " does not exist."))
  validateJava
  cp <- javaProc $ concat
    [ optJvmOptions
    , ["-jar" , optServerJar]
    , ["--max-inbound-message-size=" <> show size | Just size <- [optGrpcMaxMessageSize]]
    ]

  exitExpected <- newIORef False
  let closeStdin hdl = do
          atomicWriteIORef exitExpected True
          System.IO.hClose hdl
  withCheckedProcessCleanup' cp $ \processHdl (stdinHdl :: System.IO.Handle) stdoutSrc stderrSrc ->
          flip finally (closeStdin stdinHdl) $ handleCrashingScriptService exitExpected processHdl $ do
    let splitOutput = C.T.decode C.T.utf8 .| C.T.lines
    let printStderr line
            -- The last line should not be treated as an error.
            | T.strip line == "ScriptService: stdin closed, terminating server." =
              liftIO (optLogDebug (T.unpack ("SCRIPT SERVICE STDERR: " <> line)))
            | otherwise =
              liftIO (optLogError (T.unpack ("SCRIPT SERVICE STDERR: " <> line)))
    let printStdout line = liftIO (optLogDebug (T.unpack ("SCRIPT SERVICE STDOUT: " <> line)))
    -- stick the error in the mvar so that we know we won't get an BlockedIndefinitedlyOnMvar exception
    portMVar <- newEmptyMVar
    let handleStdout = do
          mbLine <- C.await
          case mbLine of
            Nothing ->
              liftIO (putMVar portMVar (Left "Stdout of script service terminated before we got the PORT=<port> message"))
            Just (T.unpack -> line) ->
              case splitOn "=" line of
                ["PORT", ps] | Just p <- readMaybe ps ->
                  liftIO (putMVar portMVar (Right p)) >> C.awaitForever printStdout
                _ -> do
                  liftIO (optLogError ("Expected PORT=<port> from script service, but got '" <> line <> "'. Ignoring it."))
                  handleStdout
    withAsync (runConduit (stderrSrc .| splitOutput .| C.awaitForever printStderr)) $ \_ ->
        withAsync (runConduit (stdoutSrc .| splitOutput .| handleStdout)) $ \_ ->
        -- The script service will shut down cleanly when stdin is closed so we do this at the end of
        -- the callback. Note that on Windows, killThread will not be able to kill the conduits
        -- if they are blocked in hGetNonBlocking so it is crucial that we close stdin in the
        -- callback or withAsync will block forever.
        flip finally (closeStdin stdinHdl) $ do
            port <- either fail pure =<< takeMVar portMVar
            liftIO $ optLogDebug $ "Script service backend running on port " <> show port
            -- Using 127.0.0.1 instead of localhost helps when our packaging logic falls over
            -- and DNS lookups break, e.g., on Alpine linux.
            let grpcConfig = ClientConfig
                  { clientServerEndpoint = endpoint (Host "127.0.0.1") (Port port)
                  , clientArgs = MaxReceiveMessageLength . fromIntegral <$> maybeToList optGrpcMaxMessageSize
                  , clientSSLConfig = Nothing
                  , clientAuthority = Nothing
                  }
            withGRPCClient grpcConfig $ \client -> do
                ssClient <- SS.scriptServiceClient client
                f Handle
                    { hClient = ssClient
                    , hOptions = opts
                    }

newCtx :: Handle -> IO (Either BackendError ContextId)
newCtx Handle{..} = do
  res <- performRequest
      (SS.scriptServiceNewContext hClient)
      (optGrpcTimeout hOptions)
      (SS.NewContextRequest
         (TL.pack $ LF.renderMajorVersion $ LF.versionMajor $ optDamlLfVersion hOptions)
         (TL.pack $ LF.renderMinorVersionWithRev $ LF.versionMinor $ optDamlLfVersion hOptions)
         (optEvaluationTimeout hOptions)
      )
  pure (ContextId . SS.newContextResponseContextId <$> res)

cloneCtx :: Handle -> ContextId -> IO (Either BackendError ContextId)
cloneCtx Handle{..} (ContextId ctxId) = do
  res <-
    performRequest
      (SS.scriptServiceCloneContext hClient)
      (optGrpcTimeout hOptions)
      (SS.CloneContextRequest ctxId)
  pure (ContextId . SS.cloneContextResponseContextId <$> res)

deleteCtx :: Handle -> ContextId -> IO (Either BackendError ())
deleteCtx Handle{..} (ContextId ctxId) = do
  res <-
    performRequest
      (SS.scriptServiceDeleteContext hClient)
      (optGrpcTimeout hOptions)
      (SS.DeleteContextRequest ctxId)
  pure (void res)

gcCtxs :: Handle -> [ContextId] -> IO (Either BackendError ())
gcCtxs Handle{..} ctxIds = do
    res <-
        performRequest
            (SS.scriptServiceGCContexts hClient)
            (optGrpcTimeout hOptions)
            (SS.GCContextsRequest (V.fromList (map getContextId ctxIds)))
    pure (void res)

updateCtx :: Handle -> ContextId -> ContextUpdate -> IO (Either BackendError ())
updateCtx Handle{..} (ContextId ctxId) ContextUpdate{..} = do
  res <-
    performRequest
      (SS.scriptServiceUpdateContext hClient)
      (optGrpcTimeout hOptions) $
      SS.UpdateContextRequest
          ctxId
          (Just updModules)
          (Just updPackages)
          (getSkipValidation updSkipValidation)
          (Just $ convPackageMetadata updPackageMetadata)
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
    convModule :: (LF.ModuleName, BS.ByteString) -> SS.ScriptModule
    convModule (_, bytes) = SS.ScriptModule bytes
    convPackageMetadata m =
      SS.PackageMetadata
        (TL.fromStrict $ LF.unPackageName $ LF.packageName m)
        (TL.fromStrict $ LF.unPackageVersion $ LF.packageVersion m)

mangleModuleName :: LF.ModuleName -> T.Text
mangleModuleName (LF.ModuleName modName) =
    T.intercalate "." $
    map (fromRight (error "Failed to mangle script module name") . mangleIdentifier) modName

mangleScriptName :: LF.ModuleName -> LF.ExprValName -> TL.Text
mangleScriptName modName scriptName =
  TL.fromStrict $
  mangleModuleName modName
  <> ":"
  <> fromRight (error "Failed to mangle script name") (mangleIdentifier $ LF.unExprValName scriptName)

performRequest
  :: (ClientRequest 'Normal payload response -> IO (ClientResult 'Normal response))
  -> TimeoutSeconds
  -> payload
  -> IO (Either BackendError response)
performRequest method timeoutSeconds payload = do
  method (ClientNormalRequest payload (fromIntegral timeoutSeconds) mempty) >>= \case
    ClientNormalResponse resp _ _ StatusOk _ -> return (Right resp)
    ClientNormalResponse _ _ _ status _ -> return (Left $ BErrorFail status)
    ClientErrorResponse err -> return (Left $ BErrorClient err)

runBiDiLive
  :: (SS.ScriptService ClientRequest ClientResult
      -> ClientRequest 'BiDiStreaming SS.RunScriptRequest SS.RunScriptResponseOrStatus
      -> IO (ClientResult 'BiDiStreaming SS.RunScriptResponseOrStatus))
  -> Handle -> ContextId -> TL.Text -> Logger -> MVar Bool -> (SS.ScriptStatus -> IO ())
  -> IO (Either Error SS.ScriptResult)
runBiDiLive runner Handle{..} (ContextId ctxId) scriptName logger stopSemaphore statusUpdateHandler = do
  let startReq =
        SS.RunScriptRequest $ Just $ SS.RunScriptRequestSumStart $ SS.RunScriptStart ctxId scriptName
  let cancelReq = SS.RunScriptRequest $ Just $ SS.RunScriptRequestSumCancel SS.RunScriptCancel

  (updateFinalResponse, getFinalResponse) <- do
    -- Hide finalResponse inside closure
    finalResponse <- newIORef NoResultUpdate
    let set r =
          atomicModifyIORef finalResponse $ \old -> let r' = old <> r in (r', r')
    let get = do
          resp <- readIORef finalResponse
          pure $ case resp of
            NoResultUpdate -> Left (ExceptionError (error "runBiDiLive did not get a completion"))
            ResponseResult r -> Right r
            GRPCError err -> Left (BackendError (BErrorClient (ClientIOError err)))
            ErrorResult e -> Left e
            MultipleResponses rs ->
              let errMsg =
                    unlines
                      ( "runBiDiLive got multiple results:"
                      : map (show . either ErrorResult ResponseResult) rs
                      )
              in
              Left (ExceptionError (error errMsg))
    pure (set, get)

  response <-
    runner hClient $
      ClientBiDiRequest (fromIntegral (optGrpcTimeout hOptions)) mempty $ \_clientCall _meta streamRecv sendReq _writesDone -> do
        let handleGrpcIOErr :: IO (Either GRPCIOError a) -> (a -> IO ScriptResultUpdate) -> IO ScriptResultUpdate
            handleGrpcIOErr action convert = do
              mbGrpcIOErr <- action
              case mbGrpcIOErr of
                Left grpcIOErr ->
                  pure (GRPCError grpcIOErr)
                Right a ->
                  convert a

            loop :: IO ()
            loop = do
              update <- handleGrpcIOErr streamRecv $ \case
                Just (SS.RunScriptResponseOrStatus (Just resp)) ->
                  case resp of
                    (SS.RunScriptResponseOrStatusResponseError err) ->
                      pure (ErrorResult (ScriptError err))
                    (SS.RunScriptResponseOrStatusResponseResult result) ->
                      pure (ResponseResult result)
                    (SS.RunScriptResponseOrStatusResponseStatus status) -> do
                      statusUpdateHandler status
                      pure NoResultUpdate
                _ -> pure NoResultUpdate
              result <- updateFinalResponse update
              case result of
                NoResultUpdate -> loop -- keep looping until a result is found
                _ -> pure ()

        _ <- forkIO $ do
          semaphoreId <- T.pack . show . abs <$> (randomIO :: IO Int)
          Logger.logDebug logger (semaphoreId <> " semaphore reached")
          shouldCancel <- takeMVar stopSemaphore
          Logger.logDebug logger (semaphoreId <> " semaphore finished " <> if shouldCancel then "cancelled" else "not cancelled")
          when shouldCancel $ do
            update <- handleGrpcIOErr (sendReq cancelReq) (const (pure NoResultUpdate))
            _ <- updateFinalResponse update
            pure ()

        update <- handleGrpcIOErr (sendReq startReq) (const (pure NoResultUpdate))
        result <- updateFinalResponse update
        case result of
          NoResultUpdate -> loop
          _ -> pure ()
        pure ()
  _ <- tryPutMVar stopSemaphore False -- once we exit, stop the semaphore checking thread
  case response of
    ClientBiDiResponse _ StatusOk _ -> getFinalResponse
    ClientBiDiResponse _ status _ -> pure (Left (BackendError (BErrorFail status)))
    ClientErrorResponse err -> pure (Left (BackendError (BErrorClient err)))

data ScriptResultUpdate
  = NoResultUpdate
  | ResponseResult SS.ScriptResult
  | ErrorResult Error
  | GRPCError GRPCIOError
  | MultipleResponses [Either Error SS.ScriptResult]
  deriving (Show)

instance Semigroup ScriptResultUpdate where
  (<>) NoResultUpdate result = result -- NoResults is always overwritten
  (<>) result NoResultUpdate = result
  (<>) (GRPCError _) result = result -- GRPC Errors are always overwritten by other errors or responses
  (<>) result (GRPCError _) = result -- GRPC Errors are always overwritten by other errors or responses
  (<>) res1 res2 =
    let toMultipleResponses :: ScriptResultUpdate -> [Either Error SS.ScriptResult]
        toMultipleResponses NoResultUpdate = [] -- these are overwritten by prior clauses
        toMultipleResponses (GRPCError _) = [] -- these are overwritten by prior clauses
        toMultipleResponses (ResponseResult result) = [Right result]
        toMultipleResponses (ErrorResult err) = [Left err]
        toMultipleResponses (MultipleResponses m) = m
    in
    MultipleResponses (toMultipleResponses res1 ++ toMultipleResponses res2)

runLiveScript
  :: Handle -> ContextId -> LF.ModuleName -> LF.ExprValName -> Logger -> MVar Bool -> (SS.ScriptStatus -> IO ())
  -> IO (Either Error SS.ScriptResult)
runLiveScript handle context modName scriptName =
  runBiDiLive SS.scriptServiceRunLiveScript handle context (mangleScriptName modName scriptName)
