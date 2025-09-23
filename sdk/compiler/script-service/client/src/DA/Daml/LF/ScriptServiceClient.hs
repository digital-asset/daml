-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.ScriptServiceClient
  ( Options(..)
  , ScriptServiceConfig(..)
  , defaultScriptServiceConfig
  , readScriptServiceConfig
  , LowLevel.TimeoutSeconds
  , LowLevel.findServerJar
  , Handle
  , withScriptService
  , withScriptService'
  , Context(..)
  , LowLevel.SkipValidation(..)
  , LowLevel.ContextId
  , getNewCtx
  , deleteCtx
  , gcCtxs
  , runLiveScript
  , LowLevel.BackendError(..)
  , LowLevel.Error(..)
  , LowLevel.ScriptResult(..)
  , LowLevel.ScriptStatus(..)
  , LowLevel.WarningMessage(..)
  , LowLevel.Location(..)
  , Hash
  , encodeModule
  , encodeModuleWithImports
  ) where

import Control.Concurrent.Extra
import Control.DeepSeq
import Control.Exception
import Control.Monad.Except
import qualified Data.ByteString as BS
import Data.Hashable
import Data.IORef
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import System.Directory

import DA.Daml.Options.Types (EnableScriptService(..))
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScriptServiceClient.LowLevel as LowLevel

import qualified DA.Service.Logger as Logger

import qualified Development.IDE.Types.Logger as IDELogger

data Options = Options
  { optServerJar :: FilePath
  , optScriptServiceConfig :: ScriptServiceConfig
  , optMaxConcurrency :: Int
  -- This controls the number of parallel gRPC requests
  , optLogDebug :: String -> IO ()
  , optLogInfo :: String -> IO ()
  , optLogError :: String -> IO ()
  }

toLowLevelOpts :: LF.Version -> Options -> LowLevel.Options
toLowLevelOpts optDamlLfVersion Options{..} =
    LowLevel.Options{..}
    where
        optGrpcTimeout = fromMaybe 70 $ cnfGrpcTimeout optScriptServiceConfig
        optEvaluationTimeout = fromMaybe 60 $ cnfEvaluationTimeout optScriptServiceConfig
        optGrpcMaxMessageSize = cnfGrpcMaxMessageSize optScriptServiceConfig
        optJvmOptions = cnfJvmOptions optScriptServiceConfig

data Handle = Handle
  { hLowLevelHandle :: LowLevel.Handle
  , hOptions :: Options
  , hConcurrencySem :: QSemN
  -- ^ Limits the number of concurrent gRPC requests (not just script executions).
  , hContextLock :: Lock
  -- ^ Used to make updating and cloning a context via getNewCtx atomic.
  , hLoadedPackages :: IORef (S.Set LF.PackageId)
  , hLoadedModules :: IORef (MS.Map Hash (LF.ModuleName, BS.ByteString))
  , hContextId :: IORef LowLevel.ContextId
  -- ^ The root context id, this is mutable so that rather than mutating the context
  -- we can clone it and update the clone which allows us to safely interrupt a context update.
  , hRunningHandlers :: MVar (MS.Map (LF.ModuleName, LF.ExprValName) RunInfo)
  -- ^ Track running scripts as a map between the script and all information
  -- required to cancel them or to resume from them
  -- ContextId for determining ThreadId for cancelling via asynchronous exception
  }

-- RunInfo stores information required for cancelling and resuming from script runs
data RunInfo = RunInfo
  { threadId :: ThreadId
  , context :: LowLevel.ContextId
  -- ^ If a new prospective script run has a newer context id, then threads
  -- with older contexts should be cancelled
  , stop :: MVar Bool
  -- ^ To cancel a thread, put True into this semaphore, which triggers
  -- cancellation in the corresponding lowlevel script run
  , result :: Barrier (Either LowLevel.Error LowLevel.ScriptResult)
  -- ^ To obtain the result of a script run, listen to this barrier, which will
  -- be filled by the lowlevel script run when the script run terminates
  -- Must be a barrier so that both this run and future runs can subscribe to
  -- the same value.
  }

withSem :: QSemN -> IO a -> IO a
withSem sem = bracket_ (waitQSemN sem 1) (signalQSemN sem 1)

withScriptService :: LF.Version -> Logger.Handle IO -> ScriptServiceConfig -> Maybe FilePath -> (Handle -> IO a) -> IO a
withScriptService ver loggerH scriptConfig mbScriptServiceJar f = do
  hOptions <- getOptions
  LowLevel.withScriptService (toLowLevelOpts ver hOptions) $ \hLowLevelHandle ->
      bracket
         (either (\err -> fail $ "Failed to start script service: " <> show err) pure =<< LowLevel.newCtx hLowLevelHandle)
         (LowLevel.deleteCtx hLowLevelHandle) $ \rootCtxId -> do
             hLoadedPackages <- liftIO $ newIORef S.empty
             hLoadedModules <- liftIO $ newIORef MS.empty
             hConcurrencySem <- liftIO $ newQSemN (optMaxConcurrency hOptions)
             hContextLock <- liftIO newLock
             hContextId <- liftIO $ newIORef rootCtxId
             hRunningHandlers <- liftIO $ newMVar MS.empty
             f Handle {..} `finally`
                 -- Wait for gRPC requests to exit, otherwise gRPC gets very unhappy.
                 liftIO (waitQSemN hConcurrencySem $ optMaxConcurrency hOptions)
  where getOptions = do
            serverJar <- maybe LowLevel.findServerJar pure mbScriptServiceJar
            let ssLogHandle = Logger.tagHandle loggerH "ScriptService"
            let wrapLog f = f ssLogHandle . T.pack
            pure Options
                { optMaxConcurrency = 5
                , optServerJar = serverJar
                , optScriptServiceConfig = scriptConfig
                , optLogDebug = wrapLog Logger.logDebug
                , optLogInfo = wrapLog Logger.logInfo
                , optLogError = wrapLog Logger.logError
                }

withScriptService'
    :: EnableScriptService
    -> LF.Version
    -> Logger.Handle IO
    -> ScriptServiceConfig
    -> Maybe FilePath
    -> (Maybe Handle -> IO a)
    -> IO a
withScriptService' (EnableScriptService enable) ver loggerH conf mbScriptJar f
    | enable = withScriptService ver loggerH conf mbScriptJar (f . Just)
    | otherwise = f Nothing

data ScriptServiceConfig = ScriptServiceConfig
    { cnfGrpcMaxMessageSize :: Maybe Int -- In bytes
    , cnfGrpcTimeout :: Maybe LowLevel.TimeoutSeconds
    , cnfEvaluationTimeout :: Maybe LowLevel.TimeoutSeconds
    , cnfJvmOptions :: [String]
    } deriving Show

defaultScriptServiceConfig :: ScriptServiceConfig
defaultScriptServiceConfig = ScriptServiceConfig
    { cnfGrpcMaxMessageSize = Nothing
    , cnfGrpcTimeout = Nothing
    , cnfEvaluationTimeout = Nothing
    , cnfJvmOptions = []
    }

readScriptServiceConfig :: IO ScriptServiceConfig
readScriptServiceConfig = do
    exists <- doesFileExist packageConfigName
    if exists
        then do
            package <- readPackageConfig $ PackagePath "."
            either throwIO pure $ parseScriptServiceConfig package
        else pure defaultScriptServiceConfig

parseScriptServiceConfig :: PackageConfig -> Either ConfigError ScriptServiceConfig
parseScriptServiceConfig conf = do
    checkNoScenarioServiceField conf
    cnfGrpcMaxMessageSize <- queryOpt "grpc-max-message-size"
    cnfGrpcTimeout <- queryOpt "grpc-timeout"
    cnfEvaluationTimeout <- queryOpt "evaluation-timeout"
    cnfJvmOptions <- fromMaybe [] <$> queryOpt "jvm-options"
    pure ScriptServiceConfig {..}
  where queryOpt opt = queryPackageConfig ["script-service", opt] conf

data Context = Context
  { ctxModules :: MS.Map Hash (LF.ModuleName, BS.ByteString)
  , ctxPackages :: [(LF.PackageId, BS.ByteString)]
  , ctxSkipValidation :: LowLevel.SkipValidation
  , ctxPackageMetadata :: LF.PackageMetadata
  }

getNewCtx :: Handle -> Context -> IO (Either LowLevel.BackendError LowLevel.ContextId)
getNewCtx Handle{..} Context{..} = withLock hContextLock $ withSem hConcurrencySem $ do
  loadedPackages <- readIORef hLoadedPackages
  loadedModules <- readIORef hLoadedModules
  let
    loadPackages = filter (\(pId, _) -> not (pId `S.member` loadedPackages)) ctxPackages
    unloadPackages = loadedPackages S.\\ newLoadedPackages
    newLoadedPackages = S.fromList (map fst ctxPackages)

    loadModules = ctxModules MS.\\ loadedModules
    unloadModules =
      S.fromList (map fst $ MS.elems loadedModules) S.\\
      S.fromList (map fst $ MS.elems ctxModules)

    ctxUpdate = LowLevel.ContextUpdate
      (MS.elems loadModules)
      (S.toList unloadModules)
      loadPackages
      (S.toList unloadPackages)
      ctxSkipValidation
      ctxPackageMetadata
  rootCtxId <- readIORef hContextId
  runExceptT $ do
      clonedRootCtxId <- ExceptT $ LowLevel.cloneCtx hLowLevelHandle rootCtxId
      () <- ExceptT $ LowLevel.updateCtx hLowLevelHandle clonedRootCtxId ctxUpdate
      -- Update successful, now atomically update our local state and the root context id.
      -- If we get interrupted before this stage, the new context will just be garbage collected.
      liftIO $ mask_ $ do
          writeIORef hLoadedPackages newLoadedPackages
          writeIORef hLoadedModules ctxModules
          writeIORef hContextId clonedRootCtxId
      -- clone again so that the returned context is independent of the root context
      ExceptT $ LowLevel.cloneCtx hLowLevelHandle clonedRootCtxId

deleteCtx :: Handle -> LowLevel.ContextId -> IO (Either LowLevel.BackendError ())
deleteCtx Handle{..} ctxId = withSem hConcurrencySem $
  LowLevel.deleteCtx hLowLevelHandle ctxId

gcCtxs :: Handle -> [LowLevel.ContextId] -> IO (Either LowLevel.BackendError ())
gcCtxs Handle{..} ctxIds = withLock hContextLock $ withSem hConcurrencySem $ do
    rootCtxId <- readIORef hContextId
    -- We never want to GC the root context so we always add that
    -- explicitly. Adding it twice is harmless so we do not check
    -- if it is already present.
    LowLevel.gcCtxs hLowLevelHandle (rootCtxId : ctxIds)

encodeModule :: LF.Version -> LF.Module -> (Hash, BS.ByteString)
encodeModule v m = (Hash $ hash m', m')
  where m' = LowLevel.encodeSinglePackageModule v m

encodeModuleWithImports :: LF.Version -> LF.ModuleWithImports -> (Hash, BS.ByteString)
encodeModuleWithImports v m = (Hash $ hash m', m')
  where m' = LowLevel.encodeSinglePackageModuleWithImports v m

type LiveHandler = LowLevel.ScriptStatus -> IO ()

runLiveScript :: Handle -> LowLevel.ContextId -> IDELogger.Logger -> LF.ModuleName -> LF.ExprValName -> LiveHandler -> IO (Either LowLevel.Error LowLevel.ScriptResult)
runLiveScript Handle{..} ctxId logger moduleName scriptName handler = do
  resBarrier <- newBarrier
  stopSemaphore <- newEmptyMVar

  -- If the internal or external thread receives a cancellation exception, signal to stop
  modifyMVar_ hRunningHandlers $ \runningHandlers -> do
    -- If there was an old thread handling the same script in the same
    -- way, under a different context, send a cancellation to its semaphore
    let mbOldRunInfo = MS.lookup (moduleName, scriptName) runningHandlers
    case mbOldRunInfo of
      Just oldRunInfo
        | context oldRunInfo == ctxId -> pure ()
        | otherwise -> do
          _ <- tryPutMVar (stop oldRunInfo) True
          pure ()
      Nothing -> pure ()

    -- If there was an old thread handling the same script in the same
    -- way, under a different context, or if there was no old thread, start a
    -- new thread to replace it.
    -- Otherwise (when there is an old thread with the same context id) listen
    -- to that thread's result via its result barrier
    case mbOldRunInfo of
      Just oldRunInfo
        | context oldRunInfo == ctxId -> do
          _ <- forkIO $ do
            oldResult <- waitBarrier (result oldRunInfo)
            signalBarrier resBarrier oldResult
          pure runningHandlers
      _ -> do
        handlerThread <- forkIO $ withSem hConcurrencySem $ do
          r <- try $ LowLevel.runLiveScript hLowLevelHandle ctxId moduleName scriptName logger stopSemaphore handler
          signalBarrier resBarrier $
            case r of
              Left ex -> Left $ LowLevel.ExceptionError ex
              Right r -> r

        let selfInfo =
              RunInfo
                { threadId = handlerThread
                , context = ctxId
                , stop = stopSemaphore
                , result = resBarrier
                }
        let newRunningHandlers = MS.insert (moduleName, scriptName) selfInfo runningHandlers
        pure newRunningHandlers
  waitBarrier resBarrier

newtype Hash = Hash Int deriving (Eq, Ord, NFData, Show)

instance Semigroup Hash where
    Hash a <> Hash b = Hash $ hash (a,b)

instance Monoid Hash where
    mempty = Hash 0
    mappend = (<>)
