-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.ScenarioServiceClient
  ( Options(..)
  , ScenarioServiceConfig(..)
  , defaultScenarioServiceConfig
  , readScenarioServiceConfig
  , LowLevel.TimeoutSeconds
  , LowLevel.findServerJar
  , Handle
  , withScenarioService
  , withScenarioService'
  , Context(..)
  , LowLevel.SkipValidation(..)
  , LowLevel.ContextId
  , getNewCtx
  , deleteCtx
  , gcCtxs
  , runScenario
  , runScript
  , runLiveScript
  , runLiveScenario
  , LowLevel.BackendError(..)
  , LowLevel.Error(..)
  , LowLevel.ScenarioResult(..)
  , LowLevel.ScenarioStatus(..)
  , LowLevel.WarningMessage(..)
  , LowLevel.Location(..)
  , Hash
  , encodeModule
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

import DA.Daml.Options.Types (EnableScenarioService(..), EnableScenarios(..))
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient.LowLevel as LowLevel

import qualified DA.Service.Logger as Logger

data Options = Options
  { optServerJar :: FilePath
  , optScenarioServiceConfig :: ScenarioServiceConfig
  , optMaxConcurrency :: Int
  -- This controls the number of parallel gRPC requests
  , optLogDebug :: String -> IO ()
  , optLogInfo :: String -> IO ()
  , optLogError :: String -> IO ()
  , optEnableScenarios :: EnableScenarios
  }

toLowLevelOpts :: LF.Version -> Options -> LowLevel.Options
toLowLevelOpts optDamlLfVersion Options{..} =
    LowLevel.Options{..}
    where
        optGrpcTimeout = fromMaybe 70 $ cnfGrpcTimeout optScenarioServiceConfig
        optEvaluationTimeout = fromMaybe 60 $ cnfEvaluationTimeout optScenarioServiceConfig
        optGrpcMaxMessageSize = cnfGrpcMaxMessageSize optScenarioServiceConfig
        optJvmOptions = cnfJvmOptions optScenarioServiceConfig

data Handle = Handle
  { hLowLevelHandle :: LowLevel.Handle
  , hOptions :: Options
  , hConcurrencySem :: QSemN
  -- ^ Limits the number of concurrent gRPC requests (not just scenario executions).
  , hContextLock :: Lock
  -- ^ Used to make updating and cloning a context via getNewCtx atomic.
  , hLoadedPackages :: IORef (S.Set LF.PackageId)
  , hLoadedModules :: IORef (MS.Map Hash (LF.ModuleName, BS.ByteString))
  , hContextId :: IORef LowLevel.ContextId
  -- ^ The root context id, this is mutable so that rather than mutating the context
  -- we can clone it and update the clone which allows us to safely interrupt a context update.
  , hRunningHandlers :: MVar (MS.Map RunOptions (ThreadId, MVar Bool))
  }

withSem :: QSemN -> IO a -> IO a
withSem sem = bracket_ (waitQSemN sem 1) (signalQSemN sem 1)

withScenarioService :: LF.Version -> Logger.Handle IO -> ScenarioServiceConfig -> (Handle -> IO a) -> IO a
withScenarioService = withScenarioService'' (EnableScenarios True)

withScenarioService'' :: EnableScenarios -> LF.Version -> Logger.Handle IO -> ScenarioServiceConfig -> (Handle -> IO a) -> IO a
withScenarioService'' optEnableScenarios ver loggerH scenarioConfig f = do
  hOptions <- getOptions
  LowLevel.withScenarioService (toLowLevelOpts ver hOptions) $ \hLowLevelHandle ->
      bracket
         (either (\err -> fail $ "Failed to start scenario service: " <> show err) pure =<< LowLevel.newCtx hLowLevelHandle)
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
            serverJar <- LowLevel.findServerJar
            let ssLogHandle = Logger.tagHandle loggerH "ScenarioService"
            let wrapLog f = f ssLogHandle . T.pack
            pure Options
                { optMaxConcurrency = 5
                , optServerJar = serverJar
                , optScenarioServiceConfig = scenarioConfig
                , optLogDebug = wrapLog Logger.logDebug
                , optLogInfo = wrapLog Logger.logInfo
                , optLogError = wrapLog Logger.logError
                , optEnableScenarios
                }

withScenarioService'
    :: EnableScenarioService
    -> EnableScenarios
    -> LF.Version
    -> Logger.Handle IO
    -> ScenarioServiceConfig
    -> (Maybe Handle -> IO a)
    -> IO a
withScenarioService' (EnableScenarioService enable) enableScenarios ver loggerH conf f
    | enable = withScenarioService'' enableScenarios ver loggerH conf (f . Just)
    | otherwise = f Nothing

data ScenarioServiceConfig = ScenarioServiceConfig
    { cnfGrpcMaxMessageSize :: Maybe Int -- In bytes
    , cnfGrpcTimeout :: Maybe LowLevel.TimeoutSeconds
    , cnfEvaluationTimeout :: Maybe LowLevel.TimeoutSeconds
    , cnfJvmOptions :: [String]
    } deriving Show

defaultScenarioServiceConfig :: ScenarioServiceConfig
defaultScenarioServiceConfig = ScenarioServiceConfig
    { cnfGrpcMaxMessageSize = Nothing
    , cnfGrpcTimeout = Nothing
    , cnfEvaluationTimeout = Nothing
    , cnfJvmOptions = []
    }

readScenarioServiceConfig :: IO ScenarioServiceConfig
readScenarioServiceConfig = do
    exists <- doesFileExist projectConfigName
    if exists
        then do
            project <- readProjectConfig $ ProjectPath "."
            either throwIO pure $ parseScenarioServiceConfig project
        else pure defaultScenarioServiceConfig

parseScenarioServiceConfig :: ProjectConfig -> Either ConfigError ScenarioServiceConfig
parseScenarioServiceConfig conf = do
    cnfGrpcMaxMessageSize <- queryOpt "grpc-max-message-size"
    cnfGrpcTimeout <- queryOpt "grpc-timeout"
    cnfEvaluationTimeout <- queryOpt "evaluation-timeout"
    cnfJvmOptions <- fromMaybe [] <$> queryOpt "jvm-options"
    pure ScenarioServiceConfig {..}
  where queryOpt opt = do
            a <- queryProjectConfig ["script-service", opt] conf
            case a of
                Nothing -> queryProjectConfig ["scenario-service", opt] conf
                Just a -> pure (Just a)

data Context = Context
  { ctxModules :: MS.Map Hash (LF.ModuleName, BS.ByteString)
  , ctxPackages :: [(LF.PackageId, BS.ByteString)]
  , ctxSkipValidation :: LowLevel.SkipValidation
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
  where m' = LowLevel.encodeScenarioModule v m

-- Reify name * live/unlive * scenario/script
data RunOptions = RunOptions
  { name :: LF.ValueRef
  , live :: Maybe LiveHandler
  , scenarioOrScript :: ScenarioOrScript
  }
  deriving (Show, Eq, Ord)
data ScenarioOrScript = IsScenario | IsScript
  deriving (Show, Eq, Ord)
newtype LiveHandler = LiveHandler (LowLevel.ScenarioStatus -> IO ())
instance Show LiveHandler where
  show _ = "LiveHandler"
instance Eq LiveHandler where
  (==) _ _ = True
instance Ord LiveHandler where
  compare _ _ = EQ

runScenario :: Handle -> LowLevel.ContextId -> LF.ValueRef -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
runScenario h ctxId name = runWithOptions (RunOptions name Nothing IsScenario) h ctxId

runScript :: Handle -> LowLevel.ContextId -> LF.ValueRef -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
runScript h ctxId name = runWithOptions (RunOptions name Nothing IsScript) h ctxId

runLiveScenario :: Handle -> LowLevel.ContextId -> LF.ValueRef -> (LowLevel.ScenarioStatus -> IO ()) -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
runLiveScenario h ctxId name statusUpdateHandler = runWithOptions (RunOptions name (Just (LiveHandler statusUpdateHandler)) IsScenario) h ctxId

runLiveScript :: Handle -> LowLevel.ContextId -> LF.ValueRef -> (LowLevel.ScenarioStatus -> IO ()) -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
runLiveScript h ctxId name statusUpdateHandler = runWithOptions (RunOptions name (Just (LiveHandler statusUpdateHandler)) IsScript) h ctxId

runWithOptions :: RunOptions -> Handle -> LowLevel.ContextId -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
runWithOptions options Handle{..} ctxId = do
  resVar <- newEmptyMVar
  -- When a scenario/script execution is aborted, we would like to be able to return
  -- immediately. However, we cannot cancel the actual execution of the scenario/script.
  -- Therefore, we launch run the synchronous execution request in a separate thread
  -- that takes care of managing the semaphore. This thread keeps running
  -- even if `runScenario` was aborted (we cannot abort the FFI calls anyway)
  -- and ensures that we track the actual number of running executions rather
  -- than the number of calls to `run` that have not been canceled.
  mask $ \restore ->
    modifyMVar_ hRunningHandlers $ \runningHandlers -> do
      stopSemaphore <- newEmptyMVar
      handlerThread <- forkIO $ withSem hConcurrencySem $ do
        -- Catch async exceptions so we can respond in the MVar if necessary
        r <- try $ restore (optionsToLowLevel options hLowLevelHandle ctxId stopSemaphore)
        case r of
          Left ex -> putMVar resVar (Left $ LowLevel.ExceptionError ex)
          Right r -> putMVar resVar r
        _ <- tryPutMVar stopSemaphore False
        pure ()

      -- Store the new thread into runningHandlers
      let insertLookup kx x t = MS.insertLookupWithKey (\_ a _ -> a) kx x t
      let (mbOldThread, newRunningHandlers) = insertLookup options (handlerThread, stopSemaphore) runningHandlers

      -- If there was an old thread handling the same scenario in the same way, kill it
      case mbOldThread of
        Just (oldThread, oldThreadSemaphore) -> do
          _ <- tryPutMVar oldThreadSemaphore True
          pure ()
        _ -> pure ()

      -- Return updated runningHandlers
      pure newRunningHandlers
  takeMVar resVar

optionsToLowLevel :: RunOptions -> LowLevel.Handle -> LowLevel.ContextId -> MVar Bool -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
optionsToLowLevel RunOptions{..} h ctxId mask =
  case (live, scenarioOrScript) of
    (Just (LiveHandler handler), IsScript)   -> LowLevel.runLiveScript h ctxId name mask handler
    (Just (LiveHandler handler), IsScenario) -> LowLevel.runLiveScenario h ctxId name handler
    (Nothing,                    IsScript)   -> LowLevel.runScript h ctxId name
    (Nothing,                    IsScenario) -> LowLevel.runScenario h ctxId name

newtype Hash = Hash Int deriving (Eq, Ord, NFData, Show)

instance Semigroup Hash where
    Hash a <> Hash b = Hash $ hash (a,b)

instance Monoid Hash where
    mempty = Hash 0
    mappend = (<>)
