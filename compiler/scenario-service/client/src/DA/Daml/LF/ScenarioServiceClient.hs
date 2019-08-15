-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.ScenarioServiceClient
  ( Options(..)
  , ScenarioServiceConfig
  , defaultScenarioServiceConfig
  , readScenarioServiceConfig
  , LowLevel.TimeoutSeconds
  , LowLevel.findServerJar
  , Handle
  , withScenarioService
  , Context(..)
  , LowLevel.LightValidation(..)
  , LowLevel.ContextId
  , getNewCtx
  , deleteCtx
  , gcCtxs
  , runScenario
  , LowLevel.BackendError(..)
  , LowLevel.Error(..)
  , LowLevel.ScenarioResult(..)
  , Hash
  , encodeModule
  ) where

import Control.Concurrent.Extra
import Control.DeepSeq
import Control.Exception
import Control.Monad.IO.Class
import qualified Data.ByteString as BS
import Data.Hashable
import Data.IORef
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.Set as S
import System.Directory

import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient.LowLevel as LowLevel

data Options = Options
  { optServerJar :: FilePath
  , optScenarioServiceConfig :: ScenarioServiceConfig
  , optMaxConcurrency :: Int
  -- This controls the number of parallel gRPC requests
  , optLogInfo :: String -> IO ()
  , optLogError :: String -> IO ()
  }

toLowLevelOpts :: Options -> LowLevel.Options
toLowLevelOpts Options{..} =
    LowLevel.Options{..}
    where
        optRequestTimeout = fromMaybe 60 $ cnfGrpcTimeout optScenarioServiceConfig
        optGrpcMaxMessageSize = cnfGrpcMaxMessageSize optScenarioServiceConfig

data Handle = Handle
  { hLowLevelHandle :: LowLevel.Handle
  , hOptions :: Options
  , hConcurrencySem :: QSemN
  -- ^ Limits the number of concurrent gRPC requests (not just scenario executions).
  , hContextLock :: Lock
  -- ^ Used to make updating and cloning a context via getNewCtx atomic.
  , hLoadedPackages :: IORef (S.Set LF.PackageId)
  , hLoadedModules :: IORef (MS.Map Hash (LF.ModuleName, BS.ByteString))
  , hContextId :: LowLevel.ContextId
  }

withSem :: QSemN -> IO a -> IO a
withSem sem = bracket_ (waitQSemN sem 1) (signalQSemN sem 1)

withScenarioService :: Options -> (Handle -> IO a) -> IO a
withScenarioService hOptions f = do
  LowLevel.withScenarioService (toLowLevelOpts hOptions) $ \hLowLevelHandle ->
      bracket
         (either (\err -> fail $ "Failed to start scenario service: " <> show err) pure =<< LowLevel.newCtx hLowLevelHandle)
         (LowLevel.deleteCtx hLowLevelHandle) $ \hContextId -> do
             hLoadedPackages <- liftIO $ newIORef S.empty
             hLoadedModules <- liftIO $ newIORef MS.empty
             hConcurrencySem <- liftIO $ newQSemN (optMaxConcurrency hOptions)
             hContextLock <- liftIO newLock
             f Handle {..} `finally`
                 -- Wait for gRPC requests to exit, otherwise gRPC gets very unhappy.
                 liftIO (waitQSemN hConcurrencySem $ optMaxConcurrency hOptions)

data ScenarioServiceConfig = ScenarioServiceConfig
    { cnfGrpcMaxMessageSize :: Maybe Int -- In bytes
    , cnfGrpcTimeout :: Maybe LowLevel.TimeoutSeconds
    } deriving Show

defaultScenarioServiceConfig :: ScenarioServiceConfig
defaultScenarioServiceConfig = ScenarioServiceConfig
    { cnfGrpcMaxMessageSize = Nothing
    , cnfGrpcTimeout = Nothing
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
    cnfGrpcMaxMessageSize <- queryProjectConfig ["scenario-service", "grpc-max-message-size"] conf
    cnfGrpcTimeout <- queryProjectConfig ["scenario-service", "grpc-timeout"] conf
    pure ScenarioServiceConfig {..}

data Context = Context
  { ctxModules :: MS.Map Hash (LF.ModuleName, BS.ByteString)
  , ctxPackages :: [(LF.PackageId, BS.ByteString)]
  , ctxDamlLfVersion :: LF.Version
  , ctxLightValidation :: LowLevel.LightValidation
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
      ctxDamlLfVersion
      ctxLightValidation
  writeIORef hLoadedPackages newLoadedPackages
  writeIORef hLoadedModules ctxModules
  res <- LowLevel.updateCtx hLowLevelHandle hContextId ctxUpdate
  case res of
    Left err -> pure (Left err)
    Right () -> LowLevel.cloneCtx hLowLevelHandle hContextId

deleteCtx :: Handle -> LowLevel.ContextId -> IO (Either LowLevel.BackendError ())
deleteCtx Handle{..} ctxId = withSem hConcurrencySem $
  LowLevel.deleteCtx hLowLevelHandle ctxId

gcCtxs :: Handle -> [LowLevel.ContextId] -> IO (Either LowLevel.BackendError ())
gcCtxs Handle{..} ctxIds = withLock hContextLock $ withSem hConcurrencySem $
    -- We never want to GC the root context so we always add that
    -- explicitly. Adding it twice is harmless so we do not check
    -- if it is already present.
    LowLevel.gcCtxs hLowLevelHandle (hContextId : ctxIds)

encodeModule :: LF.Version -> LF.Module -> (Hash, BS.ByteString)
encodeModule v m = (Hash $ hash m', m')
  where m' = LowLevel.encodeModule v m

runScenario :: Handle -> LowLevel.ContextId -> LF.ValueRef -> IO (Either LowLevel.Error LowLevel.ScenarioResult)
runScenario Handle{..} ctxId name = do
  resVar <- newEmptyMVar
  -- When a scenario execution is aborted, we would like to be able to return
  -- immediately. However, we cannot cancel the actual execution of the scenario.
  -- Therefore, we launch run the synchronous execution request in a separate thread
  -- that takes care of managing the semaphore. This thread keeps running
  -- even if `runScenario` was aborted (we cannot abort the FFI calls anyway)
  -- and ensures that we track the actual number of running executions rather
  -- than the number of calls to `runScenario` that have not been canceled.
  _ <- mask $ \restore -> do
      -- Rather than using a bracket in the new thread
      -- we can be a bit more clever and don’t even
      -- launch the new thread until we acquire the
      -- semaphore.
      waitQSemN hConcurrencySem 1
      _ <- forkIO $ do
        r <- try $ restore $ LowLevel.runScenario hLowLevelHandle ctxId name
        case r of
            Left ex -> putMVar resVar (Left $ LowLevel.ExceptionError ex)
            Right r -> putMVar resVar r
        signalQSemN hConcurrencySem 1
      pure ()
  takeMVar resVar

newtype Hash = Hash Int deriving (Eq, Ord, NFData, Show)

instance Semigroup Hash where
    Hash a <> Hash b = Hash $ hash (a,b)

instance Monoid Hash where
    mempty = Hash 0
    mappend = (<>)
