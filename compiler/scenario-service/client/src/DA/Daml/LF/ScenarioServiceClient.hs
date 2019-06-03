-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.ScenarioServiceClient
  ( Options(..)
  , LowLevel.TimeoutSeconds
  , LowLevel.findServerJar
  , Handle
  , withScenarioService
  , Context(..)
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
import qualified Data.Set as S

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ScenarioServiceClient.LowLevel as LowLevel

data Options = Options
  { optServerJar :: FilePath
  , optRequestTimeout :: LowLevel.TimeoutSeconds
  , optMaxConcurrency :: Int
  , optLogInfo :: String -> IO ()
  , optLogError :: String -> IO ()
  }

toLowLevelOpts :: Options -> LowLevel.Options
toLowLevelOpts Options{..} = LowLevel.Options{..}

data Handle = Handle
  { hLowLevelHandle :: LowLevel.Handle
  , hOptions :: Options
  , hConcurrencySem :: QSem
  -- ^ Limits the number of concurrent scenario executions.
  , hContextLock :: Lock
  -- ^ Used to make updating and cloning a context via getNewCtx atomic.
  , hLoadedPackages :: IORef (S.Set LF.PackageId)
  , hLoadedModules :: IORef (MS.Map Hash (LF.ModuleName, BS.ByteString))
  , hContextId :: LowLevel.ContextId
  }

withScenarioService :: Options -> (Handle -> IO a) -> IO a
withScenarioService hOptions f =
  LowLevel.withScenarioService (toLowLevelOpts hOptions) $ \hLowLevelHandle ->
  bracket
     (either (\err -> fail $ "Failed to start scenario service: " <> show err) pure =<< LowLevel.newCtx hLowLevelHandle)
     (LowLevel.deleteCtx hLowLevelHandle) $ \hContextId -> do
         hLoadedPackages <- liftIO $ newIORef S.empty
         hLoadedModules <- liftIO $ newIORef MS.empty
         hConcurrencySem <- liftIO $ newQSem (optMaxConcurrency hOptions)
         hContextLock <- liftIO newLock
         f Handle {..}

data Context = Context
  { ctxModules :: MS.Map Hash (LF.ModuleName, BS.ByteString)
  , ctxPackages :: [(LF.PackageId, BS.ByteString)]
  , ctxDamlLfVersion :: LF.Version
  }

getNewCtx :: Handle -> Context -> IO (Either LowLevel.BackendError LowLevel.ContextId)
getNewCtx Handle{..} Context{..} = withLock hContextLock $ do
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
  writeIORef hLoadedPackages newLoadedPackages
  writeIORef hLoadedModules ctxModules
  res <- LowLevel.updateCtx hLowLevelHandle hContextId ctxUpdate
  case res of
    Left err -> pure (Left err)
    Right () -> LowLevel.cloneCtx hLowLevelHandle hContextId

deleteCtx :: Handle -> LowLevel.ContextId -> IO (Either LowLevel.BackendError ())
deleteCtx Handle{..} ctxId = do
  LowLevel.deleteCtx hLowLevelHandle ctxId

gcCtxs :: Handle -> [LowLevel.ContextId] -> IO (Either LowLevel.BackendError ())
gcCtxs Handle{..} ctxIds = withLock hContextLock $
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
  -- even if `runScenario` was aborted and ensures that we track the actual
  -- number of running executions rather than the number of calls to `runScenario`
  -- that have not been canceled.
  let handleException (Right _) = pure ()
      handleException (Left ex) = putMVar resVar (Left (LowLevel.ExceptionError ex))
  _ <- flip forkFinally handleException $ bracket_ (waitQSem hConcurrencySem) (signalQSem hConcurrencySem) $ do
    r <- LowLevel.runScenario hLowLevelHandle ctxId name
    putMVar resVar r
  takeMVar resVar

newtype Hash = Hash Int deriving (Eq, Ord, NFData, Show)

instance Semigroup Hash where
    Hash a <> Hash b = Hash $ hash (a,b)

instance Monoid Hash where
    mempty = Hash 0
    mappend = (<>)
