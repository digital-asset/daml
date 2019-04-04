-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude          #-}

module DA.Sdk.Cli.Monad
    ( CliM
    , runLogging
    , runCliM
    , Env (..)
    , throwM
    , ask
    , asks
    , mockCliM
    , mockCliM_

      -- * Logging
    , logDebug
    , logInfo
    , logWarn
    , logError
    , logMessage

    , getTermWidthConfig
    ) where

import           DA.Sdk.Prelude
import           Control.Exception.Safe
import           Control.Monad.Logger   (LoggingT, MonadLogger)
import qualified Control.Monad.Logger   as L
import           Control.Monad.Reader

import           DA.Sdk.Cli.Message ( Message (..) )
import           DA.Sdk.Cli.Monad.MockIO  (MockIO (..), MockAction (..))
import           DA.Sdk.Cli.Monad.FileSystem  (MonadFS (..))
import           DA.Sdk.Cli.Monad.Timeout (MonadTimeout (..))
import           DA.Sdk.Cli.Conf.Types    (Conf(..), Project(..))
import           DA.Sdk.Cli.Monad.Locations

import qualified DA.Sdk.Pretty          as P

data Env = Env
  { envConf    :: Conf
  , envProject :: Maybe Project
  } deriving (Show, Eq)

newtype CliM a = CliM { runCliMa :: LoggingT (ReaderT Env IO) a }
  deriving
    ( Functor
    , Applicative
    , Monad
    , MonadIO
    , MonadReader Env
    , MonadLogger
    , MonadThrow
    , MonadCatch
    , MonadMask
    )

instance MockIO CliM where
    mockIO = const liftIO

instance MonadTimeout CliM where
    timeout duration cliM = do
        env <- ask
        liftIO $ timeout duration (runCliM cliM env)

runLogging :: MonadIO m => L.LogLevel -> L.LoggingT m a -> m a
runLogging targetLevel action = do
    let logPredicate _ level = level >= targetLevel
    L.runStderrLoggingT (L.filterLogger logPredicate action)

runCliM :: CliM a -> Env -> IO a
runCliM cliM env = do
  let targetLevel = confLogLevel (envConf env)
      readerT = runLogging targetLevel (runCliMa cliM)
  runReaderT readerT env

mockCliM :: MockIO m => Env -> MockAction t -> CliM t -> m t
mockCliM env mockAction cliM =
  mockIO mockAction (runCliM cliM env)

mockCliM_ :: MockIO m => Env -> CliM () -> m ()
mockCliM_ env = mockCliM env (ConstMock ())

instance MonadFS CliM

instance MonadLocations CliM

--------------------------------------------------------------------------------
-- Settings
--------------------------------------------------------------------------------

getTermWidthConfig :: CliM Int
getTermWidthConfig = fromMaybe P.defaultTermWidth <$> asks (confTermWidth . envConf)

--------------------------------------------------------------------------------
-- Logging
--------------------------------------------------------------------------------

logDebug :: MonadLogger m => Text -> m ()
logDebug = L.logDebugN

logInfo :: MonadLogger m => Text -> m ()
logInfo = L.logInfoN

logWarn :: MonadLogger m => Text -> m ()
logWarn = L.logWarnN

logError :: MonadLogger m => Text -> m ()
logError = L.logErrorN

-- | Log the error and warning to the appropriate log channel.
logMessage :: (P.Pretty e, P.Pretty w, MonadLogger m) => Message e w -> m ()
logMessage = \case
    Error er -> logError $ render er
    Warning w -> logWarn $ render w
  where
    render :: forall p. P.Pretty p => p -> Text
    render = P.renderPlain . P.pretty
