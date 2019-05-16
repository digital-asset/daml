-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE DisambiguateRecordFields #-}

-- | Compiles, generates and creates scenarios for DAML-LF
module DA.Service.Daml.Compiler.Impl.Scenario (
    SS.Handle
  , startScenarioService
  ) where

import qualified DA.Daml.LF.ScenarioServiceClient as SS
import qualified DA.Service.Logger                          as Logger
import           Control.Exception                          as E
import           Control.Monad.IO.Class                     (liftIO)
import           Control.Monad.Managed.Extended
import qualified Data.Text                                  as T
import qualified Data.Text.Extended                         as T (show)
import qualified Development.IDE.Types.LSP as LSP

startScenarioService
  :: (LSP.Event -> IO ())
  -> Logger.Handle IO
  -> Managed SS.Handle
startScenarioService eventHandler loggerH =
  mapManaged (logScenarioException eventHandler loggerH) $ do
    serverJar <- liftIO SS.findServerJar
    let ssLogHandle = Logger.tagHandle loggerH "ScenarioService"
    let wrapLog f = f ssLogHandle . T.pack
    let opts = SS.Options
          { optMaxConcurrency = 5
          , optRequestTimeout = 60  -- seconds
          , optServerJar = serverJar
          , optLogInfo = wrapLog Logger.logInfo
          , optLogError = wrapLog Logger.logError
          }
    SS.start opts >>= \case
      Left err -> error $ "Failed to start scenario service: " ++ show err
      Right h -> pure h

logScenarioException :: (LSP.Event -> IO ()) -> Logger.Handle IO -> IO a -> IO a
logScenarioException eventHandler loggerH = E.handle $ \(ex :: E.SomeException) -> do
  let msg = "Exception during scenario interpretation: " <> T.show ex
  Logger.logError loggerH msg
  eventHandler $ LSP.EventFatalError msg
  E.throw ex
