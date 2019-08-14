-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DisambiguateRecordFields #-}

-- | Compiles, generates and creates scenarios for DAML-LF
module DA.Daml.Compiler.Scenario (
    SS.Handle
  , EnableScenarioService(..)
  , withScenarioService
  , withScenarioService'
  , SS.ScenarioServiceConfig
  , SS.readScenarioServiceConfig
  , SS.defaultScenarioServiceConfig
  ) where

import DA.Daml.Options.Types
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import qualified DA.Service.Logger                          as Logger
import           Control.Monad.IO.Class                     (liftIO)
import qualified Data.Text                                  as T

withScenarioService
    :: Logger.Handle IO
    -> SS.ScenarioServiceConfig
    -> (SS.Handle -> IO a)
    -> IO a
withScenarioService loggerH conf f = do
    serverJar <- liftIO SS.findServerJar
    let ssLogHandle = Logger.tagHandle loggerH "ScenarioService"
    let wrapLog f = f ssLogHandle . T.pack
    let opts = SS.Options
          { optMaxConcurrency = 5
          , optServerJar = serverJar
          , optScenarioServiceConfig = conf
          , optLogInfo = wrapLog Logger.logInfo
          , optLogError = wrapLog Logger.logError
          }
    SS.withScenarioService opts f

withScenarioService'
    :: EnableScenarioService
    -> Logger.Handle IO
    -> SS.ScenarioServiceConfig
    -> (Maybe SS.Handle -> IO a)
    -> IO a
withScenarioService' (EnableScenarioService enable) loggerH conf f
    | enable = withScenarioService loggerH conf (f . Just)
    | otherwise = f Nothing
