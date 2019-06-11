-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE DisambiguateRecordFields #-}

-- | Compiles, generates and creates scenarios for DAML-LF
module DA.Service.Daml.Compiler.Impl.Scenario (
    SS.Handle
  , EnableScenarioService(..)
  , withScenarioService
  , withScenarioService'
  ) where

import DA.Daml.GHC.Compiler.Options
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import qualified DA.Service.Logger                          as Logger
import           Control.Monad.IO.Class                     (liftIO)
import qualified Data.Text                                  as T

withScenarioService
    :: Logger.Handle IO
    -> (SS.Handle -> IO a)
    -> IO a
withScenarioService loggerH f = do
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
    SS.withScenarioService opts f

withScenarioService'
    :: EnableScenarioService
    -> Logger.Handle IO
    -> (Maybe SS.Handle -> IO a)
    -> IO a
withScenarioService' (EnableScenarioService enable) loggerH f
    | enable = withScenarioService loggerH (f . Just)
    | otherwise = f Nothing
