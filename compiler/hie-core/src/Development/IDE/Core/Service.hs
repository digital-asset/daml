-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE FlexibleInstances #-}

-- | A Shake implementation of the compiler service, built
--   using the "Shaker" abstraction layer for in-memory use.
--
module Development.IDE.Core.Service(
    getIdeOptions,
    IdeState, initialise, shutdown,
    runAction,
    runActionSync,
    writeProfile,
    getDiagnostics, unsafeClearDiagnostics,
    ideLogger
    ) where

import           Control.Concurrent.Extra
import           Control.Concurrent.Async
import           Control.Monad.Except
import Development.IDE.Types.Options (IdeOptions(..))
import           Development.IDE.Core.FileStore
import           Development.IDE.Core.OfInterest
import Development.IDE.Types.Logger
import           Development.Shake                        hiding (Diagnostic, Env, newCache)
import Data.Either.Extra
import qualified Language.Haskell.LSP.Messages as LSP

import           Development.IDE.Core.Shake



newtype GlobalIdeOptions = GlobalIdeOptions IdeOptions
instance IsIdeGlobal GlobalIdeOptions

------------------------------------------------------------
-- Exposed API

-- | Initialise the Compiler Service.
initialise :: Rules ()
           -> (LSP.FromServerMessage -> IO ())
           -> Logger
           -> IdeOptions
           -> VFSHandle
           -> IO IdeState
initialise mainRule toDiags logger options vfs =
    shakeOpen
        toDiags
        logger
        (setProfiling options $
        shakeOptions { shakeThreads = optThreads options
                     , shakeFiles   = "/dev/null"
                     }) $ do
            addIdeGlobal $ GlobalIdeOptions options
            fileStoreRules vfs
            ofInterestRules
            mainRule

writeProfile :: IdeState -> FilePath -> IO ()
writeProfile = shakeProfile

setProfiling :: IdeOptions -> ShakeOptions -> ShakeOptions
setProfiling opts shakeOpts =
  maybe shakeOpts (\p -> shakeOpts { shakeReport = [p], shakeTimings = True }) (optShakeProfiling opts)

-- | Shutdown the Compiler Service.
shutdown :: IdeState -> IO ()
shutdown = shakeShut

-- This will return as soon as the result of the action is
-- available.  There might still be other rules running at this point,
-- e.g., the ofInterestRule.
runAction :: IdeState -> Action a -> IO a
runAction ide action = do
    bar <- newBarrier
    res <- shakeRun ide [do v <- action; liftIO $ signalBarrier bar v; return v] (const $ pure ())
    -- shakeRun might throw an exception, in which case res will finish first
    -- killing res only kills waiting for the var, it doesn't kill the actual work
    fmap fromEither $ race (head <$> res) $ waitBarrier bar


-- | `runActionSync` is similar to `runAction` but it will
-- wait for all rules (so in particular the `ofInterestRule`) to
-- finish running. This is mainly useful in tests, where you want
-- to wait for all rules to fire so you can check diagnostics.
runActionSync :: IdeState -> Action a -> IO a
runActionSync s act = fmap head $ join $ shakeRun s [act] (const $ pure ())

getIdeOptions :: Action IdeOptions
getIdeOptions = do
    GlobalIdeOptions x <- getIdeGlobalAction
    return x
