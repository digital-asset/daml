-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

-- | Module that turns SIGTERM into a UserInterrupt exception in Unix.
module DamlHelper.Signals
    ( installSignalHandlers
    ) where

#ifndef mingw32_HOST_OS
import System.Posix.Signals
import Control.Exception
import Control.Concurrent
import Control.Monad
#endif

-- | Turn SIGTERM into a UserInterrput exception in Unix. Does nothing on Windows.
installSignalHandlers :: IO ()

#ifdef mingw32_HOST_OS
installSignalHandlers = pure ()
#else
installSignalHandlers = do
    mainThread <- myThreadId
    void $ installHandler sigTERM (Catch $ throwTo mainThread UserInterrupt) Nothing
#endif

