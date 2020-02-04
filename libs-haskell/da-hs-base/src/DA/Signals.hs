-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

-- | Module that turns SIGTERM into a UserInterrupt exception in Unix.
module DA.Signals
    ( installSignalHandlers
    , withCloseOnStdin
    ) where

import Control.Exception
import Control.Concurrent.Async
import System.IO
import Control.Concurrent

#ifndef mingw32_HOST_OS
import Control.Monad
import System.Posix.Signals
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

withCloseOnStdin :: IO a -> IO a
withCloseOnStdin a = do
    mainThread <- myThreadId
    withAsync (go mainThread) (const a)
  where go mainThread = do
            b <- isEOF
            if b
                then throwTo mainThread UserInterrupt
                else threadDelay 1000000 >> go mainThread
