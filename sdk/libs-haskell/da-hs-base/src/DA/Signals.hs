-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import System.IO.Error
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
    withAsync (go mainThread) $ const a
  where go mainThread = do
            -- We cannot use hIsEOF since that is unkillable on Windows.
            r <- try $ hReady stdin
            case r of
                Left e | isEOFError e -> throwTo mainThread UserInterrupt
                       | otherwise -> throwIO e
                Right _ -> do
                    threadDelay 100_000
                    go mainThread
