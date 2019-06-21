{-# LANGUAGE CPP #-}

-- | Module that turns SIGTERM into a UserInterrput exception in unix.
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

