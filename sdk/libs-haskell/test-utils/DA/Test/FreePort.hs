-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.FreePort (getAvailablePort, LockedPort (..)) where

import Control.Exception (bracket, throwIO)
import DA.Test.FreePort.Error
import DA.Test.FreePort.PortGen
import DA.Test.FreePort.PortLock
import System.FileLock (FileLock, unlockFile)
import Data.Bifunctor (second)
import Data.Either (isRight)
import Data.Foldable.Extra (firstJustM)
import UnliftIO.Exception (tryIO)
import Test.QuickCheck

import Network.Socket

maxAttempts :: Int
maxAttempts = 100

data LockedPort = LockedPort
  { port :: Int
  , unlock :: IO ()
  }

getAvailablePort :: IO LockedPort
getAvailablePort = do
  portGen <- getPortGen
  ports <- generate $ vectorOf maxAttempts portGen
  mPortData <- firstJustM tryPort ports

  maybe (throwIO NoPortsAvailableError) (pure . uncurry LockedPort . second unlockFile) mPortData

tryPort :: Int -> IO (Maybe (Int, FileLock))
tryPort port = do
  available <- portAvailable port
  if available
    then do
      mFileLock <- lockPort port
      pure $ (port,) <$> mFileLock
    else pure Nothing

portAvailable :: Int -> IO Bool
portAvailable port = do
  let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
      checkConnection addr =
        bracket
          (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
          close
          (\s -> bind s (addrAddress addr))

  addr : _ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just $ show port)
  isRight <$> tryIO (checkConnection addr)

