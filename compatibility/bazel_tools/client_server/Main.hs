-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main(main) where

import Control.Concurrent
import Control.Exception.Safe
import Control.Monad (forM_, guard)
import Control.Monad.Loops (untilJust)
import Data.List.Split (splitOn)
import Data.Maybe (isJust)
import Network.HTTP.Client (parseUrlThrow)
import Network.Socket
import qualified Network.HTTP.Simple as HTTP
import System.Environment
import System.Process

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, runnerArgs] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let serverProc = proc serverExe (splitArgs serverArgs)
  let ports :: [Int] = read <$> splitArgs runnerArgs
  withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
    forM_ ports $ \port -> waitForConnectionOnPort (threadDelay 500000) port
    maybeHealthCheck (threadDelay 500000) ports
    callProcess clientExe (splitArgs clientArgs)


waitForConnectionOnPort :: IO () -> Int -> IO ()
waitForConnectionOnPort sleep port = do
    let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
    addr : _ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just $ show port)
    untilJust $ do
        r <- tryIO $ checkConnection addr
        case r of
            Left _ -> sleep *> pure Nothing
            Right _ -> pure $ Just ()
    where
        checkConnection addr = bracket
              (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
              close
              (\s -> connect s (addrAddress addr))

maybeHealthCheck :: IO () -> [Int] -> IO ()
maybeHealthCheck sleep (_:healthPort:_) = waitForHealthcheck sleep healthPort
maybeHealthCheck _ _ = pure ()

waitForHealthcheck :: IO () -> Int -> IO ()
waitForHealthcheck sleep port = do
    request <- parseUrlThrow $ "http://localhost:"  <> show port <> "/health"
    untilJust $ do
      r <- tryJust (\e -> guard (isIOException e || isHttpException e)) $ HTTP.httpNoBody request
      case r of
        Left _ -> sleep *> pure Nothing
        Right _ -> pure $ Just ()
  where isIOException e = isJust (fromException e :: Maybe IOException)
        isHttpException e = isJust (fromException e :: Maybe HTTP.HttpException)