-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main(main) where

import System.Environment
import Control.Exception.Safe
import Control.Monad.Loops (untilJust)
import Data.List (stripPrefix)
import Data.Maybe (mapMaybe)
import System.Process
import Data.List.Split (splitOn)
import Control.Monad (forM_)
import Network.Socket
import Control.Concurrent

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, runnerArgs] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let serverProc = proc serverExe (splitArgs serverArgs)

  let splitRunnerArgs = splitArgs runnerArgs
  let ports = map read $ mapMaybe (stripPrefix "--port=") splitRunnerArgs
  let dars = mapMaybe (stripPrefix "--dar=") splitRunnerArgs
  (ledgerApiPort:_) <- pure ports

  withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
    forM_ ports $ \port -> waitForConnectionOnPort (threadDelay 500000) port
    forM_ dars $ \dar -> callProcess clientExe ["ledger", "upload-dar", "--host", "localhost", "--port", show ledgerApiPort, dar]
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
