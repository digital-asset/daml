-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import Control.Concurrent
import Control.Exception.Safe
import Control.Monad (forM_, when)
import Control.Monad.Loops (untilJust)
import Data.List.Split (splitOn)
import Network.Socket
import System.Environment
import System.Info.Extra
import System.Process (terminateProcess)
import System.Process.Typed

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, runnerArgs] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let serverProc = proc serverExe (splitArgs serverArgs)
  let ports :: [Int] = read <$> splitArgs runnerArgs
  withProcessTerm serverProc $ \ph -> do
    forM_ ports $ \port -> waitForConnectionOnPort (threadDelay 500000) port
    runProcess_ (proc clientExe (splitArgs clientArgs))
    -- See the comment on DA.Daml.Helper.Util.withProcessWait_'
    when isWindows (terminateProcess $ unsafeProcessHandle ph)


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
