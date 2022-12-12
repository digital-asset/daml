-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import Data.Maybe (isJust)
import System.Environment
import Control.Exception.Safe
import Control.Monad.Loops (untilJust)
import System.Process.Typed
import Data.List.Split (splitOn)
import Control.Monad (guard, when)
import Network.HTTP.Client (parseUrlThrow)
import qualified Network.HTTP.Simple as HTTP
import Control.Concurrent
import System.Info.Extra
import System.Process (terminateProcess)

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, runnerArgs] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let serverProc = proc serverExe (splitArgs serverArgs)
  healthPort : _ :: [Int] <- pure $ reverse $ map read $ splitArgs runnerArgs
  withProcessTerm serverProc $ \ph -> do
    waitForHealthcheck (threadDelay 500000) healthPort
    runProcess_ (proc clientExe (splitArgs clientArgs))
    -- See the comment on DA.Daml.Helper.Util.withProcessWait_'
    when isWindows (terminateProcess $ unsafeProcessHandle ph)

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
