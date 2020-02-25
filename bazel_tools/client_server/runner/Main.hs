-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import DA.PortFile
import System.Environment
import System.Process
import System.IO.Extra (newTempFile)
import Data.List.Split (splitOn)
import Control.Monad (forM_)


appendPortFileParam :: [String] -> IO ([(FilePath, IO ())], [String])
appendPortFileParam [] = return ([], [])
appendPortFileParam ("--participant" : kvs : rest) = do
  (tempFile, cleanup) <- newTempFile
  (files, newArgs) <- appendPortFileParam rest
  return ((tempFile, cleanup) : files, ["--participant", kvs ++ ",port-file=" ++ tempFile] <> newArgs)
appendPortFileParam (x:xs) = do
  (files, newArgs) <- appendPortFileParam xs
  return (files, x : newArgs)

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "

  (files, newServerArgs) <- appendPortFileParam (splitArgs serverArgs)

  let serverProc = proc serverExe newServerArgs
  withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
    forM_ files $ \(file, _cleanup) -> readPortFile maxRetries file
    callProcess clientExe (splitArgs clientArgs)
    forM_ files snd
