-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-% LANGUAGE OverloadedStrings %-}

module Main(main) where

import Control.Concurrent
import Control.Exception.Safe
import Control.Monad (forM, forM_)
import Control.Monad.Loops (untilJust)
import Data.List.Split (splitOn)
import Network.Socket
import System.Directory (removeFile)
import System.Environment
import System.IO.Temp (emptySystemTempFile)
import System.Process
import qualified Data.Text as Text

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, _] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  (portFile, serverArgs') <- generatePortFile (splitArgs serverArgs)
  let serverProc = proc serverExe serverArgs'
  withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
    maybePort <- forM portFile $ \file -> do
      putStrLn $ "Waiting for the port in the file \"" <> file <> "\" to open..."
      waitForConnectionInPortFile (threadDelay 500000) file
    let clientArgs' = case maybePort of
                        Nothing -> splitArgs clientArgs
                        Just port -> map (interpolateVariable "PORT" (show port)) (splitArgs clientArgs)
    callProcess clientExe clientArgs'
  forM_ portFile removeFile

interpolateVariable :: String -> String -> String -> String
interpolateVariable variable replacement =
  Text.unpack . Text.replace ("%" <> Text.pack variable <> "%") (Text.pack replacement) . Text.pack

generatePortFile :: [String] -> IO (Maybe String, [String])
generatePortFile args = do
  if any (Text.isInfixOf "%PORT_FILE%" . Text.pack) args
    then do
      portFile <- emptySystemTempFile "port"
      removeFile portFile
      let replacedArgs = map (interpolateVariable "PORT_FILE" portFile) args
      return (Just portFile, replacedArgs)
    else
      return (Nothing, args)

waitForConnectionInPortFile :: IO () -> FilePath -> IO Int
waitForConnectionInPortFile sleep portFile = do
  let hints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }
  untilJust $ do
    eitherPortFileContents <- tryIO (Text.unpack . Text.strip . Text.pack <$> readFile portFile)
    case eitherPortFileContents of
      Left _ -> sleep *> pure Nothing
      Right portFileContents ->
        case reads @Int portFileContents of
          [(port, "")] -> do
            addr : _ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just $ show port)
            connectionResult <- tryIO $ checkConnection addr
            case connectionResult of
                Left _ -> sleep *> pure Nothing
                Right _ -> pure $ Just port
          _ -> sleep *> pure Nothing
  where
    checkConnection addr = bracket
      (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
      close
      (\s -> connect s (addrAddress addr))
