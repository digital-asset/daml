-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Robot(robotMain) where

import Control.Concurrent
import Control.Monad(forever)

import Data.Maybe(listToMaybe)
import Data.Foldable(forM_)
import System.Console.ANSI(Color(..))
import System.Time.Extra(sleep)

import Domain
import Logging
import NimLedger(Handle,connect)
import Interact(PlayerState(..),makePlayerState,runSubmit)
import Local(possibleActions)

robotMain :: Player -> IO ()
robotMain player = do
    let log = plainLog
    h <- connect (colourLog Red log)
    let botLog = colourLog Magenta log
    botLog $ "Running robot for: " <> show player
    runBotFor botLog h player
    forever (threadDelay maxBound)

runBotFor :: Logger -> Handle -> Player -> IO ()
runBotFor log h player = do
    ps <- makePlayerState h log player
    _tid <- forkIO (robot h log ps)
    return ()

robot :: Handle -> Logger -> PlayerState -> IO ()
robot h log ps = loop
  where
    loop = do
        sleep 2
        let PlayerState{sv} = ps
        s <- readMVar sv
        forM_ (pick (Local.possibleActions s)) $ \lt -> do
            --log $ "playing: " <> show lt
            runSubmit h log ps lt
        loop

pick :: [a] -> Maybe a
pick = listToMaybe
