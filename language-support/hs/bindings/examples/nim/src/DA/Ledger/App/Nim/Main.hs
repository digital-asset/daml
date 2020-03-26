-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.App.Nim.Main(main) where

import System.Environment(getArgs)
import DA.Ledger.App.Nim.Domain(Player(..))
import DA.Ledger.App.Nim.Robot(robotMain)
import DA.Ledger.App.Nim.UI(interactiveMain)

main :: IO ()
main = do
    args <- getArgs
    case parseArgs args of
        Just mode -> modeMain mode
        Nothing -> do
            putStrLn $ "failed to parse command line: " <> show args
            modeMain defaultMode

parseArgs :: [String] -> Maybe Mode
parseArgs = \case
    ["--robot"] -> Just (Robot defaultRobot)
    ["--robot", who] -> Just (Robot (Player who))
    [who] -> Just (Interactive (Player who))
    [] -> Just defaultMode
    _ -> Nothing

data Mode = Interactive Player | Robot Player

defaultPlayer :: Player
defaultPlayer = Player "Alice"

defaultRobot :: Player
defaultRobot = Player "Rob"

defaultMode :: Mode
defaultMode = Interactive defaultPlayer

modeMain :: Mode -> IO ()
modeMain = \case
    Interactive player -> interactiveMain player
    Robot player -> robotMain player
