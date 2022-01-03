-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Ledger.App.Chat.Main (main) where

import DA.Ledger.App.Chat.Domain (Party(..))
import DA.Ledger.App.Chat.UI (interactiveMain)
import System.Environment (getArgs)
import qualified Data.Text.Lazy as Text

main :: IO ()
main = do
    args <- getArgs
    case parseArgs args of
        Just party -> interactiveMain party
        Nothing -> do
            putStrLn $ "failed to parse command line: " <> show args
            interactiveMain defaultParty

parseArgs :: [String] -> Maybe Party
parseArgs = \case
    [who] -> Just (Party (Text.pack who))
    [] -> Just defaultParty
    _ -> Nothing

defaultParty :: Party
defaultParty = Party "Alice"
