-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Chat.Main (main) where

import DA.Chat.Domain (Party(..))
import DA.Chat.UI (interactiveMain)
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
