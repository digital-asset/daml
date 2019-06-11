-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module Main(main) where

import System.Environment (getArgs)
import Domain (Party(..))
import UI (interactiveMain)

import qualified Data.Text.Lazy as Text

main :: IO ()
main = do
    args <- getArgs
    case parseArgs args of
        Just party -> UI.interactiveMain party
        Nothing -> do
            putStrLn $ "failed to parse command line: " <> show args
            UI.interactiveMain defaultParty

parseArgs :: [String] -> Maybe Party
parseArgs = \case
    [who] -> Just (Party (Text.pack who))
    [] -> Just defaultParty
    _ -> Nothing

defaultParty :: Party
defaultParty = Party "Alice"
