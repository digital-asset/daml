-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module Main(main) where

import           Control.Concurrent
import           LedgerHL           as Ledger
import           System.Environment

main :: IO ()
main = do
    port <- fmap (Port . read . head) $ getArgs
    putStrLn$ "Connecting to ledger on port: " ++ show port
    h <- Ledger.connect port
    let id = Ledger.identity h
    putStrLn$ "LedgerIdentity = " <> show id
    putStrLn "Getting Transaction streams."
    let alice = Party "Alice"
    let bob = Party "Bob"
    aliceTs <- Ledger.getTransactionStream h alice
    bobTs <- Ledger.getTransactionStream h bob
    putStrLn "Watching Transaction streams."
    watch alice aliceTs
    watch bob bobTs
    waitForALongTime
    return ()

watch :: Show a => Party -> ResponseStream a -> IO ()
watch party rs = do _ <- forkIO $ loop (1::Int); return ()
    where loop n = do
              x <- nextResponse rs
              putStrLn $ show party <> "(" <> show n <> ") = " <> show x
              loop (n+1)

waitForALongTime :: IO ()
waitForALongTime = do
    putStrLn "Pause for a year"
    threadDelay (10 ^ (6 :: Int) * 3600 * 24 * 365)
