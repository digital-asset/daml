-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import           LedgerIdentity     (Port (..), ledgerId)
import           System.Environment

main :: IO ()
main = do
    putStrLn "Ledger App (View)..."
    [port] <- getArgs
    id <- LedgerIdentity.ledgerId (Port (read port))
    print (port, id)

