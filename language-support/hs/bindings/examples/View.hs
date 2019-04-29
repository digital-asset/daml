
module Main(main) where

import System.Environment
import LedgerIdentity(Port(..),ledgerId)

main :: IO ()
main = do
  putStrLn "Ledger App (View)..."
  [port] <- getArgs
  id <- LedgerIdentity.ledgerId (Port (read port))
  print (port,id)
