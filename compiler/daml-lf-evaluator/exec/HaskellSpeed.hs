-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module HaskellSpeed
  ( main
  ) where

import Data.Time (getCurrentTime,diffUTCTime)

main :: IO ()
main = do
  putStrLn "nfib speed test (Haskell)"
  loop 30 -- initial argument
    where
      loop :: Int -> IO ()
      loop arg = do
        -- keep incrementing the argument until the elapsed time > 0.5 second
        info@Info{elapsed} <- measure nfib arg
        if elapsed > 0.5 then print info else do
          print info
          loop (arg+1)

nfib :: Int -> Int
nfib 0 = 1
nfib 1 = 1
nfib n = nfib (n-1) + nfib (n-2) + 1


data Info = Info
  { arg :: Int
  , res :: Int
  , elapsed :: Seconds
  , speed :: Speed } deriving Show

type Seconds = Double
type Speed = Double

measure :: (Int -> Int) -> Int -> IO Info
measure f arg = do
  before <- getCurrentTime
  let !res = f arg
  after <- getCurrentTime
  let elapsed = realToFrac $ diffUTCTime after before
  let speed = fromIntegral res / elapsed
  return $ Info { arg, res, elapsed, speed }

