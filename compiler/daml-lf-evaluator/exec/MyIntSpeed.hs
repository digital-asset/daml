-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module MyIntSpeed (main) where

import Data.Int (Int64)
import Data.Time (getCurrentTime,diffUTCTime)
import System.Environment (getArgs)
import System.FilePath ((</>), isExtensionOf)
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified DA.Daml.LF.Ast as LF
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.Lazy as BSL(fromStrict)
import qualified Data.Text as Text

import DA.Bazel.Runfiles (locateRunfiles,mainWorkspace)
import DA.Daml.LF.Evaluator (decodeDalfs,simplify,runIntProgArg)
import DA.Daml.LF.Optimize (optimize)
import DA.Daml.LF.Reader (readDalfs,Dalfs(..))

main :: IO ()
main = do
  args <- getArgs
  let conf = parseArgs args
  putStrLn $ "nfib speed test (My Interpreter), " <> show conf
  nfib <- getNfib conf
  let loop arg = do
        -- keep incrementing the argument until the elapsed time > 0.5 second
        info@Info{elapsed} <- measure nfib arg
        if elapsed > 0.5 then print info else do
          print info
          loop (arg+1)
  loop 10 -- initial argument

data Conf = Conf { mode :: Mode } deriving Show
data Mode = Original | Normalized deriving Show

parseArgs :: [String] -> Conf
parseArgs = \case
  [] -> Conf { mode = Original }
  ["--norm"] -> Conf { mode = Normalized }
  args -> error $ "args:" <> show args

getNfib :: Conf -> IO (Int64 -> Int64)
getNfib Conf{mode} = do
  let funcName = "nfib"
  filename <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-evaluator/examples.dar")
  dalfs <- readDar filename
  (pkgs,[mod]) <- decodeDalfs dalfs
  mod <-
    case mode of
      Original -> return mod
      Normalized -> optimize pkgs mod
  let vn = LF.ExprValName $ Text.pack funcName
  let !prog = simplify pkgs mod vn
  return $ \arg -> do
    let (res,_counts) = runIntProgArg prog arg
    either (error . show) id res

readDar :: FilePath -> IO Dalfs
readDar inFile = do
  if "dar" `isExtensionOf` inFile then return () else fail "must be a dar"
  archiveBS <- BS.readFile inFile
  either fail pure $ readDalfs $ ZipArchive.toArchive $ BSL.fromStrict archiveBS

data Info = Info
  { arg :: Int64
  , res :: Int64
  , elapsed :: Seconds
  , speed :: Speed } deriving Show

type Seconds = Double
type Speed = Double

measure :: (Int64 -> Int64) -> Int64 -> IO Info
measure f arg = do
  before <- getCurrentTime
  let !res = f arg
  after <- getCurrentTime
  let elapsed = realToFrac $ diffUTCTime after before
  let speed = fromIntegral res / elapsed
  return $ Info { arg, res, elapsed, speed }
