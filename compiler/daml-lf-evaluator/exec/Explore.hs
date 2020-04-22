-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Explore -- explore the benefit of normalization
  ( main
  ) where

import Control.Monad (forM_,unless)
import Data.Int (Int64)
import Data.List (isPrefixOf)
import System.Environment (getArgs)
import System.FilePath ((</>), isExtensionOf)
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.Lazy as BSL(fromStrict)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text

import DA.Bazel.Runfiles (locateRunfiles,mainWorkspace)
import DA.Daml.LF.Evaluator (decodeDalfs,simplify,runIntProgArg,Prog(..),DefKey(..),ppExp)
import DA.Daml.LF.Optimize (optimize)
import DA.Daml.LF.Reader (readDalfs,Dalfs(..))
import qualified DA.Daml.LF.Ast as LF

main :: IO ()
main = do
  args <- getArgs
  let conf = parseArgs args
  runConf conf

data Mode = JustEval | EvalAndNorm deriving (Eq)

data Conf = Conf
  { mode :: Mode
  , funcName :: String
  , arg :: Int64
  }

defaultConf :: Conf
defaultConf = Conf
  { mode = EvalAndNorm
  , funcName = "thrice_sub"
  , arg = 0
  }

parseArgs :: [String] -> Conf
parseArgs args = loop args defaultConf where
  loop args conf = case args of
    "--just-eval":rest -> loop rest $ conf { mode = JustEval }
    [] -> conf
    [funcName] -> conf { funcName }
    funcName:arg:rest ->
      if "--" `isPrefixOf` arg
      then loop (arg:rest) $ conf { funcName }
      else loop rest $ conf { funcName, arg = read arg }

runConf :: Conf -> IO ()
runConf = \case
  Conf{mode,funcName,arg} -> do
    filename <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-evaluator/examples.dar")
    dalfs <- readDar filename
    (pkgs,mainPackage,[mod]) <- decodeDalfs dalfs
    let vn = LF.ExprValName $ Text.pack funcName
    putStrLn $ "==["<>funcName<>"]============================"
    let prog = simplify pkgs mod vn
    runProg "original" prog arg
    unless (mode == JustEval) $ do
      modO <- optimize pkgs mainPackage mod
      let progO = simplify pkgs modO vn
      runProg "new-norm" progO arg

runProg :: String -> Prog -> Int64 -> IO ()
runProg title prog arg = do
  let Prog{defs,start} = prog
  putStrLn $ "--["<>title<>"]----------------------------"
  putStrLn $ "(main): " <> ppExp start
  forM_ (Map.toList defs) $ \(i,(DefKey(_,name),exp)) ->
    putStrLn $ show i <> "("<> Text.unpack (LF.unExprValName name) <> "): " <> ppExp exp
  putStrLn "--------------------------------------------------"
  let (res,count) = runIntProgArg prog arg
  putStrLn $ "arg = " <> show arg <> ", result = " <> show res <> ", #apps = " <> show count
  putStrLn  "--------------------------------------------------"

readDar :: FilePath -> IO Dalfs
readDar inFile = do
  if "dar" `isExtensionOf` inFile then return () else fail "must be a dar"
  archiveBS <- BS.readFile inFile
  either fail pure $ readDalfs $ ZipArchive.toArchive $ BSL.fromStrict archiveBS

