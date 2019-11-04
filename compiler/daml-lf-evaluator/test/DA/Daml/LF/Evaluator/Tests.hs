-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Evaluator.Tests
  ( main
  ) where

import Data.Int (Int64)
import System.Environment.Blank (setEnv)
import System.FilePath ((</>))
import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.Lazy as BSL (fromStrict)
import qualified Data.Text as Text
import qualified Test.Tasty as Tasty (defaultMain,testGroup,TestTree)
import qualified Test.Tasty.HUnit as Tasty (assertBool,assertEqual,testCaseSteps)

import DA.Bazel.Runfiles (locateRunfiles,mainWorkspace)
import DA.Daml.LF.Optimize (optimizeWorld,World)
import DA.Daml.LF.Reader (readDalfs,Dalfs(..))
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Evaluator as EV

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  run tests

tests :: [Test]
tests =
  [ Test "decrement" 10 9
  , Test "fact" 4 24
  , Test "fact" 5 120

  , Test "dub_dub_dub" 1 8

  , Test "decrement" 0 (-1)
  , Test "thrice_decrement" 0 (-3)
  , Test "thrice_thrice_decrement" 0 (-27)

  , Test "length_list" 7 3
  , Test "sum_list" 7 24
  , Test "run_makeDecimal" 7 789

  , Test "nthPrime" 10 29
  , Test "nthPrime" 100 541

  , Test "run_sum_myList" 9 30
  , Test "run_sum_myList2" 99 300

  , Test "nfib" 10 177

  , Test "let1" 10 16
  , Test "let2" 10 26
  , Test "let3" 10 13
  ]

-- testing for DAML functions of type: `Int -> Int`
data Test = Test
  { functionName :: String
  , arg :: Int64
  , expected :: Int64
  }

run :: [Test] -> IO ()
run tests = do
  filename <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-evaluator/examples.dar")
  dalfs <- readDar filename
  world <- EV.decodeDalfs dalfs
  Tasty.defaultMain $ Tasty.testGroup "daml-lf-evaluator" (map (makeTasty world) tests)

readDar :: FilePath -> IO Dalfs
readDar inFile = do
  archiveBS <- BS.readFile inFile
  either fail pure $ readDalfs $ ZipArchive.toArchive $ BSL.fromStrict archiveBS

makeTasty :: World -> Test -> Tasty.TestTree
makeTasty world Test{functionName,arg,expected} = do
  let mn = LF.ModuleName ["Examples"]
  let vn = LF.ExprValName $ Text.pack functionName
  let name = Text.unpack (LF.unExprValName vn) <> "(" <> show arg <> ")"
  Tasty.testCaseSteps name $ \_step -> do

    -- check the original program evaluates as expected
    let prog = EV.simplify world mn vn
    let (actual,countsOrig) = EV.runIntProgArg prog arg
    Tasty.assertEqual "original" expected actual
    let EV.Counts{apps=a1,prims=p1,projections=q1} = countsOrig

    -- check the program constructed from optimized DAML-LF has same result
    worldO <- optimizeWorld world
    let progO = EV.simplify worldO mn vn
    let (actualO,countsOpt) = EV.runIntProgArg progO arg
    Tasty.assertEqual "optimized" expected actualO

    -- check the optimized program doesn't take more steps to evaluate
    -- in fact check that it has less application steps (i.e it has actually improved!)
    -- But check it executes the same number of primitive ops.
    let EV.Counts{apps=a2,prims=p2,projections=q2} = countsOpt
    let mkName tag x y = tag <> ":" <> show x <> "-->" <> show y
    Tasty.assertBool (mkName "apps" a1 a2) (a2 < a1)
    Tasty.assertBool (mkName "prim" p1 p2) (p2 == p1)
    Tasty.assertBool (mkName "proj" q1 q2) (q2 <= q1)

