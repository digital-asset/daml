-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Shake IDE API test suite. Run these manually with:
--
--      bazel run //compiler/damlc:damlc-shake-tests
--
-- Some tests cover open issues, these are run with 'testCaseFails'.
-- Once the issue is resolved, switch it to 'testCase'.
-- Otherwise this test suite will complain that the test is not failing.
module DA.Test.ShakeIdeClient (main, ideTests ) where

import qualified Test.Tasty.Extended as Tasty
import qualified Test.Tasty.HUnit    as Tasty
import qualified Data.Text.Extended  as T

import Data.Either
import System.Directory
import System.Environment.Blank (setEnv)
import Control.Monad.IO.Class

import DA.Daml.LF.ScenarioServiceClient as SS
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified DA.Service.Logger.Impl.Pure as Logger
import Development.IDE.Core.API.Testing
import Development.IDE.Core.Service.Daml(VirtualResource(..))

main :: IO ()
main = SS.withScenarioService Logger.makeNopHandle scenarioConfig $ \scenarioService -> do
  -- The scenario service is a shared resource so running tests in parallel doesn’t work properly.
  setEnv "TASTY_NUM_THREADS" "1" True
  -- The startup of the scenario service is fairly expensive so instead of launching a separate
  -- service for each test, we launch a single service that is shared across all tests.
  Tasty.deterministicMain (ideTests (Just scenarioService))
  where scenarioConfig = SS.defaultScenarioServiceConfig { SS.cnfJvmOptions = ["-Xmx200M"] }

ideTests :: Maybe SS.Handle -> Tasty.TestTree
ideTests mbScenarioService =
    Tasty.testGroup "IDE Shake API tests"
        [ -- Add categories of tests here
          basicTests mbScenarioService
        , minimalRebuildTests mbScenarioService
        , goToDefinitionTests mbScenarioService
        , onHoverTests mbScenarioService
        , dlintSmokeTests mbScenarioService
        , scenarioTests mbScenarioService
        , visualDamlTests
        ]

-- | Tasty test case from a ShakeTest.
testCase :: Maybe SS.Handle -> Tasty.TestName -> ShakeTest () -> Tasty.TestTree
testCase mbScenarioService testName test =
    Tasty.testCase testName $ do
        res <- runShakeTest mbScenarioService test
        Tasty.assertBool ("Shake test resulted in an error: " ++ show res) $ isRight res

-- | Test case that is expected to fail, because it's an open issue.
-- Annotate these with a JIRA ticket number.
testCaseFails :: Maybe SS.Handle -> Tasty.TestName -> ShakeTest () -> Tasty.TestTree
testCaseFails mbScenarioService testName test =
    Tasty.testCase ("FAILING " ++ testName) $ do
        res <- runShakeTest mbScenarioService test
        Tasty.assertBool "This ShakeTest no longer fails! Modify DA.Test.ShakeIdeClient to reflect this." $ isLeft res

-- | Basic API functionality tests.
basicTests :: Maybe SS.Handle -> Tasty.TestTree
basicTests mbScenarioService = Tasty.testGroup "Basic tests"
    [   testCase' "Set files of interest and expect no errors" example

    ,   testCase' "Set files of interest and expect parse error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,2,0) "Parse error"

    ,   testCase' "Set files of interest to clear parse error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,2,0) "Parse error"
            setFilesOfInterest []
            expectNoErrors

    ,   testCase' "Expect parse errors in two independent modules" $ do
            foo <- makeFile "src/Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 10"
                , "this is bad syntax"
                ]
            bar <- makeFile "src/Bar.daml" $ T.unlines
                [ "daml 1.2"
                , "module Bar where"
                , "bar : Int"
                , "bar = 10"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo, bar]
            expectOnlyErrors [((foo,4,0), "Parse error"), ((bar,4,0), "Parse error")]

    ,   testCase' "Simple module import" $ do
            foo <- makeFile "src/Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "import Bar"
                , "foo : Int"
                , "foo = bar"
                ]
            bar <- makeFile "src/Bar.daml" $ T.unlines
                [ "daml 1.2"
                , "module Bar where"
                , "bar : Int"
                , "bar = 10"
                ]
            setFilesOfInterest [foo, bar]
            expectNoErrors

    ,   testCase' "Cyclic module import" $ do
            f <- makeFile "src/Cycle.daml" $ T.unlines
                [ "daml 1.2"
                , "module Cycle where"
                , "import Cycle"
                ]
            setFilesOfInterest [f]
            expectOneError (f,2,7) "Cyclic module dependency between Cycle"

    ,   testCase' "Modify file to introduce error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            _ <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 10.5"
                ]
            expectOneError (foo,3,6) "Couldn't match expected type"

    ,   testCase' "Set buffer modified to introduce error then clear it" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            setBufferModified foo $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 10.5"
                ]
            expectOneError (foo,3,6) "Couldn't match expected type"
            setBufferNotModified foo
            expectNoErrors

    ,   testCase' "Open two modules with the same name but different directory" $ do
            f1 <- makeFile "src1/Main.daml" $ T.unlines
                [ "daml 1.2"
                , "module Main where"
                , "i : Int"
                , "i = 1"
                ]
            f2 <- makeFile "src2/Main.daml" $ T.unlines
                [ "daml 1.2"
                , "module Main where"
                ]
            setFilesOfInterest [f1, f2]
            expectNoErrors

    ,   testCase' "Run scenarios in two modules with the same name but different directory" $ do
            let header =
                    [ "daml 1.2"
                    , "module Main where" ]
                goodScenario =
                    [ "v = scenario do"
                    , "  pure ()" ]
                badScenario =
                    [ "v = scenario do"
                    , "  assert False" ]
                goodFileContent = T.unlines $ header ++ goodScenario
                badFileContent = T.unlines $ header ++ badScenario
            f1 <- makeFile "src1/Main.daml" goodFileContent
            f2 <- makeFile "src2/Main.daml" goodFileContent
            let vr1 = VRScenario f1 "v"
            let vr2 = VRScenario f2 "v"
            setFilesOfInterest [f1, f2]
            setOpenVirtualResources [vr1, vr2]
            expectNoErrors
            expectVirtualResource vr1 "Return value: {}"
            expectVirtualResource vr2 "Return value: {}"
            setBufferModified f2 badFileContent
            expectOneError (f2,2,0) "Aborted:  Assertion failed"
            expectVirtualResource vr1 "Return value: {}"
            expectVirtualResource vr2 "Aborted:  Assertion failed"

    ,   testCase' "Deleting a file you import DEL-7189" $ do
            a <- makeFile "A.daml" "daml 1.2 module A where; import B"
            setFilesOfInterest [a]
            expectOneError (a,0,32) "Could not find module"
            b <- makeFile "B.daml" "daml 1.2 module B where"
            expectWarning (a,0,25) "The import of ‘B’ is redundant"
            expectNoErrors
            liftIO $ removeFile (fromNormalizedFilePath b)
            expectOnlyDiagnostics
                [(DsError, (a,0,32), "Could not find module")
                -- the warning says around because of DEL-7199
                ,(DsWarning, (a,0,25), "The import of ‘B’ is redundant")]

    ,   testCase' "Early errors kill later warnings" $ do
            a <- makeFile "A.daml" "daml 1.2 module A where; import B"
            _ <- makeFile "B.daml" "daml 1.2 module B where"
            setFilesOfInterest [a]
            expectWarning (a,0,25) "The import of ‘B’ is redundant"
            setBufferModified a "???"
            expectOneError (a,0,0) "parse error on input"

    ,   testCase' "Loading two modules with the same name DEL-7175" $ do
            a <- makeFile "foo/Test.daml" "daml 1.2 module Test where"
            b <- makeFile "bar/Test.daml" "daml 1.2 module Test where"
            setFilesOfInterest [a, b]
            expectNoErrors

    ,   testCase' "Run two scenarios with the same name DEL-7175" $ do
            a <- makeFile "foo/Test.daml" "daml 1.2 module Test where main = scenario $ return \"foo\""
            b <- makeFile "bar/Test.daml" "daml 1.2 module Test where main = scenario $ return \"bar\""
            setFilesOfInterest [a, b]
            expectNoErrors
            let va = VRScenario a "main"
            let vb = VRScenario b "main"
            setOpenVirtualResources [va, vb]
            expectVirtualResource va "Return value: &quot;foo&quot;"
            expectVirtualResource vb "Return value: &quot;bar&quot;"

    , testCase' "Scenario with mangled names" $ do
            a <- makeFile "foo/MangledScenario'.daml" $ T.unlines
                [ "module MangledScenario' where"
                , "template T' with"
                , "    p : Party"
                , "  where"
                , "    signatory p"
                , "mangled' = scenario do"
                , "  alice <- getParty \"Alice\""
                , "  t' <- submit alice (create (T' alice))"
                , "  submit alice (exercise t' Archive)"
                ]
            setFilesOfInterest [a]
            expectNoErrors
            let va = VRScenario a "mangled'"
            setOpenVirtualResources [va]
            expectVirtualResource va "title=\"MangledScenario':T'\""


    ,   testCaseFails' "Modules must match their filename DEL-7175" $ do
            a <- makeFile "Foo/Test.daml" "daml 1.2 module Test where"
            setFilesOfInterest [a]
            expectNoErrors
            setBufferModified a "daml 1.2 module Foo.Test where"
            expectNoErrors
            setBufferModified a "daml 1.2 module Bob where"
            expectOneError (a,0,0) "HERE1"
            setBufferModified a "daml 1.2 module TEST where"
            expectOneError (a,0,0) "HERE2"

    ,   testCaseFails' "Case insensitive files and module names DEL-7175" $ do
            a <- makeFile "Test.daml" "daml 1.2 module Test where; import CaSe; import Case"
            _ <- makeFile "CaSe.daml" "daml 1.2 module Case where"
            setFilesOfInterest [a]
            expectNoErrors
    ]
    where
        testCase' = testCase mbScenarioService
        testCaseFails' = testCaseFails mbScenarioService

dlintSmokeTests :: Maybe SS.Handle -> Tasty.TestTree
dlintSmokeTests mbScenarioService = Tasty.testGroup "Dlint smoke tests"
  [    testCase' "Imports can be simplified" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "import DA.Optional"
                , "import DA.Optional(fromSome)"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Warning: Use fewer imports"
    ,  testCase' "Reduce duplication" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "import DA.List"
                , "testSort5 = scenario do"
                , "    let l = [ (2, const \"D\"), (1, const \"A\"), (1, const \"B\"), (3, const \"E\"), (1, const \"C\") ]"
                , "        m = sortOn fst l"
                , "        n = map fst m"
                , "    assert $ n == [1, 1, 1, 2, 3]"
                , "    let o = map (flip snd ()) m"
                , "    assert $ o == [\"A\", \"B\", \"C\", \"D\", \"E\"]"
                , "testSort4 = scenario do"
                , "    let l = [ (2, const \"D\"), (1, const \"A\"), (1, const \"B\"), (3, const \"E\"), (1, const \"C\") ]"
                , "        m = sortBy (\\x y -> compare (fst x) (fst y)) l"
                , "        n = map fst m"
                , "    assert $ n == [1, 1, 1, 2, 3]"
                , "    let o = map (flip snd ()) m"
                , "    assert $ o == [\"A\", \"B\", \"C\", \"D\", \"E\"]"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 7, 4) "Suggestion: Reduce duplication"
    ,  testCase' "Use language pragmas" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "{-# OPTIONS_GHC -XDataKinds #-}"
                , "daml 1.2"
                , "module Foo where"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 0, 0) "Warning: Use LANGUAGE pragmas"
    ,  testCase' "Use fewer pragmas" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "{-# LANGUAGE ScopedTypeVariables, DataKinds #-}"
                , "{-# LANGUAGE ScopedTypeVariables #-}"
                , "daml 1.2"
                , "module Foo where"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 0, 0) "Warning: Use fewer LANGUAGE pragmas"
    ,  testCase' "Use map" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "g : [Int] -> [Int]"
                , "g (x :: xs) = x + 1 :: g xs"
                , "g [] = []"]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 3, 0) "Warning: Use map"
    ,  testCase' "Use foldr" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "f : [Int] -> Int"
                , "f (x :: xs) = negate x + f xs"
                , "f [] = 0"]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 3, 0) "Suggestion: Use foldr"
    ,  testCase' "Short-circuited list comprehension" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = [x | False, x <- [1..10]]" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 6) "Suggestion: Short-circuited list comprehension"
    ,  testCase' "Redundant true guards" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = [x | True, x <- [1..10]]" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 6) "Suggestion: Redundant True guards"
    ,  testCase' "Move guards forward" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo feature = [x | x <- [1..10], feature]" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 14) "Suggestion: Move guards forward"
    ,  testCase' "Move map inside list comprehension" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = map f [x | x <- [1..10]] where f x = x * x" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 6) "Suggestion: Move map inside list comprehension"
    ,  testCase' "Use list literal" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = 1 :: 2 :: []" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 6) "Suggestion: Use list literal"
    ,  testCase' "Use list literal pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo (1 :: 2 :: []) = 1" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 4) "Suggestion: Use list literal pattern"
    ,  testCase' "Use '::'" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo x xs = [x] ++ xs" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 11) "Suggestion: Use ::"
    ,  testCase' "Use guards" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "truth i = if i == 1 then Some True else if i == 2 then Some False else None"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Suggestion: Use guards"
    ,  testCase' "Redundant guard" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo i | otherwise = True"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Suggestion: Redundant guard"
    ,  testCase' "Redundant where" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo i = i where"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Suggestion: Redundant where"
    ,  testCase' "Use otherwise" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo i | i == 1 = True | True = False"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Suggestion: Use otherwise"
    ,  testCase' "Use record patterns" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "data Foo = Foo with a : Int, b : Int, c : Int, d : Int"
                , "foo (Foo _ _ _ _) = True"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 3, 5) "Suggestion: Use record patterns"
    ,  testCase' "Used otherwise as a pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo otherwise = 1"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 4) "Warning: Used otherwise as a pattern"
    ,  testCase' "Redundant bang pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "{-# LANGUAGE BangPatterns #-}"
                , "daml 1.2"
                , "module Foo where"
                , "foo !True = 1"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 3, 4) "Warning: Redundant bang pattern"
    ,  testCase' "Redundant irrefutable pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo y = let ~x = 1 in y"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 12) "Warning: Redundant irrefutable pattern"
    ,  testCase' "Redundant as-pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo y@_ = True"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 4) "Warning: Redundant as-pattern"
    ,  testCase' "Redundant case (1)" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo i = case i of _ -> i"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 8) "Suggestion: Redundant case"
    ,  testCase' "Redundant case (2)" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo i = case i of i -> i"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 8) "Suggestion: Redundant case"
    ,  testCase' "Use let" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo g x = do"
                , "  y <- pure x"
                , "  g y"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 10) "Suggestion: Use let"
    ,  testCase' "Redundant void" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "import DA.Action"
                , "import DA.Foldable"
                , "foo g xs = void $ forA_ g xs"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 4, 11) "Warning: Redundant void"
    ,  testCase' "Use <$>" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo f g bar = do x <- bar; return (f $ g x)"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 14) "Warning: Use <$>"
    ,  testCase' "Redundant return" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo bar = do x <- bar; return x"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 10) "Warning: Redundant return"
    ]
  where
      testCase' = testCase mbScenarioService

minimalRebuildTests :: Maybe SS.Handle -> Tasty.TestTree
minimalRebuildTests mbScenarioService = Tasty.testGroup "Minimal rebuild tests"
    [   testCase' "Minimal rebuild" $ do
            a <- makeFile "A.daml" "daml 1.2\nmodule A where\nimport B"
            _ <- makeFile "B.daml" "daml 1.2\nmodule B where"
            setFilesOfInterest [a]
            expectLastRebuilt $ \_ _ -> True -- anything is legal
            expectLastRebuilt $ \_ _ -> False

            -- now break the code, should only rebuild the thing that broke
            setBufferModified a "daml 1.2\nmodule A where\nimport B\n?"
            expectLastRebuilt $ \_ file -> file == "A.daml"
            expectLastRebuilt $ \_ _ -> False

            -- now fix it
            setBufferModified a "daml 1.2\nmodule A where\nimport B\n "
            expectLastRebuilt $ \_ file -> file == "A.daml"
            expectLastRebuilt $ \_ _ -> False
    ]
    where
        testCase' = testCase mbScenarioService


-- | "Go to definition" tests.
goToDefinitionTests :: Maybe SS.Handle -> Tasty.TestTree
goToDefinitionTests mbScenarioService = Tasty.testGroup "Go to definition tests"
    [   testCase' "Go to definition in same module" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = bar"
                , "bar : Int"
                , "bar = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,2,[-1])   Missing             -- (out of range)
            -- expectGoToDefinition (foo,2,[0..2]) (At (foo,3,0))   -- "foo" [see failing test "Go to definition takes type sig to definition"]
            expectGoToDefinition (foo,2,[2..4]) Missing             -- " : "
            expectGoToDefinition (foo,2,[9])    Missing             -- "\n"
            expectGoToDefinition (foo,2,[10])   Missing             -- (out of range)
            expectGoToDefinition (foo,3,[0..2]) (At (foo,3,0))      -- "foo"
            expectGoToDefinition (foo,3,[3..5]) Missing             -- " = "
            expectGoToDefinition (foo,3,[6..8]) (At (foo,5,0))      -- "bar"
            expectGoToDefinition (foo,3,[9])    Missing             -- "\n"
            expectGoToDefinition (foo,3,[10])   Missing             -- (out of range)

    ,   testCase' "Go to definition across modules" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "import Bar"
                , "foo : Int"
                , "foo = bar"
                ]
            bar <- makeFile "Bar.daml" $ T.unlines
                [ "daml 1.2"
                , "module Bar where"
                , "bar : Int"
                , "bar = 10"
                ]
            setFilesOfInterest [foo, bar]
            expectNoErrors
            expectGoToDefinition (foo,2,[7..9]) (At (bar,0,0)) -- "Bar" from "import Bar"
            expectGoToDefinition (foo,4,[6..8]) (At (bar,3,0)) -- "bar" from "foo = bar"

    ,   testCase' "Go to definition handles touching identifiers" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = bar+++baz"
                , "bar = 10"
                , "(+++) = (+)"
                , "baz = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,2,[6..8])   (At (foo,3,0)) -- "bar"
            expectGoToDefinition (foo,2,[9..11])  (At (foo,4,0)) -- "+++"
            expectGoToDefinition (foo,2,[12..14]) (At (foo,5,0)) -- "baz"

    ,   testCase' "Take bound variable to its binding" $ do
            foo <- makeModule "Foo"
                [ "foo : Int -> Int -> Optional Int"
                , "foo x = \\y -> do"
                , "  z <- Some 10"
                , "  Some (x + y + z)"
                ]
            expectNoErrors
            expectGoToDefinition (foo,5,[8])  (At (foo,3,4)) -- "x"
            expectGoToDefinition (foo,5,[12]) (At (foo,3,9)) -- "y"
            expectGoToDefinition (foo,5,[16]) (At (foo,4,2)) -- "z"

    ,   testCase' "Go to definition should be tight" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = bar"
                , "bar=baz"
                , "baz = 10"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[0..2]) (At (foo,2,0))
            expectGoToDefinition (foo,2,[3..5]) Missing
            expectGoToDefinition (foo,2,[6..8]) (At (foo,3,0))
            expectGoToDefinition (foo,2,[9]) Missing

            expectGoToDefinition (foo,3,[0..2]) (At (foo,3,0))
            expectGoToDefinition (foo,3,[3]) Missing
            expectGoToDefinition (foo,3,[4..6]) (At (foo,4,0))
            expectGoToDefinition (foo,3,[7]) Missing

    ,   testCaseFails' "Go to definition takes type sig to definition" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 0"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[0]) (At (foo,3,0))

    ,   testCase' "Go to definition on type in type sig" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "data X = X {}"
                , "foo : X"
                , "foo = X"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,3,[6]) (At (foo,2,0))

    ,   testCase' "Go to definition on type annotation" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "data X = X {}"
                , "foo : X"
                , "foo = X : X"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,4,[10]) (At (foo,2,0))

    ,   testCase' "Go to definition should ignore negative column" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[-1]) Missing

    ,   testCaseFails' "Take variable in template to its declaration" $ do
            foo <- makeModule "Foo"
                [ "template Coin"
                , "  with"
                , "    owner: Party"
                , "  where"
                , "    signatory owner"
                , "    agreement show owner <> \" has a coin\""
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            -- This actually ends up pointing to "concat".
            expectGoToDefinition (foo,6,[14..18]) (At (foo,4,4)) -- "owner" in signatory clause
            -- We do have a codespan for "owner" at (7,[19..23])
            -- but we report (7,[4..41]) as the definition for it.
            expectGoToDefinition (foo,7,[19..23]) (At (foo,4,4)) -- "owner" in agreement

    ,   testCase' "Standard library type points to standard library" $ do
            foo <- makeModule "Foo"
                [ "foo : Optional (List Bool)"
                , "foo = Some [False]"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,2,[6..13]) (In "Prelude") -- "Optional"
            expectGoToDefinition (foo,2,[16..19]) (In "DA.Internal.Compatible") -- "List"
            -- Bool is from GHC.Types which is wired into the compiler
            expectGoToDefinition (foo,2,[20]) Missing

    ,   testCase' "Go to definition takes export list to definition" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo (foo, A(B)) where"
                , "foo : Int"
                , "foo = 0"
                , "data A = B Int"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            -- foo
            expectGoToDefinition (foo,1,[13..14]) (At (foo,3,0))
            -- A
            expectGoToDefinition (foo,1,[17..17]) (At (foo,4,0))
            -- B
            expectGoToDefinition (foo,1,[19..19]) (At (foo,4,9))

    ,    testCase' "Cross-package goto definition" $ do
            foo <- makeModule "Foo"
                [ "test = scenario do"
                , "  p <- getParty \"Alice\""
                , "  pure ()"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo, 3, [7..14]) (In "DA.Internal.LF")
    ]
    where
        testCase' = testCase mbScenarioService
        testCaseFails' = testCaseFails mbScenarioService

onHoverTests :: Maybe SS.Handle -> Tasty.TestTree
onHoverTests mbScenarioService = Tasty.testGroup "On hover tests"
    [ testCase' "Type for uses but not for definitions" $ do
        f <- makeFile "F.daml" $ T.unlines
            [ "daml 1.2"
            , "module F where"
            , "inc: Int -> Int"
            , "inc x = x + 1"
            , "six: Int"
            , "six = inc 5"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,2,[0..2]) NoInfo                 -- signature of inc
        expectTextOnHover (f,3,[0..2]) $ HasType "Int -> Int" -- definition of inc
        expectTextOnHover (f,3,[4]) $ HasType "Int"           -- binding of x
        expectTextOnHover (f,4,[0..2]) NoInfo                 -- signature of six
        expectTextOnHover (f,5,[0..2]) $ HasType "Int"        -- definition of six
        expectTextOnHover (f,5,[6..8]) $ HasType "Int -> Int" -- use of inc

    , testCase' "Type of variable bound in function definition" $ do
        f <- makeModule "F"
            [ "f: Int -> Int"
            , "f x = x + 1" ]
        setFilesOfInterest [f]
        expectTextOnHover (f,3,[6]) $ HasType "Int" -- use of x

    , testCase' "Type of literals" $ do
        f <- makeModule "F"
            [ "f: Int -> Int"
            , "f x = x + 110"
            , "hello = \"hello\"" ]
        setFilesOfInterest [f]
        expectTextOnHover (f,3,[10..12]) $ HasType "Int" -- literal 110
        expectTextOnHover (f,4,[8..14]) $ HasType "Text" -- literal "hello"

    , testCase' "Type of party" $ do
        f <- makeModule "F"
            [ "s = scenario $ do"
            , "  alice <- getParty \"Alice\""
            , "  submit alice $ pure ()"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,4,[9..13]) $ HasType "Party" -- use of alice

    , testCaseFails' "Type of signatories" $ do
        f <- makeModule "F"
            [ "template Iou"
            , "  with"
            , "    issuer : Party"
            , "  where"
            , "    signatory issuer"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,6,[14..19]) $ HasType "Party" -- issuer in signatory clause

    , testCase' "Hover over choice does not display `==` or `show`" $ do
        f <- makeModule "F"
            [ "template Coin"
            , "  with"
            , "    owner : Party"
            , "  where"
            , "    signatory owner"
            , "    controller owner can"
            , "      Delete : ()"
            , "        do return ()"
            , "      Transfer : ContractId Coin"
            , "        with newOwner : Party"
            , "        do create this with owner = newOwner"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,8,[6..11]) $ NotContaining "=="   -- Delete choice
        expectTextOnHover (f,8,[6..11]) $ NotContaining "show"
        expectTextOnHover (f,10,[6..13]) $ NotContaining "=="  -- Transfer choice
        expectTextOnHover (f,10,[6..13]) $ NotContaining "show"

    , testCase' "Type of user-defined == and show functions" $ do
        f <- makeModule "F"
            [ "(==) : Text -> Bool"
            , "(==) t = True"
            , "show : Bool -> Int"
            , "show b = 2"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,3,[0..3]) $ Contains "```daml\n==\n: Text -> Bool\n```\n"
        expectTextOnHover (f,5,[0..3]) $ Contains "```daml\nshow\n: Bool -> Int\n```\n"

    , testCaseFails' "Type of choice" $ do
        f <- makeModule "F"
            [ "template Coin"
            , "  with"
            , "    owner : Party"
            , "  where"
            , "    signatory owner"
            , "    controller owner can"
            , "      Delete : ()"
            , "        do return ()"
            , "      Transfer : ContractId Coin"
            , "        with newOwner : Party"
            , "        do create this with owner = newOwner"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,8,[6..11]) $ HasType "Update ()" -- Delete choice
        expectTextOnHover (f,10,[6..13]) $ HasType "Party -> Update (ContractId Coin)" -- Transfer choice
    , testCase' "Haddock comment" $ do
        f <- makeModule "F"
            [ "-- | Important docs"
            , "f : a -> a"
            , "f x = x"
            ]
        setFilesOfInterest [f]
        expectNoErrors
        expectTextOnHover (f,4,[0]) $ Contains "Important docs"
    ]
    where
        testCase' = testCase mbScenarioService
        testCaseFails' = testCaseFails mbScenarioService

scenarioTests :: Maybe SS.Handle -> Tasty.TestTree
scenarioTests mbScenarioService = Tasty.testGroup "Scenario tests"
    [ testCase' "Run an empty scenario" $ do
          let fooContent = T.unlines
                  [ "daml 1.2"
                  , "module Foo where"
                  , "v = scenario do"
                  , "  pure ()"
                  ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectNoErrors
          expectVirtualResource vr "Return value: {}"
    , testCase' "Run a scenario with a failing assertion" $ do
          let fooContent = T.unlines
                  [ "daml 1.2"
                  , "module Foo where"
                  , "v = scenario do"
                  , "  assert False"
                  ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectOneError (foo,2,0) "Aborted:  Assertion failed"
          expectVirtualResource vr "Aborted:  Assertion failed"
    , testCase' "Virtual resources should update when files update" $ do
          let fooContent = T.unlines
                 [ "daml 1.2"
                 , "module Foo where"
                 , "v = scenario $ assert True"
                 ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectVirtualResource vr "Return value: {}"
          setBufferModified foo $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "v = scenario $ assert False"
              ]
          expectVirtualResource vr "Aborted:  Assertion failed"
    , testCase' "Scenario error disappears when scenario is deleted" $ do
        let goodScenario =
                [ "daml 1.2"
                , "module F where"
                , "example1 = scenario $ assert True"
                ]
            badScenario = [ "example2 = scenario $ assert False" ]
        f <- makeFile "F.daml" $ T.unlines goodScenario
        setFilesOfInterest [f]
        let vr1 = VRScenario f "example1"
        setOpenVirtualResources [vr1]
        expectNoErrors
        expectVirtualResource vr1 "Return value: {}"
        setBufferModified f $ T.unlines $ goodScenario ++ badScenario
        let vr2 = VRScenario f "example2"
        setOpenVirtualResources [vr1, vr2]
        expectOneError (f, 3, 0) "Scenario execution failed"
        expectVirtualResource vr2 "Aborted:  Assertion failed"
        setBufferModified f $ T.unlines goodScenario
        expectNoErrors
        expectVirtualResource vr1 "Return value: {}"
    , testCase' "Virtual resource gets updated with a note when file compiles, but scenario is no longer present" $ do
        let scenario1F =
                [ "daml 1.2"
                , "module F where"
                , "scenario1 = scenario $ pure \"f1\""
                ]
            scenario1G =
                [ "daml 1.2"
                , "module G where"
                , "scenario1 = scenario $ pure \"g1\""
                ]
            scenario12F =
                [ "daml 1.2"
                , "module F where"
                , "scenario1 = scenario $ pure \"f1\""
                , "scenario2 = scenario $ pure \"f2\""
                ]
        f <- makeFile "F.daml" $ T.unlines scenario1F
        g <- makeFile "G.daml" $ T.unlines scenario1G
        setFilesOfInterest [f, g]
        let vr1F = VRScenario f "scenario1"
        let vr2F = VRScenario f "scenario2"
        let vr1G = VRScenario g "scenario1"

        setOpenVirtualResources [vr1F]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectNoVirtualResourceNote vr1F

        setOpenVirtualResources [vr1F, vr1G]
        expectNoErrors
        expectVirtualResource vr1G "Return value: &quot;g1&quot;"
        expectNoVirtualResourceNote vr1G

        setBufferModified f $ T.unlines scenario12F
        setOpenVirtualResources [vr1F, vr2F, vr1G]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectNoVirtualResourceNote vr1F
        expectVirtualResource vr2F "Return value: &quot;f2&quot;"
        expectNoVirtualResourceNote vr2F

        setBufferModified f $ T.unlines scenario1F
        setOpenVirtualResources [vr1F, vr2F, vr1G]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectVirtualResource vr2F "Return value: &quot;f2&quot;"
        expectVirtualResourceNote vr2F "This scenario no longer exists in the source file"
        expectVirtualResourceNote vr2F "F.daml"

        setBufferModified f $ T.unlines scenario12F
        setOpenVirtualResources [vr1F, vr2F, vr1G]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectNoVirtualResourceNote vr1F
        expectVirtualResource vr2F "Return value: &quot;f2&quot;"
        expectNoVirtualResourceNote vr2F
        expectVirtualResource vr1G "Return value: &quot;g1&quot;"
        expectNoVirtualResourceNote vr1G
    , testCase' "Virtual resource gets updated with a note when file does not compile anymore" $ do
          let scenario1F =
                  [ "daml 1.2"
                  , "module F where"
                  , "scenario1 = scenario $ pure \"f1\""
                  ]
              scenario1G =
                  [ "daml 1.2"
                  , "module G where"
                  , "scenario1 = scenario $ pure \"g1\""
                  ]
              scenario1FInvalid =
                  [ "daml 1.2"
                  , "module F where"
                  , "this is bad syntax"
                  ]
          f <- makeFile "F.daml" $ T.unlines scenario1F
          g <- makeFile "G.daml" $ T.unlines scenario1G
          setFilesOfInterest [f, g]
          let vr1F = VRScenario f "scenario1"
          let vr1G = VRScenario g "scenario1"

          setOpenVirtualResources [vr1F]
          expectNoErrors
          expectVirtualResource vr1F "Return value: &quot;f1&quot;"
          expectNoVirtualResourceNote vr1F

          setOpenVirtualResources [vr1F, vr1G]
          expectNoErrors
          expectVirtualResource vr1G "Return value: &quot;g1&quot;"
          expectNoVirtualResourceNote vr1G

          setBufferModified f $ T.unlines scenario1FInvalid
          setOpenVirtualResources [vr1F, vr1G]
          expectOneError (f,2,0) "Parse error"

          expectVirtualResource vr1F "Return value: &quot;f1&quot;"
          expectVirtualResourceNote vr1F "The source file containing this scenario no longer compiles"
          expectVirtualResourceNote vr1F "F.daml"

          expectVirtualResource vr1G "Return value: &quot;g1&quot;"
          expectNoVirtualResourceNote vr1G

          setBufferModified f $ T.unlines scenario1F
          setOpenVirtualResources [vr1F, vr1G]
          expectNoErrors
          expectVirtualResource vr1F "Return value: &quot;f1&quot;"
          expectVirtualResource vr1G "Return value: &quot;g1&quot;"
    , testCase' "Scenario in file of interest but not opened" $ do
          let fooContent = T.unlines
                  [ "daml 1.2"
                  , "module Foo where"
                  , "v = scenario $ assert False"
                  ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources []
          -- We expect to get the diagnostic here but no virtual resource.
          expectOneError (foo,2,0) "Aborted:  Assertion failed"
          expectNoVirtualResource vr
    , testCase' "Scenario opened but not in files of interest" $ do
          foo <- makeFile "Foo.daml" $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "v = scenario $ assert True"
              ]
          let vr = VRScenario foo "v"
          expectNoVirtualResource vr
          setOpenVirtualResources [vr]
          expectVirtualResource vr "Return value: {}"
    , testCase' "Update dependency of open scenario that is not in files of interest" $ do
           let fooContent = T.unlines
                  [ "daml 1.2"
                  , "module Foo where"
                  , "import Bar"
                  , "v = scenario $ bar ()"
                  ]
               barContent = T.unlines
                  [ "daml 1.2"
                  , "module Bar where"
                  , "bar : () -> Scenario ()"
                  , "bar () = assert True"
                  ]
           foo <- makeFile "Foo.daml" fooContent
           bar <- makeFile "Bar.daml" barContent
           let vr = VRScenario foo "v"
           setOpenVirtualResources [vr]
           expectNoErrors
           expectVirtualResource vr "Return value: {}"
           setBufferModified bar $ T.unlines
               [ "daml 1.2"
               , "module Bar where"
               , "bar : () -> Scenario ()"
               , "bar _ = assert False"
               ]
           expectOneError (foo,3,0) "Aborted:  Assertion failed"
           expectVirtualResource vr "Aborted:  Assertion failed"
    , testCase' "Open scenario after scenarios have already been run" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "v= scenario $ assert True"
              ]
            let vr = VRScenario foo "v"
            setFilesOfInterest [foo]
            expectNoVirtualResource vr
            setOpenVirtualResources [vr]
            expectVirtualResource vr "Return value: {}"
    , testCase' "Failing scenario produces stack trace in correct order" $ do
          let fooContent = T.unlines
                 [ "daml 1.2"
                 , "module Foo where"
                 , "boom = fail \"BOOM\""
                 , "test : Scenario ()"
                 , "test = boom"
                 ]

          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "test"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectVirtualResourceRegex vr "Stack trace:.*- boom.*Foo:3:1.*- test.*Foo:5:1"
    , testCase' "debug is lazy" $ do
        let goodScenario =
                [ "daml 1.2"
                , "module LazyDebug where"
                , "import DA.Foldable"
                , "import DA.Action.State"

                , "test = scenario $ pure $ runState go 0"
                , "go : State Int ()"
                , "go = forA_ [0,1] $ \\x -> do"
                , "    debug \"foo\""
                , "    modify (+1)"
                , "    debug \"bar\""
                ]
        f <- makeFile "LazyDebug.daml" $ T.unlines goodScenario
        setFilesOfInterest [f]
        let vr = VRScenario f "test"
        setOpenVirtualResources [vr]
        let quote s = "&quot;" <> s <> "&quot;"
        let lineBreak = "<br>  "
        expectNoErrors
        expectVirtualResource vr $ T.concat
            [ "Trace: ", lineBreak
            , quote "foo", lineBreak
            , quote "bar", lineBreak
            , quote "foo", lineBreak
            , quote "bar"
            ]
    ]
    where
        testCase' = testCase mbScenarioService


visualDamlTests :: Tasty.TestTree
visualDamlTests = Tasty.testGroup "Visual Tests"
    [   testCase' "Template with no actions (edges) from choices" $ do
            foo <- makeModule "F"
                [ "template Coin"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    controller owner can"
                , "      Delete : ()"
                , "        do return ()"
                ]
            setFilesOfInterest [foo]
            expectedGraph foo (
                ExpectedGraph {expectedSubgraphs =
                                [ExpectedSubGraph {expectedNodes = ["Create","Archive","Delete"]
                                      , expectedTplFields = ["owner"]
                                      , expectedTemplate = "Coin"}
                                ]
                                , expectedEdges = []})
        , testCase' "Fetch shoud not be an create action" $ do
            fetchTest <- makeModule "F"
                [ "template Coin"
                , "  with"
                , "    owner : Party"
                , "    amount : Int"
                , "  where"
                , "    signatory owner"
                , "    controller owner can"
                , "      nonconsuming ReducedCoin : ()"
                , "        with otherCoin : ContractId Coin"
                , "        do "
                , "        cn <- fetch otherCoin"
                , "        return ()"
                ]
            setFilesOfInterest [fetchTest]
            expectNoErrors
            expectedGraph fetchTest ( ExpectedGraph {expectedSubgraphs =
                                        [ExpectedSubGraph {expectedNodes = ["Create","Archive","ReducedCoin"]
                                            , expectedTplFields = ["owner","amount"]
                                            , expectedTemplate = "Coin"}]
                              , expectedEdges = []})
        , testCase' "Exercise should add an edge" $ do
            exerciseTest <- makeModule "F"
                [ "template TT"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    controller owner can"
                , "      Consume : ()"
                , "        with coinId : ContractId Coin"
                , "        do exercise coinId Delete"
                , "template Coin"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    controller owner can"
                , "        Delete : ()"
                , "            do return ()"
                ]
            setFilesOfInterest [exerciseTest]
            expectNoErrors
            expectedGraph exerciseTest (ExpectedGraph
                [ ExpectedSubGraph { expectedNodes = ["Create", "Archive", "Delete"]
                                   , expectedTplFields = ["owner"]
                                   , expectedTemplate = "Coin"
                                    }
                , ExpectedSubGraph { expectedNodes = ["Create", "Consume", "Archive"]
                                   , expectedTplFields = ["owner"]
                                   , expectedTemplate = "TT"}]

                [(ExpectedChoiceDetails {expectedConsuming = True
                                        , expectedName = "Consume"},
                  ExpectedChoiceDetails {expectedConsuming = True
                                        , expectedName = "Delete"})
                ])
        -- test case taken from #5726
        , testCase' "ExerciseByKey should add an edge" $ do
            exerciseByKeyTest <- makeModule "F"
                [ "template Ping"
                , "  with"
                , "    party : Party"
                , "  where"
                , "    signatory party"
                , "    key party: Party"
                , "    maintainer key"
                , ""
                , "    controller party can"
                , "      nonconsuming ArchivePong : ()"
                , "        with"
                , "          pong : ContractId Pong"
                , "        do"
                , "          exercise pong Archive"
                , ""
                , "template Pong"
                , "  with"
                , "    party : Party"
                , "  where"
                , "    signatory party"
                , ""
                , "    controller party can"
                , "      nonconsuming ArchivePing : ()"
                , "        with"
                , "          pingParty : Party"
                , "        do"
                , "          exerciseByKey @Ping pingParty Archive"
                ]
            setFilesOfInterest [exerciseByKeyTest]
            expectNoErrors
            expectedGraph exerciseByKeyTest (ExpectedGraph
                [ ExpectedSubGraph { expectedNodes = ["Create", "ArchivePong", "Archive"]
                                   , expectedTplFields = ["party"]
                                   , expectedTemplate = "Ping"
                                    }
                , ExpectedSubGraph { expectedNodes = ["Create", "Archive", "ArchivePing"]
                                   , expectedTplFields = ["party"]
                                   , expectedTemplate = "Pong"}
                ]
                [ (ExpectedChoiceDetails {expectedConsuming = False
                                         , expectedName = "ArchivePong"},
                   ExpectedChoiceDetails {expectedConsuming = True
                                         , expectedName = "Archive"})
                , (ExpectedChoiceDetails {expectedConsuming = False
                                         , expectedName = "ArchivePing"},
                   ExpectedChoiceDetails {expectedConsuming = True
                                         , expectedName = "Archive"})
                ])
        , testCase' "Create on other template should be edge" $ do
            createTest <- makeModule "F"
                [ "template TT"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    controller owner can"
                , "      CreateCoin : ContractId Coin"
                , "        do create Coin with owner"
                , "template Coin"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                ]
            setFilesOfInterest [createTest]
            expectNoErrors
            expectedGraph createTest (ExpectedGraph
                {expectedSubgraphs = [ExpectedSubGraph { expectedNodes = ["Create","Archive"]
                                                       , expectedTplFields = ["owner"]
                                                       , expectedTemplate = "Coin"}
                                     ,ExpectedSubGraph { expectedNodes = ["Create","Archive","CreateCoin"]
                                                       , expectedTplFields = ["owner"]
                                                       , expectedTemplate = "TT"
                                                       }
                                     ]
                , expectedEdges = [(ExpectedChoiceDetails {expectedConsuming = True, expectedName = "CreateCoin"}
                                   ,ExpectedChoiceDetails {expectedConsuming = False, expectedName = "Create"})]})

    ]
    where
        testCase' = testCase Nothing
-- | Suppress unused binding warning in case we run out of tests for open issues.
_suppressUnusedWarning :: ()
_suppressUnusedWarning = testCaseFails `seq` ()
