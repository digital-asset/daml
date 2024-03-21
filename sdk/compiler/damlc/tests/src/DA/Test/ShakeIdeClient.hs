-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Shake IDE API test suite. Run these manually with:
--
--      bazel run //compiler/damlc/tests:shake
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

import qualified DA.Daml.LF.Ast.Version as LF
import qualified DA.Daml.Options.Types as Daml (Options (..))
import DA.Daml.LF.ScenarioServiceClient as SS
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified DA.Service.Logger.Impl.Pure as Logger
import Development.IDE.Core.API.Testing
import Development.IDE.Core.Service.Daml(VirtualResource(..))

import DA.Test.DamlcIntegration (ScriptPackageData, withDamlScriptDep)

main :: IO ()
main = SS.withScenarioService LF.versionDefault Logger.makeNopHandle scenarioConfig $ \scenarioService -> do
  -- The scenario services are shared resources so running tests in parallel doesn’t work properly.
  setEnv "TASTY_NUM_THREADS" "1" True
  -- The startup of each scenario service is fairly expensive so instead of launching a separate
  -- service for each test, we launch a single service that is shared across all tests on the same LF version.
  
  withDamlScriptDep Nothing $ \scriptPackageData ->
    Tasty.deterministicMain $ ideTests (Just scenarioService) scriptPackageData
  where scenarioConfig = SS.defaultScenarioServiceConfig { SS.cnfJvmOptions = ["-Xmx200M"] }

ideTests :: Maybe SS.Handle -> ScriptPackageData -> Tasty.TestTree
ideTests mbScenarioService scriptPackageData =
    Tasty.testGroup "IDE Shake API tests"
        [ -- Add categories of tests here
          basicTests mbScenarioService scriptPackageData
        , minimalRebuildTests mbScenarioService
        , goToDefinitionTests mbScenarioService scriptPackageData
        , onHoverTests mbScenarioService scriptPackageData
        , dlintSmokeTests mbScenarioService
        , scriptTests mbScenarioService scriptPackageData
        ]

addScriptOpts :: Maybe ScriptPackageData -> Daml.Options -> Daml.Options
addScriptOpts = maybe id $ \(packageDbPath, packageFlags) opts -> opts
    { Daml.optPackageDbs = [packageDbPath]
    , Daml.optPackageImports = packageFlags
    }

-- | Tasty test case from a ShakeTest.
testCase :: Maybe SS.Handle -> Maybe ScriptPackageData -> Tasty.TestName -> ShakeTest () -> Tasty.TestTree
testCase mbScenarioService mScriptPackageData testName test =
    Tasty.testCase testName $ do
        res <- runShakeTestOpts (addScriptOpts mScriptPackageData) mbScenarioService test
        Tasty.assertBool ("Shake test resulted in an error: " ++ show res) $ isRight res

-- | Test case that is expected to fail, because it's an open issue.
-- Annotate these with a JIRA ticket number.
testCaseFails :: Maybe SS.Handle -> Maybe ScriptPackageData -> Tasty.TestName -> ShakeTest () -> Tasty.TestTree
testCaseFails mbScenarioService mScriptPackageData testName test =
    Tasty.testCase ("FAILING " ++ testName) $ do
        res <- runShakeTestOpts (addScriptOpts mScriptPackageData) mbScenarioService test
        Tasty.assertBool "This ShakeTest no longer fails! Modify DA.Test.ShakeIdeClient to reflect this." $ isLeft res

-- | Basic API functionality tests.
basicTests :: Maybe SS.Handle -> ScriptPackageData -> Tasty.TestTree
basicTests mbScenarioService scriptPackageData = Tasty.testGroup "Basic tests"
    [   testCase' "Set files of interest and expect no errors" example

    ,   testCase' "Set files of interest and expect parse error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,1,0) "Parse error"

    ,   testCase' "Set files of interest to clear parse error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,1,0) "Parse error"
            setFilesOfInterest []
            expectNoErrors

    ,   testCase' "Expect parse errors in two independent modules" $ do
            foo <- makeFile "src/Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                , "foo = 10"
                , "this is bad syntax"
                ]
            bar <- makeFile "src/Bar.daml" $ T.unlines
                [ "module Bar where"
                , "bar : Int"
                , "bar = 10"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo, bar]
            expectOnlyErrors [((foo,3,0), "Parse error"), ((bar,3,0), "Parse error")]

    ,   testCase' "Simple module import" $ do
            foo <- makeFile "src/Foo.daml" $ T.unlines
                [ "module Foo where"
                , "import Bar"
                , "foo : Int"
                , "foo = bar"
                ]
            bar <- makeFile "src/Bar.daml" $ T.unlines
                [ "module Bar where"
                , "bar : Int"
                , "bar = 10"
                ]
            setFilesOfInterest [foo, bar]
            expectNoErrors

    ,   testCase' "Cyclic module import" $ do
            f <- makeFile "src/Cycle.daml" $ T.unlines
                [ "module Cycle where"
                , "import Cycle"
                ]
            setFilesOfInterest [f]
            expectOneError (f,1,7) "Cyclic module dependency between Cycle"

    ,   testCase' "Modify file to introduce error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            _ <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                , "foo = 10.5: Decimal"
                ]
            expectOneError (foo,2,6) "Couldn't match type"

    ,   testCase' "Set buffer modified to introduce error then clear it" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            setBufferModified foo $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                 , "foo = 10.5: Decimal"
                ]
            expectOneError (foo,2,6) "Couldn't match type"
            setBufferNotModified foo
            expectNoErrors

    ,   testCase' "Open two modules with the same name but different directory" $ do
            f1 <- makeFile "src1/Main.daml" $ T.unlines
                [ "module Main where"
                , "i : Int"
                , "i = 1"
                ]
            f2 <- makeFile "src2/Main.daml" $ T.unlines
                [ "module Main where"
                ]
            setFilesOfInterest [f1, f2]
            expectNoErrors

    ,   testCase' "Run scripts in two modules with the same name but different directory" $ do
            let header =
                    [ "module Main where"
                    , "import Daml.Script" ]
                goodScript =
                    [ "v = script do"
                    , "  pure ()" ]
                badScript =
                    [ "v = script do"
                    , "  assert False" ]
                goodFileContent = T.unlines $ header ++ goodScript
                badFileContent = T.unlines $ header ++ badScript
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
            expectOneError (f2,2,0) "Assertion failed"
            expectVirtualResource vr1 "Return value: {}"
            expectVirtualResource vr2 "Assertion failed"

    ,   testCase' "Deleting a file you import DEL-7189" $ do
            a <- makeFile "A.daml" "module A where; import B"
            setFilesOfInterest [a]
            expectOneError (a,0,23) "Could not find module"
            b <- makeFile "B.daml" "module B where"
            expectWarning (a,0,16) "The import of ‘B’ is redundant"
            expectNoErrors
            liftIO $ removeFile (fromNormalizedFilePath b)
            expectOnlyDiagnostics
                [(DsError, (a,0,23), "Could not find module")
                -- the warning says around because of DEL-7199
                ,(DsWarning, (a,0,16), "The import of ‘B’ is redundant")]

    ,   testCase' "Early errors kill later warnings" $ do
            a <- makeFile "A.daml" "module A where; import B"
            _ <- makeFile "B.daml" "module B where"
            setFilesOfInterest [a]
            expectWarning (a,0,16) "The import of ‘B’ is redundant"
            setBufferModified a "???"
            expectOneError (a,0,0) "parse error on input"

    ,   testCase' "Loading two modules with the same name DEL-7175" $ do
            a <- makeFile "foo/Test.daml" "module Test where"
            b <- makeFile "bar/Test.daml" "module Test where"
            setFilesOfInterest [a, b]
            expectNoErrors

    ,   testCase' "Run two Scripts with the same name DEL-7175" $ do
            a <- makeFile "foo/Test.daml" "module Test where; import Daml.Script; main = script $ return \"foo\""
            b <- makeFile "bar/Test.daml" "module Test where; import Daml.Script; main = script $ return \"bar\""
            setFilesOfInterest [a, b]
            expectNoErrors
            let va = VRScenario a "main"
            let vb = VRScenario b "main"
            setOpenVirtualResources [va, vb]
            expectVirtualResource va "Return value: &quot;foo&quot;"
            expectVirtualResource vb "Return value: &quot;bar&quot;"

    -- Todo, the ' in the module name causes error in either Script runner or Scenario Service, unclear which.
    , testCaseFails' "Script with mangled names (Ticket #16585)" $ do
            a <- makeFile "foo/MangledScript'.daml" $ T.unlines
                [ "module MangledScript' where"
                , "import Daml.Script"
                , "template T' with"
                , "    p : Party"
                , "  where"
                , "    signatory p"
                , "mangled' = script do"
                , "  alice <- allocateParty \"Alice\""
                , "  t' <- submit alice (createCmd (T' alice))"
                , "  submit alice (exerciseCmd t' Archive)"
                , "  pure (T1 0)"
                , "data NestedT = T1 { t1 : Int } | T2 { t2 : Int }"
                ]
            setFilesOfInterest [a]
            expectNoErrors
            let va = VRScenario a "mangled'"
            setOpenVirtualResources [va]
            expectVirtualResource va "title=\"MangledScript':T'\""
            expectVirtualResource va "MangledScript&#39;:NestedT:T1"


    ,   testCaseFails' "Modules must match their filename DEL-7175" $ do
            a <- makeFile "Foo/Test.daml" "module Test where"
            setFilesOfInterest [a]
            expectNoErrors
            setBufferModified a "module Foo.Test where"
            expectNoErrors
            setBufferModified a "module Bob where"
            expectOneError (a,0,0) "HERE1"
            setBufferModified a "module TEST where"
            expectOneError (a,0,0) "HERE2"

    ,   testCaseFails' "Case insensitive files and module names DEL-7175" $ do
            a <- makeFile "Test.daml" "module Test where; import CaSe; import Case"
            _ <- makeFile "CaSe.daml" "module Case where"
            setFilesOfInterest [a]
            expectNoErrors
    ,   testCase' "Record dot updates" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "data Outer = Outer {inner : Inner}"
                , "data Inner = Inner {field : Int}"
                , "f : Outer -> Outer"
                , "f o = o {inner.field = 2}"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
    ,   testCase' "Record dot update errors" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "data Outer = Outer {inner : Inner}"
                , "data Inner = Inner {field : Int}"
                , "f : Outer -> Outer"
                , "f o = o {inner.fied = 2}"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo, 4, 6) "fied"
    ]
    where
        testCase' = testCase mbScenarioService (Just scriptPackageData)
        testCaseFails' = testCaseFails mbScenarioService (Just scriptPackageData)

dlintSmokeTests :: Maybe SS.Handle -> Tasty.TestTree
dlintSmokeTests mbScenarioService = Tasty.testGroup "Dlint smoke tests"
  [    testCase' "Imports can be simplified" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "import DA.Optional"
                , "import DA.Optional(fromSome)"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 0) "Warning: Use fewer imports"
    -- This hint is now disabled. See PR
    -- https://github.com/digital-asset/daml/pull/6423 for details.
    -- ,  testCase' "Reduce duplication" $ do
    --         foo <- makeFile "Foo.daml" $ T.unlines
    --             [ "module Foo where"
    --             , "import DA.List"
    --             , "testSort5 = scenario do"
    --             , "    let l = [ (2, const \"D\"), (1, const \"A\"), (1, const \"B\"), (3, const \"E\"), (1, const \"C\") ]"
    --             , "        m = sortOn fst l"
    --             , "        n = map fst m"
    --             , "    assert $ n == [1, 1, 1, 2, 3]"
    --             , "    let o = map (flip snd ()) m"
    --             , "    assert $ o == [\"A\", \"B\", \"C\", \"D\", \"E\"]"
    --             , "testSort4 = scenario do"
    --             , "    let l = [ (2, const \"D\"), (1, const \"A\"), (1, const \"B\"), (3, const \"E\"), (1, const \"C\") ]"
    --             , "        m = sortBy (\\x y -> compare (fst x) (fst y)) l"
    --             , "        n = map fst m"
    --             , "    assert $ n == [1, 1, 1, 2, 3]"
    --             , "    let o = map (flip snd ()) m"
    --             , "    assert $ o == [\"A\", \"B\", \"C\", \"D\", \"E\"]"
    --             ]
    --         setFilesOfInterest [foo]
    --         expectNoErrors
    --         expectDiagnostic DsInfo (foo, 6, 4) "Suggestion: Reduce duplication"
    ,  testCase' "Use language pragmas" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "{-# OPTIONS_GHC -XDataKinds #-}"
                , "module Foo where"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 0, 0) "Warning: Use LANGUAGE pragmas"
    ,  testCase' "Use fewer pragmas" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "{-# LANGUAGE ScopedTypeVariables, DataKinds #-}"
                , "{-# LANGUAGE ScopedTypeVariables #-}"
                , "module Foo where"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 0, 0) "Warning: Use fewer LANGUAGE pragmas"
    ,  testCase' "Use map" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "g : [Int] -> [Int]"
                , "g (x :: xs) = x + 1 :: g xs"
                , "g [] = []"]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Warning: Use map"
    ,  testCase' "Use foldr" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "f : [Int] -> Int"
                , "f (x :: xs) = negate x + f xs"
                , "f [] = 0"]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 0) "Suggestion: Use foldr"
    ,  testCase' "Short-circuited list comprehension" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = [x | False, x <- [1..10]]" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 6) "Suggestion: Short-circuited list comprehension"
    ,  testCase' "Redundant true guards" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = [x | True, x <- [1..10]]" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 6) "Suggestion: Redundant True guards"
    ,  testCase' "Move guards forward" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo feature = [x | x <- [1..10], feature]" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 14) "Suggestion: Move guards forward"
    ,  testCase' "Move map inside list comprehension" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = map f [x | x <- [1..10]] where f x = x * x" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 6) "Suggestion: Move map inside list comprehension"
    ,  testCase' "Use list literal" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = 1 :: 2 :: []" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 6) "Suggestion: Use list literal"
    ,  testCase' "Use list literal pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo (1 :: 2 :: []) = 1" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 4) "Suggestion: Use list literal pattern"
    ,  testCase' "Use '::'" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo x xs = [x] ++ xs" ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 11) "Suggestion: Use ::"
    ,  testCase' "Use guards" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "truth i = if i == 1 then Some True else if i == 2 then Some False else None"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 0) "Suggestion: Use guards"
    ,  testCase' "Redundant guard" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo i | otherwise = True"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 0) "Suggestion: Redundant guard"
    ,  testCase' "Redundant where" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo i = i where"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 0) "Suggestion: Redundant where"
    ,  testCase' "Use otherwise" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo i | i == 1 = True | True = False"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 0) "Suggestion: Use otherwise"
    ,  testCase' "Use record patterns" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "data Foo = Foo with a : Int, b : Int, c : Int, d : Int"
                , "foo (Foo _ _ _ _) = True"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 5) "Suggestion: Use record patterns"
    ,  testCase' "Used otherwise as a pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo otherwise = 1"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 4) "Warning: Used otherwise as a pattern"
    ,  testCase' "Redundant bang pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "{-# LANGUAGE BangPatterns #-}"
                , "module Foo where"
                , "foo !True = 1"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 2, 4) "Warning: Redundant bang pattern"
    ,  testCase' "Redundant irrefutable pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo y = let ~x = 1 in y"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 12) "Warning: Redundant irrefutable pattern"
    ,  testCase' "Redundant as-pattern" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo y@_ = True"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 4) "Warning: Redundant as-pattern"
    ,  testCase' "Redundant case (1)" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo i = case i of _ -> i"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 8) "Suggestion: Redundant case"
    ,  testCase' "Redundant case (2)" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo i = case i of i -> i"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 8) "Suggestion: Redundant case"
    ,  testCase' "Use let" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo g x = do"
                , "  y <- pure x"
                , "  g y"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 10) "Suggestion: Use let"
    ,  testCase' "Redundant void" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "import DA.Action"
                , "import DA.Foldable"
                , "foo g xs = void $ forA_ g xs"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 3, 11) "Warning: Redundant void"
    ,  testCase' "Use <$>" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo f g bar = do x <- bar; return (f $ g x)"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 14) "Warning: Use <$>"
    ,  testCase' "Redundant return" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo bar = do x <- bar; return x"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectDiagnostic DsInfo (foo, 1, 10) "Warning: Redundant return"
    ]
  where
      testCase' = testCase mbScenarioService Nothing

minimalRebuildTests :: Maybe SS.Handle -> Tasty.TestTree
minimalRebuildTests mbScenarioService = Tasty.testGroup "Minimal rebuild tests"
    [   testCase' "Minimal rebuild" $ do
            a <- makeFile "A.daml" "module A where\nimport B"
            _ <- makeFile "B.daml" "module B where"
            setFilesOfInterest [a]
            expectLastRebuilt $ \_ _ -> True -- anything is legal
            expectLastRebuilt $ \_ _ -> False

            -- now break the code, should only rebuild the thing that broke
            setBufferModified a "module A where\nimport B\n?"
            expectLastRebuilt $ \_ file -> file == "A.daml"
            expectLastRebuilt $ \_ _ -> False

            -- now fix it
            setBufferModified a "module A where\nimport B\n "
            expectLastRebuilt $ \_ file -> file == "A.daml"
            expectLastRebuilt $ \_ _ -> False
    ]
    where
        testCase' = testCase mbScenarioService Nothing


-- | "Go to definition" tests.
goToDefinitionTests :: Maybe SS.Handle -> ScriptPackageData -> Tasty.TestTree
goToDefinitionTests mbScenarioService scriptPackageData = Tasty.testGroup "Go to definition tests"
    [   testCase' "Go to definition in same module" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                , "foo = bar"
                , "bar : Int"
                , "bar = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,1,[-1])   Missing             -- (out of range)
            -- expectGoToDefinition (foo,1,[0..2]) (At (foo,2,0))   -- "foo" [see failing test "Go to definition takes type sig to definition"]
            expectGoToDefinition (foo,1,[2..4]) Missing             -- " : "
            expectGoToDefinition (foo,1,[9])    Missing             -- "\n"
            expectGoToDefinition (foo,1,[10])   Missing             -- (out of range)
            expectGoToDefinition (foo,2,[0..2]) (At (foo,2,0))      -- "foo"
            expectGoToDefinition (foo,2,[3..5]) Missing             -- " = "
            expectGoToDefinition (foo,2,[6..8]) (At (foo,4,0))      -- "bar"
            expectGoToDefinition (foo,2,[9])    Missing             -- "\n"
            expectGoToDefinition (foo,2,[10])   Missing             -- (out of range)

    ,   testCase' "Go to definition across modules" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "import Bar"
                , "foo : Int"
                , "foo = bar"
                ]
            bar <- makeFile "Bar.daml" $ T.unlines
                [ "module Bar where"
                , "bar : Int"
                , "bar = 10"
                ]
            setFilesOfInterest [foo, bar]
            expectNoErrors
            expectGoToDefinition (foo,1,[7..9]) (At (bar,0,0)) -- "Bar" from "import Bar"
            expectGoToDefinition (foo,3,[6..8]) (At (bar,2,0)) -- "bar" from "foo = bar"

    ,   testCase' "Go to definition handles touching identifiers" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = bar+++baz"
                , "bar = 10"
                , "(+++) = (+)"
                , "baz = 10"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,1,[6..8])   (At (foo,2,0)) -- "bar"
            expectGoToDefinition (foo,1,[9..11])  (At (foo,3,0)) -- "+++"
            expectGoToDefinition (foo,1,[12..14]) (At (foo,4,0)) -- "baz"

    ,   testCase' "Take bound variable to its binding" $ do
            foo <- makeModule "Foo"
                [ "foo : Int -> Int -> Optional Int"
                , "foo x = \\y -> do"
                , "  z <- Some 10"
                , "  Some (x + y + z)"
                ]
            expectNoErrors
            expectGoToDefinition (foo,4,[8])  (At (foo,2,4)) -- "x"
            expectGoToDefinition (foo,4,[12]) (At (foo,2,9)) -- "y"
            expectGoToDefinition (foo,4,[16]) (At (foo,3,2)) -- "z"

    ,   testCase' "Go to definition should be tight" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = bar"
                , "bar=baz"
                , "baz = 10"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,1,[0..2]) (At (foo,1,0))
            expectGoToDefinition (foo,1,[3..5]) Missing
            expectGoToDefinition (foo,1,[6..8]) (At (foo,2,0))
            expectGoToDefinition (foo,1,[9]) Missing

            expectGoToDefinition (foo,2,[0..2]) (At (foo,2,0))
            expectGoToDefinition (foo,2,[3]) Missing
            expectGoToDefinition (foo,2,[4..6]) (At (foo,3,0))
            expectGoToDefinition (foo,2,[7]) Missing

    ,   testCaseFails' "Go to definition takes type sig to definition" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo : Int"
                , "foo = 0"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,1,[0]) (At (foo,2,0))

    ,   testCase' "Go to definition on type in type sig" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "data X = X {}"
                , "foo : X"
                , "foo = X"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[6]) (At (foo,1,0))

    ,   testCase' "Go to definition on type annotation" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "data X = X {}"
                , "foo : X"
                , "foo = X : X"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,3,[10]) (At (foo,1,0))

    ,   testCase' "Go to definition should ignore negative column" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo where"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,1,[-1]) Missing

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
            expectGoToDefinition (foo,5,[14..18]) (At (foo,3,4)) -- "owner" in signatory clause
            -- We do have a codespan for "owner" at (6,[19..23])
            -- but we report (6,[4..41]) as the definition for it.
            expectGoToDefinition (foo,6,[19..23]) (At (foo,3,4)) -- "owner" in agreement

    ,   testCase' "Standard library type points to standard library" $ do
            foo <- makeModule "Foo"
                [ "foo : Optional (List Bool)"
                , "foo = Some [False]"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,1,[6..13]) (In "Prelude") -- "Optional"
            expectGoToDefinition (foo,1,[16..19]) (In "DA.Internal.Compatible") -- "List"
            -- Bool is from GHC.Types which is wired into the compiler
            expectGoToDefinition (foo,1,[20]) Missing

    ,   testCase' "Go to definition takes export list to definition" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "module Foo (foo, A(B)) where"
                , "foo : Int"
                , "foo = 0"
                , "data A = B Int"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            -- foo
            expectGoToDefinition (foo,0,[13..14]) (At (foo,2,0))
            -- A
            expectGoToDefinition (foo,0,[17..17]) (At (foo,3,0))
            -- B
            expectGoToDefinition (foo,0,[19..19]) (At (foo,3,9))

    ,    testCase' "Cross-package goto definition" $ do
            foo <- makeModule "Foo"
                [ "import Daml.Script"
                , "test = script do"
                , "  p <- allocateParty \"Alice\""
                , "  pure ()"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo, 3, [7..19]) (In "Daml.Script")

    ,    testCase' "Exception goto definition" $ do
            foo <- makeModule "Foo"
                [ "exception Err where"
                , "type E = Err"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo, 2, [9..11]) (At (foo,1,10))

    ,    testCase' "Interface goto definition" $ do
            foo <- makeModule "Foo"
                [ "interface Iface where"
                , "  viewtype EmptyInterfaceView"
                , "  getOwner : Party"
                , "  nonconsuming choice Noop : ()"
                , "    controller getOwner this"
                , "    do pure ()"
                , "type I = Iface"
                , "type C = Noop"
                , "meth : Iface -> Party"
                , "meth = getOwner"
                , "data EmptyInterfaceView = EmptyInterfaceView {}"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            -- Iface
            expectGoToDefinition (foo, 7, [9..13]) (At (foo,1,10))
            -- Noop
            expectGoToDefinition (foo, 8, [9..12]) (At (foo,4,22))
            -- getOwner
            expectGoToDefinition (foo, 10, [7..14]) (At (foo,3,2))
    ]
    where
        testCase' = testCase mbScenarioService (Just scriptPackageData)
        testCaseFails' = testCaseFails mbScenarioService (Just scriptPackageData)

onHoverTests :: Maybe SS.Handle -> ScriptPackageData -> Tasty.TestTree
onHoverTests mbScenarioService scriptPackageData = Tasty.testGroup "On hover tests"
    [ testCase' "Type for uses but not for definitions" $ do
        f <- makeFile "F.daml" $ T.unlines
            [ "module F where"
            , "inc: Int -> Int"
            , "inc x = x + 1"
            , "six: Int"
            , "six = inc 5"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,1,[0..2]) NoInfo                 -- signature of inc
        expectTextOnHover (f,2,[0..2]) $ HasType "Int -> Int" -- definition of inc
        expectTextOnHover (f,2,[4]) $ HasType "Int"           -- binding of x
        expectTextOnHover (f,3,[0..2]) NoInfo                 -- signature of six
        expectTextOnHover (f,4,[0..2]) $ HasType "Int"        -- definition of six
        expectTextOnHover (f,4,[6..8]) $ HasType "Int -> Int" -- use of inc

    , testCase' "Type of variable bound in function definition" $ do
        f <- makeModule "F"
            [ "f: Int -> Int"
            , "f x = x + 1" ]
        setFilesOfInterest [f]
        expectTextOnHover (f,2,[6]) $ HasType "Int" -- use of x

    , testCase' "Type of literals" $ do
        f <- makeModule "F"
            [ "f: Int -> Int"
            , "f x = x + 110"
            , "hello = \"hello\"" ]
        setFilesOfInterest [f]
        expectTextOnHover (f,2,[10..12]) $ HasType "Int" -- literal 110
        expectTextOnHover (f,3,[8..14]) $ HasType "Text" -- literal "hello"

    , testCase' "Type of party" $ do
        f <- makeModule "F"
            [ "import Daml.Script"
            , "s = script $ do"
            , "  alice <- allocateParty \"Alice\""
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
        expectTextOnHover (f,5,[14..19]) $ HasType "Party" -- issuer in signatory clause

    , testCase' "Hover over choice does not display `==` or `show`" $ do
        f <- makeModule "F"
            [ "template Coin"
            , "  with"
            , "    owner : Party"
            , "  where"
            , "    signatory owner"
            , "    choice Delete : ()"
            , "      controller owner"
            , "      do return ()"
            , "    choice Transfer : ContractId Coin"
            , "      with newOwner : Party"
            , "      controller owner"
            , "      do create this with owner = newOwner"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,7,[6..11]) $ NotContaining "=="   -- Delete choice
        expectTextOnHover (f,7,[6..11]) $ NotContaining "show"
        expectTextOnHover (f,9,[6..13]) $ NotContaining "=="  -- Transfer choice
        expectTextOnHover (f,9,[6..13]) $ NotContaining "show"

    , testCase' "Type of user-defined == and show functions" $ do
        f <- makeModule "F"
            [ "(==) : Text -> Bool"
            , "(==) t = True"
            , "show : Bool -> Int"
            , "show b = 2"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,2,[0..3]) $ Contains "```daml\n==\n: Text -> Bool\n```\n"
        expectTextOnHover (f,4,[0..3]) $ Contains "```daml\nshow\n: Bool -> Int\n```\n"

    , testCaseFails' "Type of choice" $ do
        f <- makeModule "F"
            [ "template Coin"
            , "  with"
            , "    owner : Party"
            , "  where"
            , "    signatory owner"
            , "    choice Delete : ()"
            , "      controller owner"
            , "      do return ()"
            , "    choice Transfer : ContractId Coin"
            , "      with newOwner : Party"
            , "      controller owner"
            , "      do create this with owner = newOwner"
            ]
        setFilesOfInterest [f]
        expectTextOnHover (f,7,[6..11]) $ HasType "Update ()" -- Delete choice
        expectTextOnHover (f,9,[6..13]) $ HasType "Party -> Update (ContractId Coin)" -- Transfer choice
    , testCase' "Haddock comment" $ do
        f <- makeModule "F"
            [ "-- | Important docs"
            , "f : a -> a"
            , "f x = x"
            ]
        setFilesOfInterest [f]
        expectNoErrors
        expectTextOnHover (f,3,[0]) $ Contains "Important docs"
    ]
    where
        testCase' = testCase mbScenarioService (Just scriptPackageData)
        testCaseFails' = testCaseFails mbScenarioService (Just scriptPackageData)

scriptTests :: Maybe SS.Handle -> ScriptPackageData -> Tasty.TestTree
scriptTests mbScenarioService scriptPackageData = Tasty.testGroup "Script tests"
    [ testCase' "Run an empty script" $ do
          let fooContent = T.unlines
                  [ "module Foo where"
                  , "import Daml.Script"
                  , "v = script do"
                  , "  pure ()"
                  ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectNoErrors
          expectVirtualResource vr "Return value: {}"
    , testCase' "Run a script with a failing assertion" $ do
          let fooContent = T.unlines
                  [ "module Foo where"
                  , "import Daml.Script"
                  , "v = script do"
                  , "  assert False"
                  ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectOneError (foo,2,0) "Assertion failed"
          expectVirtualResource vr "Assertion failed"
    , testCase' "Virtual resources should update when files update" $ do
          let fooContent = T.unlines
                 [ "module Foo where"
                 , "import Daml.Script"
                 , "v = script $ assert True"
                 ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectVirtualResource vr "Return value: {}"
          setBufferModified foo $ T.unlines
              [ "module Foo where"
              , "import Daml.Script"
              , "v = script $ assert False"
              ]
          expectVirtualResource vr "Assertion failed"
    , testCase' "Script error disappears when script is deleted" $ do
        let goodScript =
                [ "module F where"
                , "import Daml.Script"
                , "example1 = script $ assert True"
                ]
            badScript = [ "example2 = script $ assert False" ]
        f <- makeFile "F.daml" $ T.unlines goodScript
        setFilesOfInterest [f]
        let vr1 = VRScenario f "example1"
        setOpenVirtualResources [vr1]
        expectNoErrors
        expectVirtualResource vr1 "Return value: {}"
        setBufferModified f $ T.unlines $ goodScript ++ badScript
        let vr2 = VRScenario f "example2"
        setOpenVirtualResources [vr1, vr2]
        expectOneError (f, 3, 0) "Script execution failed"
        expectVirtualResource vr2 "Assertion failed"
        setBufferModified f $ T.unlines goodScript
        expectNoErrors
        expectVirtualResource vr1 "Return value: {}"
    , testCase' "Virtual resource gets updated with a note when file compiles, but script is no longer present" $ do
        let script1F =
                [ "module F where"
                , "import Daml.Script"
                , "script1 = script $ pure \"f1\""
                ]
            script1G =
                [ "module G where"
                , "import Daml.Script"
                , "script1 = script $ pure \"g1\""
                ]
            script12F =
                [ "module F where"
                , "import Daml.Script"
                , "script1 = script $ pure \"f1\""
                , "script2 = script $ pure \"f2\""
                ]
        f <- makeFile "F.daml" $ T.unlines script1F
        g <- makeFile "G.daml" $ T.unlines script1G
        setFilesOfInterest [f, g]
        let vr1F = VRScenario f "script1"
        let vr2F = VRScenario f "script2"
        let vr1G = VRScenario g "script1"

        setOpenVirtualResources [vr1F]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectNoVirtualResourceNote vr1F

        setOpenVirtualResources [vr1F, vr1G]
        expectNoErrors
        expectVirtualResource vr1G "Return value: &quot;g1&quot;"
        expectNoVirtualResourceNote vr1G

        setBufferModified f $ T.unlines script12F
        setOpenVirtualResources [vr1F, vr2F, vr1G]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectNoVirtualResourceNote vr1F
        expectVirtualResource vr2F "Return value: &quot;f2&quot;"
        expectNoVirtualResourceNote vr2F

        setBufferModified f $ T.unlines script1F
        setOpenVirtualResources [vr1F, vr2F, vr1G]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectVirtualResource vr2F "Return value: &quot;f2&quot;"
        expectVirtualResourceNote vr2F "This script no longer exists in the source file"
        expectVirtualResourceNote vr2F "F.daml"

        setBufferModified f $ T.unlines script12F
        setOpenVirtualResources [vr1F, vr2F, vr1G]
        expectNoErrors
        expectVirtualResource vr1F "Return value: &quot;f1&quot;"
        expectNoVirtualResourceNote vr1F
        expectVirtualResource vr2F "Return value: &quot;f2&quot;"
        expectNoVirtualResourceNote vr2F
        expectVirtualResource vr1G "Return value: &quot;g1&quot;"
        expectNoVirtualResourceNote vr1G
    , testCase' "Virtual resource gets updated with a note when file does not compile anymore" $ do
          let script1F =
                  [ "module F where"
                  , "import Daml.Script"
                  , "script1 = script $ pure \"f1\""
                  ]
              script1G =
                  [ "module G where"
                  , "import Daml.Script"
                  , "script1 = script $ pure \"g1\""
                  ]
              script1FInvalid =
                  [ "module F where"
                  , "this is bad syntax"
                  ]
          f <- makeFile "F.daml" $ T.unlines script1F
          g <- makeFile "G.daml" $ T.unlines script1G
          setFilesOfInterest [f, g]
          let vr1G = VRScenario g "script1"
          let vr1F = VRScenario f "script1"

          setOpenVirtualResources [vr1F]
          expectNoErrors
          expectVirtualResource vr1F "Return value: &quot;f1&quot;"
          expectNoVirtualResourceNote vr1F

          setOpenVirtualResources [vr1F, vr1G]
          expectNoErrors
          expectVirtualResource vr1G "Return value: &quot;g1&quot;"
          expectNoVirtualResourceNote vr1G

          setBufferModified f $ T.unlines script1FInvalid
          setOpenVirtualResources [vr1F, vr1G]
          expectOneError (f,1,0) "Parse error"

          expectVirtualResource vr1F "Return value: &quot;f1&quot;"
          expectVirtualResourceNote vr1F "The source file containing this script no longer compiles"
          expectVirtualResourceNote vr1F "F.daml"

          expectVirtualResource vr1G "Return value: &quot;g1&quot;"
          expectNoVirtualResourceNote vr1G

          setBufferModified f $ T.unlines script1F
          setOpenVirtualResources [vr1F, vr1G]
          expectNoErrors
          expectVirtualResource vr1F "Return value: &quot;f1&quot;"
          expectVirtualResource vr1G "Return value: &quot;g1&quot;"
    , testCase' "Script in file of interest but not opened" $ do
          let fooContent = T.unlines
                  [ "module Foo where"
                  , "import Daml.Script"
                  , "v = script $ assert False"
                  ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "v"
          setFilesOfInterest [foo]
          setOpenVirtualResources []
          -- We expect to get no diagnostics because the script is never run
          expectNoErrors
          expectNoVirtualResource vr
    , testCase' "Script opened but not in files of interest" $ do
          foo <- makeFile "Foo.daml" $ T.unlines
              [ "module Foo where"
              , "import Daml.Script"
              , "v = script $ assert True"
              ]
          let vr = VRScenario foo "v"
          expectNoVirtualResource vr
          setOpenVirtualResources [vr]
          expectVirtualResource vr "Return value: {}"
    , testCase' "Update dependency of open script that is not in files of interest" $ do
           let fooContent = T.unlines
                  [ "module Foo where"
                  , "import Daml.Script"
                  , "import Bar"
                  , "v = script $ bar ()"
                  ]
               barContent = T.unlines
                  [ "module Bar where"
                  , "import Daml.Script"
                  , "bar : () -> Script ()"
                  , "bar () = assert True"
                  ]
           foo <- makeFile "Foo.daml" fooContent
           bar <- makeFile "Bar.daml" barContent
           let vr = VRScenario foo "v"
           setOpenVirtualResources [vr]
           expectNoErrors
           expectVirtualResource vr "Return value: {}"
           setBufferModified bar $ T.unlines
               [ "module Bar where"
               , "import Daml.Script"
               , "bar : () -> Script ()"
               , "bar _ = assert False"
               ]
           expectOneError (foo,3,0) "Assertion failed"
           expectVirtualResource vr "Assertion failed"
    , testCase' "Open script after scripts have already been run" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
              [ "module Foo where"
              , "import Daml.Script"
              , "v= script $ assert True"
              ]
            let vr = VRScenario foo "v"
            setFilesOfInterest [foo]
            expectNoVirtualResource vr
            setOpenVirtualResources [vr]
            expectVirtualResource vr "Return value: {}"
    -- Scenario service doesn't pull out a location from the speedy machine
    , testCaseFails' "Failing script produces stack trace in correct order (ticket #7276)" $ do
          let fooContent = T.unlines
                 [ "module Foo where"
                 , "import Daml.Script"
                 , "boom : HasCallStack => Script ()"
                 , "boom = fail \"BOOM\""
                 , "test : Script ()"
                 , "test = boom"
                 ]

          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "test"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectVirtualResourceRegex vr "Stack trace:.*- boom.*Foo:3:1"
    , testCase' "HasCallStack constraint" $ do
          let fooContent = T.unlines
                [ "module Foo where"
                , "import Daml.Script"
                , "import DA.Stack"
                , "a : HasCallStack => () -> ()"
                , "a () = b ()"
                , "b : HasCallStack => () -> ()"
                , "b () = c ()"
                , "c : HasCallStack => () -> ()"
                , "c () = error (prettyCallStack callStack)"
                , "f = script do"
                , "  pure $ a ()"
                ]
          foo <- makeFile "Foo.daml" fooContent
          let vr = VRScenario foo "f"
          setFilesOfInterest [foo]
          setOpenVirtualResources [vr]
          expectVirtualResourceRegex vr $ T.concat
            [ "  c, called at .*Foo.daml:6:7 in main:Foo<br>"
            , "  b, called at .*Foo.daml:4:7 in main:Foo<br>"
            , "  a, called at .*Foo.daml:10:9 in main:Foo"
            ]
    , testCase' "debug is lazy" $ do
        let goodScript =
                [ "module LazyDebug where"
                , "import Daml.Script"
                , "import DA.Foldable"
                , "import DA.Action.State"

                , "test = script $ pure $ runState go 0"
                , "go : State Int ()"
                , "go = forA_ [0,1] $ \\x -> do"
                , "    debug \"foo\""
                , "    modify (+1)"
                , "    debug \"bar\""
                ]
        f <- makeFile "LazyDebug.daml" $ T.unlines goodScript
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
    , testCase' "Table view displays signatory + observer + witness + divulged" $ do
        f <- makeFile "TableView.daml" $ T.unlines
          [ "module TableView where"
          , "import Daml.Script"

          , "template IouIssuer with"
          , "    regulator: Party"
          , "    issuer: Party"
          , "  where"
          , "    signatory regulator"
          , "    observer issuer"
          , "    nonconsuming choice Issue: ContractId Iou with"
          , "        owner: Party"
          , "      controller issuer"
          , "      do create Iou with issuer; owner"

          , "template Iou with"
          , "    issuer: Party"
          , "    owner: Party"
          , "  where"
          , "    signatory issuer"
          , "    observer owner"

          , "template IouDivulger with"
          , "    sender: Party"
          , "    receiver: Party"
          , "  where"
          , "    signatory receiver"
          , "    observer sender"
          , "    nonconsuming choice Divulge: Iou with"
          , "        iouCid: ContractId Iou"
          , "      controller sender"
          , "      do fetch iouCid"

          , "test = script do"
          , "  regulator <- allocateParty \"Regulator\""
          , "  issuer <- allocateParty \"Issuer\""
          , "  owner <- allocateParty \"Owner\""
          , "  spy <- allocateParty \"Spy\""
          , "  iouIssuerCid <- submit regulator do createCmd IouIssuer with regulator; issuer"
          , "  iouCid <- submit issuer do exerciseCmd iouIssuerCid Issue with owner"
          , "  iouDivulgerCid <- submit spy do createCmd IouDivulger with sender = owner; receiver = spy"
          , "  submit owner do exerciseCmd iouDivulgerCid Divulge with iouCid"
          , "  submit issuer do createCmd Iou with issuer; owner"
          , "  pure ()"
          ]
        setFilesOfInterest [f]
        let vr = VRScenario f "test"
        setOpenVirtualResources [vr]
        -- TODO(MH): Matching on HTML via regular expressions has a high
        -- chance of becoming a maintenance nightmare. Find a better way.
        expectVirtualResourceRegex vr $ T.intercalate ".*"
            [ "<h1>TableView:Iou</h1>"
            , "<table"
            , "<tr", "Issuer", "Owner", "Regulator", "Spy", "</tr>"
            , "<tr"
            , "<td>#1:1"
            , "<td", "tooltip", ">S<", "tooltiptext", ">Signatory<", "</td>"
            , "<td", "tooltip", ">O<", "tooltiptext", ">Observer<", "</td>"
            , "<td", "tooltip", ">W<", "tooltiptext", ">Witness<", "</td>"
            , "<td", "tooltip", ">D<", "tooltiptext", ">Divulged<", "</td>"
            , "</tr>"
            , "<tr"
            , "<td>#4:0"
            , "<td", "tooltip", ">S<", "tooltiptext", ">Signatory<", "</td>"
            , "<td", "tooltip", ">O<", "tooltiptext", ">Observer<", "</td>"
            , "<td", "tooltip", ">-<", "</td>"
            , "<td", "tooltip", ">-<", "</td>"
            , "</tr"
            , "</table>"
            , "<h1>TableView:IouDivulger</h1>"
            , "<h1>TableView:IouIssuer</h1>"
            ]
    , testCase' "Table view on error" $ do
        f <- makeFile "TableView.daml" $ T.unlines
          [ "module TableView where"
          , "import Daml.Script"

          , "template T with"
          , "    p: Party"
          , "  where"
          , "    signatory p"
          , "    choice Fail : ()"
          , "      controller p"
          , "      do create this"
          , "         assert False"

          , "test = script do"
          , "  p <- allocateParty \"p\""
          , "  c <- submit p do createCmd T with p = p"
          , "  submit p $ do exerciseCmd c Fail"
          ]
        setFilesOfInterest [f]
        let vr = VRScenario f "test"
        setOpenVirtualResources [vr]
        -- This is a bit messy, we also want to to test for the absence of extra nodes
        -- so we have to be quite strict in what we match against.
        expectVirtualResourceRegex vr $ T.concat
            [ "<h1>TableView:T</h1>"
            , "<table>"
            , "<tr><th>id</th><th>status</th><th>p</th><th><div class=\"observer\">p</div></th></tr>"
            , "<tr class=\"active\">"
            , "<td>#0:0</td>"
            , "<td>active</td>"
            , "<td>&#39;p&#39;</td>"
            , "<td class=\"disclosure disclosed\">"
            , "<div class=\"tooltip\"><span>S</span><span class=\"tooltiptext\">Signatory</span>"
            , "</div>"
            , "</td>"
            , "</tr>"
            , "</table>"
            ]
    ]
    where
        testCase' = testCase mbScenarioService (Just scriptPackageData)
        testCaseFails' = testCaseFails mbScenarioService (Just scriptPackageData)
