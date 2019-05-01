-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Shake IDE API test suite. Run these manually with:
--
--      bazel run //daml-foundations/daml-ghc:daml-ghc-shake-test-ci
--
-- Some tests cover open issues, these are run with 'testCaseFails'.
-- Once the issue is resolved, switch it to 'testCase'.
-- Otherwise this test suite will complain that the test is not failing.
module DA.Test.ShakeIdeClient (main, ideTests) where

import qualified Test.Tasty.Extended as Tasty
import qualified Test.Tasty.HUnit    as Tasty
import qualified Data.Text.Extended  as T

import Data.Either
import Control.Monad
import Control.Monad.Managed (with)
import System.Directory
import System.Environment
import Control.Monad.IO.Class

import DA.Service.Daml.Compiler.Impl.Scenario as SS
import           Development.IDE.Types.LSP
import qualified DA.Service.Logger.Impl.Pure as Logger
import Development.IDE.State.API.Testing

main :: IO ()
main = with (SS.startScenarioService (\_ -> pure ()) Logger.makeNopHandle) $ \scenarioService -> do
  -- The scenario service is a shared resource so running tests in parallel doesn’t work properly.
  setEnv "TASTY_NUM_THREADS" "1"
  -- The startup of the scenario service is fairly expensive so instead of launching a separate
  -- service for each test, we launch a single service that is shared across all tests.
  Tasty.deterministicMain (ideTests (Just scenarioService))

ideTests :: Maybe SS.Handle -> Tasty.TestTree
ideTests mbScenarioService =
    Tasty.testGroup "IDE Shake API tests"
        [ -- Add categories of tests here
          basicTests mbScenarioService
        , minimalRebuildTests mbScenarioService
        , goToDefinitionTests mbScenarioService
        , onHoverTests mbScenarioService
        , scenarioTests mbScenarioService
        , stressTests mbScenarioService
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
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,1,0) "Parse error"

    ,   testCase' "Set files of interest to clear parse error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,1,0) "Parse error"
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
            liftIO $ removeFile b
            expectOnlyDiagnostics
                [(DsError, (a,0,32), "Could not find module")
                -- the warning says around because of DEL-7199
                ,(DsWarning, (a,0,25), "The import of ‘B’ is redundant")]

    ,   testCaseFails mbScenarioService "Early errors kill later warnings DEL-7199" $ do
            a <- makeFile "A.daml" "daml 1.2 module A where; import B"
            _ <- makeFile "B.daml" "daml 1.2 module B where"
            setFilesOfInterest [a]
            expectWarning (a,0,25) "The import of ‘B’ is redundant"
            setBufferModified a "???"
            -- Actually keeps the warning from the later stage, even though there is a parse error
            expectOneError (a,0,0) "Parse error"

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
            -- expectGoToDefinition (foo,2,[0..2]) (At (foo,3,0))   -- "foo"            [see failing test]
            expectGoToDefinition (foo,2,[3..5]) Missing             -- " : "
            -- expectGoToDefinition (foo,2,[6..8]) (In "GHC.Types") -- "Int"            [see failing test]
            expectGoToDefinition (foo,2,[9])    Missing             -- "\n"
            expectGoToDefinition (foo,2,[10])   Missing             -- (out of range)
            -- expectGoToDefinition (foo,3,[-1]) Missing            -- (out of range)   [see failing test]
            expectGoToDefinition (foo,3,[0..2]) (At (foo,3,0))      -- "foo"
            expectGoToDefinition (foo,3,[3,4])  Missing             -- " ="
            -- expectGoToDefinition (foo,3,[5]) Missing             -- " "              [see failing test]
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

    ,   testCaseFails' "[DEL-6941] Go to definition should be tight" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = bar"
                , "bar=baz"
                , "baz = 10"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[5]) Missing
            expectGoToDefinition (foo,2,[6]) (At (foo,3,0))
            expectGoToDefinition (foo,3,[3]) Missing
            expectGoToDefinition (foo,3,[4]) (At (foo,4,0))

    ,   testCaseFails' "[DEL-6941] Go to definition takes type sig to definition" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo : Int"
                , "foo = 0"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[0]) (At (foo,3,0))

    ,   testCaseFails' "[DEL-6941] Go to definition should ignore negative column" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "foo = 10"
                ]
            setFilesOfInterest [foo]
            expectGoToDefinition (foo,2,[-1]) Missing -- (out of range)

    ,   testCaseFails' "[DEL-6941] Take variable in template to its declaration" $ do
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
            expectGoToDefinition (foo,6,[14..18]) (At (foo,4,4)) -- "owner" in signatory clause
            expectGoToDefinition (foo,7,[19..23]) (At (foo,4,4)) -- "owner" in agreement

    ,   testCaseFails' "[DEL-6941] Standard library type points to standard library" $ do
            foo <- makeModule "Foo"
                [ "foo : Optional (List Bool)"
                , "foo = Some [False]"
                ]
            setFilesOfInterest [foo]
            expectNoErrors
            expectGoToDefinition (foo,2,[6..13]) (In "Prelude") -- "Optional"
            expectGoToDefinition (foo,2,[16..19]) (In "GHC.Types") -- "List"
            expectGoToDefinition (foo,2,[21..24]) (In "GHC.Types") -- "Bool"
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
        expectTextOnHover (f,3,[0..3]) $ Contains "==\n  : Text -> Bool"
        expectTextOnHover (f,5,[0..3]) $ Contains "show\n  : Bool -> Int"

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
    ]
    where
        testCase' = testCase mbScenarioService

-- | Do extreme things to the compiler service.
stressTests :: Maybe SS.Handle -> Tasty.TestTree
stressTests mbScenarioService = Tasty.testGroup "Stress tests"
    [   testCase' "Modify a file 2000 times" $ do
            let fooValue :: Int -> T.Text
                fooValue i = T.pack (show (i `div` 2))
                          <> if i `mod` 2 == 0 then "" else ".5"
                fooContent i = T.unlines
                    [ "daml 1.2"
                    , "module Foo where"
                    , "foo : Int"
                    , "foo = " <> fooValue i
                    ]
            foo <- makeFile "Foo.daml" $ fooContent 0
            setFilesOfInterest [foo]
            expectNoErrors
            forM_ [1 .. 999] $ \i ->
                void $ makeFile "Foo.daml" $ fooContent i
            expectOneError (foo,3,6) "Couldn't match expected type"
            forM_ [1000 .. 2000] $ \i ->
                void $ makeFile "Foo.daml" $ fooContent i
            expectNoErrors

    ,   testCase' "Set 10 files of interest" $ do
            foos <- forM [1 .. 10 :: Int] $ \i ->
                makeModule ("Foo" ++ show i) ["foo = 10"]
            timedSection 30 $ do
                setFilesOfInterest foos
                expectNoErrors

    ,   testCase' "Type check 100-deep module chain" $ do
            -- The idea for this test is we have 101 modules named Foo0 through Foo100.
            -- Foo0 imports Foo1, which imports Foo2, which imports Foo3, and so on to Foo100.
            -- Each FooN has a definition fooN that depends on fooN+1, except Foo100.
            -- But the type of foo0 doesn't match the type of foo100. So we expect a type error.
            -- Then we modify the type of foo0 to clear the type error.
            foo0 <- makeModule "Foo0"
                ["import Foo1"
                ,"foo0 : Int"
                ,"foo0 = foo1"]
            forM_ [1 .. 99 :: Int] $ \i ->
                makeModule ("Foo" ++ show i)
                    ["import Foo" <> T.show (i+1)
                    ,"foo" <> T.show i <> " = foo" <> T.show (i+1)]
            void $ makeModule "Foo100"
                ["foo100 : Bool"
                ,"foo100 = False"]
            timedSection 180 $ do -- This takes way too long. Can we get it down?
                setFilesOfInterest [foo0]
                expectOneError (foo0,4,7) "Couldn't match expected type"
                void $ makeModule "Foo0"
                    ["import Foo1"
                    ,"foo0 : Bool"
                    ,"foo0 = foo1"]
                expectNoErrors

    ]
    where
        testCase' = testCase mbScenarioService

-- | Suppress unused binding warning in case we run out of tests for open issues.
_suppressUnusedWarning :: ()
_suppressUnusedWarning = testCaseFails `seq` ()
