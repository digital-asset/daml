-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module VisualTest
   ( main
   ) where
-- import System.IO.Extra
-- import DA.Cli.Visual
-- import DA.Daml.LF.Reader
-- import Test.Tasty.Golden
-- import DA.Bazel.Runfiles
-- import System.FilePath

import Data.Either
import qualified Test.Tasty.Extended as Tasty
import qualified Test.Tasty.HUnit    as Tasty
import qualified Data.Text.Extended  as T
import Development.IDE.Core.API.Testing
import System.Environment.Blank (setEnv)


main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    Tasty.deterministicMain $ visualTests


visualTests :: Tasty.TestTree
visualTests =
    Tasty.testGroup "IDE Shake API tests" [ visualDamlTests ]

testCase :: Tasty.TestName -> ShakeTest () -> Tasty.TestTree
testCase testName test =
    Tasty.testCase testName $ do
        res <- runShakeTest Nothing test
        Tasty.assertBool ("Shake test resulted in an error: " ++ show res) $ isRight res

visualDamlTests :: Tasty.TestTree
visualDamlTests = Tasty.testGroup "Basic tests"
    [    testCase' "Set files of interest and expect no errors" example

     ,   testCase' "Set files of interest and expect parse error" $ do
            foo <- makeFile "Foo.daml" $ T.unlines
                [ "daml 1.2"
                , "module Foo where"
                , "this is bad syntax"
                ]
            setFilesOfInterest [foo]
            expectOneError (foo,2,0) "Parse error"
    ]
    where testCase' = testCase




-- main :: IO ()
-- main = defaultMain  =<< unitTests

-- unitTests :: IO TestTree
-- unitTests = do
--     withTempFile $ \path -> do
--         darPath <- locateRunfiles (mainWorkspace </> "compiler/damlc/tests/visual-test-daml.dar")
--         dotFile <- locateRunfiles (mainWorkspace </> "compiler/damlc/tests/visual/Basic.dot")
--         return $ testGroup "making sure we do not add extra edges" [
--             goldenVsFile
--                 "dot file test"
--                 dotFile
--                 path
--                 (execVisual darPath (Just path))
--             , testCase "multiline manifest file test" $
--                 assertEqual "content over multiple lines"
--                     ["Dalfs: stdlib.dalf, prim.dalf", "Main-Dalf: testing.dalf"]
--                     (multiLineContent ["Dalfs: stdlib.da", " lf, prim.dalf" , "Main-Dalf: testing.dalf"])
--             , testCase "multiline manifest file test" $
--                 assertEqual "all content in the same line"
--                     ["Dalfs: stdlib.dalf", "Main-Dalf:solution.dalf"]
--                     (multiLineContent ["Dalfs: stdlib.dalf" , "Main-Dalf:solution.dalf"])
--             ]
