-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DarReaderTest
   ( main
   ) where

import Test.Tasty
import DA.Daml.LF.Reader
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain unitTests

unitTests :: TestTree
unitTests = testGroup "testing dar reader for longer manifest lines"
    [
        testCase "multiline manifest file test" $
        assertEqual "content over multiple lines"
            ["Dalfs: stdlib.dalf, prim.dalf", "Main-Dalf: testing.dalf"]
            (multiLineContent $ unlines [ "Dalfs: stdlib.da"
                                        , " lf, prim.dalf"
                                        , "Main-Dalf: testing.dalf"
                                        ])
    , testCase "multiline manifest file test" $
        assertEqual "all content in the same line"
            ["Dalfs: stdlib.dalf", "Main-Dalf:solution.dalf"]
            (multiLineContent $ unlines [ "Dalfs: stdlib.dalf"
                                        , "Main-Dalf:solution.dalf"
                                        ])
    ]