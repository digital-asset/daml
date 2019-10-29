-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DarReaderTest
   ( main
   ) where

import qualified Data.ByteString.Char8 as BS
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
            (Right
                 [ ("Dalfs", "stdlib.dalf, prim.dalf")
                 , ("Main-Dalf", "testing.dalf")
                 , ("Sdk-Version", "0.13.30")
                 ])
            (parseManifestFile $ BS.unlines
                 [ "Dalfs: stdlib.da"
                 , " lf, prim.dalf"
                 , "Main-Dalf: testing.dalf"
                 , "Sdk-Version: 0.13.30"
                 ])
    , testCase "multiline manifest file test" $
        assertEqual "all content in the same line"
            (Right
                 [ ("Dalfs", "stdlib.dalf")
                 , ("Main-Dalf", "solution.dalf")
                 , ("Sdk-Version", "0.13.29")
                 ])
            (parseManifestFile $ BS.unlines
                [ "Dalfs: stdlib.dalf"
                , "Main-Dalf: solution.dalf"
                , "Sdk-Version: 0.13.29"
                ])
    ]
