-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- license, transmit, distribute, exhibit, perform, publish, or display any part,
-- in any form, or by any means. Reverse engineering, disassembly, or decompilation
-- of this software, unless required by law for interoperability, is prohibited.
--
-- Warranty Disclaimer
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
-- FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
-- IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
-- DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
-- ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
-- DEALINGS IN THE SOFTWARE.
--------------------------------------------------------------------------------


-- | Extended version of 'Test.Tasty.QuickCheck'.
module Test.Tasty.Extended (
  module Tasty
, deterministicMain
, mainWithSeed
, testGroupWithSeed
, discouragementMessage
) where


import qualified Test.Tasty as Tasty
import Test.Tasty
import Test.Tasty.Options
import Test.Tasty.Runners
import Test.Tasty.QuickCheck

import Control.Concurrent

defaultSeed :: Int
defaultSeed = 42

--
-- Run all QuickCheck tests in the TestTree with `defaultSeed`
-- This makes the tests deterministic
--
deterministicMain :: TestTree -> IO ()
deterministicMain = mainWithSeed defaultSeed

--
-- Run all QuickCheck tests in the TestTree with a user-specified seed.
--
mainWithSeed :: Int -> TestTree -> IO ()
mainWithSeed seed tree = defaultMain (treeWithSeed seed tree)

testGroupWithSeed :: Int -> TestName -> [TestTree] -> TestTree
testGroupWithSeed seed name trees = treeWithSeed seed (testGroup name trees)

--
-- | Transform a TestTree to a TestTree that runs QuickCheck tests with
--   a particular seed.
--
treeWithSeed :: Int -> TestTree -> TestTree
treeWithSeed seed tree =
    PlusTestOptions (setOption (QuickCheckReplay $ Just seed)) tree

discouragementMessage :: IO ()
discouragementMessage = do
    putStrLn $ unlines [
      "************************************************************",
      "             This test executable is DEPRECATED",
      "",
      "  All tests should be run in the",
      "  daml-foundations/da-haskell-tests component",
      "",
      "  Script 'da-test-haskell' now runs this component's tests",
      "",
      "  This is more convenient and facilitates program coverage",
      "  report generation.",
      "************************************************************"
      ]
    threadDelay $ 3 * microsInSeconds
  where
    microsInSeconds = 1000000
