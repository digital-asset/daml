-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module VisualTest
   ( main
   ) where
import DA.Cli.Visual
import Data.Either
import qualified Test.Tasty.Extended as Tasty
import qualified Test.Tasty.HUnit    as Tasty
import qualified Development.IDE.Types.Location as D
import qualified DA.Daml.LF.Ast as LF
import qualified Development.IDE.Core.API as API
import qualified Development.IDE.Core.Rules.Daml as API
import qualified Data.NameMap as NM
import qualified Control.Monad.Reader   as Reader
import Development.IDE.Core.API.Testing
import System.Environment.Blank (setEnv)
import Control.Monad.Except

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    Tasty.deterministicMain visualTests

visualTests :: Tasty.TestTree
visualTests =
    Tasty.testGroup "IDE Shake API tests" [ visualDamlTests ]

testCase :: Tasty.TestName -> ShakeTest () -> Tasty.TestTree
testCase testName test =
    Tasty.testCase testName $ do
        res <- runShakeTest Nothing test
        Tasty.assertBool ("Shake test resulted in an error: " ++ show res) $ isRight res

graphTest :: LF.World -> LF.Package -> Either String ()
graphTest wrld lfPkg = case concatMap (moduleAndTemplates wrld) (NM.toList $ LF.packageModules lfPkg) of
    [] -> Left "no template and Choices"
    _xs -> Right ()

worldForFile' :: D.NormalizedFilePath -> ShakeTest ()
worldForFile' damlFilePath = do
    ideState <- ShakeTest $ Reader.asks steService
    mbDalf <- liftIO $ API.runAction ideState (API.getDalf damlFilePath)
    case mbDalf of
        Just lfPkg -> do
            wrld <- Reader.liftIO $ API.runAction ideState (API.worldForFile damlFilePath)
            case graphTest wrld lfPkg of
                Right _ -> pure ()
                Left _ -> throwError (ExpectedNoErrors [])
        Nothing -> throwError (ExpectedNoErrors [])

visualDamlTests :: Tasty.TestTree
visualDamlTests = Tasty.testGroup "Visual Tests"
    [   testCase "Set files of interest" $ do
            foo <- makeModule "F"
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
            setFilesOfInterest [foo]
            worldForFile' foo
    ]




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
