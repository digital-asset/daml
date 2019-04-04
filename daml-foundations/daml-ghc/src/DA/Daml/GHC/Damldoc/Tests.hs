-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.GHC.Damldoc.Tests(mkTestTree)
  where

import           DA.Daml.GHC.Damldoc.Types
import           DA.Daml.GHC.Damldoc.HaddockParse
import           DA.Daml.GHC.Damldoc.Render
import DA.Daml.GHC.Compiler.Options

import           Control.Monad.Except.Extended
import qualified Data.Aeson.Encode.Pretty as AP
import           Data.Algorithm.Diff (getGroupedDiff)
import           Data.Algorithm.DiffOutput (ppDiff)
import qualified Data.ByteString.Lazy.Char8 as BS
import           Data.List (sort)
import           Data.List.Extra (nubOrd)
import qualified Data.Text          as T
import qualified Data.Text.Extended as T
import qualified Data.Text.Encoding as T
import           System.Directory
import           System.FilePath
import           System.IO.Extra
import qualified Test.Tasty.Extended as Tasty
import           Test.Tasty.HUnit.Extended

import DA.Prelude


testDir :: FilePath
testDir = "daml-foundations/daml-ghc/tests"

mkTestTree :: IO Tasty.TestTree
mkTestTree = do

  let isExpectationFile filePath =
        ".EXPECTED" == takeExtensions (dropExtension filePath)
  expectFiles <- filter isExpectationFile <$> listDirectory testDir

  let goldenSrcs = nubOrd $ map (flip replaceExtensions "daml") expectFiles
  goldenTests <- mapM (fileTest . (testDir </>))  goldenSrcs

  pure $ Tasty.testGroup "DA.Daml.GHC.Damldoc" $ unitTests <> concat goldenTests

unitTests :: [Tasty.TestTree]
unitTests =
    [ damldocExpect
           "Empty module is OK"
           [testModHdr]
           (assertBool "Expected empty docs" . (== emptyDocs testModule))

         , damldocExpect
           "Module header doc"
           [ "daml 1.2"
           , "-- | This is a module header"
           , "module " <> T.pack testModule <> " where"
           ]
           (\ModuleDoc{..} -> assertBool "Expected the module header" $
                              md_descr == Just "This is a module header" )

         , damldocExpect
           "Type synonym doc"
           [ testModHdr
           , "-- | Foo doc"
           , "type Foo = ()"
           ]
           (\md -> assertBool
                   ("Expected a single type synonym doc, got " <> show md)
                   (isJust $ do adt <- getSingle $ md_adts md
                                check $ ad_descr adt == Just "Foo doc"))

         , damldocExpect
           "Data type doc"
           [ testModHdr
           , "-- | Foo doc"
           , "data Foo = Foo {}"
           ]
           (\md -> assertBool
                   ("Expected a single data type doc, got " <> show md)
                   (isJust $ do adt <- getSingle $ md_adts md
                                check $ ad_descr adt == Just "Foo doc"))

         , damldocExpect
           "Prefix data constructor doc"
           [ testModHdr
           , "data Foo = Foo {}"
           , "           -- ^ Constructor"
           ]
           (\md -> assertBool
                   ("Expected single constructor with doc, got " <> show md)
                   (isJust $ do adt <- getSingle $ md_adts md
                                con <- getSingle $ ad_constrs adt
                                check $ ac_descr con == Just "Constructor"))

         , damldocExpect
           "Record data docs"
           [ testModHdr
           , "data Foo = Foo with"
           , "             field1 : () -- ^ Field1"
           ]
           (\md -> assertBool
                   ("Expected record with a field with doc, got " <> show md)
                   (isJust $ do adt <- getSingle $ md_adts md
                                con <- getSingle $ ad_constrs adt
                                check $ isNothing $ ac_descr con
                                f1  <- getSingle $ ac_fields con
                                check $ fd_descr f1 == Just "Field1"))

         , damldocExpect
           "Template docs"
           [ testModHdr
           , "-- | Template doc"
           , "template Foo with"
           , "    field1 : Party -- ^ Field1"
           , "  where"
           , "    signatory field1"
           ]
           (\md -> assertBool
                   ("Expected template and a field in doc, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                check $ Just "Template doc" == td_descr t
                                f1 <- getSingle $ td_payload t
                                check $ fd_descr f1 == Just "Field1"))

         , damldocExpect
           "Choice field docs"
           [ testModHdr
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "    controller field1 can"
           , "      DoSomething : ()"
           , "        with field: () -- ^ field"
           , "        do pure ()"
           ]
           (\md -> assertBool
                   ("Expected a choice with field in doc, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                check $ isNothing $ td_descr t
                                f1 <- getSingle $ td_payload t
                                check $ isNothing $ fd_descr f1
                                ch <- getSingle $ td_choices t
                                f2 <- getSingle $ cd_fields ch
                                check $ Just "field" == fd_descr f2))

         , damldocExpect
           "Several Choices"
           [ testModHdr
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "    controller field1 can"
           , "      DoSomething : ()"
           , "        with field: ()"
           , "        do pure ()"
           , "      DoMore : ()"
           , "        with field: ()"
           , "        do pure ()"
           ]
           (\md -> assertBool
                   ("Expected two choices in doc, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                check $ isNothing $ td_descr t
                                cs <- Just $ td_choices t
                                check $ length cs == 2
                                check $ ["DoMore", "DoSomething"] == sort (map cd_name cs)))

         , damldocExpect
           "Class doc"
           [ testModHdr
           , "-- | Class description"
           , "class C a where"
           , "    -- | Member description"
           , "    member : a"
           ]
           (\md -> assertBool
                   ("Expected a class description and a function description, got " <> show md)
                   (isJust $ do cls <- getSingle $ md_classes md
                                check (Just "Class description" == cl_descr cls)
                                member <- getSingle $ cl_functions cls
                                check (Just "Member description" == fct_descr member)))
         ]


  where getSingle :: [a] -> Maybe a
        getSingle []  = Nothing
        getSingle [x] = Just x
        getSingle _xs = Nothing

        check :: Bool -> Maybe ()
        check True = Just ()
        check False = Nothing


testModule :: String
testModule = "Testfile"


testModHdr :: T.Text
testModHdr = T.pack $ "daml 1.2 module\n  " <> testModule <> " where\n"


emptyDocs :: String -> ModuleDoc
emptyDocs name = ModuleDoc { md_name = T.pack name
                           , md_descr = Nothing
                           , md_templates = []
                           , md_adts = []
                           , md_functions = []
                           , md_classes = []
                           }


-- | Compiles the given input string (in a tmp file) and checks generated doc.s
-- using the predicate provided.
damldocExpect :: String -> [T.Text] -> (ModuleDoc -> Assertion) -> Tasty.TestTree
damldocExpect testname input check =
  testCase testname $
  withTempDir $ \dir -> do

    let testfile = dir </> testModule <.> "daml"
    -- write input to a file
    T.writeFileUtf8 testfile (T.unlines input)

    opts <- defaultOptionsIO Nothing

    -- run the doc generator on that file
    mbResult <- runExceptT $ mkDocs (toCompileOpts opts) [testfile]

    case mbResult of
      Left err -> assertFailure $ unlines
                  ["Parse error(s) for test file " <> testname, show err]

      -- first module is the root we started from, so is the one that must satisfy the invariants
      Right ms -> check $ head ms


-- | For the given file <name>.daml (assumed), this test checks if any
-- <name>.EXPECTED.<suffix> exists, and produces output according to <suffix>
-- for all files found.
fileTest :: FilePath -> IO [Tasty.TestTree]
fileTest damlFile = do

  damlFileAbs <- makeAbsolute damlFile
  let basename = dropExtension damlFileAbs
      expected = [ basename <.> "EXPECTED" <.> s
                 | s <- [ "json", "rst", "md" ]]

  expectations <- filterM doesFileExist expected

  if null expectations
    then pure []
    else do input <- T.readFileUtf8 damlFile
            mapM (goldenTest input) expectations

  where goldenTest :: T.Text -> FilePath -> IO Tasty.TestTree
        goldenTest input expectation =
          let check docs = do
                let extension = takeExtension expectation
                ref <- T.readFileUtf8 expectation
                case extension of
                  ".rst"  -> expectEqual extension ref $ renderSimpleRst docs
                  ".md"   -> expectEqual extension ref $ renderSimpleMD docs
                  ".json" -> expectEqual extension ref
                             (T.decodeUtf8 . BS.toStrict $
                               AP.encodePretty' jsonConf docs)
                  other  -> error $ "Unsupported file extension " <> other

              expectEqual :: String -> T.Text -> T.Text -> Assertion
              expectEqual extension ref actual
                | ref == actual = pure ()
                | otherwise = do
                    let actualFile = replaceExtensions expectation ("ACTUAL" <> extension)
                        asLines = lines . T.unpack
                        diff = ppDiff $ getGroupedDiff (asLines ref) (asLines actual)

                    T.writeFileUtf8 actualFile actual
                    assertFailure $
                      "Unexpected difference between " <> expectation <>
                      " and actual output.\n" <>
                      "Unexpected output has been written to " <> actualFile <> "\n" <>
                      "Differences:\n" <> diff

          in pure $  damldocExpect ("File: " <> expectation) [input] check
