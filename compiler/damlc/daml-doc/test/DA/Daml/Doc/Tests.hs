-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

module DA.Daml.Doc.Tests(mkTestTree)
  where

import DA.Bazel.Runfiles
import DA.Daml.Compiler.Output (diagnosticsLogger)
import DA.Daml.Options.Types

import DA.Daml.Doc.Extract
import DA.Daml.Doc.Render
import DA.Daml.Doc.Types
import DA.Daml.Doc.Transform
import DA.Daml.Doc.Anchor
import DA.Daml.LF.Ast.Version

import Development.IDE.Types.Location

import Control.Monad
import           Control.Monad.Trans.Maybe
import qualified Data.Aeson.Encode.Pretty as AP
import           Data.List.Extra
import qualified Data.Text          as T
import qualified Data.Text.Extended as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           System.Directory
import           System.FilePath
import           System.IO.Extra
import qualified Test.Tasty.Extended as Tasty
import           Test.Tasty.Golden
import           Test.Tasty.HUnit
import Data.Maybe

mkTestTree :: AnchorMap -> IO Tasty.TestTree
mkTestTree externalAnchors = do

  testDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/daml-test-files"

  let isExpectationFile filePath =
        ".EXPECTED" == takeExtensions (dropExtension filePath)
  expectFiles <- filter isExpectationFile <$> listDirectory testDir

  let goldenSrcs = nubOrd $ map (flip replaceExtensions "daml") expectFiles
  goldenTests <- mapM (fileTest externalAnchors  . (testDir </>))  goldenSrcs

  pure $ Tasty.testGroup "DA.Daml.Doc" $ unitTests <> concat goldenTests

unitTests :: [Tasty.TestTree]
unitTests =
    [ damldocExpect
           Nothing
           "Empty module is OK"
           [testModHdr]
           (assertBool "Expected empty docs" . (== emptyDocs testModule))

         , damldocExpect
           Nothing
           "Module header doc"
           [ "-- | This is a module header"
           , "module " <> T.pack testModule <> " where"
           ]
           (\ModuleDoc{..} -> assertBool "Expected the module header" $
                              md_descr == Just "This is a module header" )

         , damldocExpect
           Nothing
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
           Nothing
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
           Nothing
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
           Nothing
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
           Nothing
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
           Nothing
           "Choice field docs"
           [ testModHdr
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "    choice DoSomething : ()"
           , "      with field: () -- ^ field"
           , "      controller field1"
           , "      do pure ()"
           ]
           (\md -> assertBool
                   ("Expected a choice with field in doc, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                check $ isNothing $ td_descr t
                                f1 <- getSingle $ td_payload t
                                check $ isNothing $ fd_descr f1
                                ch <- getSingle $ td_choicesWithoutArchive t
                                f2 <- getSingle $ cd_fields ch
                                check $ Just "field" == fd_descr f2))

         , damldocExpect
           Nothing
           "Several Choices"
           [ testModHdr
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "    choice DoSomething : ()"
           , "      with field: ()"
           , "      controller field1"
           , "      do pure ()"
           , "    choice DoMore : ()"
           , "      with field: ()"
           , "      controller field1"
           , "      do pure ()"
           ]
           (\md -> assertBool
                   ("Expected two choices in doc, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                check $ isNothing $ td_descr t
                                cs <- Just $ td_choicesWithoutArchive t
                                check $ length cs == 2
                                check $ ["DoMore", "DoSomething"] == sort (map cd_name cs)))

         , damldocExpect
           Nothing
           "Interface implementations"
           [ testModHdr
           , "interface Bar where"
           , "  method : Update ()"
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "    implements Bar where"
           , "      method = pure ()"
           ]
           (\md -> assertBool
                   ("Expected interface implementation, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                impl <- getSingle $ td_impls t
                                check $ getTypeAppName (impl_iface impl) == Just "Bar"))

         , damldocExpect
           Nothing
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
                                member <- getSingle $ cl_methods cls
                                check (Just "Member description" == cm_descr member)))
         ]


  where getSingle :: [a] -> Maybe a
        getSingle []  = Nothing
        getSingle [x] = Just x
        getSingle _xs = Nothing

        check :: Bool -> Maybe ()
        check True = Just ()
        check False = Nothing

        td_choicesWithoutArchive :: TemplateDoc -> [ChoiceDoc]
        td_choicesWithoutArchive = filter (\ch -> cd_name ch /= "Archive") . td_choices


testModule :: String
testModule = "Testfile"


testModHdr :: T.Text
testModHdr = T.pack $ "module\n  " <> testModule <> " where\n"


emptyDocs :: String -> ModuleDoc
emptyDocs name =
    let md_name = Modulename (T.pack name)
        md_anchor = Just (moduleAnchor md_name)
        md_descr = Nothing
        md_templates = []
        md_adts = []
        md_functions = []
        md_classes = []
        md_instances = []
        md_interfaces = []
    in ModuleDoc {..}

-- | Compiles the given input string (in a tmp file) and checks generated doc.s
-- using the predicate provided.
damldocExpect :: Maybe FilePath -> String -> [T.Text] -> (ModuleDoc -> Assertion) -> Tasty.TestTree
damldocExpect importPathM testname input check =
  testCase testname $
  withTempDir $ \dir -> do

    let testfile = dir </> testModule <.> "daml"
    -- write input to a file
    T.writeFileUtf8 testfile (T.unlines input)
    doc <- runDamldoc testfile importPathM
    check doc

-- | Generate the docs for a given input file and optional import directory.
runDamldoc :: FilePath -> Maybe FilePath -> IO ModuleDoc
runDamldoc testfile importPathM = do
    let opts = (defaultOptions Nothing)
          { optHaddock = Haddock True
          , optScenarioService = EnableScenarioService False
          , optImportPath = maybeToList importPathM
          , optDamlLfVersion = versionDev
          }

    -- run the doc generator on that file
    mbResult <- runMaybeT $ extractDocs
        defaultExtractOptions
        diagnosticsLogger
        opts
        [toNormalizedFilePath' testfile]

    case mbResult of
      Nothing ->
        assertFailure $ unlines ["Parse error(s) for test file " <> testfile]

      Just docs -> do
          let docs' = applyTransform defaultTransformOptions docs
                -- apply transforms to get instance data
              name = md_name (head docs)
                -- first module in docs is the one we're testing,
                -- we need to find it in docs' because applyTransform
                -- will reorder the docs
              docM = find ((== name) . md_name) docs'
          pure $ fromJust docM

-- | For the given file <name>.daml (assumed), this test checks if any
-- <name>.EXPECTED.<suffix> exists, and produces output according to <suffix>
-- for all files found.
fileTest :: AnchorMap -> FilePath -> IO [Tasty.TestTree]
fileTest externalAnchors damlFile = do

  damlFileAbs <- makeAbsolute damlFile
  let basename = dropExtension damlFileAbs
      expected = [ basename <.> "EXPECTED" <.> s
                 | s <- [ "json", "rst", "md" ]]

  expectations <- filterM doesFileExist expected

  if null expectations
    then pure []
    else do
      doc <- runDamldoc damlFile (Just $ takeDirectory damlFile)

      pure $ flip map expectations $ \expectation ->
        goldenVsStringDiff ("File: " <> expectation) diff expectation $ pure $
          case takeExtension expectation of
            ".rst" -> TL.encodeUtf8 $ TL.fromStrict $ renderPage renderRst externalAnchors $ renderModule doc
            ".md" -> TL.encodeUtf8 $ TL.fromStrict $ renderPage renderMd externalAnchors $ renderModule doc
            ".json" -> AP.encodePretty' jsonConf doc
            other -> error $ "Unsupported file extension " <> other
  where
    diff ref new = [POSIX_DIFF, "--strip-trailing-cr", ref, new]
