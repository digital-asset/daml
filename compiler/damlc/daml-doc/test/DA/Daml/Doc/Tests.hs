-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

module DA.Daml.Doc.Tests(mkTestTree)
  where

{- HLINT ignore "locateRunfiles/package_app" -}

import DA.Bazel.Runfiles
import DA.Daml.Compiler.Output (diagnosticsLogger)
import DA.Daml.Options.Types

import DA.Daml.Doc.Extract
import DA.Daml.Doc.Render
import DA.Daml.Doc.Render.Hoogle
import DA.Daml.Doc.Types
import DA.Daml.Doc.Transform
import DA.Daml.Doc.Anchor
import DA.Test.DamlcIntegration (ScriptPackageData)

import Development.IDE.Types.Location

import Control.Monad
import           Control.Monad.Trans.Maybe
import qualified Data.Aeson.Encode.Pretty as AP
import           Data.List.Extra
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
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

import SdkVersion.Class (SdkVersioned, sdkPackageVersion)

mkTestTree :: SdkVersioned => AnchorMap -> ScriptPackageData -> IO Tasty.TestTree
mkTestTree externalAnchors scriptPackageData = do

  testDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/daml-test-files"

  let isExpectationFile filePath =
        ".EXPECTED" == takeExtensions (dropExtension filePath)
  expectFiles <- filter isExpectationFile <$> listDirectory testDir

  let goldenSrcs = nubOrd $ map (flip replaceExtensions "daml") expectFiles
  goldenTests <- mapM (fileTest externalAnchors scriptPackageData . (testDir </>)) goldenSrcs

  pure $ Tasty.testGroup "DA.Daml.Doc" $ unitTests <> concat goldenTests

unitTests :: SdkVersioned => [Tasty.TestTree]
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
                                check $ Just ["field1"] == td_signatory t
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
                                check $ Just "field" == fd_descr f2
                                check $ TypeTuple [] == cd_type ch
                                check $ Just ["field1"] == cd_controller ch))

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
           "Interface instances"
           [ testModHdr
           , "data EmptyInterfaceView = EmptyInterfaceView"
           , "interface Bar where"
           , "  viewtype EmptyInterfaceView"
           , "  method : Update ()"
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "    interface instance Bar for Foo where"
           , "      view = EmptyInterfaceView"
           , "      method = pure ()"
           ]
           (\md -> assertBool
                   ("Expected interface instance, got " <> show md)
                   (isJust $ do t  <- getSingle $ md_templates md
                                InterfaceInstanceDoc {..} <- getSingle $ td_interfaceInstances t
                                check $
                                  getTypeAppName ii_interface == Just "Bar"
                                  && getTypeAppName ii_template == Just "Foo"))

         , damldocExpect
           Nothing
           "Interface instance in interface"
           [ testModHdr
           , "data EmptyInterfaceView = EmptyInterfaceView"
           , "template Foo with"
           , "    field1 : Party"
           , "  where"
           , "    signatory field1"
           , "interface Bar where"
           , "  viewtype EmptyInterfaceView"
           , "  method : Update ()"
           , "  interface instance Bar for Foo where"
           , "    view = EmptyInterfaceView"
           , "    method = pure ()"
           ]
           (\md -> assertBool
                   ("Expected interface instance, got " <> show md)
                   (isJust $ do i  <- getSingle $ md_interfaces md
                                InterfaceInstanceDoc {..} <- getSingle $ if_interfaceInstances i
                                check $
                                  getTypeAppName ii_interface == Just "Bar"
                                  && getTypeAppName ii_template == Just "Foo"))

         , damldocExpectMany
           Nothing
           "Interface instance with qualified interface"
           [ (,) "Interface"
             [ "module Interface where"

             , "data EmptyInterfaceView = EmptyInterfaceView"

             , "interface Bar where"
             , "  viewtype EmptyInterfaceView"
             , "  method : Update ()"
             ]
           , (,) "Template"
             [ "module Template where"

             , "import qualified Interface"

             , "template Foo with"
             , "    field1 : Party"
             , "  where"
             , "    signatory field1"
             , "    interface instance Interface.Bar for Foo where"
             , "      view = Interface.EmptyInterfaceView"
             , "      method = pure ()"
             ]
           ]
           (\mds -> assertBool
                   ("Expected interface instance, got " <> show mds)
                   (isJust $ do interfaceMod <- Map.lookup (Modulename "Interface") mds
                                interface <- getSingle $ md_interfaces interfaceMod
                                interfaceAnchor <- if_anchor interface

                                templateMod <- Map.lookup (Modulename "Template") mds
                                template <- getSingle $ md_templates templateMod
                                InterfaceInstanceDoc {..} <- getSingle $ td_interfaceInstances template
                                check $
                                  getTypeAppName ii_interface == Just "Bar"
                                  && getTypeAppName ii_template == Just "Foo"
                                  && getTypeAppAnchor ii_interface == Just interfaceAnchor))

         , damldocExpectMany
           Nothing
           "Interface instance with qualified template"
           [ (,) "Template"
             [ "module Template where"
             , "template Foo with"
             , "    field1 : Party"
             , "  where"
             , "    signatory field1"
             ]
           , (,) "Interface"
             [ "module Interface where"

             , "import qualified Template"

             , "data EmptyInterfaceView = EmptyInterfaceView"

             , "interface Bar where"
             , "  viewtype EmptyInterfaceView"
             , "  method : Update ()"
             , "  interface instance Bar for Template.Foo where"
             , "    view = EmptyInterfaceView"
             , "    method = pure ()"
             ]
           ]
           (\mds -> assertBool
                   ("Expected interface instance, got " <> show mds)
                   (isJust $ do templateMod <- Map.lookup (Modulename "Template") mds
                                template <- getSingle $ md_templates templateMod
                                templateAnchor <- td_anchor template

                                interfaceMod <- Map.lookup (Modulename "Interface") mds
                                interface <- getSingle $ md_interfaces interfaceMod
                                InterfaceInstanceDoc {..} <- getSingle $ if_interfaceInstances interface
                                check $
                                  getTypeAppName ii_interface == Just "Bar"
                                  && getTypeAppName ii_template == Just "Foo"
                                  && getTypeAppAnchor ii_template == Just templateAnchor))
         , damldocExpect
           Nothing
           "Interface archive choice controller"
           [ testModHdr
           , "data View = View {}"
           , "interface I where"
           , "  viewtype View"
           ]
           (\md -> assertBool
                   ("Expected an interface with an archive choice controller, got " <> show md)
                   (isJust $ do interface <- getSingle $ md_interfaces md
                                ch <- getSingle $ if_choices interface
                                check $ "Archive" == cd_name ch
                                check $ Just ["Signatories of implementing template"] == cd_controller ch))

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
damldocExpect :: SdkVersioned => Maybe FilePath -> String -> [T.Text] -> (ModuleDoc -> Assertion) -> Tasty.TestTree
damldocExpect importPathM testname input check =
  testCase testname $
  withTempDir $ \dir -> do

    let testfile = dir </> testModule <.> "daml"
    -- write input to a file
    T.writeFileUtf8 testfile (T.unlines input)
    doc <- runDamldoc testfile importPathM Nothing
    check doc

damldocExpectMany ::
     SdkVersioned
  => Maybe FilePath
  -> String
  -> [(String, [T.Text])]
  -> (Map Modulename ModuleDoc -> Assertion)
  -> Tasty.TestTree
damldocExpectMany importPathM testname input check =
  testCase testname $
  withTempDir $ \dir -> do
    testfiles <- forM input $ \(modName, content) -> do
      let testfile = dir </> modName <.> "daml"
      T.writeFileUtf8 testfile (T.unlines content)
      pure testfile
    docs <- runDamldocMany testfiles importPathM Nothing
    check docs

-- | Generate the docs for a given input file and optional import directory.
runDamldoc :: SdkVersioned => FilePath -> Maybe FilePath -> Maybe ScriptPackageData -> IO ModuleDoc
runDamldoc testfile importPathM mScriptPackageData = do
  -- The first module is the one we're testing
  (\(names, modMap) -> modMap Map.! head names)
    <$> runDamldocMany' [testfile] importPathM mScriptPackageData

-- | Generate the docs for a given list of input files and optional import directory.
runDamldocMany :: SdkVersioned => [FilePath] -> Maybe FilePath -> Maybe ScriptPackageData -> IO (Map Modulename ModuleDoc)
runDamldocMany testfiles importPathM mScriptPackageData =
  snd <$> runDamldocMany' testfiles importPathM mScriptPackageData

-- | Generate the docs for a given list of input files and optional import directory.
-- The fst of the result has the names of Modulenames for each file path in the input.
-- The snd has a map from all the modules (including imported ones) to their docs.
runDamldocMany' :: SdkVersioned => [FilePath] -> Maybe FilePath -> Maybe ScriptPackageData -> IO ([Modulename], Map Modulename ModuleDoc)
runDamldocMany' testfiles importPathM mScriptPackageData = do
  let opts = (defaultOptions Nothing)
        { optHaddock = Haddock True
        , optScenarioService = EnableScenarioService False
        , optImportPath = maybeToList importPathM
        , optPackageDbs = maybeToList $ fst <$> mScriptPackageData
        , optPackageImports = maybe [] snd mScriptPackageData
        }

  -- run the doc generator on that file
  mbResult <- runMaybeT $ extractDocs
    defaultExtractOptions
    diagnosticsLogger
    opts
    (toNormalizedFilePath' <$> testfiles)

  case mbResult of
    Nothing ->
      assertFailure $ unlines
        ["Parse error(s) for test file(s) " <> intercalate ", " testfiles]

    Just docs -> do
      let names = md_name <$> take (length testfiles) docs
            -- extract names from docs since the front of docs matches testfiles
          docs' = applyTransform defaultTransformOptions docs
            -- apply transforms to get instance data
          moduleMap = Map.fromList
            [ (md_name docM, docM)
            | docM <- docs'
            ]
      pure (names, moduleMap)

-- | For the given file <name>.daml (assumed), this test checks if any
-- <name>.EXPECTED.<suffix> exists, and produces output according to <suffix>
-- for all files found.
fileTest :: SdkVersioned => AnchorMap -> ScriptPackageData -> FilePath -> IO [Tasty.TestTree]
fileTest externalAnchors scriptPackageData damlFile = do

  damlFileAbs <- makeAbsolute damlFile
  let basename = dropExtension damlFileAbs
      expected = [ basename <.> "EXPECTED" <.> s
                 | s <- [ "json", "rst", "md", "hoogle" ]]

  expectations <- filterM doesFileExist expected

  if null expectations
    then pure []
    else do
      doc <- runDamldoc damlFile (Just $ takeDirectory damlFile) (Just scriptPackageData)

      pure $ flip map expectations $ \expectation ->
        goldenVsStringDiff ("File: " <> expectation) diff expectation $ pure $
          case takeExtension expectation of
            ".rst" -> TL.encodeUtf8 $ TL.fromStrict $ renderPage renderRst externalAnchors $ renderModule doc
            ".md" -> TL.encodeUtf8 $ TL.fromStrict $ renderPage renderMd externalAnchors $ renderModule doc
            ".json" -> replaceSdkPackages $ AP.encodePretty' jsonConf doc
            ".hoogle" -> TL.encodeUtf8 $ TL.fromStrict $ renderSimpleHoogle (HoogleEnv mempty) doc
            other -> error $ "Unsupported file extension " <> other
  where
    diff ref new = [POSIX_DIFF, "--strip-trailing-cr", ref, new]
    -- In cases where daml-script/daml-trigger is used, the version of the package is embedded in the json.
    -- When we release, this version changes, which would break the golden file test.
    -- Instead, we omit daml-script/daml-trigger versions from .EXPECTED.json files in golden tests.
    replaceSdkPackages = 
      TL.encodeUtf8
      . TL.replace (TL.pack $ "daml-script-" <> sdkPackageVersion) "daml-script-UNVERSIONED"
      . TL.replace (TL.pack $ "daml-trigger-" <> sdkPackageVersion) "daml-trigger-UNVERSIONED"
      . TL.decodeUtf8
