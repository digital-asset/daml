-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlcUpgrades (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.Foldable
import System.Directory.Extra
import System.FilePath
import System.IO.Extra
import DA.Test.Process
import Test.Tasty
import Test.Tasty.HUnit
import SdkVersion
import DA.Daml.LF.Ast.Version
import Text.Regex.TDFA
import qualified Data.Text as T
import Data.Maybe (fromMaybe)

main :: IO ()
main = do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: FilePath -> TestTree
tests damlc =
    testGroup
        "Upgrade"
        [ test
              "Fails when template changes signatories"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A signatories:\n  The upgraded template A cannot change the definition of its signatories.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where signatory [p]"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where signatory [p, q]"
                      ]
                )
              ]
        , test
              "Fails when template changes observers"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A observers:\n  The upgraded template A cannot change the definition of its observers.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    observer p"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    observer p, q"
                      ]
                )
              ]
        , test
              "Fails when template changes ensure"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A precondition:\n  The upgraded template A cannot change the definition of its precondition.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    ensure True"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    ensure True == True"
                      ]
                )
              ]
        , test
              "Fails when template changes agreement"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A agreement:\n  The upgraded template A cannot change the definition of agreement.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    agreement \"agreement1\""
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    agreement \"agreement2\""
                      ]
                )
              ]
        , test
              "Fails when new field is added to template without Optional type"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A has added new fields, but those fields are not Optional.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Int"
                      , "    existing2 : Int"
                      , "  where signatory p"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Int"
                      , "    existing2 : Int"
                      , "    new : Int"
                      , "  where signatory p"
                      ]
                )
              ]
        , test
              "Fails when old field is deleted from template"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A is missing some of its original fields.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Int"
                      , "    existing2 : Int"
                      , "  where signatory p"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing2 : Int"
                      , "  where signatory p"
                      ]
                )
              ]
        , test
              "Fails when existing field in template is changed"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A has changed the types of some of its original fields.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Int"
                      , "    existing2 : Int"
                      , "  where signatory p"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Text"
                      , "    existing2 : Int"
                      , "  where signatory p"
                      ]
                )
              ]
        , test
              "Succeeds when new field with optional type is added to template"
              Nothing
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Int"
                      , "    existing2 : Int"
                      , "  where signatory p"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    existing1 : Int"
                      , "    existing2 : Int"
                      , "    new : Optional Int"
                      , "  where signatory p"
                      ]
                )
              ]
        , test
              "Fails when new field is added to template choice without Optional type"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded input type of choice C on template A has added new fields, but those fields are not Optional.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "        new : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
        , test
              "Fails when old field is deleted from template choice"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded input type of choice C on template A is missing some of its original fields.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
        , test
              "Fails when existing field in template choice is changed"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded input type of choice C on template A has changed the types of some of its original fields.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Text"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
        , test
              "Fails when controllers of template choice are changed"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded choice C cannot change the definition of controllers.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      controller p, q"
                      , "      do pure ()"
                      ]
                )
              ]
        , test
              "Fails when observers of template choice are changed"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded choice C cannot change the definition of observers.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      observer p"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      observer p, q"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
        , test
              "Fails when template choice changes its return type"
              (Just "Message: \n\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded choice C cannot change its return type.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : Int"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure 1"
                      ]
                )
              ]
        , test
              "Succeeds when template choice returns a template which has changed"
              Nothing
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : A"
                      , "      controller p"
                      , "      do pure (A p)"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Optional Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : A"
                      , "      controller p"
                      , "      do pure (A p (Just p))"
                      ]
                )
              ]
        , test
              "Succeeds when template choice input argument has changed"
              Nothing
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        tpl : A"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Optional Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        tpl : A"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
        , test
              "Succeeds when new field with optional type is added to template choice"
              Nothing
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "  where"
                      , "    signatory p"
                      , "    choice C : ()"
                      , "      with"
                      , "        existing1 : Int"
                      , "        existing2 : Int"
                      , "        new : Optional Int"
                      , "      controller p"
                      , "      do pure ()"
                      ]
                )
              ]
        ]
  where
    test ::
           String
        -> Maybe T.Text
        -> [(FilePath, String)]
        -> [(FilePath, String)]
        -> TestTree
    test name expectedError oldVersion newVersion =
        testCase name $
        withTempDir $ \dir -> do
            let depDir = dir </> "oldVersion"
            let dar = dir </> "out.dar"
            let depDar = dir </> "oldVersion" </> "dep.dar"
            writeFiles dir (projectFile "mylib-v2" (Just depDar) : newVersion)
            writeFiles depDir (projectFile "mylib-v1" Nothing : oldVersion)
            callProcessSilent damlc ["build", "--project-root", depDir, "-o", depDar]
            case expectedError of
              Nothing ->
                  callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
              Just regex -> do
                  stderr <- callProcessForStderr damlc ["build", "--project-root", dir, "-o", dar]
                  unless (matchTest (makeRegex regex :: Regex) stderr) $
                      assertFailure ("Regex '" <> show regex <> "' did not match stderr:\n" <> show stderr)

    writeFiles dir fs =
        for_ fs $ \(file, content) -> do
            createDirectoryIfMissing True (takeDirectory $ dir </> file)
            writeFileUTF8 (dir </> file) content

    projectFile name upgradedFile =
        ( "daml.yaml"
        , unlines $
          [ "sdk-version: " <> sdkVersion
          , "name: " <> name
          , "source: daml"
          , "version: 0.0.1"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          , "typecheck-upgrades: true"
          , "build-options:"
          , "- --target=" <>
                renderVersion
                  (fromMaybe
                    (error "DamlcUpgrades: featureMinVersion should be defined over featurePackageUpgrades")
                    (featureMinVersion featurePackageUpgrades V1))
          ] ++ ["upgrades: '" <> path <> "'" | Just path <- pure upgradedFile]
        )
