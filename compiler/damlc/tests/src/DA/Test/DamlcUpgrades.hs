-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)
import DA.Daml.LF.Ast.Version
import Text.Regex.TDFA
import qualified Data.Text as T
import Data.Maybe (fromMaybe)

main :: IO ()
main = withSdkVersions $ do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests damlc

tests :: SdkVersioned => FilePath -> TestTree
tests damlc =
    testGroup
        "Upgrade"
        [ test
              "Warns when template changes signatories"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A signatories:\n  The upgraded template A has changed the definition of its signatories.")
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
              "Warns when template changes observers"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A observers:\n  The upgraded template A has changed the definition of its observers.")
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
              "Warns when template changes ensure"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A precondition:\n  The upgraded template A has changed the definition of its precondition.")
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
              "Warns when template changes agreement"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A agreement:\n  The upgraded template A has changed the definition of agreement.")
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
              "Warns when template changes key expression"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A key:\n  The upgraded template A has changed the expression for computing its key.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    key (p, \"example\") : (Party, Text)"
                      , "    maintainer (fst key)"
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
                      , "    key (q, \"example\") : (Party, Text)"
                      , "    maintainer (fst key)"
                      ]
                )
              ]
        , test
              "Warns when template changes key maintainers"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A key:\n  The upgraded template A has changed the maintainers for its key.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    key (p, q) : (Party, Party)"
                      , "    maintainer (fst key)"
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
                      , "    key (p, q) : (Party, Party)"
                      , "    maintainer (snd key)"
                      ]
                )
              ]
        , test
              "Fails when template changes key type"
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A key:\n  The upgraded template A cannot change its key type.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    key (p, \"text\") : (Party, Text)"
                      , "    maintainer (fst key)"
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
                      , "    key (p, 1) : (Party, Int)"
                      , "    maintainer (fst key)"
                      ]
                )
              ]
        , test
              "Fails when template removes key type"
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A key:\n  The upgraded template A cannot remove its key.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
                      , "    key (p, \"text\") : (Party, Text)"
                      , "    maintainer (fst key)"
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
                      ]
                )
              ]
        , test
              "Fails when template adds key type"
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A key:\n  The upgraded template A cannot add a key where it didn't have one previously.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Party"
                      , "  where"
                      , "    signatory p"
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
                      , "    key (p, \"text\") : (Party, Text)"
                      , "    maintainer (fst key)"
                      ]
                )
              ]
        , test
              "Fails when new field is added to template without Optional type"
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A has added new fields, but those fields are not Optional.")
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
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A is missing some of its original fields.")
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
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A has changed the types of some of its original fields.")
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
              Succeed
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
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded input type of choice C on template A has added new fields, but those fields are not Optional.")
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
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded input type of choice C on template A is missing some of its original fields.")
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
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded input type of choice C on template A has changed the types of some of its original fields.")
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
              "Warns when controllers of template choice are changed"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A choice C:\n  The upgraded choice C has changed the definition of controllers.")
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
              "Warns when observers of template choice are changed"
              (SucceedWithWarning "\ESC\\[0;93mwarning while type checking template MyLib.A choice C:\n  The upgraded choice C has changed the definition of observers.")
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
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A choice C:\n  The upgraded choice C cannot change its return type.")
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
              Succeed
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
                      , "      do pure (A p (Some p))"
                      ]
                )
              ]
        , test
              "Succeeds when template choice input argument has changed"
              Succeed
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
              Succeed
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
        , test
              "Fails when a top-level record adds a non-optional field"
              (FailWithError "\ESC\\[0;91merror type checking data type MyLib.A:\n  The upgraded data type A has added new fields, but those fields are not Optional.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A { x : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A { x : Int, y : Text }"
                      ]
                )
              ]
        , test
              "Succeeds when a top-level record adds an optional field at the end"
              Succeed
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A { x : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A { x : Int, y : Optional Text }"
                      ]
                )
              ]
        , test
              "Fails when a top-level record adds an optional field before the end"
              (FailWithError "\ESC\\[0;91merror type checking data type MyLib.A:\n  The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A { x : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = A { y : Optional Text, x : Int }"
                      ]
                )
              ]
        , test
              "Succeeds when a top-level variant adds a variant"
              Succeed
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int } | Z { z : Int }"
                      ]
                )
              ]
        , test
              "Fails when a top-level variant removes a variant"
              (FailWithError "\ESC\\[0;91merror type checking <none>:\n  Data type A.Z appears in package that is being upgraded, but does not appear in this package.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int } | Z { z : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int }"
                      ]
                )
              ]
        , test
              "Fail when a top-level variant changes changes the order of its variants"
              (FailWithError "\ESC\\[0;91merror type checking data type MyLib.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Z { z : Int } | Y { y : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int } | Z { z : Int }"
                      ]
                )
              ]
        , test
              "Fails when a top-level variant adds a field to a variant's type"
              (FailWithError "\ESC\\[0;91merror type checking data type MyLib.A:\n  The upgraded variant constructor Y from variant A has added a field.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int, y2 : Int }"
                      ]
                )
              ]
        , test
              "Succeeds when a top-level variant adds an optional field to a variant's type"
              Succeed
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int }"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X { x : Int } | Y { y : Int, y2 : Optional Int }"
                      ]
                )
              ]
        , test
              "Succeed when a top-level enum adds a field"
              Succeed
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X | Y"
                      ]
                )
              ]
        , test
              "Fail when a top-level enum changes changes the order of its variants"
              (FailWithError "\ESC\\[0;91merror type checking data type MyLib.A:\n  The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X | Y | Z"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data A = X | Z | Y"
                      ]
                )
              ]
        , test
              "Succeeds when a top-level type synonym changes"
              Succeed
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data X = X"
                      , "data Y = Y"
                      , "type A = X"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "data X = X"
                      , "data Y = Y"
                      , "type A = Y"
                      ]
                )
              ]
        , test
              "Succeeds when two deeply nested type synonyms resolve to the same datatypes"
              Succeed
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "type Synonym1 a = (a, Synonym3)"
                      , "type Synonym2 = Int"
                      , "type Synonym3 = Text"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Synonym1 Synonym2"
                      , "  where signatory [p]"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "type Synonym1 a = (Synonym2, a)"
                      , "type Synonym2 = Int"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Synonym1 Text"
                      , "  where signatory [p]"
                      ]
                )
              ]
        , test
              "Fails when two deeply nested type synonyms resolve to different datatypes"
              (FailWithError "\ESC\\[0;91merror type checking template MyLib.A :\n  The upgraded template A has changed the types of some of its original fields.")
              [ ( "daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "type Synonym1 a = (a, Synonym3)"
                      , "type Synonym2 = Int"
                      , "type Synonym3 = Text"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Synonym1 Synonym2"
                      , "  where signatory [p]"
                      ]
                )
              ]
              [ ("daml/MyLib.daml"
                , unlines
                      [ "module MyLib where"
                      , "type Synonym1 a = (a, Synonym3)"
                      , "type Synonym2 = Text"
                      , "type Synonym3 = Int"
                      , "template A with"
                      , "    p : Party"
                      , "    q : Synonym1 Synonym2"
                      , "  where signatory [p]"
                      ]
                )
              ]
        ]
  where
    test ::
           String
        -> Expectation
        -> [(FilePath, String)]
        -> [(FilePath, String)]
        -> TestTree
    test name expectation oldVersion newVersion =
        testCase name $
        withTempDir $ \dir -> do
            let depDir = dir </> "oldVersion"
            let dar = dir </> "out.dar"
            let depDar = dir </> "oldVersion" </> "dep.dar"
            writeFiles dir (projectFile "mylib-v2" (Just depDar) : newVersion)
            writeFiles depDir (projectFile "mylib-v1" Nothing : oldVersion)
            callProcessSilent damlc ["build", "--project-root", depDir, "-o", depDar]
            case expectation of
              Succeed ->
                  callProcessSilent damlc ["build", "--project-root", dir, "-o", dar]
              FailWithError regex -> do
                  stderr <- callProcessForStderr damlc ["build", "--project-root", dir, "-o", dar]
                  let regexWithSeverity = "Severity: DsError\nMessage: \n" <> regex
                  unless (matchTest (makeRegex regexWithSeverity :: Regex) stderr) $
                      assertFailure ("`daml build` failed as expected, but did not give an error matching '" <> show regexWithSeverity <> "':\n" <> show stderr)
              SucceedWithWarning regex -> do
                  stderr <- callProcessForSuccessfulStderr damlc ["build", "--project-root", dir, "-o", dar]
                  let regexWithSeverity = "Severity: DsWarning\nMessage: \n" <> regex
                  unless (matchTest (makeRegex regexWithSeverity :: Regex) stderr) $
                      assertFailure ("`daml build` succeeded, but did not give a warning matching '" <> show regexWithSeverity <> "':\n" <> show stderr)

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

data Expectation
  = Succeed
  | FailWithError T.Text
  | SucceedWithWarning T.Text
  deriving (Show, Eq, Ord)
