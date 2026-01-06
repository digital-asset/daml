-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Codegen
  ( runCodegen
  , Lang(..)
  , showLang
  ) where

import DA.Daml.Helper.Util
import qualified Data.Text as T

data Lang
  = Java
  | JavaScript
  deriving (Show, Eq, Ord, Enum, Bounded)

showLang :: Lang -> T.Text
showLang = \case
  Java -> "java"
  JavaScript -> "js"

runCodegen :: Lang -> [String] -> IO ()
runCodegen lang args =
  runJar
    "daml-sdk/daml-sdk.jar"
    (Just "daml-sdk/codegen-logback.xml")
    (["codegen", T.unpack $ showLang lang] ++ args)
