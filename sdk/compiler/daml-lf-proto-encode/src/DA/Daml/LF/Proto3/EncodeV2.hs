-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV2
  ( encodeScenarioModule
  , encodePackage
  ) where

import qualified Com.Daml.DamlLfDev.DamlLf1 as LF1
import qualified Com.Daml.DamlLfDev.DamlLf2 as LF2
import DA.Daml.LF.Ast ( Version, Module, Package )
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1
import Proto3.Suite (toLazyByteString, fromByteString)
import qualified Data.ByteString.Lazy as BL
import Data.Either (fromRight)

encodePackage :: Package -> LF2.Package
encodePackage = coerceLF1toLF2 . EncodeV1.encodePackage

encodeScenarioModule :: Version -> Module -> LF2.Package
encodeScenarioModule version mod = coerceLF1toLF2 (EncodeV1.encodeScenarioModule version mod)

coerceLF1toLF2 :: LF1.Package -> LF2.Package
coerceLF1toLF2 package =
  fromRight
    (error "cannot coerce LF1 proto to LF2 proto")
    (fromByteString (BL.toStrict $ toLazyByteString package))
