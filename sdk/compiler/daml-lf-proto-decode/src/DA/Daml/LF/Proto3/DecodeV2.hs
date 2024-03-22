-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeFamilies #-}

module DA.Daml.LF.Proto3.DecodeV2
    ( decodePackage
    , decodeScenarioModule
    , Error(..)
    ) where

import qualified Com.Daml.DamlLfDev.DamlLf1 as LF1
import qualified Com.Daml.DamlLfDev.DamlLf2 as LF2
import DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Error
import Data.Bifunctor (first)
import qualified Data.ByteString.Lazy as BL
import qualified Data.NameMap as NM
import Proto3.Suite (fromByteString, toLazyByteString)
import Proto3.Wire.Decode (ParseError)

import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1

coerceLF2toLF1 :: LF2.Package -> Either ParseError LF1.Package
coerceLF2toLF1 package = fromByteString (BL.toStrict $ toLazyByteString package)

decodePackage :: LF.Version -> LF.PackageRef -> LF2.Package -> Either Error Package
decodePackage version selfPackageRef package = do
  lf1Package <- first (ParseError . show) (coerceLF2toLF1 package)
  DecodeV1.decodePackage version selfPackageRef lf1Package

decodeScenarioModule :: LF.Version -> LF2.Package -> Either Error Module
decodeScenarioModule version protoPkg = do
    Package { packageModules = modules } <- decodePackage version PRSelf protoPkg
    pure $ head $ NM.toList modules

