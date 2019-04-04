-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Proto3.VersionVDev
  ( hashDamlLfDevProto
  , versionVDev
  ) where

import qualified "cryptonite" Crypto.Hash as Crypto
import Data.ByteArray (convert)
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BSL
import "template-haskell" Language.Haskell.TH

import DA.Daml.LF.Proto3.Hash
import DA.Daml.LF.Ast.Version

hashDamlLfDevProto :: T.Text
hashDamlLfDevProto =
  $(let damlLfDevProto = "daml-lf/archive/da/daml_lf_dev.proto"
    in runIO $ LitE . StringL . T.unpack . encodeHash . convert . Crypto.hashlazy @Crypto.SHA256
               <$> BSL.readFile damlLfDevProto)

versionVDev :: Version
versionVDev = VDev hashDamlLfDevProto
