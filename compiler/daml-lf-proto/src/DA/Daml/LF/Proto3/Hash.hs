-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Hash where

import           DA.Prelude
import qualified Data.ByteString          as BS
import qualified Data.Text                as T
import qualified Numeric

-- | Encode the hash bytes of the payload in the canonical
-- lower-case ascii7 hex presentation.
encodeHash :: BS.ByteString -> T.Text
encodeHash = T.pack . reverse . foldl' toHex [] . BS.unpack
  where
    toHex xs c =
      case Numeric.showHex c "" of
        [n1, n2] -> n2 : n1 : xs
        [n2]     -> n2 : '0' : xs
        _        -> error "impossible: showHex returned [] on Word8"
