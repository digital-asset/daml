-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
module DA.Service.JsonRpc.Arbitrary where

import Data.Aeson.Types
import qualified Data.HashMap.Strict as M
import Data.Text (Text)
import qualified Data.Text as T
import DA.Service.JsonRpc.Data
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen

instance Arbitrary Text where
    arbitrary = T.pack <$> arbitrary

instance Arbitrary Ver where
    arbitrary = elements [V1, V2]

instance Arbitrary Request where
    arbitrary = oneof
        [ Request <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
        , Notif <$> arbitrary <*> arbitrary <*> arbitrary
        ]

instance Arbitrary Response where
    arbitrary = oneof
        [ Response <$> arbitrary <*> arbitrary <*> arbitrary
        , ResponseError <$> arbitrary <*> arbitrary <*> arbitrary
        , OrphanError <$> arbitrary <*> arbitrary
        ]


instance Arbitrary ErrorObj where
    arbitrary = oneof
        [ ErrorObj <$> arbitrary <*> arbitrary <*> arbitrary
        , ErrorVal <$> arbitrary
        ]

instance Arbitrary BatchRequest where
    arbitrary = oneof
        [ BatchRequest <$> arbitrary
        , SingleRequest <$> arbitrary
        ]

instance Arbitrary BatchResponse where
    arbitrary = oneof
        [ BatchResponse <$> arbitrary
        , SingleResponse <$> arbitrary
        ]

instance Arbitrary Message where
    arbitrary = oneof
        [ MsgRequest  <$> arbitrary
        , MsgResponse <$> arbitrary
        , MsgBatch    <$> batch
        ]
      where
        batch = listOf $ oneof [ MsgRequest  <$> arbitrary
                               , MsgResponse <$> arbitrary
                               ]

instance Arbitrary Id where
    arbitrary = oneof [IdInt <$> arbitrary, IdTxt <$> arbitrary]

instance Arbitrary Value where
    arbitrary = resize 10 $ oneof [nonull, lsn, objn] where
        nonull = oneof
            [ toJSON <$> (arbitrary :: Gen String)
            , toJSON <$> (arbitrary :: Gen Int)
            , toJSON <$> (arbitrary :: Gen Double)
            , toJSON <$> (arbitrary :: Gen Bool)
            ]
        val = oneof [ nonull, return Null ]
        ls   = toJSON <$> listOf val
        obj  = toJSON . M.fromList <$> listOf ps
        ps   = (,) <$> (arbitrary :: Gen String) <*> oneof [val, ls]
        lsn  = toJSON <$> listOf (oneof [ls, obj, val])
        objn = toJSON . M.fromList <$> listOf psn
        psn  = (,) <$> (arbitrary :: Gen String) <*> oneof [val, ls, obj]
