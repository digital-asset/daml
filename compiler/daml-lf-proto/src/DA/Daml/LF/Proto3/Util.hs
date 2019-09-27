-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module DA.Daml.LF.Proto3.Util (
    EitherLike,
    toEither,
    fromEither,
    ) where

import GHC.Generics

type EitherLike m1 m2 m3 m4 m5 a b e =
    (Generic e, Rep e ~ D1 m1 (C1 m2 (S1 m3 (Rec0 a)) :+: C1 m4 (S1 m5 (Rec0 b))))

toEither :: EitherLike m1 m2 m3 m4 m5 a b e => e -> Either a b
toEither e =
    case unM1 $ from e of
        L1 x -> Left $ unK1 $ unM1 $ unM1 x
        R1 y -> Right $ unK1 $ unM1 $ unM1 y

fromEither :: EitherLike m1 m2 m3 m4 m5 a b e => Either a b -> e
fromEither = to . M1 . either (L1 . M1 . M1 . K1) (R1 . M1 . M1 . K1)
