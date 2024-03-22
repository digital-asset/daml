-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
module Control.Lens.Ast
  ( leftSpine
  , rightSpine
  ) where

import Control.Lens
import Data.List

-- $setup
-- The examples for 'leftSpine' and 'rightSpine' assume we have the following
-- type and prisms given:
--
-- >>> data Expr = EFun String | EApp Expr Int | ELam String Expr deriving (Show)
-- >>> let _EApp = prism (uncurry EApp) (\e0 -> case e0 of {EApp e1 a -> Right (e1, a); _ -> Left e0})
-- >>> let _ELam = prism (uncurry ELam) (\e0 -> case e0 of {ELam x e1 -> Right (x, e1); _ -> Left e0})

-- | Turn a 'Prism'' for a left associative constructor of an AST into an 'Iso''
-- between the normal AST representation and a representation where the left
-- spine has been unwound.
--
-- >>> (EApp (EApp (EFun "f") 1) 2) ^. leftSpine _EApp
-- (EFun "f",[1,2])
--
-- >>> leftSpine _EApp # (EFun "g", [3,4])
-- EApp (EApp (EFun "g") 3) 4
leftSpine :: Prism' a (a, b) -> Iso' a (a, [b])
leftSpine p = iso unwindl rewindl
  where
    unwindl e = go e []
      where
        go e0 as = case matching p e0 of
          Left   e1     -> (e1, as)
          Right (e1, a) -> go e1 (a:as)
    rewindl (e0, as) = foldl' (curry (p #)) e0 as

-- | Analogue of 'leftSpine' for right associative constructors.
--
-- >>> (ELam "x" (ELam "y" (EFun "f"))) ^. rightSpine _ELam
-- (["x","y"],EFun "f")
--
-- >>> rightSpine _ELam # (["x", "y"], EFun "g")
-- ELam "x" (ELam "y" (EFun "g"))
rightSpine :: Prism' a (b, a) -> Iso' a ([b], a)
rightSpine p = iso unwindr rewindr
  where
    unwindr e = go [] e
      where
        go xs e0 = case matching p e0 of
          Left   e1     -> (reverse xs, e1)
          Right (x, e1) -> go (x:xs) e1
    rewindr (xs, e0) = foldr (curry (p #)) e0 xs
