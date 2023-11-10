-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TemplateOrInterface
  ( TemplateOrInterface (..)
  , TemplateOrInterface'
  ) where

-- | Glorified 'Either' to handle template or interface cases
data TemplateOrInterface a b
  = Template a
  | Interface b

-- | For the common case where both type arguments are the same
type TemplateOrInterface' a = TemplateOrInterface a a

