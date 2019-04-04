-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module SOP where

import Generics.SOP
import Generics.SOP.TH

data Tree = Leaf Int | Node Tree Tree

deriveGeneric ''Tree
