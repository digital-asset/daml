-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Drive the authorisation parts.
module DA.Daml.LF.Auth(checkAuth) where

import DA.Daml.LF.Ast
import DA.Daml.LF.Auth.Type
import DA.Daml.LF.Auth.Check
import DA.Daml.LF.Auth.Solve
import DA.Daml.LF.Auth.Environment


checkAuth :: [(PackageId, Package)] -> Package -> [Error]
checkAuth pkgs pkg = filter f $ checkPackage env pkg
    where
        env = mkEnvironment pkgs pkg
        f (EFailedToSolve _ a b) = not $ solve env a b
        f _ = True
