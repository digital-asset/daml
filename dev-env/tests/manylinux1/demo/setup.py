# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from distutils.core import setup, Extension

extension_module = Extension(
    'demo.ext',
    sources=['demo/extension.c'],
    include_dirs=['/usr/include/glib-2.0', '/usr/lib64/glib-2.0/include'],
    libraries=['glib-2.0'],
)

setup(
    name='demo',
    version='0.1',
    description='This is a demo package with a compiled C extension.',
    ext_modules=[extension_module],
    packages=['demo'],
)
