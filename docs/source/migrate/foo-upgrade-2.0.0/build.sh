#!/bin/sh
daml build --init-package-db=no --package '("foo-1.0.0",[("Foo","FooA")])' --package '("foo-2.0.0",[("Foo","FooB")])'