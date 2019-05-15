#!/bin/bash

rm check.log
# should run pylint
pylint nlpml >> check.log
# should run pyre and or mypy
# pyre --source-directory nlpml check >> check.log
mypy --ignore-missing-imports nlpml >> check.log
