#!/usr/bin/env python

import unittest
from setuptools import setup, find_packages

def mytests():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("tests", pattern="test_*.py")
    return test_suite

setup(
    name="nlpml", 
    packages=find_packages(),
    test_suite='setup.mytests'
    )

