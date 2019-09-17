#!/usr/bin/env python

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    readme = f.read()

setup(
    name="nlpml", 
    version="0.1",
    author="Johann Petrak",
    author_email="johann.petrak@gmail.com",
    description="Library for opinionated approach to NLP, ML and Multiprocessing",
    long_description=readme,
    long_description_content_type='text/markdown',
    setup_requires=["pytest-runner"],
    install_requires=["torch>=1.0"],
    python_requires=">=3.5",
    tests_require=['pytest'],
    platforms='any',
    license="MIT",
    keywords="",
    url="http://github.com/johann-petrak/nlpml",
    packages=find_packages(),
    # test_suite='???'
    )

