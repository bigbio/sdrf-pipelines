from __future__ import print_function
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
  name="sdrf-pipelines",
  version="0.0.1",
  author="BigBio Team",
  author_email="ypriverol@gmail.com",
  description="Translate, convert SDRF to configuration pipelines",
  long_description_content_type="text/markdown",
  long_description=long_description,
  license="'Apache 2.0",
  url="https://github.com/bigbio/sdrf-pipelines",
  packages=["sdrf_pipelines"],
  scripts=['sdrf_pipelines/parse_sdrf.py'],
  install_requires=['click', 'pandas'],
  platforms=['any'],
  classifiers=[
      "Programming Language :: Python :: 3",
      "License :: OSI Approved :: Apache Software License",
      "Operating System :: OS Independent",
      'Topic :: Scientific/Engineering :: Bio-Informatics'
  ],
  python_requires='>=3.5',
)
