from __future__ import print_function
from setuptools import setup, find_packages

with open("README.md", "r", encoding='UTF-8') as fh:
  long_description = fh.read()

setup(
  name="sdrf-pipelines",
  version="0.0.14",
  author="BigBio Team",
  author_email="ypriverol@gmail.com",
  description="Translate, convert SDRF to configuration pipelines",
  long_description_content_type="text/markdown",
  long_description=long_description,
  license="'Apache 2.0",
  data_files=[("", ["LICENSE", "sdrf_pipelines/openms/unimod.xml"])],
  package_data={'': ['*.xml'], },
  url="https://github.com/bigbio/sdrf-pipelines",
  packages=find_packages(),
  install_requires=['click', 'pandas', 'pandas_schema', 'requests', 'pytest'],
  entry_points={
    'console_scripts': [
      'parse_sdrf = sdrf_pipelines.parse_sdrf:main'
    ]
  },
  platforms=['any'],
  classifiers=[
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    'Topic :: Scientific/Engineering :: Bio-Informatics'
  ],
  keywords='sdrf python multiomics proteomics',
  include_package_data=True,
  python_requires='>=3.6',
)
