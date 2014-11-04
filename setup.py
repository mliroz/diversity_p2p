#!/usr/bin/env python

from setuptools import setup, find_packages

setup (
  name = "div_p2p",
  version = "0.1",

  packages = ["div_p2p"],
  scripts = ["scripts/div_p2p_engine"],

  install_requires=["execo"],

  # PyPI
  author = 'Miguel Liroz Gistau',
  author_email = 'miguel.liroz_gistau@inria.fr',
  description = "A collection of scripts and packages that help in the "
                "deployment and experimental evaluation of diversity-based"
                "clustering algorithms in P2P recommendation systems.",
  url = "https://github.com/mliroz/div_p2p",
  license = "MIT",
  keywords = "p2p recommendation diversity g5k grid5000 execo",
  
)