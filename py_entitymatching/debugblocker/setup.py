try:
    from setuptools import setup
    from setuptools import Extension
except ImportError:
    from distutils.core import setup
    from distutils.extension import Extension

#from distutils.core import setup, Extension
from Cython.Build import cythonize

setup(ext_modules = cythonize([

    Extension("debugblocker_cython", sources=["debugblocker_cython.pyx", "TopPair.cpp", "PrefixEvent.cpp",
                                              "GenerateRecomLists.cpp", "TopkHeader.cpp",
                                              "OriginalTopkPlain.cpp",
                                              ],
              language="c++"), 

 ]))
