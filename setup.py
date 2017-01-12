

# check if pip is installed. If not, raise an ImportError
PIP_INSTALLED = True

try:
    import pip
except ImportError:
    PIP_INSTALLED = False

if not PIP_INSTALLED:
    raise ImportError('pip is not installed.')

def install_and_import(package):
    import importlib
    try:
        importlib.import_module(package)
    except ImportError:
        pip.main(['install', package])
    finally:
        globals()[package] = importlib.import_module(package)

# check if setuptools is installed. If not, install setuptools
# automatically using pip.
install_and_import('setuptools')

if __name__ == "__main__":

    # find packages to be included.
    packages = setuptools.find_packages()

    with open('README.rst') as f:
        LONG_DESCRIPTION = f.read()

    setuptools.setup(
        name='py_entitymatching',
        version='0.1.0',
        description='Python library for end to end Entity Matching.',
        long_description=LONG_DESCRIPTION,
        url='https://sites.google.com/site/anhaidgroup/projects/py_entitymatching',
        author='UW Magellan Team',
        author_email='uwmagellan@gmail.com',
        license='BSD',
        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Intended Audience :: Science/Research',
            'Intended Audience :: Education',
            'License :: OSI Approved :: BSD License',
            'Operating System :: POSIX',
            'Operating System :: Unix',
            'Operating System :: MacOS',
            'Operating System :: Microsoft :: Windows',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Topic :: Scientific/Engineering',
            'Topic :: Utilities',
            'Topic :: Software Development :: Libraries',
        ],
        packages=packages,
        install_requires=[
            'PyPrind == 2.9.8',
            'py_stringsimjoin',
            # dependencies such as py_stringmatching, joblib, pyprind
            'cloudpickle >= 0.2.1',
            'pyparsing >= 2.1.4',
            'scikit-learn >= 0.18'
        ],
        include_package_data=True,
        zip_safe=False
    )
