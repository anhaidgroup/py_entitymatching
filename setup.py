from setuptools import setup

# Set this to True to enable building extensions using Cython.
# Set it to False to build extensions from the C file (that
# was previously created using Cython).
# Set it to 'auto' to build with Cython if available, otherwise
# from the C file.


setup(
    name='Magellan',
    version='0.1',
    description='End-to-End Entity Matching Management System',
    long_description="""
    EM is a critical problem and will become even more critical. Lot of EM research, but has focused mostly
    on matching algorithms. Focus mostly on maximizing some performance factors such as accuracy, time, money, etc.
    Currently there is very little help for users. For many of these steps, there is little or no work, so no
    solution. Even when there are solutions, there may be no tools. Even when there are tools, user is faced
    with a wide variety of tools, one for each step, so user is left trying to move among the tools, stitching them
    together, decifering their different data formats, import/export commands. too cumbersome and difficult.
    So often users just give up and roll their own ad-hoc solutions instead. This is obviously a major bottleneck
    that prevents the deployment of matching in practice.
    To solve the above problems, Magellan provides a set of commands that help the user to
    1) Come up with a EM workflow
    2) Iterate and debugmatcher the workflow
    3) Deploy it in production
    """,
    url='http://github.com/kvpradap/enrique',
    author='Pradap Konda',
    author_email='pradap@cs.wisc.edu',
    license=['MIT'],
    packages=['magellan'],
    install_requires=['JPype1>=0.5.7',
                      'pandas >= 0.16.0',
                      'numpy >= 1.7.0',
                      'six',
                      'scikit-learn >= 0.16.1',
                      'cloud >= 2.8.5',
                      'pyparsing >= 2.0.3',
                      'scipy >= 0.16.0',
                      'Cython >= 0.23.0',
                      'PyPrind >= 2.9.3'
                      ],
    include_package_data=True,
    zip_safe=False
)