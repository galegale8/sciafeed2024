
from setuptools import setup, find_packages

with open('README.rst') as fh:
    long_description = fh.read()

version = '0.3'

tests_require = [
        'pytest',
        'pytest-mock',
        'pytest-cov',
      ]
development_require = [
        'ipython',
        'sphinx==2.4.3',
        'sphinx_rtd_theme==0.4.3',
        'sphinx-click',
]
install_requires = [
    'click==7.0',
    'xlrd==1.2.0',
]

setup(
    name='sciafeed',
    version=version,
    description='Python tool for feeding the SCIA database',
    long_description=long_description,
    author='B-Open Solutions s.r.l.',
    author_email='info@bopen.eu',
    license='Proprietory',
    packages=find_packages(),
    zip_safe=False,
    python_requires='>=3.0',
    tests_require=tests_require,
    install_requires=install_requires,
    include_package_data=True,
    extras_require={
        'dev': tests_require + development_require,
    },
    entry_points={
        'console_scripts': [
            'make_report = sciafeed.entry_points:make_report',
            'make_reports = sciafeed.entry_points:make_reports',
        ],
    },
)