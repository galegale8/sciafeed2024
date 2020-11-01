
from setuptools import setup, find_packages

with open('README.rst') as fh:
    long_description = fh.read()

version = '1.0'

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
    'sqlalchemy==1.3.16',
    'psycopg2==2.8.5',
    'zeep==3.4.0',
    'google-api-python-client',
    # 'google-auth-httplib2',
    'oauth2client',
    'google-auth-oauthlib',
    'numpy',
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
            'download_hiscentral = sciafeed.entry_points:download_hiscentral',
            'compute_daily_indicators = sciafeed.entry_points:compute_daily_indicators',
            'find_new_stations = sciafeed.entry_points:find_new_stations',
            'upsert_stations = sciafeed.entry_points:upsert_stations',
            'check_chain = sciafeed.entry_points:check_chain',
            'download_er = sciafeed.entry_points:download_er',
            'insert_data = sciafeed.entry_points:insert_data',
            'load_unique_data = sciafeed.entry_points:load_unique_data',
            'compute_daily_indicators2 = sciafeed.entry_points:compute_daily_indicators2',
            'compute_dma = sciafeed.entry_points:compute_dma',
        ],
    },
)
