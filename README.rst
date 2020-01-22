SCIA-FEED
=========
Python tool for feeding the SCIA database.

:Author:  B-Open Solutions s.r.l.
:Version: 0.1

.. contents::


Scope
=====
This python package is intended to be used and installed as a component of a
complete service for the SCIA management for ISPRA.

SCIA-FEED is a python tool for downloading, validating, computing and inserting
climate indicators into the SCIA PostgreSQL database. The input files can be
present in the filesystem or can be downloaded through online services.

Take reference of project documentation for official and updated installation
and usage instructions.

Official tested on OS Linux Ubuntu 18.04 LTS.

Get the code
------------
This instructions assumes user can access the GIT repository of SCIA-FEED.

Create a project folder, if not exist yet, for example `SCIA-FEED`, and clone
the git code inside.

.. code:: bash

    mkdir SCIA-FEED
    cd SCIA-FEED
    git clone https://git.intranet.isprambiente.it/portale-scia/applicazioni/scia-feed.git

Install and test for developers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Create a python 3 virtual environment and install the develoment extension of the package:

.. code:: bash

    python3 -m venv ve
    ve/bin/pip install -e .[dev]

Testing:

.. code:: bash

    ve/bin/pytest tests

Testing with coverage:

.. code:: bash

    ve/bin/pytest tests --cov=drsstorage --cov-report=html

Generate technical code documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To generate html documentation from the code, install the 'dev' extension,
and launch the build script:

.. code:: bash

    ve/bin/sphinx-build docs docs/html

The documentation is readable from docs/html/index.html by a browser.

Install for standard users
~~~~~~~~~~~~~~~~~~~~~~~~~~
If you want to install SCIA-FEED inside a new python 3 virtual environment:

.. code:: bash

    python3 -m venv ve
    ve/bin/pip install -e .

Otherwise, use `pip` of your python 3 environment:

.. code:: bash

    pip install -e .

Usage
~~~~~
Take reference of project documentation for official usage instructions.
