SCIA-FEED
=========
Python tool for feeding the SCIA database.

:Author:  B-Open Solutions s.r.l.
:Version: 1.0

.. contents::


1. Scope
--------
This python package is intended to be used and installed as a component of a
complete service for the SCIA management for ISPRA.

SCIA-FEED is a python tool for downloading, validating, computing and inserting
climate indicators into the SCIA PostgreSQL database. The input files can be
present in the filesystem or can be downloaded through online services.

Take reference of project documentation for official and updated installation
and usage instructions.

Official tested on OS Linux Ubuntu 18.04 LTS.

2. Development environment
--------------------------
These instructions assume user can access the GIT repository of SCIA-FEED.
Create a project folder, if not exist yet, for example `SCIA-FEED`, and clone
the git code inside.

.. code:: bash

    mkdir SCIA-FEED
    cd SCIA-FEED
    git clone https://git.intranet.isprambiente.it/portale-scia/applicazioni/scia-feed.git

2.1 Install
~~~~~~~~~~~
If it doesn't exist yet, create a python 3 virtual environment, and install the
develoment extension of the package:

.. code:: bash

    python3 -m venv ve
    ve/bin/pip install -e .[dev]

2.2 Testing
~~~~~~~~~~~
Standard testing:

.. code:: bash

    ve/bin/pytest tests

Testing with coverage and open with a browser (for example firefox):

.. code:: bash

    ve/bin/pytest tests --cov=sciafeed --cov-report=html
    firefox htmlcov/index.html

2.3 Create source distribution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To create source distribution (inside the `dist` folder):

.. code:: bash

    ve/bin/python setup.py sdist --formats=zip

2.4 Generate technical code documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To generate html documentation from the code, launch the build script:

.. code:: bash

    ve/bin/sphinx-build -E docs docs/html

The documentation is readable from docs/html/index.html by a browser (for example, firefox):

.. code:: bash

    firefox docs/html/index.html

3. Installation
---------------
These instructions aim to install SCIA-FEED in a not-development environment.
Create a project folder, if not exist yet, for example `SCIA-FEED`:

.. code:: bash

    mkdir SCIA-FEED

To install SCIA-FEED, you need to access the source code.  This means that you
need a source distribution (something like `sciafeed-<version>.zip`), or
you need to have access to the GIT repository of SCIA-FEED.

3.1 Installation from the GIT repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you have access to the GIT repository of SCIA-FEED, get the code
via the `git clone` command:

.. code:: bash

    cd SCIA-FEED
    git clone https://git.intranet.isprambiente.it/portale-scia/applicazioni/scia-feed.git

Then you can decide to install SCIA-FEED inside a new python 3 virtual environment or using
`pip` of your already existing python 3 environment.
If you want to create a new python 3 virtual environment, use:

.. code:: bash

    python3 -m venv ve
    ve/bin/pip install -e .

Otherwise, use `pip` of your python 3 environment:

.. code:: bash

    pip install -e .

3.2 Installation from a source distribution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you have a source distribution, copy it inside the project folder, for example:

.. code:: bash

    cp /media/cdrom/sciafeed-<version>.zip SCIA-FEED
    cd SCIA-FEED

Then you can decide to install SCIA-FEED inside a new python 3 virtual environment or using
`pip` of your python 3 environment.
If you want to create a new python 3 virtual environment, use:

.. code:: bash

    python3 -m venv ve
    ve/bin/pip install sciafeed-<version>.zip

Otherwise, use `pip` of your python 3 environment:

.. code:: bash

    pip install sciafeed-<version>.zip

4. Usage
--------
Take reference of project documentation for official usage instructions.
