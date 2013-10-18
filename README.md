Quimby - DB Core Tests
======================

Yep.

Running
-------

All you need to do is get dbcore running with a quimby's required credentials:

    $ dev/run --admin=adm:pass

Once this has completed startup you can simply invoke the run script in the
quimby repository root:

    $ $QUIMBY_ROOT/run [optionally pass subsections to run]

And you should get test output. If you don't have to wait for the virtual
environment to be created then you probably need to install virtualenv.


ToDo
----

* reorganize test directories and names
* use the quimby namespace for tests
