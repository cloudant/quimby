Quimby - Clustered CouchDB Tests
================================

Yep.

Running
-------

If you clone this repo somewhere all you need to do is this:

    $ # In your couch repo
    $ # edit rel/overlay/etc/local.ini to look like this:
    $ cat rel/overlay/etc/local.ini
    ; local customizations are stored here
    [admins]
    adm = pass
    $ # and then...
    $ ./dev/run

Once you have a clustered Couch running and requiring the adm account then all
you need to do in this repository is run:

    $ ./run

And you should get test output. If you don't have to wait for the virtual
environment to be created then you probably need to install virtualenv.


ToDo
----

Need to rearrange tests so that they're more better organized
and so that everything is under the quimby namespace except cloudant.py
which I may end up breaking out into a separate project.
