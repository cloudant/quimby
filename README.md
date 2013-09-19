Quimby - DB Core Tests
======================

Yep.

Running
-------

Until we get dbcore/#158 merged this is a bit manual but totally doable. If
you clone this repo somewhere all you need to do is this:

    $ # In your dbcore repo
    $ # edit rel/overlay/etc/local.ini to look like this:
    $ cat rel/overlay/etc/local.ini
    ; local customizations are stored here
    [admins]
    adm = pass
    $ # and then...
    $ ./dev/run

Once you have DB Core running and requiring the adm account then all you
need to do in this repository is run:

    $ ./run

And you should get test output. If you don't have to wait for the virtual
environment to be created then you probably need to install virtualenv.


ToDo
----

I need to rearrange tests sot hat they're more better organized
and so that everythign is under the quimby namespace except cloudant.py
which I may end up breaking out into a separate project.
