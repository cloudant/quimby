Things that would make testing better
=====================================

node discovery
--------------

It could help with config things for tests if we exposed some sort of
ability to automatically discover a cluster configuration only using a
single root URL. This would be a mismash of node name, public/private
ports and the like.


since sequences
---------------

We need to add a set of APIs for handling since sequences, both in generating
and decoding them. Things like, "give me a current since sequence", give me
a since sequence representing these shards and update sequences, etc etc.


shard maps
----------

Perry asked for an update to the `/dbname/_shards` API to allow for a
`?by_node=true` style return. Basically allow us to selectively request
the `by_range` or `by_node` section of a shard map. Also, we should add
at least an option to include the ushards. Although we need to beware that
ushards changes depending on the machine servicing the request (and I should
check if that's really true cause dbcopy would be weird, right?)


Also, I'd like to include suffices as well.


dbcopy
------

The dbcopy tests are quite racy as we wait for the dbcopy dbs to update. It'd
be nice to have a thing to wait for a change. We could do this in cloudant.py
by grabbing a last\_seq and then doing a longpoll after triggering a stale=ok
view.

