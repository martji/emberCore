# emberCore
Ember Project. Ember is a universal moddleware for distributed memory store to balance the load of cluster dynamically. The whole project consists of three part: rmemcached_client, rmemcached_server and rmemcached_monitor.

- **rmemcached_server** is the core of project, which acts as an universal middleware;
- **rmemcached_client** is the client for user;
- **rmemcached_monitor** is a global node to monitor the cluster.
