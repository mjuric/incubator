A small program to drop Linux kernel buffer caches.

The idea is to build this, make it setuid-root, and therefore allow
non-privileged (but trusted) users to drop caches on a local machine for
(e.g.) benchmarking purposes.
