# loadavg

A Windows version of Linux /proc/loadavg.

Requirements
------------

* Python 2.6 or 2.7
* pywin32

Installation
------------

Installation is not required.  To install as a Windows service, type

    loadavg.py install

To start the service, type

    loadavg.py start

Or, it can be run as a service within a use process using

    loadavg.py --service

Use
---

To check the load average, type

    loadavg.py

Use a time value to query repeatedly, like so

    loadavg.py 60

For more information, type

    loadavg.py --help

