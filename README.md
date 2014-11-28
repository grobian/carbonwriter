carbonwriter
============

Simple whisper file writer.

This project aims to be a replacement of the carbon-cache.py daemon which writes incoming metrics to disk in whisper files.

The main reason to build a replacement is performance.  At the time of
this writing, there is nothing to be said for sure about it, though.

For retrieval of metrics from whisper files, see carbonserver.  For
combining the output of multiple servers (using carbonserver) see
carbonzipper.  For operations on the metric data, such as understood by
the graphite-web frontend, see carbonapi.


Author
------
Fabian Groffen


Acknowledgement
---------------
This program was originally developed for Booking.com.  With approval
from Booking.com, the code was generalised and published as Open Source
on github, for which the author would like to express his gratitude.
