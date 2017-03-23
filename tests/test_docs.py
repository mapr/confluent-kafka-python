#!/usr/bin/env python

import mapr_streams_python
import re
from types import ModuleType

def test_verify_docs():
    """ Make sure all exported functions, classes, etc, have proper docstrings
    """
    fails = 0

    for n in dir(mapr_streams_python):
        if n.startswith('__'):
            # Skip internals
            continue

        o = mapr_streams_python.__dict__.get(n)
        d = o.__doc__
        if not d:
            print('Missing __doc__ for: %s (type %s)' % (n, type(o)))
            fails += 1
        elif not re.search(r':', d):
            print('Missing Doxygen tag for: %s (type %s)' % (n, type(o)))
            if not isinstance(o, ModuleType):
                fails += 1

    assert fails == 0

    
