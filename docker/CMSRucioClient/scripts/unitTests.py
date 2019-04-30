#! /bin/env python

from rucio.tests.test_schema_cms import TestSchemaCMS 

x = TestSchemaCMS()

public_method_names = [
    method for method in dir(x)
    if callable(getattr(x, method))
    if not method.startswith('_')
]  # 'private' methods start from _

for method in public_method_names:
    print("Calling: %s" % method)
    getattr(x, method)()  # call

