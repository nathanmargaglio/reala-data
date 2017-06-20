from expired_crawler import *
import sys

if 'help' in sys.argv:
    print("Add 'xp' for collecting Expired Listings")
    print("Add 'yp' to update numbers via Yellow Pages")
    print("Add nothing for the full shabang\n")

if 'xp' in sys.argv:
    sources = crawler()
    for s in sources:
        mls_parser(s)
    
if 'yp' in sys.argv:
    yp_crawler()
    
if len(sys.argv) == 1:
    print("Starting...")
    sources = crawler()
    for s in sources:
        mls_parser(s)
    yp_crawler()