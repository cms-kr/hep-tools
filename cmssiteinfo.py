#!/usr/bin/env python3

import os, sys
import json

class CMSProtocolInfo:
    def __init__(self, sitename):
        self.protocol={}
        if( os.path.isdir("/cvmfs/cms.cern.ch")):
            infile = f"/cvmfs/cms.cern.ch/SITECONF/{sitename}/storage.json"
            with open(infile) as f:
                cfg = json.load(f)
                for prtc in cfg[0]["protocols"]:
                    self.protocol[prtc['protocol']] = prtc['prefix']
                self.sitename = sitename
        else:
            print("CVMFS is not mounted")
            sys.exit(-1)
    def print_protocol(self):
        print(self.sitename)
        print(self.protocol)
    def get_prefix(self,protocol):
        print(self.protocol[protocol])

if __name__== "__main__":
    cp = CMSProtocolInfo("T3_KR_KISTI")
    cp.print_protocol()
    cp.get_prefix("XRootD")
