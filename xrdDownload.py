#!/usr/bin/env python3

import os, subprocess, sys, socket
from multiprocessing import Pool, Value
from optparse import OptionParser


XRDFB="root://cms-xrd-global.cern.ch/"
XRDDEST="root://cms-xrdr.private.lo:2094//xrd/"


def init(total, success, fail):
    global g_total
    global g_success
    global g_fail
    g_total = total
    g_success = success
    g_fail = fail



def subprocess_open(command):
    popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdoutdata, stderrdata) = popen.communicate()
    returncode = popen 
    return stdoutdata, stderrdata

def migration(lfn):
    source = XRDFB+lfn
    dest = XRDDEST+lfn
    xrd_locate = '/xrd'+lfn
    xrd_dir = '/xrd'+os.path.dirname(lfn)
    cmd = 'xrdfs cms-xrdr.private.lo:2094 rm %s'%(xrd_locate)
    os.system(cmd)

    global g_total
    global g_success
    global g_fail


    print("Source: ",source)
    print("Destination: ",dest)
    cmd = 'xrdcp -p -f %s %s'%(source ,dest)
    return_value = os.system(cmd)
    if( return_value !=0 ):
        print("%s is failed."%(lfn))
        with g_fail.get_lock():
            g_fail.value += 1
    else:
        with g_success.get_lock():
            g_success.value += 1
    print("Total: %d / Success: %d / Fail: %d\n"%(g_total.value, g_success.value, g_fail.value))
    return None

class xrdDownload():
    lfnsize={}
    target=[]
    def __init__(self):
        self.hostname = socket.gethostname()
        if ( "sdfarm.kr" not in self.hostname):
            print("This server is not KISTI.")
            sys.exit(-1)
        if ( "CMSSW_BASE" not in os.environ):
            print("Please, \"cmsenv\" for this command.")
            sys.exit(-1)
        if (os.system("voms-proxy-info -exist -valid 8:0") !=0):
            os.system("voms-proxy-init --voms cms")
        self.parseOptions()
        self.checkDestFiles()
        self.runDownload()
    def parseOptions(self):
        usage="Usage: xrdDownload.py [options] \n(ex) xrdDownload.py -i datalist.txt -p 4"
        parser = OptionParser(usage)
        parser.add_option("-i", "--infile", dest="listfile",default="datalist.txt",help="A list file which includes the input files.")
        parser.add_option("-f", "--force" , dest="force"   ,action="store_true", default=False, help ="force to copy")
        parser.add_option("-p", "--parallel", dest="nparallel",default=4, help="number of copy jobs to be run simultaneously")
        parser.add_option("-v", "--verbose", dest="verbose",action="store_true", help="Verbose mode")
        (options, args) = parser.parse_args()
        self.nparallel = int(options.nparallel)
        self.force = options.force
        self.verbose = options.verbose
        if ( self.verbose): 
            print("Verbose mode is on.")
        self.readList(options.listfile)
    def readList(self,listfile):
        lines = open(listfile).readlines()
        for line in lines:
            line_column = len(line.strip().split())

            if( self.verbose) : 
                print("Column size is %d"%(line_column))

            if ( line_column == 2):
                lfn, size = line.strip().split()
                if( not lfn.startswith("/store")):
                    lfn = "/store"+lfn.split("/store")[-1]
                self.lfnsize[lfn] = size
                if ( self.force ):
                    self.lfnsize[lfn] = -1
            elif (line_column == 1):
                lfn = line.strip().split()[0]
                if( not lfn.startswith("/store")):
                    lfn = "/store"+lfn.split("/store")[-1]
                self.lfnsize[lfn] = -1
            else:
                print("Wrong data file")
                sys.exit(-1)
        if ( self.verbose):
            print( self.lfnsize.keys())

    def printTarget(self):
        print(self.target)
    def runDownload(self):
        g_total = Value('i',len(self.target))
        g_success = Value('i',0)
        g_fail = Value('i',0)
        with Pool(initializer = init, initargs = (g_total, g_success, g_fail,), processes = self.nparallel) as p:
            p.map(migration, self.target)
        with open("failed_list.txt","w") as f:
            for failed_file in failed_list:
                f.write("%s\n"%failed_file)
    def sourceFileName(self, lfn):
        return(XRDFB+lfn)
    def destFileName(self, lfn):
        return(XRDDEST+lfn)
    def pnfsFileName(self, lfn):
        return("/xrootd/"+lfn)
    def checkDestFiles(self):
        for lfn in self.lfnsize.keys():
            pnfs = self.pnfsFileName(lfn)
            if( not os.path.isfile(pnfs)):
                if( self.verbose):
                    print("missing file %s"%(lfn))
                self.target.append(lfn)
                continue
            if ( self.force ): 
                if( self.verbose):
                    print("Verbose mode: Add %s to list"%(lfn))
                self.target.append(lfn)
                continue
            pfn_size = os.path.getsize(pnfs)
            if ( int(pfn_size) == int(self.lfnsize[lfn])):
                if(self.verbose):
                    print("File is already existed. Skip this file.(%s)"%(lfn))
                pass
            else:
                if(self.verbose):
                    print("Wrong size of %s"%(lfn))
                self.target.append(lfn)



if __name__ == '__main__':

    lfn_list=[]
    failed_list=[]
    xrdDownload()
        

