#!/usr/bin/env python3

import os, subprocess, sys, socket
from multiprocessing import Pool, Value, Manager
from itertools import repeat

from optparse import OptionParser

from cmssiteinfo import CMSProtocolInfo

from pipe_open import pipe_open 
from xrdhelper import XRootDHelper


def init(total, success, fail):
    global g_total
    global g_success
    global g_fail
    g_total = total
    g_success = success
    g_fail = fail

def migration(lfn, source, dest, return_list):
    global g_total
    global g_success
    global g_fail

    ### 전송 시작 ###
    cmd = f"xrdcp -p -f root://{source+lfn} root://{dest+lfn}"
    print(cmd)
    return_value = os.system(cmd)
    if( return_value !=0 ):
        print("%s is failed."%(lfn))
        with g_fail.get_lock():
            g_fail.value += 1
            return_list.append(lfn)
    else:
        with g_success.get_lock():
            g_success.value += 1
    print("Total: %d / Success: %d / Fail: %d\n"%(g_total.value, g_success.value, g_fail.value))
    return None

class CMSDownload():
    target=[]
    def __init__(self):
        manager = Manager()
        self.return_list = manager.list()
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
        self.runDownload()
    def parseOptions(self):
        usage="Usage: %prog [options] \n(Ex1) %prog --source T2_CH_CERN --dest T2_KR_KISTI -l datalist.txt -p 4\n(Ex2) %prog --source T2_CH_CERN --dest T2_KR_KISTI -d /store/group/phys_heavyions -p 4"
        parser = OptionParser(usage)
        parser.add_option("-l", "--listfile", dest="listfile",metavar="[FILE]",help="The list [FILE] of the files you want to send.")
        parser.add_option("-s", "--source", dest="source", metavar="[SITE]",help="[SITE] name of data source")
        parser.add_option("-t", "--dest", dest="dest", metavar="[SITE]",help="[SITE] name of data destination")
        parser.add_option("-d", "--dirname", dest="dirname", metavar="[DIR]",help="[DIR] name of transfer files")
        parser.add_option("-p", "--parallel", dest="nparallel",default=4, help="number of copy jobs to be run simultaneously")
        parser.add_option("-v", "--verbose", dest="verbose",action="store_true", help="Verbose mode")
        (options, args) = parser.parse_args()
        self.nparallel = int(options.nparallel)
        self.source  = options.source
        self.dest    = options.dest
        self.verbose = options.verbose
        if ( self.verbose): 
            print("Verbose mode is on.")
        if( options.listfile == None and options.dirname ==None):
            print("Wrong option. One of the list files or directory names is essential.")
            sys.exit(-1)
        elif( options.listfile ==None and options.dirname!=None):
            self.read_list_from_dir(options.dirname)
        elif( options.listfile !=None and options.dirname == None):
            self.read_list_from_file(options.listfile)
        else:
            print("Wrong option. Only one of the list files or directory names should be set.")
            sys.exit(-1)
    def read_list_from_dir(self, dirname):
        lines = XRootDHelper(self.source).get_filelist(dirname)
        self.read_list(lines)
    def read_list_from_file(self,listfile):
        lines = open(listfile).readlines()
        self.read_list(lines)
    def read_list(self,lines):
        lfns=[]
        for line in lines:
            lfn = line.strip().split()[0]
            if( not lfn.startswith("/store")):
                lfn = "/store"+lfn.split("/store")[-1]
            lfns.append(lfn)
        self.check_files(lfns)

    def printTarget(self):
        print(self.target)
    def runDownload(self):
        g_total = Value('i',len(self.target))
        g_success = Value('i',0)
        g_fail = Value('i',0)
        with Pool(initializer = init, initargs = (g_total, g_success, g_fail,), processes = self.nparallel) as p:
            p.starmap(migration, zip(self.target,repeat(self.source_prefix),repeat(self.dest_prefix),repeat(self.return_list) ))
        with open("failed_list.txt","w") as f:
            for failed_file in self.return_list:
                f.write("%s\n"%failed_file)
    def check_files(self,lfns):
        srcxh = XRootDHelper(self.source)
        destxh = XRootDHelper(self.dest)
        self.source_prefix = srcxh.get_prefix()
        self.dest_prefix = destxh.get_prefix()
        print(f"Total target files : {len(lfns)}") 
        for lfn in lfns:
            ## 송신측 파일 존재여부 확인
            if(not srcxh.isfile(lfn)):
                print("Wrong Source file.")
                continue
            if(destxh.isfile(lfn)):
                source_filesize = srcxh.get_filesize(lfn)
                dest_filesize = destxh.get_filesize(lfn)
                ## 송신측 과 수신측의 파일 상태 확인 / 파일 사이즈 비교
                if (source_filesize == dest_filesize):
                    print("동일 파일 발견.")
                    continue
                else:
                    if self.verbose:
                        print("Verbose mode: Add %s to list"%(lfn))
                    self.target.append(lfn)
            else:
                self.target.append(lfn)


if __name__ == '__main__':

    lfn_list=[]
    failed_list=[]
    CMSDownload()
        

