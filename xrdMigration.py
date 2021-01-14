#!/usr/bin/env python3

import pwd,os,sys
import subprocess
from optparse import OptionParser
import socket
import getpass
from pathlib import Path

def subprocess_open(command):
    popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdoutdata, stderrdata) = popen.communicate()
    return stdoutdata, stderrdata

class XrdMigration:
    def __init__(self):
        self.localid = getpass.getuser()
        self.source=[]
        self.dest={}
        self.hostname = socket.gethostname()
        self.parseOptions()
        self.doMigration()
    def parseOptions(self):
        usage="Usage: xrdMigration.py [options] [source dir]\n(ex) xrdMigration.py -d -c geonmo /cms/ldap_home/migration_test/"
        parser = OptionParser(usage)
        parser.add_option("-f", "--file", dest="listfile",help="A list file which includes the input files.(Not yet)")
        parser.add_option("-c", "--cernid", dest="cernid",help="Input your CERN username. (Default) Query from CRIC.")
        parser.add_option("-d", "--delete", dest="delete",action="store_true",default= False, help="Delete copied files. (Default) False")
        (options, args) = parser.parse_args()
        self.delete_flag= options.delete
        if options.cernid == None:
            self.getCernId()
        else:
            self.cernId = options.cernid
        if options.listfile is not None:
            print("Checking the file list...")
            with open(options.listfile) as f:
                for infile in f.readlines():
                    infile = infile.strip()
                    if os.path.isfile(infile):
                        self.source.append(infile)
            print("Number of files is %d\n"%(len(self.source)))
            if ( len(args) == 0) :
                for tofile in self.source:
                    self.dest[tofile] = self.getDestDir() 
            elif( len(args) == 1) :
                pass
            else:
                print("Wrong arguments.")
        else:
            if( len(args) >0 and len(args)<3 ):
                for infile in Path(args[0]).rglob("*"):
                    self.source.append(infile)
                print("Number of files is %d\n"%(len(self.source)))
                if "sdfarm.kr" in self.hostname:
                    self.dest_default="root://cms-xrdr.private.lo:2094//xrd/store/user/%s"%(self.cernId)
                self.getDestDir()
            elif( len(args) == 2) :
                self.dest_default = args[1]
                self.getDestDir()
                print("Set destination : %s"%(self.dest))
            else:
                print("Wrong arguments.")
        print("options : ",options)
        print("args : ",args)
    def doMigration(self):
        for src in self.source:
            cmd = "xrdcp -p %s %s"%(src,self.dest[src])
            print(cmd)
            os.system(cmd)
            out, err = subprocess_open("xrdadler32 %s"%(src))
            srcHash  = out.decode('utf-8').split()[0]
            out, err = subprocess_open("xrdadler32 %s"%(self.dest[src]))
            destHash = out.decode('utf-8').split()[0]
            if srcHash==destHash:
                print("Hash is corrected.")
                if self.delete_flag:
                    cmd = "rm %s"%(src)
                    print(cmd)
                    os.system(cmd)
    def getDestDir(self):
        for src in self.source:
            dirname = os.path.dirname(src).split(self.localid)[-1]
            self.dest[src] = self.dest_default+dirname+ "/"+os.path.basename(src) 
    def getCernId(self):
        if os.system("voms-proxy-info -exists") != 0:
            os.system("voms-proxy-init --voms cms")
        issues, err = subprocess_open("voms-proxy-info -issuer")
        issues_url = issues.decode('utf-8').strip().replace(" ","%20")
        cmd = "curl -skS 'https://cms-cric.cern.ch/api/accounts/user/query/?json&dn=%s'|grep login"%(issues_url)
        out, err =subprocess_open(cmd)
        self.cernId = out.decode('utf-8').strip().split(':')[-1].replace("\"","").replace(",","").strip()
    def printCernId(self):
        print("CERN ID is %s"%(self.cernId))
    def printSource(self,idx=5):
        print(self.source[:idx])
    def printDest(self,idx=5):
        print(self.dest[self.source[0]])
    def printAll(self):
        self.printCernId()
        self.printSource()
        self.printDest()
if __name__ == "__main__":
    xm = XrdMigration()
