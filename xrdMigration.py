#!/usr/bin/env python3

import pwd,os,sys
import subprocess
from optparse import OptionParser
import socket
import getpass
from pathlib import Path
import glob

def subprocess_open(command):
    popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdoutdata, stderrdata) = popen.communicate()
    return stdoutdata, stderrdata

class XrdMigration:
    localid = None
    source = []
    srcdirs = []
    dest = {}
    destdir = {}
    destprepend=""
    hostname = None
    options = None
    args = None
    nFiles = 0
    def __init__(self):
        self.localid = getpass.getuser()
        self.hostname = socket.gethostname()
        self.parseOptions()
    def parseOptions(self):
        usage="Usage: xrdMigration.py [options] [source dir]\n"
        usage+="(ex) xrdMigration.py -d -c geonmo -t xrdUpload /cms/ldap_home/geonmo/migration_test/\n"
        usage+="/cms/ldap_home/geonmo/migration_test => /xrootd/store/user/geonmo/xrdUpload/migration_test"
        parser = OptionParser(usage)
        parser.add_option("-f", "--file", dest="listfile",help="A list file which includes the input files.")
        parser.add_option("-t", "--dest", dest="destprepend",default="",help="Path of the Destination directory")
        parser.add_option("-c", "--cernid", dest="cernid",help="Input your CERN username. (Default) Query from CRIC.")
        parser.add_option("-d", "--delete", dest="delete",action="store_true",default= False, help="Delete copied files. (Default) False")
        parser.add_option("-n", "--dryrun", dest="dryrun",action="store_true",default= False, help="Dry Run")
        (options, args) = parser.parse_args()
        self.delete_flag= options.delete
        self.destprepend=options.destprepend
        self.dryrun=options.dryrun
        ## Get CERN Account name
        if options.cernid == None:
            self.getCernId()
        else:
            self.cernId = options.cernid
        ## Get Dest host domain
        if "sdfarm.kr" in self.hostname:
            print("Your site is KISTI-GSDC CMS Tier-3")
            self.dest_default="root://cms-xrdr.private.lo:2094//xrd/store/user/%s"%(self.cernId)
        elif "knu.ac.kr" in self.hostname:
            print("Your site is KNU CMS Tier-3")
            self.dest_default="root://cluster142.knu.ac.kr//store/user/%s"%(self.cernId)
        elif "sscc.uos.ac.kr" in self.hostname:
            print("Your site is UOS CMS Tier-3")
            print("Not yet supported")
        else:
            print("Wrong Site")
            sys.exit(-1)
        
        ## Extract input file list from file or directory
        if options.listfile is not None:
            print("Checking the file list...")
            with open(options.listfile) as f:
                for infile in f.readlines():
                    infile = infile.strip()
                    if os.path.isdir(infile):
                        self.srcdirs.append(infile)
                    elif os.path.isfile(infile):
                        self.source.append(infile)
            self.nFiles = len(self.source)
            print("Number of files is %d\n"%(self.nFiles))
            self.setupDestDir()
        else:
            if( len(args) ==1 or len(args)==2 ):
                print(f"args : {args[0]}")
                self.srcdirs.append(args[0])
                for (path, dirs, files) in os.walk(args[0]):
                    for dirname in dirs:
                        self.srcdirs.append(path+'/'+dirname)
                    for filename in files:
                        self.source.append(path+'/'+filename)
                print(self.srcdirs)
                print(self.source)
                self.nFiles = len(self.source)
                print(f"Number of files is {self.nFiles}")
                self.setupDestDir()
            elif( len(args) == 2) :
                self.dest_default = args[1]
                self.setupDestDir()
                print("Set destination : %s"%(self.dest))
            else:
                print("Wrong arguments.")
                self.printOptions()
                print("")
                parser.print_help()
                sys.exit(-4)
        self.options = options
        self.args = args
    def printOptions(self):
        if ( self.options is not None or self.args is not None):
            print("Option and arguments are")
        if ( self.options is not None):
            print("options : ",self.options)
        if ( self.args is not None):
            print("args : ",self.args)
    def doMigration(self):
        if ( self.nFiles==0):
            print("No file copied. Terminate program.")
        for idx, src in enumerate(self.source):
            cmd = "xrdcp -p %s %s"%(src,self.dest[src])
            print("Copying file (%d/%d)"%(idx+1,self.nFiles))
            print(cmd)
            if not self.dryrun:
                os.system(cmd)
                out, err = subprocess_open("xrdadler32 %s"%(src))
                srcHash  = out.decode('utf-8').split()[0]
                out, err = subprocess_open("xrdadler32 %s"%(self.dest[src]))
                destHash = out.decode('utf-8').split()[0]
                if srcHash==destHash:
                    print("Hash is corrected.")
                    if self.delete_flag:
                        cmd = "rm -f %s"%(src)
                        print("Removed the copied file.(%s)"%(src))
                        os.system(cmd)
        for sdir in self.srcdirs:
            destdir = self.destdir[sdir]
            cmd = "xrdfs cms-xrdr.private.lo:2094 locate -r %s"%(destdir)
            print("Refreshing directory cached(%s)"%(destdir))
            if not self.dryrun:
                os.system(cmd)
    def setupDestDir(self):
        for src in self.source:
            srcs = str(src)
            if( srcs.startswith("/cms/ldap_home") or srcs.startswith("/cms/scratch") or srcs.startswith("/cms_scratch")):
                dirname = os.path.dirname(src).split(self.localid)[-1]
                self.dest[src] = f"{self.dest_default}/{self.destprepend}/{dirname}/{os.path.basename(src)}"
            else:
                print("Wrong source location.")
                sys.exit(-1)
        for srcdir in self.srcdirs:
            srcdir_str = str(srcdir)
            if( srcdir_str.startswith("/cms/ldap_home") or srcdir_str.startswith("/cms/scratch") or srcdir_str.startswith("/cms_scratch")):
                dirname = srcdir_str.split(self.localid)[-1]
                self.destdir[srcdir] = f"/xrd/store/user/{self.cernId}/{self.destprepend}/{dirname}"
            
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
    if (sys.version_info.minor <6):
        print("Python version is lower than 3.6.")
        sys.exit(-2)
    xm = XrdMigration()
    xm.doMigration()
