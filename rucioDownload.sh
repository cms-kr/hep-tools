#!/bin/bash
DATASET=$1

voms-proxy-info -exists
if [ $? -eq 1 ]; then
  voms-proxy-init --voms cms
fi
USERDN=$(voms-proxy-info -issuer)
USERDN_URL=${USERDN// /%20}

CERNID_RAW=$(curl -skS "https://cms-cric.cern.ch/api/accounts/user/query/?json&dn=${USERDN_URL}"|grep login)
CERNID_RAW2=($CERNID_RAW)
ee=${CERNID_RAW2[1]}
ff=${ee//\"/}
CERNID=${ff//,/}
echo $CERNID
source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/cms.cern.ch/rucio/setup.sh
export RUCIO_ACCOUNT=$CERNID

rucio add-rule --ask-approval cms:$DATASET 1 T3_KR_KISTI
