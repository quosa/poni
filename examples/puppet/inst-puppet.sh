#! /bin/bash -ue
# Usage: Requires defining AWS_KEYPAIR

REPO="$HOME/tmp/puppet"
TIMELOG="/tmp/puppet-timelog"
MAX_SPOT_PRICE=0.090

# remove old repo
rm -rf $REPO

# create new puppet repo with the example system
poni -L "$TIMELOG" -d $REPO script - -v <<EOF
init
vc init

add-config -cd ec2-deb6/ template/ec2-deb6 hacks
set -M template template:bool=true
vc checkpoint "added templates"

add-config -cd puppet-master/ software puppet-master-v1.0
add-config -cd puppet-agent/ software puppet-agent-v1.0
set -M software template:bool=true
vc checkpoint "added software"

add-node example/master -i template/ec2-deb6
add-config example/master puppet-master -i software/puppet-master-v1.0
set example/master cloud.billing="spot" cloud.spot.max_price:float=$MAX_SPOT_PRICE cloud.vm_name=puppet-master cloud.provider=aws-ec2 cloud.region=us-east-1 cloud.image=ami-daf615b3 cloud.kernel=aki-6eaa4907 cloud.ramdisk=ari-42b95a2b cloud.type=m1.small cloud.key-pair=$AWS_KEYPAIR user=root

add-node example/demo/server{id:02} -n2 -i template/ec2-deb6
add-config example/demo/server puppet-agent -i software/puppet-agent-v1.0
set example/demo/server cloud.billing=spot cloud.spot.max_price:float=$MAX_SPOT_PRICE cloud.provider=aws-ec2 cloud.region=us-east-1 cloud.image=ami-daf615b3 cloud.kernel=aki-6eaa4907 cloud.ramdisk=ari-42b95a2b cloud.type=m1.small cloud.key-pair=$AWS_KEYPAIR user=root
set example/demo/server01 cloud.vm_name=puppet-slave01
set example/demo/server02 cloud.vm_name=puppet-slave02
vc checkpoint "added nodes"
report
EOF
