#!/bin/bash


usage(){
	echo "Usage: $0 [-h|--help] [-d|--demo] [-c|--clean] [-l|--launch] [-n <nb_nodes>|--nb-nodes <nb_nodes>] [-p|--podman]"
	echo "           -h|--help                              show this message"
	echo "           -d|--demo                              launch cockroachdb's multi region demo"
	echo "           -c|--clean                             deletes the containers, the volumes and the network"
	echo "           -l|--launch                            launch cockroachdb on multiple containers and run the workload"
	echo "           -n|--nb-nodes <nb nodes>               change the number of nodes to <nb nodes>, default 3"
	echo "           -w|--workload <A|B|C|D>                change the workload, default C"
	echo "           -p|--podman                            uses the podman engine instead of docker"
}
source "utils.sh"
nb_nodes=3
port_speak=26256
port_listen=8079
list_of_ip=""
engine="docker"
cleanup=false
to_launch=false
demo=false
POSITIONAL_ARGS=()
workload=C

parse(){
	while [[ $# -gt 0 ]]; do
		case $1 in
			-w|--workload)
				workload=$2
				shift
				shift
				;;
			-d|--demo)
				demo=true
				shift
				;;
			-c|--clean)
				cleanup=true
				shift
				;;
			-l|--launch)
				to_launch=true
				shift
				;;
			-h|--help)
				usage $@
				exit 0
				;;
			-p|--podman)
				echo "Using podman!"
				engine="podman"
				shift # past value
				;;
			-n|--nb-nodes)
				if [[ $2 -gt 1 ]]; then
					nb_nodes="$2"
				else
					echo "ERROR: At least 2 nodes"
					exit 1
				fi
				echo "Using ${nb_nodes} nodes!"
				shift # past argument
				shift # past value
				;;
			-*|--*)
				usage $@
				exit 1
				;;
			*)
				POSITIONAL_ARGS+=("$1") # save positional arg
				shift # past argument
				;;
		esac
	done
}


launch_demo(){
	# Create the network
	${engine} network create -d bridge roachnet

	# Run the ycsb demo
	${engine} run -d \
		--name="roach_demo" --hostname="roach_demo" \
		--net=roachnet \
		-p "$((port_speak+1)):$((port_speak+1))" \
		-p "$((port_listen+ 1)):$((port_listen+ 1))" \
		-v "roach_demo:/cockroach/cockroach-data" 0track/cockroachdb:latest \
		demo ycsb --workload="$workload" --duration=1m --global --nodes="$nb_nodes" --insecure
}

launch(){
	# Create Network
	${engine} network create -d bridge roachnet

	# Create the list to join
	for i in $(seq 1 $nb_nodes)
	do
		local node="roach${i}:$((port_speak+101))" 	
		list_of_ip="${list_of_ip},${node}"
	done
	# Remove first char so no additional "," at the begining
	list_of_ip=${list_of_ip:1}

	echo "${list_of_ip}"
	for i in `seq 1 $nb_nodes`
	do
		# Create Volume i
		${engine} volume create "roach${i}"
		# Start roach i
		echo "doing ${i}"
		${engine} run -d --name="roach${i}" --hostname="roach${i}" --net=roachnet \
			--cap-add=NET_ADMIN \
			-p "$((port_speak+i)):$((port_speak+i))"\
		       	-p "$((port_listen+ i)):$((port_listen+ i))"\
		       	-v "roach${i}:/cockroach/cockroach-data" \
			0track/cockroachdb:latest start  \
				--advertise-addr="roach${i}:$((port_speak+ 101))" \
				--http-addr="roach${i}:$((port_listen+ i))"  \
			       	--listen-addr="roach${i}:$((port_speak+ 101))"    \
				--sql-addr="roach${i}:$((port_speak+ i))"   \
				--insecure   --join="${list_of_ip}" --locality=region="roach${i}",zone=1
	done
}
init(){
	# Initialize the cluster
	echo "init"
	${engine} exec -it roach1 ./cockroach init --host="roach1:$((port_speak+1))"  --insecure


	python3 ./emulate_latency.py $nb_nodes
	#split_command="Set CLUSTER setting kv.range_split.by_load_enabled = false;"

	#${engine} exec "roach1" cockroach sql --url "postgresql://root@roach1" --insecure -e "${split_command}"
	
	${engine} exec -it roach1 ./cockroach workload init ycsb --workload="$workload"\
		"postgresql://root@roach1:$((port_speak +1))?sslmode=disable"
#
#	local range_max_bytes=$(config "cockroachdb.range_max_bytes")
#	local zonecfg_command="ALTER TABLE ycsb.public.usertable CONFIGURE ZONE USING num_replicas = ${nb_nodes};"
#	${engine} exec "roach1" cockroach sql --url "postgresql://root@roach1" --insecure -e "${zonecfg_command}"
#
#	local shard_command="ALTER TABLE ycsb.public.usertable CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = ${range_max_bytes};"
#	${engine} exec "roach1" cockroach sql --url "postgresql://root@roach1" --insecure -e "${shard_command}"
#
#	local stmt="ALTER TABLE usertable CONFIGURE ZONE USING constraints = '{+region=roach1: 1}', lease_preferences = '[[\"+region=roach1\"]]';"
#	${engine} exec "roach1" cockroach sql --url "postgresql://root@roach1" --insecure -e "${stmt}"
#	
}	
run(){	
	
	# Run the Workload for 5min
	
	${engine} exec -t roach1 ./cockroach workload run ycsb --workload $workload --concurrency 10 --seed 2004 --drop --insert-count 1000 --duration=5m \
		"postgresql://root@roach1:$((port_speak +1))?sslmode=disable"  > roach1.log &
	sleep 5
	for i in `seq 2 $nb_nodes`
	do
		echo "$i launching"
		${engine} exec -t roach${i} ./cockroach workload run ycsb --workload $workload --concurrency 10 --seed 2004 --insert-count 1000 --duration=5m \
			"postgresql://root@roach${i}:$((port_speak +${i}))?sslmode=disable"  > roach$i.log &
		echo "$i launched"
		sleep 5
	done
}

clean(){
	local list_of_nodes=""
	for i in $(seq 1 $nb_nodes)
	do
		local node="roach${i}" 	
		list_of_nodes="${list_of_nodes} roach${i}"
	done
	echo "stop nodes :"
	${engine} stop ${list_of_nodes}
	echo "delete nodes :"
	${engine} rm  ${list_of_nodes}
	echo "delete volumes:"
	${engine} volume rm  ${list_of_nodes}
	echo "delete network :"
	${engine} network rm roachnet
}
main(){
	parse $@
	# do demo of clean up
	if ${demo}; then
		launch_demo
	elif ${to_launch}; then
		launch
		init
		run
		sleep 300
	fi
	if ${cleanup}; then
	echo "cleaning up"
		clean
	fi
	exit 0
}
main $@
