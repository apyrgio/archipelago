#! /bin/bash

#####################
# Haelper functions #
#####################

usage() {
	echo "Usage: ./stress_cached [-test <i>] [-ff <i>] [until <i>] [seed <n>]"
	echo "                     [-v <i>] [-y] [-c] [-h]"
	echo ""
	echo "Options: -test <i>:  run only test <i>"
	echo "         -ff <i>:    fast-forward to test <i>, run every test from "
	echo "                     there on"
	echo "         -until <i>: run every test until AND test <i>"
	echo "         -seed <n>:  use <n> as a seed for the test (9-digits only)"
	echo "         -v <l>:     set verbosity level to <l>"
	echo "         -y:         do not wait between tests"
	echo "         -c:         just clean the segment"
	echo "         -h:         rint this message"
	echo ""
	echo "---------------------------------------------------------------------"
	echo "Additional info:"
	echo ""
	echo "* None of the above options needs to be used. By default,"
	echo "  stress_cached will iterate the list of tests and pause between "
	echo "  each one so that the user can take a look at the logs."
	echo ""
	echo "* If the user has not given a seed, stress_cached will pick a random "
	echo "  seed value."
	echo ""
	echo "* The -ff and -until options can be used together to run tests"
	echo "  within a range"
	echo ""
	echo "* An easy way to check out the output would be to start 3 terminals"
	echo "  and simply have them do: tail -F /var/log/cached* (or bench*,"
	echo "  mt-pfiled*). Thus, when a file is rm'ed or a new file with the same"
	echo "  prefix has been added, tail will read it and you won't have to do"
	echo "  anything."
}

parse_args() {
	# ${1} is for threads
	if [[ ${1} = 'single' ]]; then
		T_MTPF=1
		T_CACHED=1
		T_BENCH=1
	elif [[ ${1} = 'multi' ]]; then
		T_MTPF=3
		T_CACHED=4
		T_BENCH=1
	else
		red_echo "${1} is not a valid thread option"
		exit
	fi

	# ${2} is for benchmark size
	if [[ ${2} = '1/2' ]]; then
		BENCH_SIZE=$(( ${CACHE_SIZE}/2 * 4))'M'
	elif [[ ${2} = '1+1/2' ]]; then
		BENCH_SIZE=$(( (${CACHE_SIZE} + (${CACHE_SIZE}/2)) * 4 ))'M'
	elif [[ ${2} = '2+1/2' ]]; then
		BENCH_SIZE=$(( (2*${CACHE_SIZE} + (${CACHE_SIZE}/2)) * 4 ))'M'
	else
		red_echo "${2} is not a valid bench size option"
		exit
	fi

	# ${3} is for cache size and affects cached's number of ops
	if [[ ${3} = 4 ]]; then
		NR_OPS=4
	else
		NR_OPS=16
	fi
}

# Create a random (or user-provided) 9-digit seed
create_seed() {
	# if $1 is not empty then the user has provided a seed value
	if [[ -n $1 ]]; then
		SEED=$(($1 % 1000000000))
		if [[ $1 != $SEED ]]; then
			red_echo "Provided seed was larger than expected:"
			red_echo "\tOnly its first 9 digits will be used."
		fi
		return
	fi
	SEED=$(od -vAn -N4 -tu4 < /dev/urandom)
	SEED=$(($SEED % 1000000000))
}

# This is a bit tricky so an explanation is needed:
#
# Each peer command is a single-quoted string with non-evaluated variables. This
# string will be passed later on to 'eval' in order to start each peer. So,
# granted this input, we want to print the command that will be fed to eval, but
# with evaluated variables, wrapped at 54 characters, tabbed and with a back-
# slash appended to the end of every line.
#
# So, if our input was: 'a very nice cow' our output (wrapped at 4 chars) should
# be:
#
# a \
#     very \
#     nice \
#     cow
#
# And now to the gritty details on how we do that:
# 1) First, we append a special character (#) at the end of the peer command.
# 2) Then, we add at the start a new string ("echo "). This converts the
#    peer command to an echo command in a string.
# 2) The echo command is passed to eval. This way, eval will not run the peer
#    command but will simply evaluate the variables in the string and echo them.
# 3) Then, we pipe the output to fmt, which wraps it at 54 chars and tabs every
#    line but the first one. Note that every new line will be printed separately
# 4) The output is then fed to sed, which appends a back-slash at the end of
#    each line.
# 5) Finally, the output is fed for one last time to sed, which removes the
#    backslash from the last line (the line with the (#) character.
print_test() {
	echo ""
	grn_echo "Summary of Test${I}:"
	echo "WCP=${WCP} THREADS=${THREADS} IODEPTH=${IODEPTH} SEED=${SEED}"
	echo "CACHE_SIZE=${CACHE_SIZE}($((CACHE_SIZE * 4))M) BENCH_SIZE=${BENCH_SIZE}"
	grn_echo "-------------------------------------------------------"
	eval "echo "${BENCH_COMMAND}""#"" \
		| fmt -t -w 54 | sed -e 's/$/ \\/g' | sed -e 's/\# \\$//g'
	echo ""
	eval "echo "${CACHED_COMMAND}""#"" \
		| fmt -t -w 54 | sed -e 's/$/ \\/g' | sed -e 's/\# \\$//g'
	echo ""
	eval "echo "${MT_PFILED_COMMAND}""#"" \
		| fmt -t -w 54 | sed -e 's/$/ \\/g' | sed -e 's/\# \\$//g'
	grn_echo "-------------------------------------------------------"
	echo ""
}

init_log() {
	LOG=/var/log/${1}

	# Truncate previous logs
	cat /dev/null > $LOG

	echo "" >> $LOG
	blu_echo "******************" >> $LOG
	blu_echo " TEST ${I} STARTED" >> $LOG
	blu_echo "******************" >> $LOG
	echo "" >> $LOG
}

# The following two functions manipulate the stdout and stderr.
# They must always be used in pairs.
suppress_output() {
	exec 11>&1
	exec 22>&2
	exec 1>/dev/null 2>/dev/null
}

restore_output() {
	exec 1>&11 11>&-
	exec 2>&22 22>&-
}

nuke_xseg() {
	suppress_output

	# Clear previous tries
	killall -9 bench
	killall -9 cached
	killall -9 mt-pfiled

	# Re-build segment
	xseg posix:apyrgio:16:1024:12 destroy create
	xseg posix:apyrgio: set-next 2 1
	xseg posix:apyrgio: set-next 3 1
	xseg posix:apyrgio: set-next 4 1
	xseg posix:apyrgio: set-next 5 1

	restore_output
}

##########################
# Script initializations #
##########################

# Find script location
ARCH_SCRIPTS=$(dirname "$(readlink /proc/$$/fd/255)")

#Include basic functions
source $ARCH_SCRIPTS/init.sh

#############
# Read args #
#############

VERBOSITY=1
I=0
WAIT=true
BENCH_OP=write

# Remember, in Bash 0 is true and 1 is false
while [[ -n $1 ]]; do
	if [[ $1 = '-ff' ]]; then
		shift
		FF=0
		FLIMIT=$1
	elif [[ $1 = '-test' ]]; then
		shift
		TEST=0
		TLIMIT=$1
	elif [[ $1 = '-until' ]]; then
		shift
		UNTIL=0
		ULIMIT=$1
	elif [[ $1 = '-seed' ]]; then
		shift
		SEED=$1
	elif [[ $1 = '-v' ]]; then
		shift
		VERBOSITY=$1
	elif [[ $1 = '-y' ]]; then
		WAIT=false
	elif [[ $1 = '-c' ]]; then
		CLEAN=0
	elif [[ $1 = '-h' ]]; then
		usage
		exit
	else
		usage
		red_echo "${1}: Unknown option. Aborting..."
		exit
	fi
	shift
done

############################
# Clean all previous tries #
############################

# Delete mt-pfiled files
suppress_output
find /tmp/pithos1/ -name "*" -exec rm -rf {} \;
find /tmp/pithos2/ -name "*" -exec rm -rf {} \;
mkdir /tmp/pithos1/ /tmp/pithos2/
restore_output

# Call nuke_xseg to clear the segment and kill all peer processes
nuke_xseg

if [[ $CLEAN ]]; then exit; fi

##############################
# Create arguments for peers #
##############################

create_seed $SEED

BENCH_COMMAND='bench -g posix:apyrgio: -p 2 -tp 0 -v ${VERBOSITY} --seed ${SEED}
			-op ${BENCH_OP} --pattern rand -ts ${BENCH_SIZE} --progress yes
			--iodepth ${IODEPTH} --verify meta
			-l /var/log/bench${I}.log'

CACHED_COMMAND='cached -g posix:apyrgio: -p 1 -bp 0 -t ${T_CACHED}
			-v ${VERBOSITY} -wcp ${WCP} -n ${NR_OPS} -cs ${CACHE_SIZE}
			-l /var/log/cached${I}.log'

MT_PFILED_COMMAND='mt-pfiled -g posix:apyrgio: -p 0 -t ${T_MTPF} -v ${VERBOSITY}
			--pithos /tmp/pithos1/ --archip /tmp/pithos2/
			-l /var/log/mt-pfiled${I}.log'

#############
# Main loop #
#############

#set -e  #exit on error

for CACHE_SIZE in 4 16 64 512; do
	for WCP in writeback writethrough; do
		for IODEPTH in 1 16; do
			for THREADS in single multi; do
				for BENCH_SIZE in '1/2' '1+1/2' '2+1/2'; do
					# Check if user has asked to fast-forward or run a specific
					# test
					I=$(( $I+1 ))
					if [[ $TEST ]]; then
						if [[ $I -lt $TLIMIT ]]; then continue
						elif [[ $I -gt $TLIMIT ]]; then exit
						fi
					elif [[ $FF ]]; then
						if [[ $I -lt $FLIMIT ]]; then continue
						elif [[ $I -eq $FLIMIT ]]; then FF=1
						fi
					fi

					# Make test-specific initializations
					init_log bench${I}.log
					init_log cached${I}.log
					init_log mt-pfiled${I}.log

					parse_args $THREADS $BENCH_SIZE $CACHE_SIZE
					print_test

					if [[ $WAIT == true ]]; then
						read -rsn 1 -p "Press any key to continue..."
					fi
					echo ""

					# Start mt-pfiled
					eval ${MT_PFILED_COMMAND}" &"
					PID_MTPF=$!

					# Start cached
					eval ${CACHED_COMMAND}" &"
					PID_CACHED=$!
					# Wait a bit to make sure both cached and mt-pfiled is up
					sleep 1

					# Start bench (write mode)
					BENCH_OP=write
					eval ${BENCH_COMMAND}" &"
					PID_BENCH=$!
					echo -n "Waiting for bench to finish writing... "
					wait $PID_BENCH
					grn_echo "DONE!"

					# Start bench (read mode)
					BENCH_OP=read
					eval ${BENCH_COMMAND}" &"
					PID_BENCH=$!
					echo -n "Waiting for bench to finish reading... "
					wait $PID_BENCH
					grn_echo "DONE!"

					# Since cached's termination has not been solved yet, we
					# have to resort to weapons of mass destruction
					nuke_xseg

					if [[ ($UNTIL && $I -eq $ULIMIT) ]]; then exit; fi
				done
			done
		done
	done
done
