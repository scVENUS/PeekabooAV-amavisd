#!/bin/sh

# PROVIDE: amavisd_snmp_subagent
# REQUIRE: DAEMON
# KEYWORD: shutdown

. /etc/rc.subr

name="amavisd_snmp_subagent"
desc="Amavis SNMP AgentX"
rcvar="amavisd_snmp_subagent_enable"

command=/usr/local/sbin/amavisd-snmp-subagent-zmq
command_interpreter=/usr/bin/perl

load_rc_config $name

: ${amavisd_snmp_subagent_enable:="NO"}
: ${amavisd_snmp_subagent_pidfile:="/var/run/amavisd-snmp-subagent.pid"}
: ${amavisd_snmp_subagent_flags:="-P ${amavisd_snmp_subagent_pidfile}"}

pidfile=${amavisd_snmp_subagent_pidfile}

run_rc_command "$1"
