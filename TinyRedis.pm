#
# Copyright (c) 2013-2014 Mark Martinec
# All rights reserved.
#
# See LICENSE AND COPYRIGHT section in POD text below for usage
# and distribution rights.
#

package Redis::TinyRedis;

use strict;
use re 'taint';
use warnings;

use Errno qw(EINTR EAGAIN EPIPE ENOTCONN ECONNRESET ECONNABORTED);
use IO::Socket::UNIX;
use Time::HiRes ();

use vars qw($VERSION $io_socket_module_name);
BEGIN {
  $VERSION = '1.001';
  if (eval { require IO::Socket::IP }) {
    $io_socket_module_name = 'IO::Socket::IP';
  } elsif (eval { require IO::Socket::INET6 }) {
    $io_socket_module_name = 'IO::Socket::INET6';
  } elsif (eval { require IO::Socket::INET }) {
    $io_socket_module_name = 'IO::Socket::INET';
  }
}

sub new {
  my($class, %args) = @_;
  my $self = bless { args => {%args} }, $class;
  my $outbuf = ''; $self->{outbuf} = \$outbuf;
  $self->{batch_size} = 0;
  $self->{server} = $args{server} || $args{sock} || '127.0.0.1:6379';
  $self->{on_connect} = $args{on_connect};
  return if !$self->connect;
  $self;
}

sub DESTROY {
  my $self = $_[0];
  local($@, $!, $_);
  undef $self->{sock};
}

sub disconnect {
  my $self = $_[0];
  local($@, $!);
  undef $self->{sock};
}

sub connect {
  my $self = $_[0];

  $self->disconnect;
  my $sock;
  my $server = $self->{server};
  if ($server =~ m{^/}) {
    $sock = IO::Socket::UNIX->new(
              Peer => $server, Type => SOCK_STREAM);
  } elsif ($server =~ /^(?: \[ ([^\]]+) \] | ([^:]+) ) : ([^:]+) \z/xs) {
    $server = defined $1 ? $1 : $2;  my $port = $3;
    $sock = $io_socket_module_name->new(
              PeerAddr => $server, PeerPort => $port, Proto => 'tcp');
  } else {
    die "Invalid 'server:port' specification: $server";
  }
  if ($sock) {
    $self->{sock} = $sock;

    $self->{sock_fd} = $sock->fileno; $self->{fd_mask} = '';
    vec($self->{fd_mask}, $self->{sock_fd}, 1) = 1;

    # an on_connect() callback must not use batched calls!
    $self->{on_connect}->($self)  if $self->{on_connect};
  }
  $sock;
}

# Receive, parse and return $cnt consecutive redis replies as a list.
#
sub _response {
  my($self, $cnt) = @_;

  my $sock = $self->{sock};
  if (!$sock) {
    $self->connect  or die "Connect failed: $!";
    $sock = $self->{sock};
  };

  my @list;

  for (1 .. $cnt) {

    my $result = <$sock>;
    if (!defined $result) {
      $self->disconnect;
      die "Error reading from Redis server: $!";
    }
    chomp $result;
    my $resp_type = substr($result, 0, 1, '');

    if ($resp_type eq '$') {  # bulk reply
      if ($result < 0) {
        push(@list, undef);  # null bulk reply
      } else {
        my $data = ''; my $ofs = 0; my $len = $result + 2;
        while ($len > 0) {
          my $nbytes = read($sock, $data, $len, $ofs);
          if (!$nbytes) {
            $self->disconnect;
            defined $nbytes  or die "Error reading from Redis server: $!";
            die "Redis server closed connection";
          }
          $ofs += $nbytes; $len -= $nbytes;
        }
        chomp $data;
        push(@list, $data);
      }

    } elsif ($resp_type eq ':') {  # integer reply
      push(@list, 0+$result);

    } elsif ($resp_type eq '+') {  # status reply
      push(@list, $result);

    } elsif ($resp_type eq '*') {  # multi-bulk reply
      push(@list, $result < 0 ? undef : $self->_response(0+$result) );

    } elsif ($resp_type eq '-') {  # error reply
      die "$result\n";

    } else {
      die "Unknown Redis reply: $resp_type ($result)";
    }
  }
  \@list;
}

sub _write_buff {
  my($self, $bufref) = @_;

  if (!$self->{sock}) { $self->connect or die "Connect failed: $!" };
  my $nwrite;
  for (my $ofs = 0; $ofs < length($$bufref); $ofs += $nwrite) {
    # to reliably detect a disconnect we need to check for an input event
    # using a select; checking status of syswrite is not sufficient
    my($rout, $wout, $inbuff); my $fd_mask = $self->{fd_mask};
    my $nfound = select($rout=$fd_mask, $wout=$fd_mask, undef, undef);
    defined $nfound && $nfound >= 0 or die "Select failed: $!";
    if (vec($rout, $self->{sock_fd}, 1) &&
        !sysread($self->{sock}, $inbuff, 1024)) {
      # eof, try reconnecting
      $self->connect  or die "Connect failed: $!";
    }
    local $SIG{PIPE} = 'IGNORE';  # don't signal on a write to a widowed pipe
    $nwrite = syswrite($self->{sock}, $$bufref, length($$bufref)-$ofs, $ofs);
    next if defined $nwrite;
    $nwrite = 0;
    if ($! == EINTR || $! == EAGAIN) {  # no big deal, try again
      Time::HiRes::sleep(0.1);  # slow down, just in case
    } else {
      $self->disconnect;
      if ($! == ENOTCONN   || $! == EPIPE ||
          $! == ECONNRESET || $! == ECONNABORTED) {
        $self->connect  or die "Connect failed: $!";
      } else {
        die "Error writing to redis socket: $!";
      }
    }
  }
  1;
}

# Send a redis command with arguments, returning a redis reply.
#
sub call {
  my $self = shift;

  my $buff = '*' . scalar(@_) . "\015\012";
  $buff .= '$' . length($_) . "\015\012" . $_ . "\015\012"  for @_;

  $self->_write_buff(\$buff);
  local($/) = "\015\012";
  my $arr_ref = $self->_response(1);
  $arr_ref && $arr_ref->[0];
}

# Append a redis command with arguments to a batch.
#
sub b_call {
  my $self = shift;

  my $bufref = $self->{outbuf};
  $$bufref .= '*' . scalar(@_) . "\015\012";
  $$bufref .= '$' . length($_) . "\015\012" . $_ . "\015\012"  for @_;
  ++ $self->{batch_size};
}

# Send a batch of commands, returning an arrayref of redis replies,
# each array element corresponding to one command in a batch.
#
sub b_results {
  my $self = $_[0];
  my $batch_size = $self->{batch_size};
  return if !$batch_size;
  my $bufref = $self->{outbuf};
  $self->_write_buff($bufref);
  $$bufref = ''; $self->{batch_size} = 0;
  local($/) = "\015\012";
  $self->_response($batch_size);
}

1;

__END__
=head1 NAME

Redis::TinyRedis - client side of the Redis protocol

=head1 SYNOPSIS

EXAMPLE:

  use Redis::TinyRedis;

  sub on_connect {
    my($r) = @_;
  # $r->call('AUTH', 'xyz');
    $r->call('SELECT', 3);
    $r->call('CLIENT', 'SETNAME', "test[$$]");
    1;
  }

# my $server = '/tmp/redis.sock';
  my $server = '[::1]:6379';

  my $r = Redis::TinyRedis->new(server => $server,
                                on_connect => \&on_connect);
  $r or die "Error connecting to a Redis server: $!";

  $r->call('SET', 'key123', 'val123');  # will die on error
  $r->call('SET', 'key456', 'val456');  # will die on error

  my $v = $r->call('GET', 'key123');
  if (defined $v) { printf("got %s\n", $v) }
  else { printf("key not in a database\n") }

  my @keys = ('key123', 'key456', 'keynone');
  my $values = $r->call('MGET', @keys);
  printf("got %s => %s\n", $_, shift @$values // 'UNDEF') for @keys;

  # batching (pipelining) multiple commands saves on round-trips
  $r->b_call('DEL',     'keyxxx');
  $r->b_call('HINCRBY', 'keyxxx', 'cnt1', 5);
  $r->b_call('HINCRBY', 'keyxxx', 'cnt2', 1);
  $r->b_call('HINCRBY', 'keyxxx', 'cnt2', 2);
  $r->b_call('EXPIRE',  'keyxxx', 120);
  $r->b_results;  # collect response ignoring results, dies on error

  my $counts = $r->call('HMGET', 'keyxxx', 'cnt1', 'cnt2', 'cnt3');
  printf("count %s\n", $_ // 'UNDEF') for @$counts;

  # Lua server side scripting
  my $lua_results = $r->call('EVAL',
    'return redis.call("HGETALL", KEYS[1])', 1, 'keyxxx');
  printf("%s\n", join(', ', @$lua_results));

  # traversing all keys
  for (my $cursor = 0; ; ) {
    my $pair = $r->call('SCAN', $cursor, 'COUNT', 20);
    ($cursor, my $elements) = @$pair;
    printf("key: %s\n", $_) for @$elements;
    last if !$cursor;
  }

  # another batch of commands
  $r->b_call('DEL', $_) for @keys;
  my $results = $r->b_results;  # collect response
  printf("delete status for %s: %d\n", $_, shift @$results) for @keys;

  # monitor activity on a database through Redis keyspace notifications
  $r->call('CONFIG', 'SET', 'notify-keyspace-events', 'KEA');
  $r->call('PSUBSCRIBE', '__key*__:*');
  for (1..20) {
    my $msg = $r->call;  # collect one message at a time
    printf("%s\n", join(", ",@$msg));
  }
  $r->call('UNSUBSCRIBE');
  $r->call('CONFIG', 'SET', 'notify-keyspace-events', '');

  undef $r;  # DESTROY cleanly closes a connection to a redis server

=head1 DESCRIPTION

This is a Perl module Redis::TinyRedis implementing a client side of
the Redis protocol, i.e. a unified request protocol as introduced
in Redis 1.2. Design priorities were speed, simplicity, error checking.

=head1 METHODS

=head2 new

Initializes a Redis::TinyRedis object and established a connection
to a Redis server. Returns a Redis::TinyRedis object if the connection
was successfully established (by calling a connect() method implicitly),
or false otherwise, leaving a failure reason in $! .

=over 4

=item B<server>

Specifies a socket where a Redis server is listening. If a string
starts with a '/' an absolute path to a Unix socket is assumed,
otherwise it is interpreted as an INET or INET6 socket specification
in a syntax as recognized by a C<PeerAddr> option of the underlying
socket module (IO::Socket::IP, or IO::Socket::INET6, or IO::Socket::INET),
e.g. '127.0.0.1:6379' or '[::1]:6379' or 'localhost::6379'.
Port number must be explicitly specified.

A default is '127.0.0.1:6379'.

=item B<on_connect>

Specifies an optional callback subroutine, to be called by a connect()
method after each successful establishment of a connection to a redis server.
Useful as a provider of a Redis client authentication or for database
index selection.

The on_connect() callback is given a Redis::TinyRedis object as its
argument. This object also carries all arguments that were given in
a call to new() when it was created, including any additional options
unrecognized and ignored by Redis::TinyRedis->new().

An on_connect() callback subroutine must not use batched calls
(b_call / b_results), but may use the call() method.

=back

=head2 connect

Establishes a connection to a Redis server. Returns a socket object,
or undef if the connection failed, leaving error status in $! .
The connect() method is called implicitly by new(), or by call() or
b_results() if a connection was dropped due to some previous failure.
It may be called explicitly by an application, possibly combined with
a disconnect() method, to give more control to the application.

=head2 disconnect

Closes a connection to a Redis server if it is established,
does nothing if a connection is not established. The connection
will be re-established by subsequent calls to connect() or call()
or b_results().

Closing a connection is implied by a DESTROY method, so dropping
references to a Redis::TinyRedis object also cleanly disconnects
an established session with a Redis server.

=head2 call

Sends a redis command with arguments, returning a redis reply.
The first argument is expected to be a name of a Redis command,
followed by its arguments according to Redis documentation.

The command will die if a Redis server returns an error reply.
It may also die if it needs to implicitly re-establish a connection
and the connect() call fails.

The returned value is an integer if a Redis server returns an integer
reply, is a status string if a status reply is returned, is undef if a
null bulk reply is returned, is a string in case of a bulk reply, and
is a reference to an array of results in case of a multi-bulk reply.

=head2 b_call

Appends a redis command with arguments to a batch.
The first argument is expected to be a name of a Redis command,
followed by its arguments according to Redis documentation.

=head2 b_results

Sends a batch of commands, then resets the batch. Returns a reference
to an array of redis replies, each array element corresponding to one
command in a batch.

The command will die if a Redis server returns an error reply.
It may also die if it needs to implicitly re-establish a connection
and the connect() call fails.

Returns a reference to an array of results, each one corresponding
to one command of a batch. A data type of each array element is the
same as described in a call() method.

=head1 AUTHOR

Mark Martinec, C<< <Mark.Martinec@ijs.si> >>

=head1 BUGS

Please send bug reports to the author.

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013-2014 Mark Martinec
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are
those of the authors and should not be interpreted as representing official
policies, either expressed or implied, of the Jozef Stefan Institute.

(the above license is the 2-clause BSD license, also known as
 a "Simplified BSD License", and pertains to this program only)

=cut
