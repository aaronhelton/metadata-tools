
# wrapper for Sybase isql CLI

use strict;
use warnings;
use feature 'say';
use sigtrap;

package Get::Hzn;
use API;
use Carp;
use File::Slurp;
use Data::Dumper;
use MARC::Decoder;
use MARC::Set2;

has 'database' => (
	is => 'rw',
	param => 0,
	default => 'horizon'
);
has 'sql' => (
	is => 'rw',
	param => 0,
	trigger => sub {
		my ($self,$val) = @_;
		mkdir 'sql' if ! -e 'sql';
		my $path = 'sql/'.time.'.sql';
		$val .= "\ngo";
		write_file($path,$val);
		$self->{script} = $path;
		my $db = $self->database;
		$self->{cmd} = qq/isql -S Horizon -U dhlstaff -P dhlstaff -s "\x{9}" -h 0 -w 500000 -D $db -i $path -J cp850/;
		return $val;
	}
);
has 'script' => (
	is => 'rw',
	param => 0,
	trigger => sub {
		my ($self,$val) = @_;
		confess qq/script not found at $val/ if ! -e $val;
		my $sql = read_file($val);
		$self->sql($sql);
		return $val;
	}
);
has 'encoding' => (
	is => 'rw',
	param => 0,
	default => 'cp850',
	trigger => sub {
		my ($self,$val,$att) = @_;
		if ($val) {
			confess 'encoding can only be "marc8" or "utf8"' unless $val =~ /^(marc8|utf8)/;
			return $val;
		}
	}
);
has 'delimiter', is => 'rw', default => "\t";
has 'delim', is => 'alias', method => 'delimiter'; 
has 'header', is => 'ro';
has 'results', is => 'ro';
has 'cmd' => (
	is => 'ro',
	trigger => sub {
		my $self = shift;
		confess q/sql execution method not available without sql statement or script/ unless $self->sql; 
	}
);

has 'execute' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my %params = @_;
		my ($iterate,$callback) = @params{qw/iterate callback/};
		my $pid = open my $raw, '-|', $self->cmd;
		local $SIG{INT} = sub {kill 1, $pid; die "SIGINT"}; # unlink
		my $decoder;
		if ($self->encoding eq 'utf8') {
			$decoder = MARC::Decoder->new if $self->encoding eq 'utf8';
			binmode STDOUT, ':utf8';
		}		
		if ($params{iterate}) {
			require MARC::Record2;
		}
		my (%index,$ids,$record);
		while (<$raw>) {
			! $_ && next;
			$_ !~ /\t/ && next;
			FORMAT: {
				$_ = $decoder->decode($_) if $_ =~ /[\x{80}-\x{FF}]|\x{1B}/ and $self->encoding eq 'utf8';
				#my $i = index "\x{10}", $str;
				$_ =~ s/[\r\n]//g;
				$_ =~ s/[\x10-\x1A]//g;
				$_ =~ s/^\t *//;
				$_ =~ s/\t^//;
				$_ =~ s/ +\t/\t/g;
				$_ =~ s/\t +/\t/g;
				$_ =~ s/NULL//g;
			}
			my @row = split "\t", $_;
			$self->header(@row) if $. == 0;
			next unless $. > 2;
			
			my ($id,$tag,$inds,$auth_inds,$text,$xref,$place) = @row[0..6];
			#$inds =~ s/;.*// if $inds;
			my $type;
			if ($iterate) {
				next if ($id and $id !~ /^\d+$/) or (! $id);
				if (! $index{$id}) {
					if ($record && $callback) {
						confess 'invalid callback' if ref $callback ne 'CODE';
						$callback->($record);
					}
					if ($type) {
						$record = "MARC::Record::$type"->new(id => $id);
					} else {
						$record = MARC::Record->new->id($id);
					}
					$index{$id} = 1;
					$ids++;
				}
				$_ =~ s/^NULL$// for ($tag,$inds,$text,$xref);
				my $field = MARC::Field->new(tag => $tag,indicators => $inds,auth_indicators => $auth_inds,text => $text,xref => $xref);
				$record->add_field($field);
			} else {
				$callback ? $callback->(\@row) : say join $self->delim, @row;
			}
		}
		if ($record && $callback) {
			confess 'invalid callback' if ref $callback ne 'CODE';
			$callback->($record);
		}
		
		unlink $self->script unless $params{debug};
		return $self;
	}
);

sub get_sub {
	my (undef,$text,$sub) = @_;
	my $d = "\x1F";
	my $t = "\x1E";
	$text || return;
	my @vals = ($text =~ m/$d$sub([^$d$t]+)/g);
	return @vals > 1 ? \@vals : $vals[0];
}

package Get::Hzn::Dump;
use API;
use Carp;
use parent -norequire, 'Get::Hzn';
use Data::Dumper;

has 'criteria' => (
	is => 'rw',
	param => 0,
	trigger => sub {
		my ($self,$val,$att) = @_;
		if ($val) {
			# confess qq/first column in select statement must be "bib#" or "auth#"/ if $val !~ /select (bib#|auth#)/i;
			return $val;
		}
	}
);

has 'iterate' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my %params = @_;
		my ($criteria,$callback,$enc) = @params{qw/criteria callback encoding/};
		my $type = $self->type;
		my $get = "Get::Hzn::Dump::$type"->new;
		$get->encoding($enc) if $enc;
		$get->criteria($criteria)->execute (
			iterate => 1, 
			callback => $callback,
		); 
	}
);

package Get::Hzn::Dump::Bib;
use API;
use parent -norequire, 'Get::Hzn', 'Get::Hzn::Dump';

has 'type' => (is => 'ro', default => 'Bib');

has 'sql' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		my $criteria = $self->criteria;
		my $sql = qq {
		select 
			b.bib#,  
			b.tag,
			b.indicators,
			a.indicators as auth_inds,
			str_replace (
				str_replace( 
					b.text+a.text+convert(varchar(8000),bl.longtext)+convert(varchar(8000),al.longtext), 
					char(10), 
					"" 
				),
				char(13),
				""
			) as text,
			b.cat_link_xref#,
			b.tagord
		from 
			bib b, 
			auth a, 
			bib_longtext bl, 
			auth_longtext al
		where 
			b.bib# in ( $criteria )  
			and b.cat_link_xref# *= a.auth#
			and a.tag like "1%"
			and b.bib# *= bl.bib#
			and b.tag *= bl.tag
			and b.tagord *= bl.tagord
			and a.auth# *= al.auth#
			and a.tagord *= al.tagord
			order by b.bib#, b.tag, b.tagord
		};
		$self->SUPER::sql($sql);
	}
);

package Get::Hzn::Dump::Auth;
use API;
use parent -norequire, 'Get::Hzn', 'Get::Hzn::Dump';

has 'type' => (is => 'ro', default => 'Auth');

has 'sql' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		my $criteria = $self->criteria;
		my $sql = qq {
		select 
			a.auth#,  
			a.tag,
			a.indicators,
			aa.indicators as auth_inds,
			str_replace (
				str_replace(
					a.text+aa.text+convert(varchar(8000),al.longtext)+convert(varchar(8000),al2.longtext), 	
					char(10), 
					"" 
				),
				char(13),
				""
			) as text,
			a.cat_link_xref#,
			a.tagord
		from 
			auth a, 
			auth aa,
			auth_longtext al,
			auth_longtext al2
		where 
			a.auth# in ( $criteria )
			and a.cat_link_xref# *= aa.auth#
			and aa.tag like "1%"
			and a.auth# *= al.auth#
			and a.tag *= al.tag
			and a.tagord *= al.tagord
			and aa.auth# *= al2.auth#
			and aa.tagord *= al2.tagord
			order by a.auth#, a.tag, a.tagord
		};
		$self->SUPER::sql($sql);
	}
);



package Hzn::end;

'xaipe';

__END__