use strict;
use warnings;
use feature 'say';

package Vote;
use API;
use Data::Dumper;

ATTRIBUTES: {
	has qw/votes is rw/, param => 0;
	has 'results' => (is => 'ro');
	has 'resolution' => (is => 'rw');
	has 'body', is => 'rw',
		trigger => sub {
			my ($self,$val) = @_;
			chop $val if substr($val,-1) eq '/';
			return $val;	
		};
	has	'session' => (is => 'rw');
	has	'id' => (is => 'rw',param => 0);
	has 'title' => (is => 'rw');
	has 'meeting' => (is => 'rw');
	has 'draft' => (is => 'rw');
	has 'report' => (is => 'rw');
	has	'date' => (is => 'rw',
		trigger => sub {
			my ($self,$val) = @_;
			$val =~ s/^(\d{4})(\d{2})(\d{2})$/$1-$2-$3/;
			return $val;
		}
	);
} 
METHODS: {
	has 'add_country_vote' => (
		is => 'method',
		code => sub {
			my ($self,$country,$vote) = @_;
			$vote ||= 'NV';
			push @{$self->{votes}}, {country => $country,vote => $vote};
		}
	);
	has 'add_result' => (
		is => 'method',
		code => sub {
			my ($self,$type,$count) = @_;
			$count ||= undef;
			$self->{results}->{$type} = $count;
			return;
		}
	);
}

package Votes;
use API;
use JSON;
use Data::Dumper;

has 'votes' => (
	is => 'rw',
	trigger => sub {
		my ($self,$val) = @_;
		$val && push @{$self->{votes}}, $val;
		return $self->{votes};
	}
);
has 'add_vote' => (
	is => 'method',
	code => sub {
		my ($self,$vote) = @_;
		push @{$self->{votes}}, $vote;
	},
);
has 'json' => (
	is => 'method',
	code => \&_json
);

sub _json {
	my $self = shift;
	my %params = @_;
	my @objs;
	for my $vote (@{$self->votes}) {
		my $obj;
		for (qw/id resolution body session title date meeting draft report votes results vetoed vetoed_by/) {
			next unless $vote->can($_);
			$obj->{$_} = $vote->$_;
		}
		push @objs, $obj; 
	}
	my $j = JSON->new;
	$j->pretty(1) if $params{pretty};
	return $j->encode(\@objs);
}

1;