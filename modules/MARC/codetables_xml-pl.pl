#!/usr/bin/perl

use strict;
use warnings;
use feature 'say';
use lib 'c:\drive\modules';
use lib '/Users/JB/Drive/modules';

package Class;

sub new {
	my $class = shift;
	my %params = @_; # || die "named arguments required";
	return bless {params => \%params}, $class;
}

package MARC::Map;
use base 'Class';
use Data::Dumper;

sub map {
	my $self = shift;
	
	return $self->{map} if $self->{map};
	
	use XML::LibXML;
	my %map;
	open my $fh,'<',$self->{params}->{file};
	my $dom = XML::LibXML->load_xml(IO => $fh);
	my $root = $dom->getDocumentElement;
	
	for my $set ($root->getElementsByTagName('characterSet')) {
		my $name = $set->getAttribute('name');
		my $iso = $set->getAttribute('ISOcode');
		$map{$iso}{name} = $name;
		for my $code ($set->getElementsByTagName('code')) {
			my %codes;
			for (qw/isCombining marc ucs utf-8 name alt altutf-8/) {
				my $field = $_; 
				$field = 'is_combining' if $_ eq 'isCombining';
				my $elements = $code->getElementsByTagName($_);
				$codes{$field} = $elements->[0]->textContent if $elements;
			}
			$map{$iso}{marc}{$codes{marc}} = \%codes;
		}
	}
	$self->{map} = \%map;
}

sub ucs {
	my ($self,$marc,$set) = @_;
	
	my $map = $self->map;
	return $map->{$set}->{$marc}->{ucs};
}

package main;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use Getopt::Std;

MAIN( options() );

sub options {
	my $opts = 
		'h'.
		'x:'. # xml file
		'p:' # perl data dump file
		;
	getopts ($opts, \my %opts);
	print help(\%opts) and exit if $opts{h};
	return \%opts;
}

sub help {
	my $opts = shift;
	
	return <<"	EOF";
	help text
	EOF
}

sub MAIN {
	my $opts = shift;
	
	my $map = MARC::Map->new(file => $opts->{x});
	
	if (my $out = $opts->{p}) {
		open my $fh,'>',$out;
		print $fh Dumper $map->map;
	} else {
		print Dumper $map->map;
	}
}

__END__

