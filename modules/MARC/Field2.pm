
use strict;
use warnings;
use feature 'say';

package MARC::Field;
use Data::Dumper;
use Carp qw/confess carp cluck/;
use API;
use MARC::Subfield2;


has 'is_leader' => (
	is => 'ro',
	default => sub {
		my ($self,$att) = @_;
		$self->tag || confess 'tag must be set to determine if leader';
		return $self->{tag} eq '000' ? 1 : 0;
	}
);
has 'is_controlfield' => (
	is => 'ro',
	default => sub {
		my ($self,$att) = @_;
		$self->tag || confess 'tag must be set to determine if controlfield';
		return substr($self->{tag},0,2) eq '00' ? 1 : 0;
	}
);
has 'is_datafield' => (
	is => 'ro',
	default => sub {
		my ($self,$att) = @_;
		$self->tag || confess 'tag must be set to determine if datafield';
		return 1 if $self->tag =~ /^[A-Z]{3}$/;
		return substr($self->{tag},0,2) eq '00' ? 0 : 1;
	}
);
has 'is_header' => (
	is => 'ro',
	default => sub {
		my ($self,$att) = @_;
		$self->tag || confess 'tag must be set to determine if header field';
		return substr($self->{tag},0,1) eq '1' ? 1 : 0;
	}
);
has 'is_overwritable' => (
	is => 'rw',
);
has 'place' => (
	is => 'rw'
);
has 'is_authority_controlled' => (
	is => 'rw',
);
has 'authority_controlled_subfields' => (
	is => 'rw',
	default => []
);
has 'delimiter' => (
	is => 'rw',
	param => 0,
	default => "\x{1F}"
);
has 'delim' => (
	is => 'alias',
	method => 'delimiter',
	param => 0
);
has 'terminator' => (
	is => 'rw',
	default => "\x{1E}"
);
has 'tag' => (
	is => 'rw',
	param => 0
);
has 'indicators' => (
	is => 'rw',
	param => 0,
	default => sub {
		my ($self,$att) = @_;
		return '' unless $self->is_datafield;
		
		my $return = '  ';
		substr($return,0,1) = $self->ind1 if $self->ind1;
		substr($return,1,1) = $self->ind2 if $self->ind2;
		
		return $return;
	},
	trigger => sub {
		my ($self,$val) = @_;
		return unless $self->is_datafield;
		my $att = \$self->{indicators};
		
		#$val =~ s/ /_/g;  
		$val .= ' ' while length $val < 2;
		($self->{ind1},$self->{ind2}) = map {substr($val,$_,1)} (0,1);
		$$att = $val;
	
		return $$att;
	}
);
has 'inds' => (
	is => 'alias',
	param => 0,
	method => 'indicators'
);
has 'ind1' => (
	is => 'rw',
	param => 0,
	trigger => sub {
		my ($self,$val) = @_;
		my $att = \$self->{ind1};
		return $self->_ind_x($att,1,$val);
	}
);
has 'ind2' => (
	is => 'rw',
	param => 0,
	trigger => sub {
		my ($self,$val) = @_;
		my $att = \$self->{ind2};
		return $self->_ind_x($att,2,$val);
	}
);
has 'auth_indicators' => (
	is => 'rw',
	param => 0
);
has 'text' => (
	is => 'rw',
	param => 0,
	default => sub {
		my $self = shift;
		$self->_build_text;
		return $self->{text};
	},
	trigger => sub {
		my ($self,$val) = @_;
		my $att = \$self->{text};
		if ($val) {
			$$att = $val;
			$self->_parse_text;
		}
		return if ! $$att;
		$$att .= $self->terminator if $self->is_datafield && substr($$att,-1) ne $self->terminator; # ?
		return $$att;
	}
);
has 'chars' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my $text = $self->text;
		return if ! $text;
		my $term = $self->terminator;
		$text =~ s/$term$//;
		return $text;
	}	
);
has 'xref' => (
	is => 'rw',
	param => 0,
	trigger => sub {
		my ($self,$val) = @_;
		$self->set_subfield('0',$val,replace => 1) if $val;
		return $val;
	}
);
has 'position' => (
	is => 'method',
	code => sub {
		my $self = shift;
		return substr $self->text,$_[0],1 if scalar(@_) == 1;
		my %params = @_;
		my ($val,$pos,$len) = @params{qw/value start length/};
		confess 'method "position" only available for leader and controlfields' unless $self->is_leader or $self->is_controlfield;
		my $text = \$self->{text};
		$$text ||= '';
		if ($val) {
			confess 'named argument "start" required for method "position"' unless defined $pos;
			$len ||= length $val;
			$$text .= '|' while length $$text < ($pos + $len);
			substr($$text,$pos,$len) = $val;
		}
		if ($len) {
			$$text .= '|' while length($$text) < ($pos + $len);
			return substr $self->text,$pos,$len;
		} elsif (my $text = $self->text) {
			return substr $text,$pos;
		} else {
			return;
		}
	}
);
has 'field_length' => (
	is => 'method',
	code => sub {
		my ($self,$return) = @_;
		if ($self->is_datafield) {
			$return += length($self->$_) for (qw/text ind1 ind2/);
		} else {
			$return = length($self->text);
		}
		$return;
	}
);
has 'change_tag' => (
	is => 'method',
	code => sub {
		my ($self,$to_tag) = @_;
		$self->tag($to_tag);
		undef $self->{'is_'.$_} for qw/leader controlfield datafield header/;
		return $self;
	}
);
has 'check' => (
	is => 'method',
	code => sub {
		my ($self,$sub,$match) = @_;
		my $type = ref $match;
		my $subs;
		#$subs = [$self->list_subfield_values($sub)] if $sub ne '*';
		#$subs || ($subs = [$self->list_subfield_values($sub)]);
		$sub = '' if $sub eq '*';
		$subs = [$self->list_subfield_values($sub)];
		for my $sub (@$subs) {
			if ($type eq 'Regexp') {
				return 1 if $sub =~ /$match/;
			} elsif (index '*', $type > -1) {
				$match =~ s/\*{2,}/\*/g;
				my @parts = map {"\Q$_\E"} split /\*/, $match;
				my $rx = join '.*?', @parts;
				substr($match,0,1) ne '*' && ($rx = '^'.$rx);
				substr($match,-1) ne '*' && ($rx .= '$');
				my $check = qr/$rx/;
				return 1 if $sub =~ /$check/;
			} else {
				return 1 if $sub eq $match;
			}
		}
		return 0;
	}
);


has 'subfield_order' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my @order;
		for (@{$self->{subfields}}) {
			push @order, (keys %$_)[0];
		}
		return @order;
	}
);
has 'set_subfield' => (
	is => 'method',
	code => sub {
		my ($self,$sub,$val,%params) = @_;
	
		if ($params{replace} and $self->get_sub($sub)) {
			
			$self->delete_subfield($sub);
			$self->set_sub($sub,$val);
			
			#for (@{$self->{subfields}}) {
			#	my $key = (keys %$_)[0];
			#	$_->{$sub} = $val and last if (keys %$_)[0] eq $sub;
			#}
		} else {
			push @{$self->{subfields}}, {$sub => $val};
		}	
		return $self;
	}
);
has 'set_sub' => (
	is => 'alias',
	method => 'set_subfield'
);
has 'replace_sub' => (
	is => 'method',
	code => sub {
		my ($self,$sub,$val) = @_;
		#say $val;
		my @subfields = grep {(keys %$_)[0] eq $sub} @{$self->{subfields}};
		my $subfield = shift @subfields;
		$subfield->{$sub} = $val;
		
		return $self;
	}
);
has 'subfield_count' => (
	is => 'method',
	code => sub {
		my ($self,$sub) = @_;
		my $subs = $self->get_subs($sub);
		return scalar @$subs;
	}
);
has 'subfield' => (
	is => 'method',
	code => sub {
		my ($self,$sub,$val) = @_; 
		$self->set_subfield($sub,$val) if $val;
		return $self->get_subfield($sub);
	}
);
has 'sub' => (
	is => 'alias',
	method => 'subfield'
);
has 'get_subfield' => (
	is => 'method',
	code => sub {
		my ($self,$sub,$incidence) = @_;
		my $subs = $self->get_subs($sub) || return '';
		return '' if scalar @$subs == 0;
		if ($incidence) {
			(return wantarray ? @$subs : $subs) if $incidence eq '*';
			return $subs->[$incidence+1] if $incidence >= 0;
		}
		return $subs->[0];
	}
);
has 'get_sub' => (
	is => 'alias',
	method => 'get_subfield'
);
has 'get_subfield_arrayref' => (
	is => 'method',
	code => sub {
		my ($self,$sub) = @_;
		return [$self->list_subfield_values] if ! $sub;
		my $subs = $self->{subfields};
		my @vals;
		for (@$subs) {
			push @vals, $_->{$sub} if $_->{$sub};
		}
		return \@vals;
	}
);
has 'get_subs' => (
	is => 'alias',
	method => 'get_subfield_arrayref'
);
has 'subfields' => (
	is => 'rw',
	default => [],
	# arraref of pairs
);
has 'list_subfield_values' => (
	is => 'method',
	code => sub {
		my ($self,@subs) = @_;
		@subs = $self->subfield_order if ! @subs || ! $subs[0] || ! $subs[0] eq '*';
		my @return;
		for my $pair (@{$self->subfields}) {
			push @return, (values %$pair)[0];
		}
		return @return;
	}
);
has 'delete_subfield' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my @deletes = @_;
		@deletes = $self->subfield_order if ! @deletes;
		my $subs = $self->{subfields};
		for my $sub (@deletes) {	
			for (0..$#$subs) {
				my $pair = $subs->[$_];
				if ($pair->{$sub}) {
					splice @$subs,$_,1;
					#@{$self->{subfield_order}} = grep {$_ ne $sub} @{$self->{subfield_order}}
				}
			}
		}
		$self->_build_text;
		
		return $self;
	}
);
	
sub _ind_x {
	my ($self,$ref,$ind,$val) = @_;
	
	my $pos = $ind - 1;
	my $att = $ref;
	my $inds = \$self->{indicators};
	if (defined $val) {
		#$self->_validate_input('ind'.$ind,$val);
		$$inds //= '';
		$$inds .= ' ' while length $$inds < 2;
		substr($$inds,$pos,1) = $val;
		$$att = $val; # if defined $val;
	}
	return $$att if $$att;
	
	if ($$inds) {
		$$att = substr $$inds,$pos,1;
	} else {
		$$att = ' ';
	}	
	return $$att;
}

# handle concatenated auth text that causes subfields to be out of order:
# replace empty subfields with next found instance of subfield
sub _normalize_subfield_order {
	my $self = shift;
	my ($d,$t) = ($self->delimiter,$self->terminator);
	my @blanks = ($self->{text} =~ m/($d.)(?=$d)/g);
	for my $sub (@blanks) {
		$self->{text} =~ s/$sub(.*?)($sub[^$d$t]+)/$2$1/;	
	}
}

sub _parse_text {
	my $self = shift;
	
	confess 'must set tag before parsing text' unless defined $self->tag;
	$self->{text} =~ s/ /\\/g if $self->is_leader or $self->is_controlfield;
	return unless $self->is_datafield;
	
	my ($d,$t) = ($self->delimiter,$self->terminator);
	chop $self->{text} if substr($self->{text},-1) eq $t;
	$self->_normalize_subfield_order;
	
	return unless $self->{text};
	confess 'no delim' unless index($self->{text},$d) > -1;
	
	undef $self->{subfields};
	my (@subfields,$newtext);
	
	for (split /[\Q$d$t\E]/, $self->text) {
		my $sub = substr $_,0,1,'';
		next if ! $_;
		$self->set_subfield($sub,$_);
		$newtext .= $self->delimiter.$sub.$_;
	}
	$newtext //= '';
	$self->{text} = $newtext.$self->terminator;
}

sub _build_text {
	my $self = shift;
	
	return if ! $self->is_datafield;
	return if ! $self->{subfields};
	
	my $att = \$self->{text};
	
	undef $$att;
	my $subs = $self->{subfields};
	for (@$subs) {
		my @sub = keys %$_;
		my @val = values %$_; 
		$$att .= $self->delimiter.$sub[0].$val[0] if $val[0];
	}
	$$att .= $self->terminator if $$att;
}

package end;

1

__END__