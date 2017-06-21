use strict;
use warnings;
use feature 'say';

package MARC::Record;
use API;
use Carp qw/carp cluck confess/;
use Data::Dumper;
use MARC::Field2;

has 'directory', is => 'rw';
has 'field_terminator', is => 'rw', default => "\x{1E}";
has 'record_terminator', is => 'rw', default => "\x{1D}";
has 'type', is => 'rw';

template 'ctr', code => \&_controlfield_element;

has 'leader', is => 'ctr', tag => '000', from => 0, length => 24;
has 'control_number', is => 'ctr', tag => '001', from => 0, length => 'x';
has 'id', is => 'alias', method => 'control_number', param => 0; 
has 'record_length', is => 'ctr', tag => '000', from => 0, length => 5;
has 'base_address_of_data', is => 'ctr', tag => '000', from => 12, length => 5;
		 
has 'import_record' => (
	is => 'method',
	code => sub {
		my ($self,$record) = @_;
		return 'invalid record' if ! ref $record or ref $record !~ /^MARC::Record/;
		undef $self->{fields};
		$self->add_field($_) for @{$record->fields};
		return $self;
	}
);
has 'import_file' => (
	is => 'method',
	code => sub {
		my ($self,$type,$path) = @_;
		confess 'file type not recognized' if ! grep {$_ eq $type} qw/marc21 mrk xml excel/;
		my $set = MARC::Set->new;
		$set->import_file($type,$path);
		my $r = ($set->records)[0];
		$self->add_field($_) for @{$r->fields};
	}
);
has 'import_hash' => ( 
	is => 'method',
	code => sub {
		my ($self,%hash) = @_;
		for my $tag (keys %hash) {
			for my $place (keys %{$hash{$tag}}) {
				my $field = MARC::Field->new(tag => $tag);
				for my $sub (sort keys %{$hash{$tag}{$place}}) {
					my $val = $hash{$tag}{$place}{$sub};
					if (substr($sub,0,3) ne 'ind') {
						if (! ref $val) {
							$field->set_sub($sub,$val);
						} elsif (ref $val eq 'ARRAY') {
							$field->set_sub($sub,$_) for @$val;
						} else {
							confess "invalid value";
						}
					} else {
						$field->$sub($val);
					}
				}
				next if ! $field->text and ! $field->indicators;
				$self->add_field($field);
			}
		}
	}
);
has 'defaults' => (
	is => 'rw',
	param => 0,	
	trigger => sub {
		use Storable 'dclone';
		my ($self,$record) = @_;
		
		confess 'defaults must be a MARC::Record' if ref $record ne 'MARC::Record';
		my $defaults = dclone($record);
		
		$defaults->delete_tag('001');
		my $fields = $defaults->fields;
		if ($fields) {
			confess 'cannot add defaults after fields already exist' if $self->field_count > 0;
			$self->add_field($_) for (@$fields);
			$_->is_overwritable(1) for @{$self->fields};
			return;
		}
	}
);
has 'named_fields' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my @return;
		my @family = split '::', ref $self;
		my $base = shift(@family).'::'.shift(@family);
		my %meths = $base->methods;
		for (keys %meths) {
			push @return, $_ . ': '.$self->$_ for keys %{$meths{$_}};
		}
		for (@family) {
			my $class = $base."::$_";
			my %meths = $class->methods;
			for (keys %meths) {
				push @return, $_ . ': '.$self->$_ for keys %{$meths{$_}};
			}
			$base .= '::'.$_;
		}
		#push @return, '*' x 100;
		return join "\n", @return;
	}
);
has 'add_field' => (
	is => 'method',
	code => sub {
		my ($self,$field,%params) = @_;
		confess 'invalid MARC::Field' if ref $field ne 'MARC::Field';
		my $tag = $field->tag;
		if ($params{overwrite}) {
			$self->{fields}->{$field->tag}->[0] = $field;
			$field->place(1);
		} else {
			my $fields = $self->fields($tag);
			if (grep {$_->is_overwritable} @$fields) {
				for my $e (@$fields) {
					if ($e->is_overwritable) {
						for (qw/ind1 ind2/) {
							my $ind = $field->$_;
							$ind ||= '__';
							$e->$_($ind) if $ind =~ /\d/;
						}
						for my $subfield (@{$field->subfields}) {
							my $sub = (keys %$subfield)[0];
							my $val = $subfield->{$sub};
							#$e->delete_subfield($sub);
							$e->set_subfield($sub,$val,replace => 1);
						}
						$e->_build_text;
						$e->is_overwritable(0);
						last;
					}
				}
			} else {
				push @{$self->{fields}->{$tag}}, $field;
				$field->place(scalar @{$self->{fields}->{$tag}});
			}
		}
		return $self;
	}
);
has 'field_count' => (
	is => 'method',
	code => sub {
		my ($self,$tag) = @_;
		if ($tag) {
			my $fields = $self->{fields}->{$tag};
			return scalar @$fields;
		} 
		my $count = 0;
		for my $tag (keys %{$self->{fields}}) {
			my $fields = $self->{fields}->{$tag};
			$count += scalar @$fields;
		}
		return $count;
	}
);
has 'tag_count' => (
	is => 'alias',
	method => 'field_count'
);
has 'delete_tag' => (
	is => 'method',
	code => sub {
		my ($self,$tag,$i) = @_;
		#undef $self->{fields}->{$tag}->[$i-1] if $i;
		splice @{$self->{fields}->{$tag}},$i-1,1 if $i;
		delete $self->{fields}->{$tag} if ! $i;
	}
);
has 'delete_field' => (
	is => 'method',
	code => sub {
		my ($self,$field) = @_;
		for (@{$self->{fields}->{$field->tag}}) {
			delete $self->{fields}->{$field->tag} if $field == $_;
		}
	}
);
has 'change_tag' => (
		is => 'method',
		code => sub {
		my ($self,$from_tag,$to_tag) = @_;
		my $fields = $self->get_fields($from_tag);
		$_->tag($to_tag) for @$fields;
	}
);
has 'get_field' => (
	is => 'method',
	code => sub {
		my ($self,$tag,$incidence) = @_;
		$incidence-- if $incidence;
		#return unless ref $self->{fields}->{$tag} eq 'ARRAY';
		if ($incidence) {
			if (my $field = $self->{fields}->{$tag}->[$incidence]) {
				return $field;
			} else {
				#carp 'incidence '.($incidence + 1).' of tag '.$tag.' does not exist';
				return;
			}
		}
		if (my $field = $self->{fields}->{$tag}->[0]) {
			return $field;
		} else {
			
		}
	}
);
has 'get_field_sub' => (
	is => 'method',
	code => sub {
		my ($self,$tag,$sub) = @_;
		return 0 unless $self->has_tag($tag);
		my $return = $self->get_field($tag)->get_sub($sub);
		$return //= 0;
		return $return;
	}
);
has 'get_field_subs' => (
	is => 'method',
	code => sub {
		my ($self,$tag,$sub) = @_;
		return 0 unless $self->has_tag($tag);
		my @returns;
		for my $field (@{$self->get_fields($tag)}) {
			push @returns, $field->get_sub($sub);
		}
		#my $return = scalar @returns > 1 ? \@returns : $returns[0];
		return \@returns;
	}
);
has 'field' => (
	is => 'alias', 
	method => 'get_field_sub'
);
has 'get_field_arrayref' => (
	is => 'method',
	code => sub {
		my ($self,@tags) = @_;
		my @fields;
		if (@tags) {
			for my $tag (sort @tags) {
				my $fields = $self->{fields}->{$tag};
				push @fields, $_ for map {$_} @$fields;
			}
			return \@fields;
		} 
		for my $tag (sort keys %{$self->{fields}}) {
			my $fields = $self->{fields}->{$tag};
			push @fields, $_ for map {$_} @$fields;
		}
		return wantarray ? @fields : \@fields;
	}
);
has 'get_fields' => (
	is => 'alias',
	method => 'get_field_arrayref'
);
has 'fields' => (
	is => 'alias',
	method => 'get_field_arrayref'
);
has 'has_field' => (
	is => 'method',
	code => sub {
		my ($self,$tag) = @_;
		#print Dumper $self->get_field($tag);
		return 1 if ref $self->get_field($tag) eq 'MARC::Field';
	}
);
has 'has_tag' => (
	is => 'alias',
	method => 'has_field'
);
has 'list_subfields' => (
	is => 'method',
	code => sub {
		my ($self,$tag) = @_;
		my @return;
		for my $field (@{$self->get_fields($tag)}) {
			return $field->text if ! $field->is_datafield;
			push @return, $field->list_subfield_values;
		}
		return @return;
	}
);
has 'check' => (
	is => 'method',
	code => sub {
		my ($self,$tag,$sub,$match) = @_;
		die 'invalid check' unless ($tag && $sub && $match);
		for (@{$self->get_fields($tag)}) {
			return 1 if $_->check($sub,$match);
		}
		return 0;
	}
);
has 'grep' => (
	is => 'method',
	code => sub {
		my ($self,$tag,$sub,$qr) = @_;
		return grep {$_->check($sub,$qr) == 1} @{$self->get_fields($tag)};
	}
);


sub serialize {
	my ($self,%params) = @_;
	
	my %record;
	for my $field (@{$self->fields}) {
		if ($field->is_leader) {
			$record{leader} = $field->text;
		} elsif ($field->is_controlfield) {
			push @{$record{controlfield}}, {tag => $field->tag, value => $field->text}
		} else {
			my @subfields = map {{code => (keys %$_)[0], value => (values %$_)[0]}} @{$field->subfields};
			push @{$record{datafield}}, {tag => $field->tag, subfield => \@subfields, ind1 => $field->ind1, ind2 => $field->ind2};
		}
	}

	return \%record;
}

sub to_json {
	require JSON;
	my $self = shift;
	return JSON->new->pretty(1)->encode($self->serialize);
}

sub to_yaml {
	require YAML;
	my $self = shift;
	return YAML::Dump($self->serialize);
}

sub to_marc21 {
	my $self = shift;
	
	my ($directory,$data);
	my $next_start = 0;
	for my $field (@{$self->fields}) {
		next if $field->is_leader or ! $field->text;
		my $length = $field->field_length;
		my $inds = $field->indicators;
		$field->is_datafield ? ($inds =~ s/_/ /g) : ($inds = '');
		$directory .= $field->tag.sprintf("%04d", $length).sprintf("%05d", $next_start);
		$data .= $inds.$field->text; #.$self->field_terminator;
		$next_start += $length;
	}
	$data .= $self->record_terminator;
	$self->directory($directory.$self->field_terminator);
	my $leader_dir_len = length($self->directory) + 24; # leader length = 24
	my $total_len = $leader_dir_len + length($data);
	$self->record_length(sprintf("%05d",$total_len));
	$self->base_address_of_data(sprintf("%05d",$leader_dir_len));
	
	return $self->leader.$self->directory.$data;
}

sub to_mrk {
	my $self = shift;
	
	my $str;
	for my $field (@{$self->fields}) {
		$str .= '=';
		my $tag = $field->tag;
		$tag = 'LDR' if $field->is_leader;
		$str .= $tag . '  ';
		my $inds = $field->indicators;
		$inds ||= '  ';
		$inds =~ s/ /\\/g;
		$str .= $inds;
		my $text = $field->text;
		next if ! $text;
		my $delim = $field->delimiter;
		$text =~ s/$delim/\$/g;
		my $term = $field->terminator;
		$text =~ s/$term//g;
		$str .= $text;
	} continue {
		$str .= "\n";
	}
	$str .= "\n";
	
	return $str;
}

sub to_xml {
	require XML::Writer;
	my $self = shift;
	
	return if $self->field_count == 0;
	
	my $str;
	my $writer = XML::Writer->new(OUTPUT => \$str);
	
	$writer->setDataMode(1) and $writer->setDataIndent(4); # if $self->pretty;
	$writer->startTag('record');
	
	for my $field (@{$self->fields}) {
		goto SKIP; # Tind doesn't use the "leader" tag. use controlfield 000 instead for tind export :\
		if ($field->is_leader) {
			$writer->startTag('leader');
			$writer->characters($field->chars);
			$writer->endTag('leader');
		} 
		SKIP:
		if ($field->is_controlfield) {
			$writer->startTag('controlfield','tag' => $field->tag);
			$writer->characters($field->chars);
			$writer->endTag('controlfield');
		} elsif ($field->is_datafield) {
			$writer->startTag('datafield','tag' => $field->tag, 'ind1' => $field->ind1, 'ind2' => $field->ind2);
			my $subs = $field->subfields;
			my @order = $field->subfield_order;
			for (@$subs) {
				my $sub = shift @order;
				my $val = $_->{$sub};
				$writer->startTag('subfield', 'code' => $sub);
				$writer->characters($val);
				$writer->endTag('subfield');
			}
			$writer->endTag('datafield');
		}
	}
		
	$writer->endTag('record');
	$writer->end;
			
	return $str;
}

sub _controlfield_element {
	#my ($args,$params) = @_;
	#my ($self,$val) = @$args;
	
	my ($name,$properties,$args) = @_;
	#say shift @_;
	
	my ($self,$val) = @$args;
	my ($tag,$pos,$len,$code) = @{$properties}{qw/tag from length code/};
	undef $len if $len eq 'x';
	my $att = \$self->{$name};
	if ($val) {
		#$self->_validate_input($name,$val);
		$self->add_field(MARC::Field->new(tag => $tag)) if (! $self->has_tag($tag));
		$self->get_field($tag)->position(start => $pos, value => $val);
		$code->($self,$val) if $code;
		$$att = $val;
		return $self;
	} elsif (my $field = $self->get_field($tag)) {
		#$len ||= 1;
		$$att = $field->position(start => $pos, length => $len);
	} else {
		#warn $name.' not available: '.$tag.' has not been added';
		#$$att = ''; # x $len;
	}
	$code->($self) if $code;
	$$att ||= '';
	$$att =~ s/\x{1E}$//;
	return $$att;
}

package MARC::Record::Auth;
use API;
use parent -norequire, 'MARC::Record';

has 'header' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		for (@{$self->fields}) {
			if ($_->is_header) {
				return $_;
			}
		}
	}
);
