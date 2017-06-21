use strict;
use warnings;
use feature 'say';

package MARC::Set;
use API;
use Carp;
use List::Util 'max';
use Data::Dumper;
use MARC::Record2;

has 'add_record' => (
	is => 'method',
	code => sub {
		my ($self,$record,$id) = @_;
		
		return if ! defined $record;
		
		if ($id) {
			$record->id($id);
		} elsif (my $rid = $record->id) {
			$id = $rid;
		} else {
			$id = $self->max_id + 1;
			$record->id($id);
		}
		
		$self->{records}->{$id} = $record;
		$self->{order}->{$id} = $self->record_count;
	}
);
has 'max_id' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my $max = max(keys %{$self->{records}});
		$max //= 0;
		return $max;
	}
);
has 'get_record' => (
	is => 'method',
	code => sub {
		my ($self,$id) = @_;
		return $self->{records}->{$id};
	}
);
has 'records' => (
	is => 'method',
	code => sub {
		my $self = shift;
		return wantarray ? @{$self->_sort_records} : $self->_sort_records;
	}
);

sub _sort_records {
	my $self = shift;
	my $records = $self->{records};
	my $order = $self->{order};
	
	my @ret = map {$records->{$_}} sort {$order->{$a} <=> $order->{$b}} keys %$records;
	return \@ret;
}

has 'record_count' => (
	is => 'method',
	code => sub {
		my $self = shift;
		return scalar keys %{$self->{records}};
	}
);
has 'import_file' => (
	is => 'method',
	code => sub {
		my ($self,$type,$path) = @_;
		
		my $method = 'iterate_'.$type;
		$self->$method (
			path => $path,
			limit => 1,
			callback => sub {
				my ($self,$record) = @_;
				$self->add_record($record);
			}
		);
	}
);
has 'to_table' => (
	is => 'method',
	code => sub {
		my $self = shift;
		
		my %tags;
		for my $r (@{$self->records}) {
			for my $f (@{$r->fields}) {
				for my $s (@{$f->subfields}) {
					$tags{$f->tag}{$f->place}{$_} ||= 1 for keys %$s;
				}
			}
		}
		
		my (@table,%header,@header);
		for my $record (@{$self->records}) {
			my @row = $record->id;
			for my $tag (sort keys %tags) {
				for my $place (sort {$a <=> $b} keys %{$tags{$tag}}) {
					for my $sub (sort keys %{$tags{$tag}{$place}}) {
						my $head = qq/$place.$tag\$$sub/;
						push @header, $head unless $header{$head};
						$header{$head} ||= 1;
						my $field = $record->get_field($tag,$place);
						my $val = $field->get_sub($sub) if $field;
						$val //= '';
						push @row, $val;
					}
				}
			}
			push @table, \@row;
		}
		unshift @header, 'id';
		unshift @table, \@header;
	
		return \@table;
	}
);
has 'to_xml' => (
	is => 'method',
	code => sub {
		my ($self,%params) = @_;
		my ($in,$out) = @params{qw/in out/};
		
		#unfinished
	}	
);

sub iterate_db {
	my ($self,%params) = @_;
	my ($dbh,$sql,$type,$callback) = @params{qw/dbh sql type callback limit/};
	
	my $sth = $dbh->prepare($sql);
	$sth->bind_columns(\my ($id,$tag,$inds,$text,$xref,$place));
	$sth->execute;
	my (%index,$ids,$record);
	my $i = 0;
	while ($sth->fetch) {
		$inds =~ s/;.*//;
		if (! $index{$id}) {
			if ($record && $callback) {
				confess 'invalid callback' if ref $callback ne 'CODE';
				$callback->($self,$record);
				$i++;
				return if $params{limit} and $i == $params{limit};
			}
			if ($type) {
				$record = "MARC::Record::$type"->new(id => $id);
			} else {
				$record = MARC::Record->new(id => $id);
			}
			$index{$id} = 1;
			$ids++;
		}
		$_ =~ s/^NULL$// for ($tag,$inds,$text,$xref);
		my $field = MARC::Field->new(tag => $tag,indicators => $inds,text => $text,xref => $xref);
		$record->add_field($field);
	}
	if ($record && $callback) {
		confess 'invalid callback' if ref $callback ne 'CODE';
		$callback->($self,$record);
	}
}

# to do: convert from marc8
sub iterate_marc21 {
	my ($self,%params) = @_;
	my ($path,$callback,$limit) = @params{qw/path callback limit/};
	
	my $fh;
	ref $path eq 'GLOB' ? open($fh,"<",$path) : ($fh = $path); 
	#open my $fh,"<",$path;
	local $/ = "\x{1D}";
	while (my $str = <$fh>) {
		my $record = MARC::Record->new;
		my $leader = substr($str,0,24,'');
		$record->add_field(MARC::Field->new(tag => '000',text => $leader));
		my $base_address = substr($leader,12,5);
		my $dir_len = $base_address - 24;	
		my $dir = substr($str,0,$dir_len,'');
		chop $dir; # chop field terminator
		while ($dir) {
			my ($tag,$len,$start) = map {substr($dir,0,$_,'')} 3,4,5;
			#my $tag = substr($dir,0,3,'');
			#my $len = substr($dir,0,4,'');
			#my $start = substr($dir,0,5,'');
			my $text = substr($str,$start,$len);
			my $inds = substr($text,0,2,'') if substr($tag,0,2) ne '00';
			$inds ||= '';
			my $field = MARC::Field->new(tag => $tag,indicators => $inds,text => $text);
			$record->add_field($field);
		}
		$callback->($self,$record) if $callback;
		#$callback->($record) if $callback;
		
		$limit && $. == $limit && last;
	}
}

sub iterate_mrk {
	my ($self,%params) = @_;
	my ($path,$defaults,$callback) = @params{qw/path defaults callback/};
	
	my $fh;
	ref $path eq 'GLOB' ? open($fh,"<",$path) : ($fh = $path); 
	#say $fh;
	#open my $fh,"<",$path;
	my $record;
	while (<$fh>) {
		if (! $record or $_ =~ /^\s/) {
			$callback->($self,$record) if $record;
			$record = MARC::Record->new;
			$record->defaults($defaults) if $defaults;
		}
		if (my ($tag,$inds,$text) = $_ =~ /^=(\d{3}|LDR)  (..)(.*)$/) {
			$tag = '000' if $tag eq 'LDR';
			$inds =~ s/\\/ /g;
			$text =~ s/[\r\n]//g;
			$text =~ s/\$/\x{1F}/g;
			my $field = MARC::Field->new(tag => $tag,indicators => $inds,text => $text); #,delimiter => 
			$record->add_field($field);
		}
		$callback->($self,$record) if $record->field_count > 0; # and eof;
	}
}

sub iterate_xml {
	require XML::LibXML;
	my ($self,%params) = @_;
	my ($path,$str,$defaults,$callback) = @params{qw/path string defaults callback/};
	
	my $dom;
	if ($path) {
		$dom = XML::LibXML->load_xml(location => $path);
	} elsif ($str) {
		$dom = XML::LibXML->load_xml(string => $str);
	}
	my $root = $dom->getDocumentElement;
	for my $r ($root->getElementsByTagName('record')) {
		
		my $record = MARC::Record->new;
		
		for my $f ($r->getElementsByTagName('leader')) {
			my $field = MARC::Field->new (
				tag => '000',
				text => $f->textContent
			);
			$record->add_field($field);
		}
			
		for my $f ($r->getElementsByTagName('controlfield')) {
			my $field = MARC::Field->new (
				tag => $f->getAttribute('tag'),
				text => $f->textContent
			);
			$record->add_field($field);
		}
		
		for my $f ($r->getElementsByTagName('datafield')) {
			my $field = MARC::Field->new (
				tag => $f->getAttribute('tag'),
				ind1 => $f->getAttribute('ind1'),
				ind2 => $f->getAttribute('ind2'),
			);
			for my $s ($f->getElementsByTagName('subfield')) {
				$field->set_subfield($s->getAttribute('code'),$s->textContent);
			}
			$record->add_field($field);
		}
		
		$callback->($self,$record);
		#$callback->($record) if $callback;
	}
}

sub iterate_excel {
	require Spreadsheet::XLSX;
	my ($self,%params) = @_;
	my ($path,$worksheet,$defaults,$callback) = @params{qw/path worksheet defaults callback/};
	
	my $excel = Spreadsheet::XLSX->new($path);
	for my $sheet (@{$excel->{Worksheet}}) {
		next if $worksheet and $sheet ne $worksheet;
		my @header;
		for my $r (0..$sheet->{MaxRow}) {
			my $record = MARC::Record->new;
			$record->defaults($defaults) if $defaults;
			my (%hash,@vals);
			for my $c (0..$sheet->{MaxCol}) {
				my $cell = $sheet->get_cell($r,$c);
				my $val = $cell->value if $cell;
				next if ! $val;
				#$val ||= '';
				if ($r == 0) {
					my ($place,$tag,$sub) = $val =~ /^(\d+\.)?(\d{3})\$(.)/g;
					warn 'invalid header in column '.$c and next if ! $tag or ! $sub;
					push @header, [$place,$tag,$sub];
					next;
				} else {
					my ($place,$tag,$sub) = @{$header[$c]}[0,1,2];
					$place ||= 1;
					$hash{$tag}{$place}{$sub} = $val;
				}
			}
			$record->import_hash(%hash);
			$callback->($self,$record) if $callback and $r > 0;
		}
	}
}

package end;

1;

__END__
