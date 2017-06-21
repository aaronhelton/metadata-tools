# DC.pm
use feature 'say';

package DC::Field;
use Moose;
use Data::Dumper;

has 'schema' => (is => 'rw', required => 1);
has 'element' => (is => 'rw', required => 1);
has 'qualifier' => (is => 'rw');
has 'language' => (is => 'rw');
has 'value' => (is => 'rw');
has 'label' => (is => 'rw',	builder => '_build_label', lazy => 1);
has 'combine' => (is => 'rw');
has 'combo_order' => (is => 'rw');
	

sub _build_label {
	my $self = shift;
	
	my $label = $self->schema.'.'.$self->element;
	$label .= '.'.$self->qualifier if $self->qualifier;
	$label .= '['.$self->language.']' if $self->language;
	
	return $label;
}

no Moose;
DC::Field->meta->make_immutable();

package DC::Record;
use Moose;
use Data::Dumper;

has 'fields' => (is => 'rw', isa => 'ArrayRef[DC::Field]',
	#traits => ['Array']
);
has 'bib' => (is => 'rw');
has 'item_id' => (is => 'rw');
has 'collection' => (is => 'rw');
has 'xml' => (is => 'rw', isa => 'ArrayRef', builder => '_xml', lazy => 1);
#has 'csv_line' => (is => 'ro', isa => 'ArrayRef', builder => '_csv_line', lazy => 1);

sub BUILD {
	my $self = shift;
	$self->fields([]);
}

sub add_field {
	my ($self, $field) = @_;
	
	# filter out dupes
	my %cmp;
	for( @{ $self->fields } ) {
		my $label = $_->label;
		push @{ $cmp{$label} }, $_->value;
	}
	my $label = $field->label;
	if (grep /^\Q$label\E$/, keys %cmp) {
		for ( @{ $cmp{$label} } ) {
			return if $_ eq $field->value;
		}
	}
		
	push @{ $self->fields }, $field;
}

sub get_fields {
	my $self = shift;
	return \@{$self->fields};
}

sub get {
	# returns first instance of field 
	my ($self, $field) = @_;
	
	for ( @{ $self->fields } ) {
		return $_->value if $_->label eq $field;
	}
	
	return;
}

sub remove {
	# removes all instances of field
	my ($self, $remfield) = @_;
	
	my $status;
	for( @{ $self->fields } ) {
		undef $_ and $status = 1 if $_->label eq $remfield;
	}
	my @defined = grep defined, @{ $self->fields };
	$self->fields(\@defined);
	
	return $status;
}

sub combine {
	my $self = shift;
	
	my %combos;
	for my $field ( @{ $self->{fields} } ) {	
		next if ! $field->{combine};
		my $head = $field->{label};
		
		$combos{$head}{char} = $field->{combine};
		if ( $field->{combo_order} ) {
			unshift @{ $combos{$head}{parts} }, $field->{value};
		} else { 
			push @{ $combos{$head}{parts} }, $field->{value};
		}
		undef $field;
	}
	return if ! %combos;
	@{ $self->{fields} } = grep defined, @{ $self->{fields} };
	
	for my $head ( keys %combos ) {
		my $combo = $combos{$head}{char};
		
		my @combos = @{ $combos{$head}{parts} };
		my $text = join $combo, @combos;
		$text .= '.' if $text;
		my @parts = split /\./, $head;
		$parts[-1] =~ s/\[(..)\]$//;
		my $language = $1 if $1;
		my ($schema, $element, $qualifier) = @parts;
		my $new_field = DC::Field->new(
			schema => $schema,
			element => $element
		);
		$new_field->qualifier($qualifier) if $qualifier;
		$new_field->language($language) if $language;
		$new_field->value($text);
		$self->add_field($new_field);
	}
}

sub _xml {
	my $self = shift;
	
	my $dc_xml = DC::XML->new('dc');
	my $undr_xml = DC::XML->new('undr');
	
	for ( @{ $self->fields } ) {
		$dc_xml->add_line($_->label, $_->value) if $_->schema eq 'dc';
		$undr_xml->add_line($_->label, $_->value) if $_->schema eq 'undr';
	}
	
	my $dc = join "\n", @{$dc_xml->xml};
	my $undr = join "\n", @{$undr_xml->xml};
	
	my @xml = ($dc, $undr);
	
	return \@xml;
}

sub print_xml {
	my ($self, $dir, $id) = @_;
	
	my $dc_xml = DC::XML->new('dc');
	my $undr_xml = DC::XML->new('undr');
	
	for ( @{ $self->fields } ) {
		$dc_xml->add_line($_->label, $_->value) if $_->schema eq 'dc';
		$undr_xml->add_line($_->label, $_->value) if $_->schema eq 'undr';
	}
	
	$dc_xml->print_xml($dir);
	$undr_xml->print_xml($dir);
}

no Moose;
DC::Record->meta->make_immutable();

package DC::Set;
use Moose;
use Data::Dumper;
#use Carp;

has 'records' => (
	is => 'rw', isa => 'ArrayRef[DC::Record]', 
	traits => ['Array'],
	handles => {
		push_record => 'push'
	},
);
has 'count' => ( 
	is => 'rw', isa => 'Num', default => 0, 
	traits => ['Counter'],
	handles => {
		increment => 'inc'
	},
);
has 'csv' => (is => 'ro', isa => 'Str', builder => '_csv', lazy => 1);
has 'csv_delimiter' => (is => 'ro', default => "\t");
has 'csv_max_rows' => (is => 'ro', isa => 'Int');

sub add_record {
	my ($self, $record) = @_;
	
	$record || die "no record";
	$self->push_record($record);
	$self->increment;
}

sub get_records {
	my $self = shift;
	return \@{$self->records};
}

sub relate {
	my $self = shift;
	
	my %rel = (
		Add => 'addendum',
		Corr => 'corrigendum',
		Rev => 'revision',
		Resumption => 'resumption',
	);
	
	my @relations;
	for my $rec ( @{ $self->records } ) {
		my $sym = $rec->get('undr.identifier.symbol') or next;;
		if ( $sym =~ m!/RES/! ) {
			my $res = $sym;
			my $rel = DC::Relation->new('resolution',$res);
			my $draft = $rec->get('undr.relation.draft');
			$rel->add('draft',$draft);
			my $meeting_num = $rec->get('undr.relation.meeting') or goto SKIPPED;
			$rec->remove('undr.relation.meeting');
			my $meeting = 'S/PV.'.$meeting_num if $res =~ m/^S/;
			$meeting = 'A/'.$1.'/PV.'.$meeting_num if $res =~ m!A/RES/(\d+)/!;
			#say $sym if ! $meeting_num;
			$rel->add('meeting',$meeting);
			SKIPPED:
			push @relations, $rel;
		}
		if ( $sym =~ m!/PV\.! ) {
			my $rel = DC::Relation->new('meeting',$sym);
			my $agenda = $rec->get('undr.relation.agenda');
			$rel->add('agenda',$agenda);
			my $prst = $rec->get('undr.relation.statement');
			$rel->add('statement',$prst) if $prst;
			push @relations, $rel;
		}
		
		for my $type (keys %rel) {
			next if $sym !~ m/$type[^A-z]*$/;
			my $rel = DC::Relation->new($rel{$type},$sym);
			my $original = $1 if $sym =~ m!(.*?)\s{0,1}[\/|\(]{0,1}$type!;
			$rel->add('original',$original);
			push @relations, $rel;
		}
	}
	
	for my $rec ( @{ $self->{records} } ) {
		my $qsym = $rec->get('undr.identifier.symbol') or next;
		
		for my $rel (@relations) {
			next if ! grep m/^\Q$qsym\E$/, ( values %{ $rel } );
			for my $type ( keys %{ $rel } ) {
				next if $rel->{$type} eq $qsym;
				
				my $field = DC::Field->new(
					schema => 'undr',
					element => 'relation',
					qualifier => $type
				);
				
			    $field->value ( $rel->{$type} );
				$rec->add_field($field);
			}
		}
	}
}

sub combine {
	my $self = shift;
	
	for my $dc ( @{ $self->{records} } ) {
		my %combos;
		for my $field ( @{ $dc->{fields} } ) {	
			next if ! $field->{combine};
			my $head = $field->{label};
			
			$combos{$head}{char} = $field->{combine};
			if ( $field->{combo_order} ) {
				unshift @{ $combos{$head}{parts} }, $field->{value};
			} else { 
				push @{ $combos{$head}{parts} }, $field->{value};
			}
			undef $field;
		}
		next if ! %combos;
		@{ $dc->{fields} } = grep defined, @{ $dc->{fields} };
		
		for my $head ( keys %combos ) {
			my $combo = $combos{$head}{char};
			
			my @combos = @{ $combos{$head}{parts} };
			#for my $i (0..$#combos) {
			#	say $combos[$i];
			#	$combos[$i] =~ s/\.$// if $i ne $#combos;
			#}
			my $text = join $combo, @combos;
			$text .= '.';
			#my $text = join $combo, @{ $combos{$head}{parts} };
			
			my @parts = split /\./, $head;
			$parts[-1] =~ s/\[(..)\]$//;
			my $language = $1 if $1;
			my ($schema, $element, $qualifier) = @parts;
			my $new_field = DC::Field->new(
				schema => $schema,
				element => $element
			);
			$new_field->qualifier($qualifier) if $qualifier;
			$new_field->language($language) if $language;
			$new_field->value($text);
			$dc->add_field($new_field);
		}
	}
}

sub _csv {
	my $self = shift;
	
	my %header;
	my $records = $self->get_records;
	
	for my $record (@$records) {
		my $fields = $record->get_fields;
		for my $field (@$fields) {
			my $label = $field->label;
			$header{$label} = 1;
		}
	}
	my @header;
	for (sort keys %header) {
		#say $_;
		push @header, $_ if $_ !~ /^(id|collection)$/;
	}
	
	push my @data, \@header;
	
	for my $record (@$records) {
		my %fields;
		my $fields = $record->get_fields;
		for my $field (@$fields) {
			$fields{$field->label} .= '||'.$field->value if $fields{$field->label};
			$fields{$field->label} ||= $field->value;
		}
		my @row;
		for my $i (0..$#header) {
			my $val = $fields{$header[$i]};
			$row[$i] = $val;
			$row[$i] ||= '';
		}
		push @data, \@row;
	}

	my $csv;
	for (@data) {
		my $row = join $self->csv_delimiter, @$_;
		$csv .= "$row\n";
	}
	
	return $csv;
}

sub dspace_csv {
	my $self = shift;
	my $bib_item = shift || die "bib-item map required";
	
	my @csv = split "\n", $self->csv;
	my $header = shift @csv;
	my @header = split "\t", $header;
	my ($bib_col, $sym_col);
	for my $i (0..$#header) {
		chomp $header[$i];
		$bib_col = $i if $header[$i] eq 'undr.identifier.bib';
		$sym_col = $i if $header[$i] eq 'undr.identifier.symbol';
	}
	die "no bib column" if ! $bib_col;
	my @new_csv;
	for (@header) {
		$_ .= '[]' if substr($_,-1) ne ']';
	}
	for (@csv) {
		my @row = split "\t";
		my $bib = $row[$bib_col];
		die "no bib" if ! $bib;
		my $symbol = $row[$sym_col];
		next if ! $symbol;
		my $item = $bib_item->{$bib};
		$item ||= '+';
		#next if ! $item;
		
		my $collection = map_collection($symbol);
		
		
		unshift @row, $collection;
		unshift @row, $item;
		push @new_csv, \@row;
	}
	unshift @header, 'collection';
	unshift @header, 'id';
	
	#unshift @new_csv, \@header;
	
	$header = join ",", @header;
	my $csv = $header."\n";
	for (@new_csv) {
		for my $cell (@$_) {
			if ($cell =~ /"/) {
				$cell =~ s/"/""/g;
				$cell = '"'.$cell.'"';
			} elsif ($cell =~ /,/) {
				$cell = '"'.$cell.'"';
			}
		}
		my $line = join ",", @$_;
		$csv .= "$line\n";
	}

	return $csv;
}


sub saf_xml {
	my ($self, $dir, $id) = @_;
	
	my @xml;
	for my $record (@{ $self->records } ) {
		my $id = $record->get($id);
		die "id not found" if ! $id;
		$record->print_xml("$dir/$id");
	}
	
	#say "@xml";
}

sub map_collection {
	my $syms = shift;
	my @syms = split '\|\|', $syms;

	my %map = (
		A => '11176/18068',
		E => '11176/90564',
		S => '11176/9',
		ST => '11176/90562',
		T => '11176/90563',
		other => '11176/90565'
	);
	
	my @collections;
	for my $sym (@syms) {
		my $body = $1 if $sym =~ /^([A-Z]+)/;
		my $label = $map{$body} if $body;
		if ( $label )	{
			push @collections, $label if ! grep {$_ eq $label} @collections;
		} else {
			my $other = $map{other};
			push @collections, $other if ! grep {$_ eq $other} @collections; 
		}
	}
	
	
	my $collection = join '||', @collections;
		
	return $collection;
}


DC::Set->meta->make_immutable();

package DC::Relation;

sub new {
	my ($class, $type, $text) = @_;
	my $self->{$type} = $text;
	bless $self, $class;
}

sub add {
	my ($self, $type, $text) = @_;
	$self->{$type} = $text if $text;
}

package DC::XML;
use feature 'say';

sub new {
	my ($class, $schema) = @_;
	die "new DC::XML requires valid schema" if $schema !~ /^(dc|undr)$/i;
	
	my $self->{schema} = $schema;
	$self->{header} = '<?xml version="1.0" encoding="UTF-8"?>';
	$self->{top} = '<dublin_core>' if $schema eq 'dc';
	$self->{top} = '<dublin_core schema="undr">' if $schema eq 'undr';
	$self->{end} = '</dublin_core>';
	$self->{lines} = ();
	
	bless $self, $class;
	return $self;
}

sub add_line {
	my ($self, $head, $value) = @_;
	
	$value = charsub($value);
	
	my $lang = $1 if $head =~ m/\[(..)\]/;
	$head =~ s/\[(..)\]//;
	my @field = split /\./, $head;
	my $schema = $field[0]; 
	my $element = $field[1]; 
	my $qualifier = $field[2];
	
	my $xmline = "\t".'<dcvalue element="'.$element.'"';
	$xmline .= ' qualifier="'.$qualifier.'"' if $qualifier;
	$xmline .= ' language="'.$lang.'"' if $lang;
	$xmline .= '>'.$value.'</dcvalue>';
	
	push @{ $self->{lines} }, $xmline;
	
	@{$self->{xml}} = ( $self->{header},$self->{top},@{$self->{lines}},$self->{end} );	
}

sub xml {
	my $self = shift;
		
	return $self->{xml};
}

sub print_xml {
	my ($self, $destination) = @_;
	warn "printing XML without any content" if ! $self->{lines};
	
	mkdir $destination if ! -e $destination;
	#say "skipping $destination - no files" and return if ! -e $destination;
	my $file;
	$file = "$destination/dublin_core.xml" if $self->{schema} eq 'dc';
	$file = "$destination/metadata_undr.xml" if $self->{schema} eq 'undr';
	open (my $xml,">",$file) or die "destination file location does not exist\n";
	for my $line ( @{ $self->{xml} } ) {
		print $xml $line,"\n";
	}
	close $xml;
}

sub charsub {
	my $val = shift;
	
	$val =~ s/&/&amp;/g;
	$val =~ s/"/&quot;/g;
	$val =~ s/'/&apos;/g;
	$val =~ s/</&lt;/g;
	$val =~ s/>/&gt;/g;
	
	return $val;
}




1;
