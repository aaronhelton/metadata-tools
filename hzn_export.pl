use strict;
use warnings;
use feature 'say';
use utf8;
use lib 'modules';

package main;
use Data::Dumper;
$Data::Dumper::Indent = 1;
use Getopt::Std;
use URI::Escape;
use Carp;
use DBI;
use Get::Hzn;
use Utils2 qw/date_hzn_8601 date_8601_hzn/;

use constant HEADER => 
q|<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE marc [
<!ELEMENT collection (record*)>
<!ATTLIST collection xmlns CDATA "">
<!ELEMENT record (leader,controlfield+,datafield+)>
<!ELEMENT leader (#PCDATA)>
<!ELEMENT controlfield (#PCDATA)>
<!ATTLIST controlfield tag CDATA "">
<!ELEMENT datafield (subfield+)>
<!ATTLIST datafield tag CDATA "" ind1 CDATA "" ind2 CDATA "">
<!ELEMENT subfield (#PCDATA)>
<!ATTLIST subfield code CDATA ""> ]>
<collection>|;
# Tind doesn't like the xmlns attribute, 
# which is ok since their marcxml doesn't conform to to it anyway  
# <collection xmlns="http://www.loc.gov/MARC21/slim">|;

use constant TYPE => {
	map => 'Maps',
	sp => 'Speeches',
	vot => 'Voting Data',
	img => 'Images and Sounds',
	docpub => 'Documents and Publications',
	rd => 'Resolutions and Decisions',
	rpt => 'Reports',
	mr => 'Meeting Records',
	lnv => 'Letters and Notes Verbales',
	pub => 'Publications',
	drpt => 'Draft Reports',
	drd	=> 'Draft Resolutions and Decisions',
	pr => 'Press Releases',
	ai => 'Administrative Issuances',
	ta => 'Treaties and Agreements',
	lco => 'Legal Cases and Opinions',
	nws => 'NGO Written Statements',
	pet => 'Petitions',
	cor => 'Concluding Observations and Recommendations',
	res => 'Resolutions',
	dec => 'Decisions',
	prst => 'Presidential Statements',
	sgr => 'Secretary-General\'s Reports',
	asr => 'Annual and Sessional Reports',
	per => 'Periodic Reports',
	vbtm => 'Verbatim Records',
	sum => 'Summary Records',
	sgl	=> 'Secretary-General\'s Letters',
};

use constant AUTH_TYPE => {
	100 => 'PERSONAL',
	110 => 'CORPORATE',
	111 => 'MEETING',
	130 => 'UNIFORM',
	150 => 'TOPICAL',
	151 => 'GEOGRAPHIC',
	190 => 'SYMBOL',
	191 => 'AGENDA'
};

use constant DESC => {
	'[cartographic information]' => 'a',
	'[video recording]' => 'v',
	'[sound recording]' => 's',
	'ORAL HISTORY' => 's'
};

use constant LANG_1 => {
	A => 'AR',
	C => 'ZH',
	E => 'EN',
	F => 'FR',
	R => 'RU',
	S => 'ES',
	O => 'DE'
};

use constant LANG_2 => {
	AR => ' العربية ',
	ZH => '中文 => ',
	EN => 'English ',
	FR => 'Français',
	RU => 'Русский ',
	ES => 'Español ',
	DE => 'Other',
};

use constant LANG_3 => {
	العربية => 'AR',
	中文 => 'ZH',
	Eng => 'EN',
	English => 'EN',
	Français => 'FR',
	Русский => 'RU',
	Español => 'ES',
	Other => 'DE'
};

RUN: {
	MAIN(options());
}

sub options {
	my $opts = {
		'h' => 'help',
		'a' => 'export auths',
		'b' => 'export bibs',
		#'t' => 'merge + export thesaurus',
		'o:' => 'xml output directory',
		'd:' => 'sync db',
		#'x:' => 'xml thesaurus path',
		#'3:' => 's3 index file',
		'm:' => 'modified since',
		'u:' => 'modified until',
		#'c:' => 'criteria sql statement that returns IDs to export',
		#'q:' => 'criteria as sql script that returns IDs to export'
	};
	getopts ((join '',keys %$opts), \my %opts);
	if ($opts{h} or ! keys %opts) {
		say "$_ - $opts->{$_}" for keys %$opts;
		exit; 
	} else {
		my $reqs = scalar(grep {$opts{$_}} qw/a b t/);
		die q{must choose only one of opts "a", "b"} if $reqs > 1;
		! $reqs && die q{boolean opt "a", "b" required}."\n";
		$opts{a} && ($opts{t} = 'auth');
		$opts{b} && ($opts{t} = 'bib');
	}
	return \%opts;
}

sub MAIN {
	my $opts = shift;

	my $db = $opts->{d};
	$opts->{dbh} = DBI->connect("dbi:SQLite:dbname=$db","","") if $db;
	$opts->{dbh}->{AutoCommit} = 0;

	update_sync_db($opts);
	
	my %dispatch = (
		s => \&export_range,
		m => \&export_from,
		c => \&export_by_criteria,
		q => \&export_by_criteria,
		x => \&thesaurus,
	);
	
	for my $opt (qw/s m c q x/) {
		next unless $opts->{$opt};
		$dispatch{$opt}->($opts);
	}
	
	$opts->{dbh}->commit;
}

sub export_from {
	my $opts = shift;
	
	my $type;
	$opts->{a} && ($type = 'auth');
	$opts->{b} && ($type = 'bib');
	my ($inc,$dir) = @{$opts}{qw/i o/};
	#say "reading s3 file" and my $s3_data = s3_data($opts->{3}) if $opts->{3} and $opts->{b};
	#$opts->{b} && ! $s3_data && ($s3_data = {}) && warn "warning: no s3 data provided\n";
	my $dups = dup_035s($opts) if $type eq 'bib';
	$dups ||= {};
	#my $dups = {1111 => [1,2]};
	
	print Dumper $dups;
	
	my $ids = modified_since($opts);
	say 'ok. found '.scalar @$ids.' export candidates';
	my $chunks = int(scalar @$ids / 1000) + 1;
	#$chunks ||= 1;
	my $total = 0;
	
	my $t = time;
	my $out;
	if ($dir) {
		mkdir $dir if ! -e $dir;
		my $fn = "$dir/$type\_from_$opts->{m}";
		$fn .= "_until_$opts->{u}" if $opts->{u};
		$fn .= '.xml';
		open $out, ">:utf8", $fn; 
		say {$out} HEADER;
	}
	
	my $from;
	for my $chunk (0..$chunks-1) {
		say "gathering data for chunk $chunk...";
		$from += ($chunk * 1000);
		my $to = $from + 999;
		my $filter = join ',', grep {defined} @$ids[$from..$to];
		my $item = item_data($filter);
		my $audit = audit_data($type,$filter);
		my $dls = dls_data($opts->{dbh},$filter);
		my $s3 = s3_data($opts->{dbh},$filter);
		say "writing xml...";
		$total += write_xml($type,$filter,$s3,$dls,$item,$dups,$audit,$out,$dir,$opts);
	}
		
	print {$out} '</collection>' if $dir;
	say 'done. wrote '.$total.' records in '.(time - $t).' seconds'; 
}

my $comment = <<'#';
sub export_by_criteria {
	my $opts = shift;
	my $type;
	$opts->{a} && ($type = 'auth');
	$opts->{b} && ($type = 'bib');
	my ($inc,$dir) = @{$opts}{qw/i d/};
	
	my $s3_data = s3_data($opts->{3}) if $opts->{3} and $opts->{b};
	$opts->{b} && ! $s3_data && ($s3_data = {}) && warn "warning: no s3 data provided\n";
	my $dups = dup_035s();
	
	my $sql = $opts->{c};
	if (! $sql) {
		require File::Slurp;
		$sql = read_file($opts->{q});
	}	
	
	my $ids = by_criteria($sql);
	my $chunks = int(scalar @$ids / 1000);
	$chunks ||= 1;
	my $total = 0;
	
	my $t = time;
	my $out;
	if ($dir) {
		mkdir $dir if ! -e $dir;
		my $fn = "$dir/$type\_since_$opts->{m}.xml";
		open $out, ">:utf8", "$dir/$type\_since_$opts->{m}.xml"; 
		say {$out} HEADER;
	}
	
	my $from;
	for my $chunk (0..$chunks-1) {
		$from += ($chunk * 1000);
		my $to = $from + 999;
		my $filter = join ',', grep {defined} @$ids[$from..$to];
		my $item = item_data($filter);
		my $audit = audit_data($type,$filter);
		$total += write_xml($type,$filter,$s3_data,$item,$audit,$dups,$out,$dir,$opts);
	}
		
	print {$out} '</collection>' if $dir;
	say 'done. wrote '.$total.' records in '.(time - $t).' seconds'; 
}
#

sub write_xml {
	my ($type,$criteria,$s3_data,$dls_data,$item,$dups,$audit,$out,$dir,$opts) = @_;	
	my $ctype = ucfirst $type;
	my $count;
	"Get::Hzn::Dump::$ctype"->new->iterate (
		criteria => $criteria,
		encoding => 'utf8',
		callback => sub {
			my $record = shift;
			_000($record);
			_005($record);
			_035($record,$type,$dups);
			_998($record,$audit->{$record->id});
			if ($type eq 'bib') {
				return if ((! $record->has_tag('191')) 
					and (! $record->has_tag('791')) 
					and ($record->get_field_sub('099','b') ne 'DHU'));
				#return unless $record->get_field_sub('099','b') ne 'DHU';
				_007($record);
				_020($record);
				_650($record);
				_856($record,$s3_data,$dls_data); # also handles FFT
				_949($record,$item->{$record->id});
				_993($record);
				_967($record);
				_996($record);
				_989($record);
			} elsif ($type eq 'auth') {
				if (my $_035a = $record->get_field_subs('035','a')) {
					for my $a (@$_035a) {
						my $str = substr $a,0,1;
						return if $str eq 'P' or $str eq 'T';
					}
				}
				return if grep {$_->xref > $record->id} @{$record->get_fields(qw/400 410 411 450 451/)};
				_150($record); # also handles 450 and 550
				_4xx($record);
				_980($record);
			}
			_xrefs($record);
			$dir ? print {$out} $record->to_xml : print $record->to_xml;
			$count++;
		}
	);
	
	$count //= 0;
	return $count;
}

sub _xrefs {
	my $record = shift;
	for my $field (@{$record->fields}) {
		if (my $xref = $field->xref) {
			$xref = '(DHLAUTH)'.$xref;
			$field->xref($xref);
		}
	}
}

sub _000 {
	my $record = shift;
	my $l = substr($record->leader,0,24); # chop off end of illegally long leaders in some old records
	$l =~ s/\x{1E}/|/g; # special case for one record with \x1E in leader (?)
	$record->get_field('000')->text($l);
}

sub _005 {
	my $record = shift;
	$record->delete_tag('005');
}

sub _007 {
	my $record = shift;
	for (qw/191 245/) {
		if (my $fields = $record->get_fields($_)) {
			for my $key (keys %{&DESC}) {
				for my $field (@$fields) {
					if ($field->text =~ /\Q$key\E/) {
						$record->add_field(MARC::Field->new(tag => '007', text => DESC->{$key}));
					}
				}
			}
		}
	}
}

sub _020 {
	my $record = shift;
	if (my $fields = $record->get_fields('020')) {
		$_->delete_subfield('c') for @$fields;
	}
}

sub _035 {
	my ($record,$type,$dups) = @_;
	if (my $cns = $dups->{$record->id}) {
		for my $field (@{$record->get_fields('035')}) {
			my ($sub,$d,$t) = ('a',$field->delim,$field->terminator);
			for my $cn (@$cns) {
				my $text = $field->text;
				for my $sub_a ($field->text =~ m/$d a ([^$d$t]+)/gx) {
					next unless $sub_a eq $cn;
					my $pre = substr $cn,0,1;
					my $new = $record->id.'X';
					if ($pre =~ /[A-Z]/) {
						$text =~ s/$cn/$pre$new/;
					} else {
						$text =~ s/$cn/$new/;
					}
					$text =~ s/$d\a$cn//g;
					$field->text($text);
					$field->set_sub('z',$cn);
				}
			}
		}
	}	
	my $pre = $type eq 'bib' ? '(DHL)' : '(DHLAUTH)';
	my $nf = MARC::Field->new(tag => '035');
	$nf->sub('a',$pre.$record->id);
	$record->delete_tag('001');
	$record->add_field($nf);
}

sub _150 {
	my $record = shift;
	if (my $field = $record->get_field('150')) {
		if ($field->ind1 eq '9') {
			$field->change_tag('151');
			for (@{$record->get_fields('450')}) {
				$_->change_tag('451');
			}
			for (@{$record->get_fields('550')}) {
				$_->change_tag('551') if $_->ind1;
			}		
		}
	}
}

sub _4xx {
	my $record = shift;
	for (qw/400 410 411 430 450 490/) {
		if (my $fields = $record->get_fields($_)) {
			$_->delete_subfield('0') for @$fields;
		}
	}
}

sub _650 {
	my $record = shift;
	for (@{$record->fields('650')}) {
		my $ai = $_->auth_indicators;
		$ai && (substr($ai,0,1) eq '9') && $_->change_tag('651');
	}
}

sub _856 {
	my ($record,$s3,$dls) = @_;
	FIELDS: for my $field (@{$record->fields('856')}) {
		my $url = $field->sub('u');
		if (index($url,'http://daccess-ods.un.org') > -1) {
			$record->delete_field($field);
			my $lang = $field->get_sub('3');
			say $field->get_sub('3').join(' ',' ',keys(%{&LANG_3})) if ! LANG_3->{$lang};
			die "could not detect language for file in bib# ".$record->id if ! $lang;;
			
			goto S3; # don't use 856
			if (my $data = $dls->{$record->id}->{LANG_3->{$lang}}) {
				my ($url,$size) = @$data;
				my $dls_856 = MARC::Field->new(tag => '856');
				$dls_856->set_sub('u',$url);
				$dls_856->set_sub('s',$size);
				$dls_856->set_sub('y',$lang);
				#$dls_856->set_sub('y',(split /\//,$url)[-1]);
				$record->add_field($dls_856);
				next FIELDS;
			}
			
			S3:
			my $key = $s3->{$record->id}->{LANG_3->{$lang}};
			if (! $key) {
				$s3->{$record->id}->{$lang} = 'MISSING';
				return;
			}
			my $newfn = (split /\//,$key)[-1];
			$newfn = (split /;/, $newfn)[0];
			$newfn =~ s/\.pdf//;
			$newfn =~ s/\s//;
			$newfn =~ tr/./-/;
			if (! grep {$_ eq substr($newfn,-2)} keys %{&LANG_2}) {
				$newfn .= '-'.LANG_3->{$lang};
			}
			$newfn .= '.pdf';
			my $FFT = MARC::Field->new(tag => 'FFT')->set_sub('a','http://undhl-dgacm.s3.amazonaws.com/'.uri_escape($key));
			$FFT->set_sub('d',$field->sub('3'));
			$FFT->set_sub('n',$newfn);
			$record->add_field($FFT);
		} elsif (index($url,'s3.amazonaws') > -1) {
			$record->delete_field($field);
			my $oldfn = (split /\//,$url)[-1];
			my $newfn = uri_escape($oldfn);
			$url =~ s/$oldfn/$newfn/;
			my $FFT = MARC::Field->new(tag => 'FFT')->set_sub('a',$url);
			$FFT->set_sub('d',$field->sub('3'));
			$FFT->set_sub('n',uri_escape($field->sub('q')));
			$record->add_field($FFT);
		}
	}
}

sub _949 {
	my ($record,$data) = @_;
	$record->delete_tag('949');
	for (keys %$data) {
		$_ eq 'places' && next;
		my $vals = $data->{$_};
		my $field = MARC::Field->new(tag => '949');
		$_ =~ s/[\x{1}-\x{1F}]//g for @$vals;
		$field->set_sub($_,shift(@$vals)) for qw/9 b i k c l z m d/;
		$record->add_field($field);
	}
}

sub _967 {
	my $record = shift;
	for my $field (@{$record->get_fields(qw/968 969/)}) {
		$field->change_tag('967');
	}
}

sub _980 {
	my $record = shift;
	$record->add_field(MARC::Field->new(tag => '980')->set_sub('a','AUTHORITY'));
	for (keys %{&AUTH_TYPE}) {
		if ($record->has_tag($_)) {
			$record->add_field(MARC::Field->new(tag => '980')->set_sub('a',AUTH_TYPE->{$_}));
			last;
		}
	}
}

sub _989 {
	my $record = shift;
			
	Q_1: {
		last unless $record->check('245','*','*[cartographic material]*')
			|| $record->check('007','*','a')
			|| $record->check('089','b','B28')
			|| $record->check('191','b','ST/LEG/UNTS/Map*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{map});
		$record->add_field($_989);
	}
	Q_2: {
		last unless $record->check('089','b','B22');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{sp});
		$record->add_field($_989);
	}
	Q_3: {
		last unless $record->check('089','b','B23');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{vot});
		$record->add_field($_989);
	}
	Q_4: {
		last unless $record->check('245','*',qr/(video|sound) recording/)
			|| $record->check('007','*','s')
			|| $record->check('007','*','v')
			|| $record->check('191','*','*ORAL HISTORY*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{img});
		$record->add_field($_989);
	}
	Q_5: {
		last unless $record->check('191','*','*/RES/*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rd});
		$_989->set_sub('c',TYPE->{res});
		$record->add_field($_989);
	}
	Q_6: {
		last unless $record->check('191','a','*/DEC/*')
			&& $record->check('089','b','B01');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rd});
		$_989->set_sub('c',TYPE->{dec});
		$record->add_field($_989);
	}
	Q_7: {
		last unless $record->check('191','a','*/PRST/*')
			|| $record->check('089','b','B17');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rd});
		$_989->set_sub('c',TYPE->{prst});
		$record->add_field($_989);
	}
	Q_8: {
		last unless $record->check('089','b','B01')
			&& ! $record->check('989','b',TYPE->{rd});
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rd});
		$record->add_field($_989);
	}
	Q_9: {
		last unless $record->check('089','b','B15')
			&& $record->check('089','b','B16')
			&& ! $record->check('245','*','*letter*from the Secretary-General*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rpt});
		$_989->set_sub('c',TYPE->{sgr});
		$record->add_field($_989);
	}
	Q_10: {
		last unless $record->check('089','b','B04');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rpt});
		$_989->set_sub('c',TYPE->{asr});
		$record->add_field($_989);
	}
	Q_11: {
		last unless $record->check('089','b','B14')
			&& ! $record->check('089','b','B04');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rpt});
		$_989->set_sub('c',TYPE->{per});
		$record->add_field($_989);
	}
	Q_12: {
		last unless $record->check('089','b','B16')
			&& $record->check('245','*','*Report*')
			&& $record->check('989','b','Reports');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{rpt});
		$record->add_field($_989);
	}
	Q_13: {
		last unless $record->check('191','a','*/PV.*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{mr});
		$_989->set_sub('c',TYPE->{vbtm});
		$record->add_field($_989);
	}
	Q_14: {
		last unless $record->check('191','a','*/SR.*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{mr});
		$_989->set_sub('c',TYPE->{sum});
		$record->add_field($_989);		
	}
	Q_15: {
		last unless $record->check('089','b','B03')
			&& ! $record->check('989','b','Meeting Records');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{mr});
		$record->add_field($_989);
	}
	Q_16: {
		last unless $record->check('089','b','B15')
			&& $record->check('089','b','B15')
			&& ! $record->check('989','c','Secretary-General\'s*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{lnv});
		$_989->set_sub('c',TYPE->{sgl});
		$record->add_field($_989);
	}
	Q_17: {
		last unless $record->check('089','b','B18')
			&& ! $record->check('089','b','Letters*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{lnv});
		$record->add_field($_989);
	}
	Q_18: {
		last unless $record->has_tag('022')
			|| $record->has_tag('020')
			|| $record->check('089','b','B13')
			|| $record->has_tag('079');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{pub});	
		$record->add_field($_989);		
	}
	Q_19: {
		last unless $record->check('089','b','B08');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{drpt});
		$record->add_field($_989);
	}
	Q_20: {
		last unless $record->check('089','b','B02');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{drd});
		$record->add_field($_989);
	}
	Q_21: {
		last unless $record->check('191','a','*/PRESS/*')
			|| $record->check('089','b','B20');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{pr});
		$record->add_field($_989);
	}	
	Q_22: {
		last unless $record->check('089','b','B12')
			|| $record->check('191','a',qr/\/(SGB|AI|IC|AFS)\//);
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{ai});	
		$record->add_field($_989);
	}
	Q_23: {
		last unless $record->check('089','b','A19');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{ta});
		$record->add_field($_989);		
	}
	Q_24: {
		last unless $record->check('089','b','A15')
			|| $record->check('089','b','B25');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{lco});
		$record->add_field($_989);
	}
	Q_25: {
		last unless $record->check('089','b','B21')
			|| $record->check('191','a','*/NGO/*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{nws});
		$record->add_field($_989);
	}
	Q_26: {
		last unless $record->check('191','a','*/PET/*');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{pet});
		$record->add_field($_989);
	}	
	Q_27: {
		last unless $record->check('089','b','B24');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$_989->set_sub('b',TYPE->{cor});
		$record->add_field($_989);
	}	
	Q_28: {
		last unless ! $record->has_tag('989');
		my $_989 = MARC::Field->new(tag => '989');
		$_989->set_sub('a',TYPE->{docpub});
		$record->add_field($_989);		
	}
}

sub _993 {
	my $record = shift;
	
	PRSTS: {
		my %prsts;
		for (@{$record->fields('991')}) {
			if (my $text = $_->get_sub('e')) {
				if ($text =~ /.*?(S\/PRST\/[0-9]+\/[0-9]+)/) {
					$prsts{$1} = 1;
				}
			}
		}
		for (keys %prsts) {
			my $field = MARC::Field->new;
			$field->tag('993')->inds('5')->sub('a',$_);
			$record->add_field($field);
		}
	}
	SPLIT: {
		for my $field (@{$record->fields('993')}) {
			if (my $text = $field->sub('a')) {
				my @syms = split_993($text);
				my $inds;
				if ($syms[0]) {
					$inds = $field->inds;
					$field->ind1('9');
				} 
				for (@syms) {
					my $newfield = MARC::Field->new (
						tag => '993',
						indicators => $inds,
					);
					$newfield->sub('a',$_);
					$record->add_field($newfield);
				}
			}
		}
	}
}

sub _996 {
	my $record = shift;
	if (my $field = $record->get_field('996')) {
		if (my $pv = pv_from_996($record)) {
			my $newfield = MARC::Field->new (
				tag => '993',
				indicators => '4'
			);
			$newfield->sub('a',$pv);
			$record->add_field($newfield);
		}
	}
}

sub _998 {
	my ($record,$data) = @_;
	confess $record->id if ! $data;
	my ($cr_date,$cr_time,$cr_user,$ch_date,$ch_time,$ch_user) = @$data;
	$_ ||= '' for $cr_date,$cr_time,$cr_user,$ch_date,$ch_time,$ch_user;
	my %data = ('a' => date_hzn_8601($cr_date,$cr_time),'b' => $cr_user,'c' => date_hzn_8601($ch_date,$ch_time),'d' => $ch_user);
	my $_998 = MARC::Field->new(tag => '998');
	$_998->sub($_,$data{$_}) for grep {$data{$_}} sort keys %data;
	$record->add_field($_998);
}

sub s3_data_0 {
	my $s3_file = @_;
	my %s3;
	open my $s3,'<',$s3_file;
	S3: while (<$s3>) {
		chomp;
		my @post = split /\//, $_;
		my @pref = split /\s+/, $post[0];
		my $key = join '/', $pref[-1], @post[1..4];
		my $bib = $post[3];
		my $lang = substr $key,-6,2;
		$s3{$bib}{$lang} = $key;
	}
	return \%s3;
}

sub dls_data {
	my ($dbh,$filter) = @_;
	my ($url,$size);
	my $sth = $dbh->prepare("select hzn_id,url,file_size,lang from hzn_to_dls_files where hzn_id in ($filter)");
	$sth->execute;
	my %data;
	while (my $row = $sth->fetch) {
		my ($id,$url,$size,$lang) = @$row;
		$data{$id}{$lang} = [$url,$size];
	}
	return \%data;	
}

sub s3_data {
	my ($dbh,$filter) = @_;
	my $sth = $dbh->prepare("select hzn_id,url,lang from hzn_to_s3_files where hzn_id in ($filter)");
	$sth->execute;
	my %data;
	while (my $row = $sth->fetch) {
		$data{$row->[0]}{$row->[2]} = $row->[1];
	}
	return \%data;	
}

sub modified_since {
	my $opts = shift;
	my ($type,$from,$to) = @{$opts}{qw/t m u/};
	$opts->{modified_type} ||= 'all';
	$from = date_8601_hzn($from);
	my $fdate = $from->[0];
	my $ftime = $from->[1];
	my %sql = (
		all => qq {
			select $type\#
			from $type\_control
			where (((create_date = $fdate and create_time > $ftime) or create_date > $fdate)  
					or ((change_date = $fdate and change_time > $ftime) or change_date > $fdate))
		},
		new => qq {
			select $type\#
			from $type\_control
			where ((create_date = $fdate and create_time > $ftime) or create_date > $fdate)  
		},
		changed => qq {
			select $type\#
			from $type\_control
			where ((change_date = $fdate and change_time > $ftime) or change_date > $fdate)
		},
	);
	my $sql = $sql{$opts->{modified_type}};
	if ($to) {
		$to = date_8601_hzn($to);
		my $tdate = $to->[0];
		my $ttime = $to->[1];
		my %more = (
			all => qq {
				and (((create_date = $tdate and create_time < $ttime) or create_date < $tdate)
					and ((change_date = $tdate and change_time < $ttime) or change_date < $tdate))
			},
			new => qq {
				and ((create_date = $tdate and create_time < $ttime) or create_date < $tdate)
			},
			changed => qq {
				and ((change_date = $tdate and change_time < $ttime) or change_date < $tdate)
			}
		);
		$sql .= $more{$opts->{modified_type}};
	}
	my @ids;
	Get::Hzn->new(sql => $sql)->execute (
		callback => sub {
			my $row = shift;
			my $id = shift @$row;
			push @ids, $id;
		}
	);
	return \@ids;
}

sub by_criteria {
	my $criteria = shift;
	my $get = Get::Hzn->new (
		sql => "select bib# from bib where bib# in ($criteria)"
	);
	my @ids;
	$get->execute (
		callback => sub {
			my $row = shift;
			my $id = shift @$row;
			push @ids, $id;
		}
	);
	return \@ids;
}

sub item_data {
	my ($filter) = @_;
	my %data;
	my $get = Get::Hzn->new (
		sql => qq {
			select 
				bib#,
				call_reconstructed,
				str_replace(ibarcode,char(9),"") as barcode,
				item#,
				collection,
				copy_reconstructed,
				location,
				item_status,
				itype,
				creation_date 
			from 
				item 
			where 
				bib# in ($filter)
		}
	);
	$get->execute (
		callback => sub {
			my $row = shift;
			my $bib = shift @$row;
			$row->[-1] = date_hzn_8601($row->[-1]) if $row->[-1];
			$data{$bib}{places}++; 
			my $place = $data{$bib}{places};
			$data{$bib}{$place} = $row;
		}
	);
	return \%data;
}

sub audit_data {
	my ($type,$filter) = @_;
	my %data;
	my $get = Get::Hzn->new (
		sql => qq {
			select 
				$type\#,
				create_date,
				create_time,
				create_user,
				change_date,
				change_time,
				change_user 
			from 
				$type\_control
			where 
				$type\# in ($filter)
		}
	);
	$get->execute (
		callback => sub {
			my $row = shift;
			my $id = shift @$row;
			$data{$id} = $row;
		}
	);
	return \%data;
}

sub dup_035s {
	my $opts = shift;
	my $sth = $opts->{dbh}->prepare('select hzn_id,hzn_ctrl from main join ctrl on main.main_id = ctrl.main_id');
	$sth->execute;
	my %return;
	while (my $row = $sth->fetch) {
		push @{$return{$row->[1]}}, $row->[0];
	}
	for (keys %return) {
		delete $return{$_} unless scalar @{$return{$_}} > 1;
	}
	return \%return;
}

sub dup_035s_2 {
	my (%_035,%return);
	my $i = 0;
	local $| = 1;
	#print "finding duplcated control#s. bibs scanned: ";
	Get::Hzn->new->sql(q{select bib#, text from bib where tag = "035"})->execute (
		callback => sub {
			my $row = shift; 
			my ($bib,$text) = @$row;
			$text || return;
			my $cns = (MARC::Field->new(tag => '035',text => $text)->get_subs('a'));
			#$cn eq 'Q1000000' && return;
			push @{$_035{$_}}, $bib for @$cns;
			my $check = $i / 50000;
			my $check2 = $i / 13000;
			print $i if $check == int $check;
			print '.' if $check2 == int $check2;
			$i++;
		}
	);
	print "\n";
	for my $cn (keys %_035) {
		if (scalar @{$_035{$cn}} > 1) {
			#print "$_ is duplicated in bibs ".join ',', @{$_035{$_}};
			push @{$return{$_}}, $cn for @{$_035{$cn}};
		}
	}
	
	return \%return;
}

sub dup_035s_1 {
	my $dbh = shift;
	my %return;
	my $dups = $dbh->selectcol_arrayref('select hzn_ctrl from dup_ctrl');
	$return{$_} = 1 for @$dups;
	return \%return;
}

sub facet_data {
	my %rules;
	while (<DATA>) {
		my @row = split /\t/, $_;
		my $q = "\Q$row[4]\E";
		my $stop = q{%"'\/};
		my $begin = $1 if $q =~ /^(.*?[$stop].*?[$stop])/;
		(my @or = $q) =~ /OR ([^\s]+)/g;
		(my @and = $q) =~ /AND ([^\s]+)/g;
		(my @not = $q) =~ /NOT ([^\s]+)/g;
		#
	}
}

sub split_993 {
	my $text = shift;
	
	return unless $text && $text =~ /([&;,]|and)/i;
	
	$text =~ s/^\s+|\s+$//;
	$text =~ s/ {2,}/ /g;
	my @parts = split m/\s?[,;&]\s?|\s?and\s?/i, $text;
	s/\s?Amended by //i for @parts;
	my $last_full_sym;
	my @syms;
	for (0..$#parts) {
		my $part = $parts[$_];
		$last_full_sym = $part if $part =~ /^[AES]\//;
		if ($part !~ /\//) {
			$part =~ s/ //g;
			if ($part =~ /^(Add|Corr|Rev)[ls]?\.(\d+)$/i) {
				push @syms, $last_full_sym.'/'.$1.".$2";
			} elsif ($part =~ /(.*)\.(\d)\-(\d)/) {
				my ($type,$start,$end) = ($1,$2,$3);
				push @syms, $last_full_sym.'/'.$type.".$_" for $start..$end;
			} elsif ($part =~ /^(\d+)$/) {
				my $type = $1 if $syms[$_-1] =~ /(Add|Corr|Rev)\.\d+$/i;
				push @syms, $last_full_sym.'/'.$type.".$_";
			} 
		} elsif ($part =~ /\//) {
			if ($part =~ /((Add|Corr|Rev)\.[\d]+\/)/i) {
				my $rep = $1;
				$part =~ s/$rep//;
				push @syms, $last_full_sym.'/'.$part;
			} elsif ($part =~ /^[AES]\//) {
				push @syms, $part;
			} 
		}
	}
	
	return @syms;
}

sub pv_from_996 {
	my $record = shift;
	my ($symfield,$body,$session);
	my $text = $record->get_field('996')->get_sub('a');
	my $meeting = $1 if $text =~ /(\d+).. (plenary )?meeting/i;
	return if ! $meeting;
	
	for (qw/191 791/) {
		if ($symfield = $record->get_field($_)) {
			return if index($symfield->get_sub('a'),'CONF') > -1;
			$body = $symfield->get_sub('b');
			if ($session = $symfield->get_sub('c')) {
				$session =~ s/\/$//;
			}
		} else {
			next;
		}
	}
	
	say $record->id.' 996 could not detect session' and return if ! $session;
	say $record->id.' 996 could not detect body' and return if ! $body;			
	
	return if ! grep {$body eq $_} qw|A/HRC/ A/ S/|;
	
	my $pv;
	if (substr($session,-4) eq 'emsp') {
		my $num = substr($session,0,-4);
		$session = 'ES-'.$num;
		if ($num > 7) {
			$pv = $body.$session.'/PV.'.$meeting;
		} else {
			$pv = $body.'PV.'.$meeting;
		}
	} elsif (substr($session,-2) eq 'sp') {
		my $num = substr($session,0,-2);
		$session = 'S-'.$num;
		if ($num > 5) {
			$pv = $body.$session.'/PV.'.$meeting;
		} else {
			$pv = $body.'PV.'.$meeting;
		}
	} elsif ((substr($body,0,1) eq 'A') and ($session > 30)) {
		$pv = $body.$session.'/PV.'.$meeting;
	} else {
		$pv = $body.'PV.'.$meeting;
	}
	
	return $pv;	
}

sub update_sync_db {
	my $opts = shift;
	
	my $dbh = $opts->{dbh};
	my $last_updated = $dbh->selectcol_arrayref('select max(hzn_updated) from main')->[0];
	my $ids = modified_since({t => 'bib', m => $last_updated, modified_type => 'new'});
	$ids = join ',', @$ids;
	my %map;
	MAIN: {
		my $sth = $opts->{dbh}->prepare('insert into main (hzn_id,hzn_updated) values (?,?)');
		Get::Hzn->new->sql(qq/select bib#, text from bib where tag = "035" and bib# in ($ids)/)->execute (
			callback => sub {
				my $row = shift; 
				my $bib = shift @$row;
				$sth->execute($bib,time);
			}
		);
	}
	$dbh->commit;
	my $map = $dbh->selectall_hashref('select main_id, hzn_id from main','hzn_id');
	CTRL: {
		my $sth = $opts->{dbh}->prepare('insert into ctrl (main_id, hzn_ctrl) values (?,?)');
		Get::Hzn->new->sql(qq/select bib#, text from bib where tag = "035" and bib# in ($ids)/)->execute (
			callback => sub {
				my $row = shift; 
				my ($bib,$text) = @$row;
				$text || return;
				my $cns = (MARC::Field->new(tag => '035',text => $text)->get_subs('a'));
				$sth->execute($map->{$bib}->{main_id},$_) for @$cns;
			}
		);
	}
	$dbh->commit;
}

__END__
