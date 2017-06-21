package Utils2;
use strict;
use warnings;
use feature 'say';

use Exporter qw/import/;
our @ISA =   qw/Exporter/;
our @EXPORT_OK = qw/
	date_time
	get_range
	split_file
	date_hzn_unix
	date_unix_hzn
	current_hzn_datetime
	date_hzn_8601
	date_8601_hzn
	date_8601_unix
	date_unix_8601
	dateref
	date_269_260
/;

sub date_time {
	my $format = shift;
	
	my $dateref = date_ref();	
}

sub date_269_260 {
	my $date = shift;	
	my $len = length($date);
	die 'invalid date' unless grep {$_ eq $len} 4,6,8;
	die 'invalid date' unless $date =~ /^\d+$/;
	my %month = qw/01 Jan. 02 Feb. 03 Mar. 04 Apr. 05 May 06 Jun. 07 Jul. 08 Aug. 09 Sep. 10 Oct. 11 Nov. 12 Dec./;
	$date =~ /(?<year>\d{4})(?<month>\d{2})?(?<day>\d{2})?/;
	die 'invalid date' if ! $+{year};
	
	if (! $+{month} and ! $+{day}) {
		return $+{year};
	} elsif (! $+{day}) {
		return join ' ', $month{$+{month}}, $+{year};
	} else {
		return join ' ', $+{day}, $month{$+{month}}, $+{year};
	}
}

sub date_260_269 {
	my $date = shift;
	$date =~ /(\d{1,2})? ?([A-Z][a-z]{2,3})?\.? ?(\d{4})/;
	my ($day,$mon,$yr) = ($1,$2,$3);
}

sub dateref {
	my $unix = shift;
	$unix ||= time;
	
	my %date;
	@date{qw/seconds minutes hours monthday month year weekday yearday is_dst/} = localtime($unix);
	$date{year} += 1900;
	$date{month}++;
	$_ = sprintf("%02d",$_) for @date{qw/seconds minutes hours monthday month weekday/};
	
	return \%date;
}

sub date_hzn_unix {
	my ($hzn_date,$hzn_time,$gmt_adjust) = @_;
	return if ! $hzn_date;
	$hzn_time ||= 0;
	#$gmt_adjust ||= 0;
	#my @dt = localtime;
	#$gmt_adjust = 4 if $dt[8];
	#$gmt_adjust ||= 5;
	$gmt_adjust ||= 0;
	return ($hzn_date * 86400) + ($hzn_time * 60) + ($gmt_adjust * 3600);
}

sub date_unix_hzn {
	my $unix = shift;
	$unix ||= time;
	my $days = int ($unix / 86400);
	my $mins = int (($unix - ($days * 86400)) / 60);
	return [$days, $mins];
}

sub current_hzn_datetime {
	my $unix = time;
	return date_unix_hzn($unix);
}

sub date_unix_8601 {
	my $unix = shift;
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($unix);
	$year += 1900;
	$mon += 1;
	$_ = sprintf("%02d",$_) for ($sec,$min,$hour,$mday,$mon);
	
	return "$year$mon$mday$hour$min$sec";
}

# GMT
sub date_8601_unix {
	my $dt = shift;
	#say $dt;
	$dt .= '0' while length $dt < 14;
	my @parts = $dt =~ /(....)(..)(..)(..)(..)(..)/ or die 'invalid iso 8601 date';
	my ($yr,$mon,$day,$hr,$min,$sec) = @parts;
	my %monthdays  = qw/1 31 2 28 3 31 4 30 5 31 6 30 7 31 8 31 9 30 10 31 11 30 12 31/;
	
	my $yeardays = ($yr - 1970) * 365;
	my $since = $yr - 1972;
	my $leapdays = (int ($since / 4)) + 1;
	$leapdays-- if ($yr % 4 == 0) and ($mon < 3);
	$yeardays += $leapdays if $yr >= 1972;
	#say $yeardays;
	my $monthdays = 0;
	for (1..$mon-1) {
		$monthdays += $monthdays{$_};
	}
	$monthdays += ($day - 1);
	#say $monthdays;
	my $days = $yeardays + $monthdays;
	
	return ($days * 86400) + ($hr * 3600) + ($min * 60) + $sec;
}

sub date_hzn_8601 {
	my ($hzn_date,$hzn_time,$gmt_adjust) = @_;
	return '' if ! $hzn_date or $hzn_date eq 'NULL';
	$hzn_time ||= 0; # if $hzn_time eq 'NULL';
	my $unix = date_hzn_unix($hzn_date,$hzn_time,$gmt_adjust);
	return date_unix_8601($unix);
}

sub date_8601_hzn {
	my $_8601 = shift;
	my $unix = date_8601_unix($_8601);
	return date_unix_hzn($unix);
}

sub range {
	# 
	
	my ($index,$interval) = @_;
	$interval ||= 1000;
	
	return [0,$interval] if $index <= $interval;
	
	my $base = int ($index / $interval);
	my $min = $base * $interval;
	my $max = $min + $interval;
	
	return [$min,$max];
}

sub file_name {
	# takes $path returns filename without extension
	my $path = shift;
	return $1 if $path =~ /([^\/\.]+)(\.[a-z]+)?$/;	
}

sub file_ext {
	# takes $path returns extension
	my $path = shift;
	return $2 if $path =~ /([^\/\.]+)(\.[a-z]+)?$/;	
}

sub split_file {
	# splits tabular $file into files with $split number of rows in each
	# returns list of new file paths

	my ($file,$split) = @_;
	
	die "file not found" if ! -e $file;
	my $name = file_name($file);
	my $extension = file_ext($file);
	my $dir = $1 if $file =~ /(.*)\/[^\/]+$/;
	$dir ||= '.';
	
	my ($header, @file, @files);
		
	open my $fh,"<",$file;
	while (<$fh>) {
		next if $_ !~ /\w/; 
		$header = $_ and next if $. eq 1;
		push @file, $_;
		my $line = $. - 1;
		if ( ($line / $split) =~ /^\d+$/ ) {
			unshift @file, $header;
			my @copy = @file;
			push @files, \@copy;
			undef @file;
		}
	}
	# leftovers
	if (@file) {
		unshift @file, $header;
		push @files, \@file;
	}
	
	my $start = 1;
	my $end = $split;
	my @results;
	for my $file (@files) {
		$end = $start + (scalar(@$file)-1) if $file eq $files[-1];
		my $fn = "$name.$start-$end";
		$fn .= $extension if $extension;
		my $path = "$dir/$fn";	
		open my $fh,">",$path;
		print {$fh} join "\n", @$file;
		push @results, $path;
		$start += $split;
		$end += $split;
	}
	
	return @results;	
}

1

