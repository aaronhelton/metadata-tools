use strict;
use warnings;
use feature 'say';

package S3::Bucket;
use API;
use Data::Dumper;
use WWW::Mechanize;
use XML::LibXML;

use constant MECH => WWW::Mechanize->new ( 
	agent => 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36',
	cookie_jar => {},
	timeout => 10,
	autocheck => 0,
	stack_depth => 5
);

has 'name' => (
	is => 'rw',
	param => 1,
);

has 'url' => (
	is => 'ro',
	default => sub {
		my $self = shift; 
		return 'http://'.$self->name.'.s3.amazonaws.com/'
	}
);

has 'objects' => (
	is => 'ro',
	default => []
);

has 'count' => (
	is => 'method',
	code => sub {
		my $self = shift;
		return scalar @{$self->objects};
	}
);

has 'index' => (
	is => 'method',
	code => sub {
		my ($self,%params) = @_;
		my $c = \$self->{chunks};
		$$c++;
		
		my $url = $self->url.'?list-type=2';
		if (my @params = grep {$params{$_} and ! ref $params{$_}} keys %params) {
			$url .= "&$_=".$params{$_} for @params;
		}
		
		say "indexing chunk $$c...";
		
		my $xml = XML::LibXML->load_xml(string => MECH->get($url)->content);
		my $root = $xml->getDocumentElement;
		FILES: for my $file ($root->getElementsByTagName('Contents')) {
			my %data;			
			$data{$_->tagName} = $_->textContent for ($file->childNodes);
			my $o = S3::Object->new (
				bucket => $self->name, 
				key => $data{Key},
				modified => $data{LastModified}
			);
			DATE_FILTER: {
				my $date = $o->modified;
				$date = substr $date,0,10;
				$date =~ s/-//g;
				my ($bef,$aft) = @params{qw/exclude_before exclude_after/};
				next FILES if (($bef and $date < $bef) or ($aft and $date > $aft));
			}
			push @{$self->{objects}}, $o;
		}
	
		#$self->scan('continuation-token' => $_->textContent) for $root->getElementsByTagName('NextContinuationToken');
		my @is_t = $root->getElementsByTagName('IsTruncated');
		if ($is_t[0]->textContent eq 'true') {
			$self->index(%params,'start-after' => $root->lastChild->firstChild->textContent);
			return;
		} 
		
		say "found ".$self->count." files";
		return;	
	}
);

package S3::Object;
use API;
use Data::Dumper;

has 'bucket' => (
	is => 'rw',
	param => 0
);

has 'key' => (
	is => 'rw',
	param => 0
);

has 'url' => (
	is => 'ro',
	default => sub {
		my $self = shift; 
		return 'http://'.$self->bucket.'.s3.amazonaws.com/'.$self->key
	},
);

has 'filename' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		return (split('/', $self->key))[-1];
	}
);

has 'base' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		my @s = split('\.',$self->filename);
		return join '.', @s[0..($#s-1)];
	}
);

has 'extension' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		return (split('\.',$self->filename))[-1];
	}
);

has 'modified' => (
	is => 'rw',
	param => 0
);



