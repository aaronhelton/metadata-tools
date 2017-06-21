use strict;
use warnings;
use feature 'say';

package Get::DLS;
use API;
use Data::Dumper;
use WWW::Mechanize;
use MARC::Set2;

has 'mech' => (
	is => 'ro',
	default => WWW::Mechanize->new ( 
		agent => 'DHLbot',
		cookie_jar => {},
		timeout => 10,
		autocheck => 0,
		stack_depth => 5,
		ssl_opts => {verify_hostname => 0},
	    protocols_allowed => ['https'],
	)
);

has 'results_per_page' => (
	is => 'rw',
	default => 200
);

has 'login_url' => (
	is => 'rw',
	default => 'https://digitallibrary.un.org/youraccount/login'
	#default => 'https://undl.tind.io/youraccount/login'
);

has 'search_url' => (
	is => 'rw',
	default => 'https://digitallibrary.un.org/search'
	#default => 'https://undl.tind.io/search'
);

has 'parameters' => (
	is => 'rw'
);

has 'result_count' => (
	is => 'ro'
);

has 'total_pages' => (
	is => 'ro',
	default => sub {
		my $self = shift;
		return 0 unless $self->result_count;
		my $pages = $self->result_count / $self->results_per_page;
		$pages++ and ($pages = int $pages) unless $pages == int $pages;
		return $pages;
	}
);

has 'start_at' => (
	is => 'rw',
	default => 1,
);

has 'current_page' => (
	is => 'method',
	code => sub {
		my $self = shift;
		return int (($self->start_at / $self->results_per_page) + 1)
	}
);

has 'username', is => 'rw';
has 'passworrd', is => 'rw';

has 'login' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my ($name,$pw) = @_;
		$self->mech->get($self->login_url);
		#print Dumper $self->mech; <STDIN>;
		local $| = 1;
		print "logging in... ";
		$self->mech->submit_form (
			form_number => 1,
			fields => {
				p_un => $name, 
				p_pw => $pw
			}
		);
		if ($self->mech->status == 200) {
			die 'login error' unless $self->mech->content =~ /You are logged in as $name/;
			say qq{ok. logged in as "$name"};
			$self->{logged_in} = 1;
			return 1;
		} else {
			die 'http error '.$self->mech->status." on login attempt at $self->{login_url}\n";
		}
	}
);

has 'logged_in' => (
	is => 'ro',
);

has 'query' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my %params = @_;
		my $q_str = $self->search_url;
		$q_str .= '?jrec='.$self->start_at;
		$q_str .= '&rg='.$self->results_per_page; # max results # limit 200 if not logged in 
		$q_str .= '&of=xm';
		for my $key (keys %params) {
			my $term = $params{$key};
			$q_str .= "&p=$key:$term";
		}
		say 'issuing http request: '.$q_str; #  if ! $self->result_count;
		my $t = time;
		my $response = $self->mech->get($q_str);
		$t = time - $t;
		say " -> response time: $t seconds";
		return $self->mech->content;
	}	
);

has 'get' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my %params = @_;
		my $retfields = \$params{return_fields};
		
	}
);

has 'iterate' => (
	is => 'method',
	code => sub {
		my $self = shift;
		my %params = @_;
		my $callback = $params{callback};
		delete $params{callback} if $callback;
		my $q = \%params;
		$self->parameters(%$q);
		die unless scalar keys %$q > 0;
		$self->login($self->unsername,$self->password) if ! $self->logged_in;
		my $xml = $self->query(%$q);
		$xml =~ /<!-- Search-Engine-Total-Number-Of-Results: (\d+) -->/;
		if ($1 and ! $self->result_count) {
			$self->{result_count} ||= $1;
			say 'ok. there are '.$self->result_count.' results on '.$self->total_pages.' pages';
			say join '', 'page ',$self->current_page,'/',$self->total_pages,': ';
		} elsif (! $self->result_count) {
			say $self->mech->content;
			say 'no results' and return;
		}
		my ($i,$t) = (0,time);
		MARC::Set->new->iterate_xml (
			string => $xml,
			callback => sub {
				my ($set,$record) = @_;
				$callback->($record) if $callback;
				my $check = $i / 50;
				local $| = 1;
				my $c = $i + 1;
				if ($check == int $check) {
					print "$c... "; 
				} elsif ($i == ($self->results_per_page - 1)) {
					$t = time - $t;
					say "$c results processed in $t seconds";
				}
				$i++;
			}
		);
		#say $self->mech->uri;
		$t = time - $t;
		say "$i results processed in $t seconds" and return if $self->current_page == $self->total_pages or $self->total_pages == 0;
		$self->{start_at} += $self->results_per_page;
		say join '', 'getting page ', $self->current_page,'/',$self->total_pages,'... ';
		$self->iterate(@_);
	}
);
