# Class::API
# J Bukhari 2016

use v5.10;
use strict;
use warnings;
use feature qw/state say/;

package META;
use Carp;
use Data::Dumper;

sub DATA {
	state $instance;
	unless (defined $instance) {
		$instance = bless {},shift;
		$instance->{templates} = {
			'ro_attribute' => \&_ro_attribute,
			'ro' => \&_ro_attribute,
			'rw_attribute' => \&_rw_attribute,
			'rw' => \&_rw_attribute,
			'public_method' => \&_public_method,
			'method' => \&_public_method,
			'alias' => \&_alias
		};
	}
	return $instance;
}

sub req {_list(@_)}
sub opt {_list(@_)}
sub ro {_list(@_)}
sub rw {_list(@_)}
sub default {_list(@_)}
sub method {_list(@_)}
sub alias {_list(@_)}
sub templates {_list(@_)}
sub all {_list(@_)}
	
sub _list {
	my ($self,$class,$val) = @_;
	my $field = \$self->{$class}->{(split '::',(caller(1))[3])[-1]};
	$val && ($$field->{$val} = 1);
	$$field && return keys %{$$field};
	return ();
}

DEFAULT_TEMPLATES: {
	sub _ro_attribute {
		my ($name,$properties,$args) = @_;
		my $self = shift @$args;
		@$args && confess qq/"$name" is read-only/;
		my $att = \$self->{$name};
		$$att && return $$att;
		my ($default,$trigger) = @{$properties}{qw/default trigger/};
		if ($default && (ref $default eq 'CODE')) {
			$$att = $default->($self,$att);
		} else {
			$default && ($$att //= $default);
		}
		$trigger && $trigger->($self,$$att);
		return $$att;
	}
	
	sub _rw_attribute {
		my ($name,$properties,$args) = @_;
		my ($self,$val) = @$args;
		my ($default,$trigger) = @{$properties}{qw/default trigger/};
		my $att = \$self->{$name};
		if ($default && (ref $default eq 'CODE')) {
			$$att //= $default->($self,$att);
		} else {
			$default && ($$att //= $default);
		}
		if (defined $val) {
			$trigger && ($val = $trigger->($self,$val,$$att));
			$$att = $val;
			return $self;
		} else {
			$trigger && ($$att //= $trigger->($self,undef,$att));
		}
		return $$att;
	}
	
	sub _public_method {
		my ($name,$properties,$args) = @_;
		my ($self,@args) = @$args;
		my ($code) = @{$properties}{qw/code/};
		return $code->($self,@args);
	}
	
	sub _alias {
		my ($name,$properties,$args) = @_;
		my $self = shift @$args;
		my $method = $properties->{method};
		return $self->$method(@$args);
	}
}

package API;
#use Scalar::Util qw/blessed/;
use Carp;
use Data::Dumper;

use Exporter qw/import/;
our @ISA =   qw/Exporter/;
our @EXPORT = qw/has template new ro rw methods all/;
#our @EXPORT_OK = qw/ro rw methods all/;
#our %EXPORT_TAGS = (DEFAULT => [qw/has new/], introspect => [qw/has new ro rw methods all/]);

INITIALIZER: {
	sub has {
		_make_method(_register_method((caller(0))[0],@_));
	}
	
	sub template {
		my ($name,%params) = @_;
		META->DATA->{templates}->{$name} = $params{code};
	}
	
	sub _register_method {
		my ($class,$name,%properties) = @_;
		my $props = \%properties;
		my $m = META->DATA;
		my $is = $props->{is};
		$is || croak qq/"is" required to make method "$name"/;
		#ref _templates()->{$is} eq 'CODE' || croak qq/invalid "is" for method "$name"/;
		_register_param($class,$name,$props);
		$m->$is($class,$name) if $m->can($is);
		$m->all($class,$name);
		return ($class,$name,$props);
	}
	
	sub _register_param {
		my ($class,$name,$props) = @_;
		if (defined (my $param = $props->{param})) {
			my $m = META->DATA;
			#my $check_rw = sub {$props->{is} eq 'rw' || croak qq/"$name" must be rw to allow as parameter/};
			$param == 1 && $m->req($class,$name) and return;
			$param == 0 && $m->opt($class,$name) and return;
			croak qq/property "param" of method "$name" must be 0 (optional) or 1 (required)/;
		}
		return;
	}
	
	sub _make_method {
		my ($class,$name,$props) = @_;
		
		my $template = META->DATA->{templates}->{$props->{is}};
		
		no strict 'refs';
		*{"${class}::$name"} = sub {
			# local *__ANON__ = $name; # allows caller() to get method name. undocumented.
			$template->($name,$props,[@_]);
		};
	}
}

CONSTRUCTOR: {
	sub new {	
		my $class = shift;
		my $self = bless {class => $class}, $class;
		$self->API::_build_args(@_);
		$self->$_ for META->DATA->default($class);
		return $self;
	}

	sub _build_args {
		my ($self,@args) = @_;
		my $class = $self->{class};
		my $m = META->DATA;
		my @req = $m->req($class);
		my @opt = $m->opt($class);
		my @all = $m->all($class);
		INHERIT: {
			no strict 'refs';
			for my $parent (@{"${class}::ISA"}) {
				for my $req ($m->req($parent)) {
					grep {$_ eq $req} @all && push @req, $req;
				}
				for my $opt ($m->opt($parent)) {
					grep {$_ eq $opt} @all && push @opt, $opt;
				}
			}
		}
		my %args = @args;
		$args{$_} || croak qq/missing required agument "$_"/ for @req;
		for my $arg (@args) {
			next if ! $args{$arg};
			croak qq/unexpected argument "$arg"/ if ! grep {$arg eq $_} (@req,@opt);
			$self->{params}->{$arg} = $args{$arg};
			$self->$arg($args{$arg}) if $args{$arg};
		}
		return;
	}
	
	sub _this {
		return (split '::',(caller(3))[3])[-1];
	}
}

INTROSPECT: {
	sub ro {_get(@_)}
	sub rw {_get(@_)}
	sub methods {_get(@_)}
	sub all {_get(@_)}
	
	sub _get {
		my $self = shift;
		my $class = $self->{class};
		my $stuff = (split '::',(caller(1))[3])[-1];
		$stuff eq 'methods' && chop $stuff;
		return META->DATA->$stuff($class);
	}
}

'xaipe';

__END__

