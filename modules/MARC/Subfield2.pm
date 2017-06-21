use strict;
use warnings;
use feature 'say';

package MARC::Subfield;
use API;

has 'code' => (
	is => 'rw',
);
has 'value' => (
	is => 'rw',
);

