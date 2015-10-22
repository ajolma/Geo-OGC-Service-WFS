=pod

=head1 NAME

Geo::OGC::Service::WFS - Perl extension for geospatial web feature services

=head1 SYNOPSIS

The process_request method of this module is called by the
Geo::OGC::Service framework.

=head1 DESCRIPTION

This module aims to provide the operations defined by the Open
Geospatial Consortium's Web Feature Service standard. These are (in
the version 2.0 of the standard)

GetCapabilities (discovery operation)
DescribeFeatureType (discovery operation)
GetPropertyValue (query operation)
GetFeature (query operation)
GetFeatureWithLock (query & locking operation)
LockFeature (locking operation)
Transaction (transaction operation)
CreateStoredQuery (stored query operation)
DropStoredQuery (stored query operation)
ListStoredQueries (stored query operation)
DescribeStoredQueries (stored query operation)

This module is a plugin for the Geo::OGC::Service framework.

=head2 EXPORT

None by default.

=head2 METHODS

=cut

package Geo::OGC::Service::WFS;

use 5.022000;
use strict;
use warnings;

our $VERSION = '0.01';

=pod

=head3 process_request

=cut

sub process_request {
    my ($self, $responder) = @_;
    error($responder, "Not ready yet!");
}


sub error {
    my ($responder, $msg) = @_;
    my $writer = $responder->([500, [ 'Content-Type' => 'text/plain',
                                      'Content-Encoding' => 'UTF-8' ]]);
    $writer->write($msg);
    $writer->close;
}

1;
__END__

=head1 SEE ALSO

Discuss this module on the Geo-perl email list.

L<https://list.hut.fi/mailman/listinfo/geo-perl>

For PSGI/Plack see 

L<http://plackperl.org/>

=head1 AUTHOR

Ari Jolma, E<lt>ari.jolma at gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015 by Ari Jolma

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.22.0 or,
at your option, any later version of Perl 5 you may have available.

=cut
