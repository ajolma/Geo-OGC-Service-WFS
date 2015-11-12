use strict;
use warnings;

use Test::More tests => 4;
use Plack::Test;
use HTTP::Request::Common;
use Geo::OGC::Service;
use XML::LibXML;
BEGIN { use_ok('Geo::OGC::Service::WFS') };

my ($a,$b,$c) = $Geo::GDAL::GDAL_VERSION =~ /^(\d+)\.(\d+)\.(\d+)/;
my $version = 1000000*$a+10000*$b+100*$c;
my $gdal_version = $version;
#$gdal_version = 2000100;

{
    no warnings;
    package Geo::GDAL;
    sub VersionInfo {
        return $version;
    }
}

my $app = Geo::OGC::Service->new({ config => 't/Geo-OGC-Service-WFS.conf', 
                                   services => { WFS => 'Geo::OGC::Service::WFS' }})->to_app;

SKIP: {
    skip "GDAL version is < 2.1", 1 if $gdal_version < 2010000;

    test_psgi $app, sub {
        my $cb = shift;
        my $res = $cb->(GET "/?service=WFS&request=GetFeature&typename=test");
        my $parser = XML::LibXML->new(no_blanks => 1);
        my $dom;
        eval {
            $dom = $parser->load_xml(string => $res->content);
        };
        if ($@) {
            is $@, 0;
        } else {
            is 1, 1;
        }
    };

}

SKIP: {
    skip "GDAL version is < 2.0.2", 1 if $gdal_version < 2000200;

    $version = 2000200;

    test_psgi $app, sub {
        my $cb = shift;
        my $res = $cb->(GET "/?service=WFS&request=GetFeature&typename=test");
        my $parser = XML::LibXML->new(no_blanks => 1);
        my $dom;
        eval {
            $dom = $parser->load_xml(string => $res->content);
        };
        if ($@) {
            is $@, 0;
        } else {
            is 1, 1;
        }
    };

}

$version = 2000000;

test_psgi $app, sub {
    my $cb = shift;
    my $res = $cb->(GET "/?service=WFS&request=GetFeature&typename=test");
    my $parser = XML::LibXML->new(no_blanks => 1);
    my $dom;
    eval {
        $dom = $parser->load_xml(string => $res->content);
    };
    if ($@) {
        is $@, 0;
    } else {
        is 1, 1;
    }
};
