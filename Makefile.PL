use 5.022000;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Geo::OGC::Service::WFS',
    VERSION_FROM      => 'lib/Geo/OGC/Service/WFS.pm', # finds $VERSION, requires EU::MM from perl >= 5.5
    PREREQ_PM         => { "Modern::Perl" => 0,
                           "File::Basename" => 0,
                           "Capture::Tiny" => 0,
                           "Clone" => 0,
                           "JSON" => 0,
                           "DBI" => 0,
                           "XML::LibXML::PrettyPrint" => 0, # for debugging
                           "Geo::GDAL" => 2.00013, # we need a working vsistdout implementation, which will really be only in 2.0002, 2.00013 will get and build the latest 2.0 branch GDAL
                           "Geo::OGC::Service" => 0.05,
    },
    TEST_REQUIRES     => { "Plack::Test" => 0,
                           "HTTP::Request::Common" => 0,
                           "XML::LibXML" => 0,
                           "XML::SemanticDiff" => 0 },
    ABSTRACT_FROM     => 'lib/Geo/OGC/Service/WFS.pm', # retrieve abstract from module
    AUTHOR            => 'Ari Jolma <ari.jolma at gmail.com>',
    LICENSE           => 'perl'
);
