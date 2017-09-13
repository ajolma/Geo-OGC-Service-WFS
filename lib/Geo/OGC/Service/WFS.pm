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

 GetCapabilities (discovery operation) (*)
 DescribeFeatureType (discovery operation) (*)
 GetPropertyValue (query operation)
 GetFeature (query operation) (*)
 GetFeatureWithLock (query & locking operation)
 LockFeature (locking operation)
 Transaction (transaction operation) (*)
 CreateStoredQuery (stored query operation)
 DropStoredQuery (stored query operation)
 ListStoredQueries (stored query operation)
 DescribeStoredQueries (stored query operation)

(*) are at least somehow implemented.

This module is a plugin for the Geo::OGC::Service framework.

=head2 EXPORT

None by default.

=head2 METHODS

=cut

package Geo::OGC::Service::WFS;

use 5.010000; # say // and //=
use feature "switch";
use Carp;
use File::Basename;
use Modern::Perl;
use Capture::Tiny ':all';
use Clone 'clone';
use JSON;
use DBI;
use Geo::GDAL;
use HTTP::Date;
use File::MkTemp;

use Data::Dumper;
use XML::LibXML::PrettyPrint;

use Geo::OGC::Service;
use Geo::OGC::Service::Filter ':all';
use vars qw(@ISA);
push @ISA, qw(Geo::OGC::Service::Filter);

our $VERSION = '0.10';

# GDAL and PostgreSQL data type to XML data type
our %type_map = (
    geometry => "gml:GeometryPropertyType",
    Short => "xs:short",
    Integer => "xs:integer",
    Integer64 => "xs:long",
    Real => "xs:double",
    integer => "xs:integer",
    int => "xs:integer",
    bigint => "xs:long",
    decimal => "xs:decimal",
    numeric => "xs:decimal",
    real => "xs:float",
    double => "xs:double",
    "double precision" => "xs:decimal",
    timestamp => "xs:date",
    "timestamp with time zone" => "xs:date",
    date => "xs:date",
    time => "xs:time",
    "time with time zone" => "xs:time",
    boolean => "xs:boolean",
    );

our %well_known_w3_ns = (
    'xmlns:xlink' => "http://www.w3.org/1999/xlink",
    'xmlns:xs'    => "http://www.w3.org/2001/XMLSchema",
    'xmlns:xsi'   => "http://www.w3.org/2001/XMLSchema-instance",
    );

our %wfs_1_1_0_ns = (
    %well_known_w3_ns,
    xmlns => "http://www.opengis.net/wfs",
    'xmlns:wfs' => "http://www.opengis.net/wfs",
    'xmlns:gml' => "http://www.opengis.net/gml",
    'xmlns:ows' => "http://www.opengis.net/ows",
    'xmlns:ogc' => "http://www.opengis.net/ogc",
    'xsi:schemaLocation' => "http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
    );

our %wfs_2_0_0_ns = (
    %well_known_w3_ns,
    xmlns => "http://www.opengis.net/wfs/2.0",
    'xmlns:wfs' => "http://www.opengis.net/wfs/2.0",
    'xmlns:gml' => "http://schemas.opengis.net/gml",
    'xmlns:ows' => "http://www.opengis.net/ows/1.1",
    'xmlns:fes' => "http://www.opengis.net/fes/2.0",
    'xmlns:srv' => "http://schemas.opengis.net/iso/19139/20060504/srv/srv.xsd",
    'xmlns:gmd' => "http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd",
    'xmlns:gco' => "http://schemas.opengis.net/iso/19139/20060504/gco/gco.xsd",
    'xsi:schemaLocation' => "http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd"
    );

# Value in request => GDAL GML creation option
our %OutputFormats = (
    'XMLSCHEMA' => 'GML2',
    'text/xml; subtype=gml/2.1.2' => 'GML2',
    'text/xml; subtype=gml/3.1.1' => 'GML3',
    'text/xml; subtype=gml/3.2' => 'GML3.2',
    'GML3Deegree' => 'GML3Deegree',
    'application/gml+xml; version=3.2' => 'GML3.2',
    'application/json' => 'GeoJSON',
    );

our @GDAL_GML_Creation_options = (qw/XSISCHEMAURI XSISCHEMA PREFIX
STRIP_PREFIX TARGET_NAMESPACE FORMAT GML3_LONGSRS SRSNAME_FORMAT
SRSDIMENSION_LOC WRITE_FEATURE_BOUNDED_BY SPACE_INDENTATION GML_ID
NAME DESCRIPTION/);

=pod

=head3 process_request

The entry method into this service. Fails unless the request is well known.

=cut

sub process_request {
    my ($self, $responder) = @_;
    $self->parse_request;
    $self->{debug} = $self->{config}{debug} // 0;
    $self->{responder} = $responder;
    if ($self->{parameters}{debug}) {
        $self->error({ 
            debug => { 
                config => $self->{config}, 
                parameters => $self->{parameters}, 
                env => $self->{env},
                request => $self->{request} 
            } });
        return;
    }
    if ($self->{debug}) {
        $self->log({ request => $self->{request}, parameters => $self->{parameters} });
        my $parser = XML::LibXML->new(no_blanks => 1);
        my $pp = XML::LibXML::PrettyPrint->new(indent_string => "  ");
        if ($self->{posted}) {
            my $dom = $parser->load_xml(string => $self->{posted});
            $pp->pretty_print($dom); # modified in-place
            say STDERR "posted:\n",$dom->toString;
        }
        if ($self->{filter}) {
            my $dom = $parser->load_xml(string => $self->{filter});
            $pp->pretty_print($dom); # modified in-place
            say STDERR "filter:\n",$dom->toString;
        }
    }
    for ($self->{request}{request} // '') {
        if (/^GetCapabilities/ or /^capabilities/) { $self->GetCapabilities() }
        elsif (/^DescribeFeatureType/)             { $self->DescribeFeatureType() }
        elsif (/^GetFeature/)                      { $self->GetFeature() }
        elsif (/^Transaction/)                     { $self->Transaction() }
        elsif (/^$/)                               { 
            $self->error({ exceptionCode => 'MissingParameterValue',
                           locator => 'request' }) }
        else                                       { 
            $self->error({ exceptionCode => 'InvalidParameterValue',
                           locator => 'request',
                           ExceptionText => "$self->{parameters}{request} is not a known request" }) }
    }
}

=pod

=head3 GetCapabilities

Service the GetCapabilities request. The configuration JSON is used to
control the contents of the reply. The config contains root keys, which
are either simple or complex. The simple root keys are

 Key                 Default             Comment
 ---                 -------             -------
 version             2.0.0
 Title               WFS Server
 resource                                required
 ServiceTypeVersion
 AcceptVersions      2.0.0,1.1.0,1.0.0
 Transaction                             optional

FeatureTypeList is a root key, which is a list of FeatureType
hashes. A FeatureType defines one layer (key Layer exists) or a group
of layers (key Layer does not exist). The key DataSource is required -
it is a GDAL data source string. Currently only PostGIS layer groups
are supported.

If Layer is specified, then also keys Name, Title, Abstract, and
DefaultSRS are required, and LowerCorner is optional.

For PostGIS groups the default is open. The names of the layers are
constructed from a prefix, which is a required key, name of the table
or view, and the name of the geometry column, e.g.,
prefix.table.geom. The listed layers can be restricted with keys
TABLE_TYPE, TABLE_PREFIX, deny, and allow. TABLE_TYPE, deny, and allow
are, if exist, hashes. The keys of the first are table types as DBI
reports them, and the keys of the deny and allow are layer names (the
prefix can be left out). TABLE_PREFIX is a string that sets a
requirement for the table/view names for to be listed.

=cut

sub GetCapabilities {
    my ($self) = @_;

    my $writer = Geo::OGC::Service::XMLWriter::Caching->new();

    
    
    my %ns;
    if ($self->{version} eq '2.0.0') {
        %ns = (%wfs_2_0_0_ns);
        # updateSequence="260" ?
    } else {
        %ns = (%wfs_1_1_0_ns);
    }
    $ns{version} = $self->{version};
    
    $writer->open_element('wfs:WFS_Capabilities', \%ns);
    $self->DescribeService($writer);
    $self->OperationsMetadata($writer);
    $self->FeatureTypeList($writer);
    $self->Filter_Capabilities($writer);
    $writer->close_element;
    $writer->stream($self->{responder});
}

sub OperationsMetadata  {
    my ($self, $writer) = @_;
    $writer->open_element('ows:OperationsMetadata');
    my @versions = split /,/, $self->{config}{AcceptVersions} // '2.0.0,1.1.0,1.0.0';
    $self->Operation($writer, 'GetCapabilities',
                     { Get => 1, Post => 1 },
                     [{service => ['WFS']}, 
                      {AcceptVersions => \@versions}, 
                      {AcceptFormats => ['text/xml']}]);
    $self->Operation($writer, 'DescribeFeatureType', 
                     { Get => 1, Post => 1 },
                     [{outputFormat => [sort keys %OutputFormats]}]);
    $self->Operation($writer, 'GetFeature',
                     { Get => 1, Post => 1 },
                     [{resultType => ['results']}, {outputFormat => [sort keys %OutputFormats]}]);
    $self->Operation($writer, 'Transaction',
                     { Get => 1, Post => 1 },
                     [{inputFormat => ['text/xml; subtype=gml/3.1.1']}, 
                      {idgen => ['GenerateNew','UseExisting','ReplaceDuplicate']},
                      {releaseAction => ['ALL','SOME']}
                     ]);
    # constraints
    my %constraints = (
        ImplementsBasicWFS => 1,
        ImplementsTransactionalWFS => 1,
        ImplementsLockingWFS => 0,
        KVPEncoding => 1,
        XMLEncoding => 1,
        SOAPEncoding => 0,
        ImplementsInheritance => 0,
        ImplementsRemoteResolve => 0,
        ImplementsResultPaging => 0,
        ImplementsStandardJoins => 1,
        ImplementsSpatialJoins => 1,
        ImplementsTemporalJoins => 1,
        ImplementsFeatureVersioning => 0,
        ManageStoredQueries => 0,
        PagingIsTransactionSafe => 1,
        );
    for my $key (keys %constraints) {
        $writer->element('ows:Constraint', {name => $key}, 
                         [['ows:NoValues'], 
                          ['ows:DefaultValue' => $constraints{$key} ? 'TRUE' : 'FALSE']]);
    }
    $writer->element('ows:Constraint', {name => 'QueryExpressions'}, 
                     ['ows:AllowedValues' => ['ows:Value' => 'wfs:Query']]);
    $writer->close_element;
}

sub FeatureTypeList  {
    my ($self, $writer) = @_;
    $writer->open_element('wfs:FeatureTypeList');
    my $operations = $self->{config}{Operations} // 'Query';
    my @operations = list2element('wfs:Operation', $operations);
    $writer->element('wfs:Operations', \@operations);

    my $list_layer = sub {
        my ($type) = @_;
        # should we test that the layer is really available?
        my @formats;
        for my $format (sort keys %OutputFormats) {
            push @formats, ['wfs:Format', $format];
        }
        my @FeatureType = (
            ['wfs:Name', $type->{Name}],
            ['wfs:Title', $type->{Title}],
            ['wfs:Abstract', $type->{Abstract}],
            ['wfs:DefaultSRS', $type->{DefaultSRS}],
            ['wfs:OtherSRS', 'EPSG:3857'],
            ['wfs:OutputFormats', \@formats]);
        push @FeatureType, ['ows:WGS84BoundingBox', {dimensions=>2}, 
                            [['ows:LowerCorner',$type->{LowerCorner}],
                             ['ows:UpperCorner',$type->{UpperCorner}]]]
                                 if exists $type->{LowerCorner};
        push @FeatureType, ['wfs:Operations', 
                            [list2element('wfs:Operation', $type->{Operations})]] if exists $type->{Operations};
        $writer->element('wfs:FeatureType', \@FeatureType);
    };

    eval {
        $self->read_feature_type_list;
        for my $type (@{$self->{feature_types}}) {
            for my $t (@$type) {
                $list_layer->($t);
            }
        }
    };
    say STDERR $@ if $@;
    $writer->close_element;
}

=pod

=head3 DescribeFeatureType

Service the DescribeFeatureType request.

=cut

sub DescribeFeatureType {
    my ($self) = @_;

    my @typenames;
    for my $query (@{$self->{request}{queries}}) {
        push @typenames, split(/\s*,\s*/, $query->{typename});
    }

    unless (@typenames) {
        $self->error({ exceptionCode => 'MissingParameterValue',
                       locator => 'typeName' },
                     [$self->CORS]);
        return;
    }
    
    my %types;

    for my $name (@typenames) {
        eval {
            $types{$name} = $self->get_feature_type($name);
        };
        unless ($types{$name}) {
            $@ =~ s/ at \/.*//;
            $self->error({ exceptionCode => 'InvalidParameterValue',
                           locator => 'typeName',
                           ExceptionText => $@ },
                         [$self->CORS]);
            return;
        }
    }

    my $writer = Geo::OGC::Service::XMLWriter::Caching->new();
    
    $writer->open_element(schema => { 
        targetNamespace => $self->{config}{targetNamespace},
        xmlns => "http://www.w3.org/2001/XMLSchema",
        'xmlns:gml' => "http://schemas.opengis.net/gml",
        elementFormDefault => "qualified",
        attributeFormDefault => "unqualified" });
    
    $writer->element(import => { 
        namespace => "http://www.opengis.net/gml",
        schemaLocation => "http://schemas.opengis.net/gml/2.1.2/feature.xsd" });

    for my $name (sort keys %types) {
        my $type = $types{$name};
        next if $type->{"gml:id"} && $type->{Name} eq $type->{"gml:id"};

        my ($pseudo_credentials) = pseudo_credentials($type);
        my @elements;
        for my $property (keys %{$type->{Schema}}) {

            next if $pseudo_credentials->{$property};

            my $minOccurs = 0;
            push @elements, ['element', 
                             { name => $type->{Schema}{$property}{out_name},
                               type => $type->{Schema}{$property}{out_type},
                               minOccurs => "$minOccurs",
                               maxOccurs => "1" } ];

        }
        $writer->element(
            'complexType', {name => $type->{Name}.'Type'},
            ['complexContent', 
             ['extension', { base => 'gml:AbstractFeatureType' },
              ['sequence', \@elements
              ]]]);
        $writer->element(
            'element', { name => $type->{Name},
                         type => 'ogr:'.$type->{Name}.'Type',
                         substitutionGroup => 'gml:_Feature' } );
    }
    
    $writer->close_element();
    $writer->stream($self->{responder});
}

=pod

=head3 GetPropertyValue

Not yet implemented.

=cut

sub GetPropertyValue {
}

=pod

=head3 GetFeature

Service the GetFeature request. The response is generated with the GML
driver of GDAL using options TARGET_NAMESPACE, PREFIX, and
FORMAT. They are from the root of the configuration (TARGET_NAMESPACE
from FeatureType falling back to root) falling back to default ones
"http://www.opengis.net/wfs", "wfs", and "GML3.2". The content type of
the reponse is from configuration (root key 'Content-Type') falling
back to default "text/xml".

The "gml:id" attribute of the features is GDAL generated or from a
field defined by the key "gml:id" in the configuration (in the hash
with layer name falling back to FeatureType).

=cut

sub GetFeature {
    my ($self) = @_;

    my $query = $self->{request}{queries}[0]; # actually we should loop through all queries?
    my ($typename) = split(/\s*,\s*/, $query->{typename});
    
    unless ($typename) {
        $self->error({ exceptionCode => 'MissingParameterValue',
                       locator => 'typeName' },
                     [$self->CORS]);
        return;
    }

    my $type;
    eval {
        $type = $self->get_feature_type($typename);
    };
    unless ($type) {
        $@ =~ s/ at \/.*//;
        $self->error({ exceptionCode => 'InvalidParameterValue',
                       locator => 'typeName',
                       ExceptionText => $@ },
                     [$self->CORS]);
        return;
    }

    my $ds = Geo::OGR::Open($type->{DataSource});
    my $layer;
    my $epsg = $query->{EPSG} // epsg_number($type->{DefaultSRS});

    my @bbox;
    my $bbox_crs;
    if ($query->{BBOX}) {
        @bbox = @{$query->{BBOX}};
        $bbox_crs = 4326; # WFS 1.1.0 14.3.3
        if (@bbox == 5) {
            $bbox_crs = epsg_number(pop @bbox);
        }
    }

    if ($type->{DataSource} =~ /^PG:/) {

        my $filter = filter2sql($query->{filter}, $type) // '';

        # pseudo_credentials: these fields are required to be in the filter and they are not included as attributes
        my ($pseudo_credentials, @pseudo_credentials) = pseudo_credentials($type);
        if (@pseudo_credentials) {
            # test for pseudo credentials in filter
            my $pat1 = "\\(\\(\"$pseudo_credentials[0]\" = '.+?'\\) AND \\(\"$pseudo_credentials[1]\" = '.+?'\\)\\)";
            my $pat2 = "\\(\\(\"$pseudo_credentials[1]\" = '.+?'\\) AND \\(\"$pseudo_credentials[0]\" = '.+?'\\)\\)";
            my $n = join(' and ', @pseudo_credentials);
            unless ($filter and ($filter =~ /$pat1/ or $filter =~ /$pat2/)) {
                $self->error({ exceptionCode => 'InvalidParameterValue',
                               locator => 'filter',
                               ExceptionText => "Not authorized. Please provide '$n' in filter." });
                return;
            }
        }

        my $Geometry = $type->{GeometryColumn};
        
        my @columns;
        # reverse the field names
        my $gml_id = $type->{"gml:id"} // '';
        for my $column (keys %{$type->{table}{columns}}) {
            next if $pseudo_credentials->{$column};
            my $Column = '"'.$column.'"';

            my $name = $type->{table}{columns}{$column}{out_name};
            next if $query->{properties} && not $query->{properties}{$name};
            my $Name = '"'.$name.'"';

            if ($Column eq $Geometry) {
                push @columns, "ST_Transform($Column,$epsg) as $Name";
            } elsif ($type->{table}{columns}{$column}{out_type} eq 'gml:GeometryPropertyType') {
                # only one geometry property in the output
            } else {
                $filter =~ s/$name/$column/g if $filter;
                $name = 'gml_id' if $column eq $gml_id;
                push @columns, "$Column as $Name";
            }
        }
        
        my $sql = "SELECT ".join(',',@columns)." FROM $type->{Table} WHERE ST_IsValid($Geometry)";

        if ($filter) {
            $filter =~ s/GeometryColumn/$Geometry/g;
            $sql .= " AND $filter";
        }
        
        if (@bbox) {
            my $bbox = join(",", @bbox);
            $sql .= " AND (ST_Transform($Geometry,$epsg) && ST_Transform(ST_MakeEnvelope($bbox,$bbox_crs),$epsg))";
        }
        
        print STDERR "$sql\n" if $self->{debug};
        eval {
            $layer = $ds->ExecuteSQL($sql);
        };
        unless ($layer) {
            say STDERR $sql;
            $@ =~ s/ at \/.*//;
            $self->error({ exceptionCode => 'Error',
                           ExceptionText => 'Internal error' });
            return;
        }
        
    } else {
        
        $layer = $ds->GetLayer($type->{LayerName});
        $layer->SetSpatialFilterRect(@bbox) if @bbox;
        
    }

    my $i = 0;
    my $count = min($query->{count}, $self->{config}{max_count}, 1000000);
    my $result_type = $query->{resulttype} // 'results'; # real data instead of number of features
    my ($content_type, $driver);

    # for GML creation options see http://www.gdal.org/drv_gml.html
    # for GeoJSON creation options see http://www.gdal.org/drv_geojson.html

    # the FORMAT, TARGET_NAMESPACE, and PREFIX need to be set for OpenLayers
    # the following has worked
    # OpenLayers 2: FORMAT (not set), TARGET_NAMESPACE (http://ogr.maptools.org/), and PREFIX (ogr)
    # OpenLayers 4: FORMAT (GML3), TARGET_NAMESPACE (http://www.opengis.net/gml), and PREFIX (ogr)

    # tweaked GML is for getting transactions to work with OpenLayers v4

    my %creation_options;
    $creation_options{FORMAT} = $self->{request}{outputformat} // $self->{config}{FORMAT} // $type->{FORMAT} // 'GML2';
    for my $format ($creation_options{FORMAT}) {
        # convert some known values to those understood by GDAL
        $format = $OutputFormats{$format} if $format && $OutputFormats{$format};
        if ($format =~ /GML/) {
            for my $key (@GDAL_GML_Creation_options) {
                next if $key eq 'FORMAT';
                next unless exists $self->{config}{$key} || exists $type->{$key};
                $creation_options{$key} = $self->{config}{$key} // $type->{$key};
            }
            $content_type = $self->{config}{'Content-Type'} // 'text/xml; charset=utf-8';
            $driver = Geo::OGR::Driver('GML');
        } elsif ($format =~ /JSON/) {
            $content_type = 'application/json; charset=utf-8';
            $driver = Geo::OGR::Driver('GeoJSON');
        } else {
            $self->error({ exceptionCode => 'Error',
                           locator => 'outputFormat',
                           ExceptionText => "Format '$format' is not supported." });
            return;
        }
    }

    print STDERR "format is $creation_options{FORMAT}\n" if $self->{debug};

    if ($creation_options{FORMAT} eq 'tweaked GML') {
        my $output = Geo::OGC::Service::XMLWriter::Streaming->new($self->{responder});
        $output->prolog;
        my $ns = $creation_options{PREFIX};
        my %ns = (
            %wfs_1_1_0_ns,
            "xmlns:$ns" => $creation_options{TARGET_NAMESPACE},
            #numberOfFeatures => "13",
            #timeStamp => "2017-09-12T12:01:14.358Z",
            );
        #$ns{'xsi:schemaLocation'} .=
            #"&amp;version=1.1.0".
            #"&amp;request=DescribeFeatureType".
            #"&amp;typeName=smartsea%3Awfs%3Awfs%3Ageometry"
        $output->open_element('wfs:FeatureCollection' => \%ns);
        $output->open_element('gml:featureMembers');
        $layer->ResetReading;
        my $gml_polygon = '<gml:Polygon srsName="http://www.opengis.net/gml/srs/epsg.xml#3857" srsDimension="2">';
        while (my $f = $layer->GetNextFeature) {
            my $row = $f->Row;
            $output->open_element("$ns:wfs" => {'gml:id' => $row->{id}});
            for my $key (keys %$row) {
                next if $key eq 'FID';
                next if $key eq 'id';
                my $value = $row->{$key};
                if ($key eq 'Geometry' or $key eq 'geometryProperty') {
                    my $type = $value->GeometryType;
                    $type =~ s/M//;
                    $type =~ s/Z//;
                    $type =~ s/25D//;
                    my $points = $value->Points;
                    my $gml;
                    if ($type eq 'Polygon') {
                        my $exterior = $points->[0];
                        $gml = $gml_polygon;
                        $gml .= '<gml:exterior><gml:LinearRing><gml:posList>';
                        $_ = "@$_[0..1]" foreach @$exterior;
                        $gml .= join(' ', @$exterior);
                        $gml .= '</gml:posList></gml:LinearRing></gml:exterior>';
                        $gml .= '</gml:Polygon>';
                    }
                    $output->element("$ns:geometry", $gml);
                } else {
                    $output->element("$ns:".lc($key), $value);
                }
            }
            $output->close_element;
            $i++;
            last if $i >= $count;
        }
        $output->close_element;
        $output->close_element;
        return;
    }

    my $writer = $self->{responder}->([200, [ 'Content-Type' => $content_type, $self->CORS ]]);
    my $output = Geo::OGR::Driver('GML')->Create($writer, \%creation_options );
    
    my $l2 = $output->CreateLayer($type->{Name});
    my $d = $layer->GetLayerDefn;
    for (0..$d->GetFieldCount-1) {
        my $f = $d->GetFieldDefn($_);
        $l2->CreateField($f);
    }
    $layer->ResetReading;
    while (my $f = $layer->GetNextFeature) {
        $l2->CreateFeature($f);
        $i++;
        last if defined $count and $i >= $count; 
    }
    
    print STDERR "$i features served, max is $count\n" if $self->{debug};
}

=pod

=head3 GetFeatureWithLock

Not yet implemented.

=cut

sub GetFeatureWithLock {
}

=pod

=head3 LockFeature

Not yet implemented.

=cut

sub LockFeature {
}

=pod

=head3 Transaction

Service the Transaction request. Transaction support is only
implemented for PostgreSQL data sources. "Replace" operations
are not yet supported.

=cut

sub Transaction {
    my ($self) = @_;
    my $dbisql;
    eval {
        $dbisql = $self->transaction_sql;
    };
    unless ($dbisql) {
        $@ =~ s/ at \/.*//;
        $self->error({ exceptionCode => 'Error building transaction SQL',
                       ExceptionText => $@ });
        return;
    }
    my %rows = (
        Insert => 0,
        Update => 0,
        Replace => 0,
        Delete => 0 
        );
    my %results = (
        Insert => [],
        Update => [],
        Replace => [],
        Delete => [] 
        );
    my $version2 = $self->{version} =~ /^2/;
    for my $dbi (keys %$dbisql) {
        my ($db, $user, $pass) = split / /, $dbi;
        my $dbh = DBI->connect($db, $user, $pass);
        if ($dbh) {
            my $error;
          try:
            for my $op (keys %{$dbisql->{$dbi}}) {
                for my $i (0..$#{$dbisql->{$dbi}{$op}{SQL}}) {
                    my $sql = $dbisql->{$dbi}{$op}{SQL}[$i];
                    my $table = $dbisql->{$dbi}{$op}{table}[$i];
                    my $rows = $dbh->do($sql);
                    if ($rows) {
                        my $id = '';
                        $id = $dbh->last_insert_id(undef, $table->{schema}, $table->{name}, undef) if $op eq 'Insert';
                        my @id = $version2 ? ('fes:ResourceId' => {rid => $id}) : ('ogc:FeatureId' => {fid => $id});
                        push @{$results{$op}}, ['wfs:Feature', [\@id]];
                        $rows{$op} += ($rows == 0) ? 1 : $rows;
                    } else {
                        $error .= $dbh->errstr;
                        last try;
                    }
                }
            }
            if ($error) {
                $self->error({ exceptionCode => 'Error executing transaction SQL',
                               ExceptionText => $error });
                last;
            }
        } else {
            $self->error({ exceptionCode => 'Database connection error',
                           ExceptionText => "Can't connect to database '$db'" });
        }
    }
        
    my $writer = Geo::OGC::Service::XMLWriter::Caching->new();
    my %ns = $version2 ? (%wfs_2_0_0_ns) : (%wfs_1_1_0_ns);
    $ns{version} = $self->{version};
    $writer->open_element('wfs:TransactionResponse' => \%ns);
    $writer->element('wfs:TransactionSummary',
                     [['wfs:totalInserted' => $rows{Insert}],
                      ['wfs:totalUpdated' => $rows{Update}],
                      ['wfs:totalReplaced' => $rows{Replace}],
                      ['wfs:totalDeleted' => $rows{Delete}]
                     ]);
    for my $op (keys %results) {
        $writer->element('wfs:'.$op.'Results', $results{$op}) if @{$results{$op}};
    }
    $writer->close_element;
    $writer->stream($self->{responder});
}

=pod

=head3 CreateStoredQuery

Not yet implemented.

=cut

sub CreateStoredQuery {
}

=pod

=head3 DropStoredQuery

Not yet implemented.

=cut

sub DropStoredQuery {
}

=pod

=head3 ListStoredQueries

Not yet implemented.

=cut

sub ListStoredQueries {
}

=pod

=head3 DescribeStoredQueries

Not yet implemented.

=cut

sub DescribeStoredQueries {
}

# "private" methods below, these may use croak

# parse request from posted, parameters, config
# into version, request
# filter is not parsed because type object is not available
sub parse_request {
    my $self = shift;
    my @queries;
    if ($self->{posted}) {
        $self->{request} = ogc_request($self->{posted});
    } elsif ($self->{parameters}) {
        $self->{request} = {
            request => $self->{parameters}{request},
            version => $self->{parameters}{version},
            outputformat => $self->{parameters}{outputformat},
            queries => [
                {
                    # typenames is WFS 2, and can be a space separated list
                    typename => ($self->{parameters}{typename} // $self->{parameters}{typenames}),
                    filter => $self->{filter},
                    EPSG => epsg_number($self->{parameters}{srsname}),
                    count => get_integer($self->{parameters}{count}, $self->{parameters}{maxfeatures}),
                    BBOX => split_to_listref($self->{parameters}{bbox})
                }
                ]
        };
        $self->{request}{outputformat} = 'GML2' if $self->{request}{version} && $self->{request}{version} eq '1.0.0';
    }

    my %defaults = (
        version => '2.0.0',
        Title => 'WFS Server',
        );
    for my $key (keys %defaults) {
        $self->{config}{$key} //= $defaults{$key};
    }
    for my $key (qw/resource ServiceTypeVersion/) {
        $self->{config}{$key} //= '';
    }
    # version negotiation
    $self->{version} = 
        latest_version($self->{parameters}{acceptversions}) // # not in standard, QGIS WFS 2.0 Client uses
        $self->{request}{version} // 
        $self->{config}{version};
}

sub get_feature_type {
    my ($self, $typename) = @_;
    $typename =~ s/^\w+://; # remove possible namespace
    $self->read_feature_type_list unless $self->{feature_types} && @{$self->{feature_types}};
    for my $t (@{$self->{feature_types}}) {
        for my $t2 (@$t) {
            if ($t2->{Name} eq $typename) {
                return $t2;
            }
        }
    }
    croak "Feature type '$typename' is not available.";
}

# read FeatureTypeList from config and create
# feature_types as an array of arrays of feature_types
# a feature_type should contain information for
# operations (GetCapabilities etc)
# this sub parses the FeatureTypeList of config
# TO DO: make this boot time thing
sub read_feature_type_list {
    my $self = shift;

    # to do: basic config
    $self->{config}{targetNamespace} //= $self->{config}{resource};
    
    $self->{feature_types} = [];
    for my $type (@{$self->{config}{FeatureTypeList}}) {
        #say STDERR "process $type->{DataSource}";
        if ($type->{require_user}) {
            # simple test now
            # also, this should probably be runtime check
            next unless $self->{env}{REMOTE_USER} eq $type->{require_user};
        }
        if (exists $type->{DataSource}) {
            if ($type->{DataSource} =~ /^PG:/ && !(exists $type->{Layer})) {
                push @{$self->{feature_types}}, $self->feature_types_in_data_source($type);
            } else {
                unless ($type->{DefaultSRS}) {
                    carp "FeatureType missing DefaultSRS.";
                    next;
                }
                my $DataSource = $type->{DataSource};
                my ($name, $path) = fileparse($0);
                $DataSource =~ s/\$path/$path/;
                if (my $ds = Geo::OGR::DataSource::Open($DataSource)) {
                    my $layer = $ds->GetLayer($type->{Layer}); # the first if no Layer in config
                    my $schema = $layer->GetSchema;
                    my %geometry_types = map {$_ => 1} Geo::OGR::GeometryTypes();
                    my %columns;
                    my $Column;
                    for my $field (@{$schema->{Fields}}) {
                        my $column = $field->{Name};
                        my $in_type = $field->{Type};
                        if ($geometry_types{$type}) {
                            $column = "ogrGeometry" if $column eq '';
                            $in_type = "geometry";
                            if (not exists $type->{GeometryColumn}) {
                                $Column //= $column; # pick the first
                            } elsif ($field->{Name} eq $type->{GeometryColumn}) {
                                $Column = $column;
                            }
                        }
                        $columns{$column}{in_type} = $type;
                    }
                    my $feature_type = {
                        Name => $type->{Name} // $layer->GetName,
                        Title => $type->{Title},
                        Abstract => $type->{Abstract},
                        DefaultSRS => $type->{DefaultSRS},
                        DataSource => $DataSource,
                        LayerName => $layer->GetName,
                        table => {columns => \%columns}
                    };
                    $feature_type->{GeometryColumn} = '"'.$Column.'"' if defined $Column;
                    push @{$self->{feature_types}}, [$feature_type];
                } else {
                    carp "Can't open '$type->{DataSource}' as a data source.";
                }
                say STDERR "added $type->{Name}" if $self->{debug} > 2;
            }
        } else {
            carp "'DataSource' missing from a feature type in FeatureTypeList.";
        }
    }
    for my $type (@{$self->{feature_types}}) {
        for my $t (@$type) {
            for my $in_name (keys %{$t->{table}{columns}}) {
                my $in_type = $t->{table}{columns}{$in_name}{in_type};
                my $out_type = $type_map{$in_type} // 'xs:string';
                my $out_name = $in_name;

                # field name adjustments as GDAL does them
                $out_name =~ s/ /_/g;
                $out_name =~ s/-/_/g;
                
                # extra name adjustments, needed by QGIS
                # this has no effect now since no 'use utf8'.. FIXME?
                $out_name =~ s/[åö]/o/g;
                $out_name =~ s/ä/a/g;
                $out_name =~ s/[ÅÖ]/O/g;
                $out_name =~ s/Ä/A/g;

                # GDAL will use geometryProperty for geometry elements when producing GML:
                $out_name = 'geometryProperty' if $out_type eq 'gml:GeometryPropertyType';

                $t->{table}{columns}{$in_name}{out_name} = $out_name;
                $t->{table}{columns}{$in_name}{out_type} = $out_type;
            }
        }
    }
}

sub feature_types_in_data_source {
    my ($self, $type) = @_;
    #$self->{debug} = 3;
    my $dbi = DataSource2dbi($type->{DataSource});
    my ($db, $user, $pass) = split / /, $dbi;
    my $dbh = DBI->connect($db, $user, $pass, { PrintError => 0, RaiseError => 0 }) 
        or croak("Can't connect to database '$db': ".$DBI::errstr);
    my $schema = $type->{Schema} // 'public';
    my $sth = $dbh->table_info( '', $schema, undef, "'TABLE','VIEW'" );
    my @tables;
    while (my $data = $sth->fetchrow_hashref) {
        my $table = $data->{TABLE_NAME};
        #say STDERR 'table:',$table;
        # in open policy, can restrict to specific table types (TABLE_TYPE is a Perl DBI attribute)
        next if $type->{TABLE_TYPE} && !$type->{TABLE_TYPE}{$data->{TABLE_TYPE}};
        # in open policy, can restrict to tables with a specific prefix
        next if $type->{TABLE_PREFIX} && !(index($table, $type->{TABLE_PREFIX}) != -1);
        push @tables, $table;
    }
    for my $table (@tables) {
        my %columns;
        my $sth = $dbh->column_info( '', $schema, $table, '' );
        while (my $data = $sth->fetchrow_hashref) {
            #say STDERR "col: $data->{COLUMN_NAME} $data->{TYPE_NAME}";
            $columns{$data->{COLUMN_NAME}}{in_type} = $data->{TYPE_NAME};
        }
        $table = {
            schema => $schema,
            name => $table,
            columns => \%columns,
        };
    }
    
    my @feature_types;
    for my $table (@tables) {
        for my $column (keys %{$table->{columns}}) {
            next unless $table->{columns}{$column}{in_type} eq 'geometry';
            
            my $Table = "\"$table->{schema}\".\"$table->{name}\"";
            my $Column = "\"$column\"";
            
            print STDERR "Testing $table->{schema}.$table->{name}.$column: " if $self->{debug} > 2;
            
            my $sql = "select auth_name,auth_srid from $Table ".
                "join spatial_ref_sys on spatial_ref_sys.srid=st_srid($Column) ".
                "limit 1";
            my $sth = $dbh->prepare($sql) or croak($dbh->errstr);
            my $rv = $sth->execute;
            # the execute may fail because of no permission, in that case we skip but log an error
            unless ($rv) {
                print STDERR "No permission to serve table $table->{schema}.$table->{name}.\n";
                next;
            }
            my ($auth_name, $auth_srid) = $sth->fetchrow_array;
            unless ($auth_srid) {
                next if $table->{name} eq 'raster_columns';
                carp "$table->{schema}.$table->{name}.$column has no SRS.";
                next;
            }
            
            my $prefix = $type->{prefix};
            my $shortname = $table->{name}.'.'.$column;
            $shortname =~ s/ /_/g; # XML layer names can't have spaces
                
            my $name = $prefix.'.'.$shortname;
            if ($type->{allow} and !($type->{allow}{$shortname} or $type->{allow}{$name})) {
                print STDERR "not allowed.\n" if $self->{debug} > 2;
                next;
            }
            if ($type->{deny} and ($type->{deny}{$shortname} or $type->{deny}{$name})) {
                print STDERR "denied.\n" if $self->{debug} > 2;
                next;
            }

            my $feature_type = {
                Name => $name,
                Title => $table->{name}.'('.$column.')',
                Abstract => "Layer from $table->{name} in $prefix using column $column.",
                DefaultSRS => "$auth_name:$auth_srid",
                DataSource => $type->{DataSource},
                Table => $Table,
                GeometryColumn => $Column,
                table => $table,
                'gml:id' => 'id'
            };
            for my $n ($shortname, $name) {
                if ($type->{$n}) {
                    for my $key (keys %{$type->{$n}}) {
                        $feature_type->{$key} = $type->{$n}{$key};
                    }
                }
            }
            for my $key (keys %$type) {
                $feature_type->{$key} //= $type->{$key} unless ref $key;
            }
            my ($h, @c) = pseudo_credentials($feature_type);
            my $ok = 1;
            for my $c (@c) {
                unless ($feature_type->{table}{columns}{$c}) {
                    print STDERR "pseudo credential column '$c' not in table.\n" if $self->{debug} > 2;
                    $ok = 0;
                    next;
                }
            }
            next unless $ok;
            print STDERR "added.\n" if $self->{debug} > 2;
            #say STDERR "add $feature_type->{Name}";
            push @feature_types, $feature_type;
        }
    }
    return \@feature_types;
}

# return WFS request in a hash
# this function is written according to WFS 1.1.0 / 2.0.0
sub ogc_request {
    my ($node) = @_;
    my ($ns, $name) = parse_tag($node);
    if ($name eq 'GetCapabilities') {
        return { service => $node->getAttribute('service'), 
                 request => $name,
                 version => $node->getAttribute('version') };

    } elsif ($name eq 'DescribeFeatureType') {
        my $request = { service => $node->getAttribute('service'), 
                        request => $name,
                        version => $node->getAttribute('version') };
        $request->{queries} = [];
        for ($node = $node->firstChild; $node; $node = $node->nextSibling) {
            my ($ns, $name) = parse_tag($node);
            if ($name eq 'TypeName') {
                push @{$request->{queries}}, { typename => $node->textContent };
            }
        }
        return $request;

    } elsif ($name eq 'GetFeature') {
        my $request = { request => 'GetFeature' };
        for my $a (qw/service version resultType outputFormat count maxFeatures/) {
            my %map = (maxFeatures => 'count');
            my $key = $map{$a} // $a;
            my $b = $node->getAttribute($a);
            $request->{lc($key)} = $b if $b;
        }
        $request->{queries} = [];
        for ($node = $node->firstChild; $node; $node = $node->nextSibling) {
            push @{$request->{queries}}, ogc_request($node);
        }
        return $request;

    } elsif ($name eq 'Transaction') {
        my $request = { request => 'Transaction' };
        for my $a (qw/service version/) {
            my $b = $node->getAttribute($a);
            $request->{$a} = $b if $b;
        }
        for ($node = $node->firstChild; $node; $node = $node->nextSibling) {
            my $name = $node->nodeName;
            $name =~ s/^wfs://;
            $request->{$name} = [] unless $request->{$name};
            if ($name eq 'Insert') {
                for (my $n = $node->firstChild; $n; $n = $n->nextSibling) {
                    push @{$request->{$name}}, $n;
                }
            } else {
                push @{$request->{$name}}, $node;
            }
        }
        return $request;

    } elsif ($name eq 'Query') {
        my $query = {};
        for my $a (qw/typeName typeNames srsName/) {
            my %map = (typeNames => 'typeName');
            my $key = $map{$a} // $a;
            my $b = $node->getAttribute($a);
            $query->{lc($key)} = $b if $b;
        }
        if (defined $query->{srsname}) {
            $query->{EPSG} = epsg_number($query->{srsname});
        }
        for my $property ($node->getChildrenByTagNameNS('*', 'PropertyName')) {
            $query->{properties} = {} unless $query->{properties};
            $query->{properties}{strip($property->textContent)} = 1;
        }
        for my $filter ($node->getChildrenByTagNameNS('*', 'Filter')) {
            $query->{filter} = $filter; # there is only one filter
        }
        return $query;

    }
    return {};
}

# convert OGC Transaction XML to SQL
sub transaction_sql {
    my ($self) = @_;
    my $get_type = sub {
        my $name = shift;
        my $type = $self->get_feature_type($name);
        unless ($type->{DataSource} =~ /^PG:/) {
            say STDERR "The datasource is not PostGIS for '$name'.";
            return;
        }
        my $dbi = DataSource2dbi($type->{DataSource});
        return ($name, $type, $dbi);
    };
    my $get_filter = sub {
        my ($node, $type) = @_;
        my $where = '';
        for my $filter ($node->getChildrenByTagNameNS('*', 'Filter')) {
            $where = filter2sql($filter, $type);
        }
        return $where;
    };
    my %dbisql;
    for my $node (@{$self->{request}{Insert}}) {
        my ($type_name, $type, $dbi) = $get_type->($node->nodeName);
        next unless $type_name;
        my @cols;
        my @vals;
        for (my $field = $node->firstChild; $field; $field = $field->nextSibling) {
            my ($ns, $name) = parse_tag($field);
            my $col;
            my $val;
            if ($name eq 'geometry' or $name eq 'geometryProperty' or $name eq 'null') {
                $col = $type->{GeometryColumn};
                $val = node2sql($field->firstChild);
            } else {
                for my $c (keys %{$type->{table}{columns}}) {
                    $col = $c if $name eq $type->{table}{columns}{$c}{out_name};
                }
                next unless $col;
                $col = '"'.$col.'"';
                $val = $field->firstChild->data;
                $val = "'".$val."'";
            }
            unless ($col) {
                say STDERR "Can't find column name for property '$name'.";
                next;
            }
            push @cols, $col;
            push @vals, $val;
        }
        my $cols = join(',',@cols);
        my $vals = join(',',@vals);
        say STDERR "Insert: ($cols) VALUES ($vals)" if $self->{debug} > 1;
        push @{$dbisql{$dbi}{Insert}{SQL}}, "INSERT INTO $type->{Table} ($cols) VALUES ($vals)";
        push @{$dbisql{$dbi}{Insert}{table}}, $type->{table};
    }
    for my $node (@{$self->{request}{Update}}) {
        my ($type_name, $type, $dbi) = $get_type->($node->getAttribute('typeName'));
        next unless $type_name;
        my $set;
        for my $property ($node->getChildrenByTagNameNS('*', 'Property')) {
            # 1.1 Name and Value
            # 2.0 ValueReference and Value
            my $col;
            my @name = $property->getChildrenByTagNameNS('*', 'Name');
            @name = $property->getChildrenByTagNameNS('*', 'ValueReference') unless @name;
            unless (@name) {
                say STDERR "Can't find Name nor ValueReference.";
                next;
            }
            my $name = $name[0]->textContent();
            $name =~ s/^\w+://; # remove namespace
            $name =~ s/^\w+\///; # remove type name
            next if $name eq $type->{"gml:id"};
            my @val = $property->getChildrenByTagNameNS('*', 'Value');
            my $val;
            if ($name eq 'geometry' or $name eq 'geometryProperty' or $name eq 'null') {
                $col = $type->{GeometryColumn};
                $val = @val ? node2sql($val[0]->firstChild) : 'NULL';
            } else {
                for my $c (keys %{$type->{table}{columns}}) {
                    $col = $c if $name eq $type->{table}{columns}{$c}{out_name};
                }
                $col = '"'.$col.'"' if $col;
                $val = @val ? "'".$val[0]->textContent()."'" : 'NULL';
            }
            unless ($col) {
                say STDERR "Can't find column name for property '$name'.";
                next;
            }
            $set .= "$col = $val, ";
        }
        $set =~ s/, $//;
        my $where = $get_filter->($node, $type);
        say STDERR "Update: SET $set WHERE $where" if $self->{debug} > 1;
        push @{$dbisql{$dbi}{Update}{SQL}}, "UPDATE $type->{Table} SET $set WHERE $where";
        push @{$dbisql{$dbi}{Update}{table}}, $type->{table};
    }
    for my $node (@{$self->{request}{Delete}}) {
        my ($type_name, $type, $dbi) = $get_type->($node->getAttribute('typeName'));
        next unless $type_name;
        my $where = $get_filter->($node, $type);
        say STDERR "Delete: $where" if $self->{debug} > 1;
        push @{$dbisql{$dbi}{Delete}{SQL}}, "DELETE FROM $type->{Table} WHERE $where";
        push @{$dbisql{$dbi}{Delete}{table}}, $type->{table};
    }
    return \%dbisql;
}

# determine the EPSG number
sub epsg_number {
    my $str = shift;
    return undef unless $str;
    my ($epsg) = $str =~ /^EPSG:(\d+)$/;
    return $epsg if $epsg;
}

sub DataSource2dbi {
    my $DataSource = shift;
    my $dbi = $DataSource;
    $dbi =~ s/^PG/Pg/;
    $dbi =~ s/ host/;host/;
    $dbi =~ s/user=//;
    $dbi =~ s/password=//;
    return 'dbi:'.$dbi;
}

sub split_to_listref {
    my $s = shift;
    return undef unless $s;
    return [split /\s*,\s*/, $s];
}

sub list2element {
    my ($tag, $list) = @_;
    my @element;
    my @t = split /\s*,\s*/, $list;
    for my $t (@t) {
        push @element, [$tag, $t];
    }
    return @element;
}

sub pseudo_credentials {
    my $type = shift;
    my $c = $type->{pseudo_credentials};
    return ({}) unless $c;
    my($c1,$c2) = $c =~ /(\w+),(\w+)/;
    return ({$c1 => 1,$c2 => 1},$c1,$c2);
}

sub min {
    my $retval = shift;
    for my $x (@_) {
        next unless defined $x;
        $retval = $x unless defined $retval;
        $retval = $x if $x < $retval;
    }
    return $retval;
}

1;
__END__

=head1 SEE ALSO

Discuss this module on the Geo-perl email list.

L<https://list.hut.fi/mailman/listinfo/geo-perl>

For the WFS standard see 

L<http://www.opengeospatial.org/standards/wfs>

=head1 REPOSITORY

L<https://github.com/ajolma/Geo-OGC-Service-WFS>

=head1 AUTHOR

Ari Jolma, E<lt>ari.jolma at gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015- by Ari Jolma

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.22.0 or,
at your option, any later version of Perl 5 you may have available.

=cut
