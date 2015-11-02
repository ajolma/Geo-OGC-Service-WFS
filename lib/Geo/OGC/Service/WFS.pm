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

use 5.022000;
use feature "switch";
use Carp;
use File::Basename;
use Modern::Perl;
use Capture::Tiny ':all';
use Clone 'clone';
use JSON;
use DBI;
use Geo::GDAL;

use Data::Dumper;
use XML::LibXML::PrettyPrint;

our $VERSION = '0.02';

our %gml_geometry_type = (
    Envelope => 1,
    Point => 1,
    MultiPoint => 1,
    LineString => 1,
    MultiLineString => 1,
    Polygon => 1,
    MultiPolygon => 1,
    MultiGeometry => 1,
    );

our %spatial2op = ( 
    Disjoint => 1,
    Equals => 1,
    Touches => 1,
    Within => 1,
    Overlaps => 1,
    Crosses => 1,
    Intersects => 1,
    Contains => 1
    ); # there are more in PostGIS

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

=pod

=head3 process_request

The entry method into this service. Fails unless the request is well known.

=cut

sub process_request {
    my ($self, $responder) = @_;
    $self->parse_request;
    $self->{debug} = $self->{config}{debug};
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

    my $writer = Geo::OGC::Service::XMLWriter::Caching->new('text/xml');

    my %inspireNameSpace = (
        'xmlns:inspire_dls' => "http://inspire.ec.europa.eu/schemas/inspire_dls/1.0",
        'xmlns:inspire_common' => "http://inspire.ec.europa.eu/schemas/common/1.0"
        );
    my $inspireSchemaLocations = 
        'http://inspire.ec.europa.eu/schemas/common/1.0 '.
        'http://inspire.ec.europa.eu/schemas/common/1.0/common.xsd '.
        'http://inspire.ec.europa.eu/schemas/inspire_dls/1.0 '.
        'http://inspire.ec.europa.eu/schemas/inspire_dls/1.0/inspire_dls.xsd';
    
    my %ns;
    if ($self->{version} eq '2.0.0') {
        %ns = (
            xmlns => "http://www.opengis.net/wfs/2.0" ,
            'xmlns:gml' => "http://schemas.opengis.net/gml",
            'xmlns:wfs' => "http://www.opengis.net/wfs/2.0",
            'xmlns:ows' => "http://www.opengis.net/ows/1.1",
            'xmlns:xlink' => "http://www.w3.org/1999/xlink",
            'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance",
            'xmlns:fes' => "http://www.opengis.net/fes/2.0",
            'xmlns:xs' => "http://www.w3.org/2001/XMLSchema",
            'xmlns:srv' => "http://schemas.opengis.net/iso/19139/20060504/srv/srv.xsd",
            'xmlns:gmd' => "http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd",
            'xmlns:gco' => "http://schemas.opengis.net/iso/19139/20060504/gco/gco.xsd",
            'xsi:schemaLocation' => "http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd",
            );
        # updateSequence="260" ?
    } else {
        %ns = (
            xmlns => "http://www.opengis.net/wfs",
            'xmlns:gml' => "http://www.opengis.net/gml",
            'xmlns:wfs' => "http://www.opengis.net/wfs",
            'xmlns:ows' => "http://www.opengis.net/ows",
            'xmlns:xlink' => "http://www.w3.org/1999/xlink",
            'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance",
            'xmlns:ogc' => "http://www.opengis.net/ogc",
            'xsi:schemaLocation' => "http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
            );
    }
    $ns{version} = $self->{version};

    $writer->open_element('wfs:WFS_Capabilities', \%ns);
    $writer->element('ows:ServiceIdentification', 
                     [['ows:Title', $self->{config}{Title}],
                      ['ows:Abstract'],
                      ['ows:ServiceType', {codeSpace=>"OGC"}, 'OGC WFS'],
                      ['ows:ServiceTypeVersion', $self->{config}{ServiceTypeVersion}],
                      ['ows:Fees'],
                      ['ows:AccessConstraints']]);
    $writer->element('ows:ServiceProvider',
                     [['ows:ProviderName'],
                      ['ows:ProviderSite', {'xlink:type'=>"simple", 'xlink:href'=>""}],
                      ['ows:ServiceContact']]);
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
                     [{service => ['WFS']}, 
                      {AcceptVersions => \@versions}, 
                      {AcceptFormats => ['text/xml']}]);
    $self->Operation($writer, 'DescribeFeatureType', 
                     [{outputFormat => ['XMLSCHEMA','text/xml; subtype=gml/2.1.2','text/xml; subtype=gml/3.1.1']}]);
    $self->Operation($writer, 'GetFeature',
                     [{resultType => ['results']}, {outputFormat => ['text/xml; subtype=gml/3.1.1']}]);
    $self->Operation($writer, 'Transaction',
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

sub Operation {
    my($self, $writer, $name, $parameters) = @_;
    my @parameters;
    for my $p (@$parameters) {
        for my $n (keys %$p) {
            my @values;
            for my $v (@{$p->{$n}}) {
                push @values, ['ows:Value', $v];
            }
            push @parameters, ['ows:Parameter', {name=>$n}, \@values];
        }
    }
    $writer->element(
        'ows:Operation', 
        {name => $name}, 
        [['ows:DCP', 
          ['ows:HTTP', 
           [['ows:Get', {'xlink:type'=>'simple', 'xlink:href'=>$self->{config}{resource}}],
            ['ows:Post', {'xlink:type'=>'simple', 'xlink:href'=>$self->{config}{resource}}]]
          ]],
         @parameters]);
}

sub FeatureTypeList  {
    my ($self, $writer) = @_;
    $writer->open_element('wfs:FeatureTypeList');
    my $operations = $self->{config}{Operations} // 'Query';
    my @operations = list2element('wfs:Operation', $operations);
    $writer->element('wfs:Operations', \@operations);

    my $list_layer = sub {
        my ($type) = @_;
        $self->get_layer($type); # test only, now opens.. could be just name
        my @FeatureType = (
            ['wfs:Name', $type->{Name}],
            ['wfs:Title', $type->{Title}],
            ['wfs:Abstract', $type->{Abstract}],
            ['wfs:DefaultSRS', $type->{DefaultSRS}],
            ['wfs:OtherSRS', 'EPSG:3857'],
            ['wfs:OutputFormats', ['wfs:Format', 'text/xml; subtype=gml/3.1.1']]);
        push @FeatureType, ['ows:WGS84BoundingBox', {dimensions=>2}, 
                            [['ows:LowerCorner',$type->{LowerCorner}],
                             ['ows:UpperCorner',$type->{UpperCorner}]]]
                                 if exists $type->{LowerCorner};
        push @FeatureType, ['wfs:Operations', 
                            [list2element('wfs:Operation', $type->{Operations})]] if exists $type->{Operations};
        $writer->element('wfs:FeatureType', \@FeatureType);
    };

    for my $type (@{$self->{config}{FeatureTypeList}}) {

        eval {
            if ($self->open_datasource($type)) {
                if (exists $type->{Layer}) {
                    # announce only the named layer
                    $list_layer->($type);
                }
                else {
                    # announce a group of layers
                    for my $t ($self->feature_types_in_data_source($type)) {
                        $list_layer->($t);
                    }
                }
            }
        };
        if ($@) {
            say STDERR $@;
            next;
        }
    }
    $writer->close_element;
}

sub Filter_Capabilities  {
    my ($self, $writer) = @_;
    my $ns = $self->{version} eq '2.0.0' ? 'fes' : 'ogc';
    $writer->open_element($ns.':Filter_Capabilities');

    # fes:Conformance missing

    my @ids;
    if ($ns eq 'ogc') {
        @ids = ([$ns.':FID']);
    } else {
        @ids = (['fes:ResourceIdentifier', {name => 'fes:ResourceId'}]);
    }
    $writer->element($ns.':Id_Capabilities', \@ids);

    my @operators = ();    
    for my $o (qw/LessThan GreaterThan LessThanOrEqualTo GreaterThanOrEqualTo EqualTo NotEqualTo Like Between Null/) {
        if ($ns eq 'ogc') {
            push @operators, [$ns.':ComparisonOperator', 'PropertyIs'.$o];
        } else {
            push @operators, [$ns.':ComparisonOperator', { name => 'PropertyIs'.$o}];
        }
    }
    $writer->element($ns.':Scalar_Capabilities', 
                [[$ns.':LogicalOperators'], # empty ?
                 [$ns.':ComparisonOperators', \@operators]]);

    my @operands = ();
    for my $o (keys %gml_geometry_type) {
        if ($ns eq 'ogc') {
            push @operands, [$ns.':GeometryOperand', 'gml:'.$o];
        } else {
            push @operands, [$ns.':GeometryOperand', { name => 'gml:'.$o }];
        }
    }
    @operators = ();
    my @op = keys %spatial2op;
    push @op, (qw/DWithin BBOX/);
    for my $o (@op) {
        push @operators, [$ns.':SpatialOperator', { name => $o }];
    }
    $writer->element($ns.':Spatial_Capabilities', 
                [[$ns.':GeometryOperands', \@operands],
                 [$ns.':SpatialOperators', \@operators]]);

    # fes:Temporal_Capabilities missing

    # fes:Functions missing

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
                       locator => 'typeName' });
        return;
    }
    
    my %types;

    for my $name (@typenames) {
        $types{$name} = $self->feature_type($name);
        unless ($types{$name}) {
            $self->error({ exceptionCode => 'InvalidParameterValue',
                           locator => 'typeName',
                           ExceptionText => "Type '$name' is not available" });
            return;
        }
    }

    my $writer = Geo::OGC::Service::XMLWriter::Caching->new('text/xml');
    $writer->open_element(
        'schema', 
        { version => '0.1',
          targetNamespace => "http://mapserver.gis.umn.edu/mapserver",
          xmlns => "http://www.w3.org/2001/XMLSchema",
          'xmlns:ogr' => "http://ogr.maptools.org/",
          'xmlns:ogc' => "http://www.opengis.net/ogc",
          'xmlns:xsd' => "http://www.w3.org/2001/XMLSchema",
          'xmlns:gml' => "http://www.opengis.net/gml",
          elementFormDefault => "qualified" });
    $writer->element(
        'import', 
        { namespace => "http://www.opengis.net/gml",
          schemaLocation => "http://schemas.opengis.net/gml/2.1.2/feature.xsd" } );

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
                       locator => 'typeName' });
        return;
    }
    
    my $type = $self->feature_type($typename);

    unless ($type) {
        $self->error({ exceptionCode => 'InvalidParameterValue',
                       locator => 'typeName',
                       ExceptionText => "Type '$typename' is not available" });
        return;
    }

    my $layer;

    if ($type && $type->{Layer}) {
        $layer = $self->get_layer($type);
        $layer->SetSpatialFilterRect(@{$query->{BBOX}}) if $query->{BBOX};
    }

    elsif ($type && $type->{Table}) {

        my $filter = filter2sql($query->{filter}, $type) // '';
        my $epsg = $query->{EPSG} // '';

        # pseudo_credentials: these fields are required to be in the filter and they are not included as attributes
        my ($pseudo_credentials, @pseudo_credentials) = pseudo_credentials($type);
        if (@pseudo_credentials) {
            # test for pseudo credentials in filter
            my $pat1 = "\\(\\(\"$pseudo_credentials[0]\" = '.+?'\\) AND \\(\"$pseudo_credentials[1]\" = '.+?'\\)\\)";
            my $pat2 = "\\(\\(\"$pseudo_credentials[1]\" = '.+?'\\) AND \\(\"$pseudo_credentials[0]\" = '.+?'\\)\\)";
            my $n = join(' and ', @pseudo_credentials);
            $self->error({ exceptionCode => 'InvalidParameterValue',
                           locator => 'filter',
                           ExceptionText => "Not authorized. Please provide '$n' in filter." })
                unless $filter and ($filter =~ /$pat1/ or $filter =~ /$pat2/);
            return;
        }
        
        my @cols;
         # reverse the field names
        for my $col (keys %{$type->{Schema}}) {
            next if $pseudo_credentials->{$col};

            my $is_geometry = $col eq $type->{GeometryColumn};
            my $name = $type->{Schema}{$col}{out_name};

            next if $query->{properties} && not $query->{properties}{$name};

            # only one geometry property in the output
            next if $type->{Schema}{$col}{out_type} eq 'gml:GeometryPropertyType' && not $is_geometry;

            if ($epsg and $is_geometry) {
                push @cols, "st_transform(\"$col\",$epsg) as \"$name\"";
            } else {
                $filter =~ s/$name/$col/g if $filter;
                $name = 'gml_id' if defined $type->{"gml:id"} && $col eq $type->{"gml:id"};
                push @cols, "\"$col\" as \"$name\"";
            }
        }

        my $geom = $type->{GeometryColumn};
        
        my $sql = "select ".join(',',@cols)." from \"$type->{Table}\" where ST_IsValid(\"$geom\")";

        $geom = "st_transform(\"$geom\",$epsg)" if defined $epsg;
        $filter =~ s/GeometryColumn/$geom/g if $filter;
        $sql .= " AND $filter" if $filter;
        $sql .= " AND (\"$geom\" && ST_MakeEnvelope(".join(",", @{$query->{BBOX}})."))" if $query->{BBOX};

        print STDERR "$sql\n" if $self->{debug} > 2;
        $layer = $self->{DataSource}->ExecuteSQL($sql);
    }

    unless ($layer) {
        $self->error({ exceptionCode => 'InvalidParameterValue',
                       locator => 'typeName',
                       ExceptionText => "Type '$typename' is not available (or there was an internal or configuration error)" });
        return;
    }

    # query may have resulttype outputformat set

    # note that OpenLayers does not seem to like the default target namespace, at least with outputFormat: "GML2"
    # use "TARGET_NAMESPACE": "http://ogr.maptools.org/", "PREFIX": "ogr", in config or type section
    my $ns = $self->{config}{TARGET_NAMESPACE} // $type->{TARGET_NAMESPACE} // 'http://www.opengis.net/wfs';
    my $prefix = $self->{config}{PREFIX} // $type->{PREFIX} // 'wfs';
    my $format = $self->{config}{FORMAT} // 'GML3.2';

    my $vsi = '/vsistdout/';
    my $gml;
    my $l2;

    my $stdout = capture_stdout {
        $gml = Geo::OGR::Driver('GML')->Create($vsi, { TARGET_NAMESPACE => $ns, PREFIX => $prefix, FORMAT => $format });
        $l2 = $gml->CreateLayer($type->{Name});
        my $d = $layer->GetLayerDefn;
        for (0..$d->GetFieldCount-1) {
            my $f = $d->GetFieldDefn($_);
            $l2->CreateField($f);
        }
    };

    my $content_type = $self->{config}{'Content-Type'} // 'text/xml';
    my $writer = Geo::OGC::Service::XMLWriter::Streaming->new($self->{responder}, $content_type);
    $writer->write($stdout);

    my $count = $query->{count};
    my $i = 0;
    $layer->ResetReading;
    while (1) {
        my $f = $layer->GetNextFeature;
        my $stdout = capture_stdout {
            if ($f) {
                $l2->CreateFeature($f);
                $i++;
            }
            if (!$f or (defined $count and $i >= $count)) {
                undef $l2;
                undef $gml;
            }
        };
        $writer->write($stdout);
        last unless $gml;
    }
    print STDERR "$i features served, max is ",$count//'not set',"\n" if $self->{debug};
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
    my $dbisql = $self->transaction_sql;
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
    for my $dbi (keys %$dbisql) {
        my($connect, $user, $pass) = split / /, $dbi;
        if ($self->{debug}) {
            say STDERR $connect,' ',$dbisql->{$dbi};
        }
        my $error;
        my $dbh = DBI->connect($connect, $user, $pass) or ($error = "Can't connect to database '$connect'.");
        say STDERR $error if $error;
        undef $error;
        $dbh->{pg_enable_utf8} = 1;
        for my $op (keys %{$dbisql->{$dbi}}) {

            my $n = @{$dbisql->{$dbi}{$op}{SQL}};

            for my $i (0..$#{$dbisql->{$dbi}{$op}{SQL}}) {

                my $sql = $dbisql->{$dbi}{$op}{SQL}[$i];
                my $table = $dbisql->{$dbi}{$op}{Table}[$i];
                my $rows = $dbh->do($sql) or ($error = "Error executing SQL: ".$dbh->errstr);
                if (!$error) {
                    my $id = '';
                    $id = $dbh->last_insert_id(undef, undef, $table, undef) if $op eq 'Insert';
                    push @{$results{$op}}, ['wfs:Feature', 
                                            [['ogc:FeatureId', {fid => $id}],
                                             ['fes:ResourceId', {rid => $id}]]];
                    $rows{$op} += ($rows == 0) ? 1 : $rows;
                } else {
                    say STDERR $error; # or does do do it already?
                    undef $error;
                }
                
            }
            
        }
    }
    my $writer = Geo::OGC::Service::XMLWriter::Caching->new('text/xml');
    $writer->open_element(
        'TransactionResponse', 
        { version => $self->{version},
          'xmlns:ogc' => "http://www.opengis.net/ogc",
          'xmlns:wfs' => "http://www.opengis.net/wfs/2.0",
          'xmlns:fes' => "http://www.opengis.net/fes/2.0",
          'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance",
          'xsi:schemaLocation' => "http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0.0/wfs.xsd"
        });
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
            queries => [
                {
                    typename => fallback($self->{parameters}{typename}, $self->{parameters}{typenames}),
                    filter => $self->{filter},
                    EPSG => get_integer($self->{parameters}{srsname}),
                    count => get_integer($self->{parameters}{count}, $self->{parameters}{maxfeatures}),
                    BBOX => split_to_listref($self->{parameters}{bbox})
                }
                ]
        };
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

sub open_datasource {
    my ($self, $type) = @_;
    croak "Missing DataSource parameter in FeatureType." unless exists $type->{DataSource};
    my $n = $type->{DataSource};
    my ($name, $path) = fileparse($0);
    $n =~ s/\$path/$path/;
    $self->{DataSource} = Geo::OGR::DataSource::Open($n);
    croak "Can't open '$type->{DataSource}' as a data source." unless $self->{DataSource};
}

sub feature_type {
    my ($self, $type_name) = @_;
    for my $type (@{$self->{config}{FeatureTypeList}}) {
        my $ret;
        eval {
            if ($self->open_datasource($type)) {
                if (exists $type->{Layer}) {
                    if ($type->{Name} eq $type_name) {
                        $ret = clone($type);
                        my $schema = $self->get_layer($type)->GetSchema;
                        my %geometry_types = map {$_ => 1} Geo::OGR::GeometryTypes();
                        for my $field (@{$schema->{Fields}}) {
                            my $name = $field->{Name};
                            my $type = $field->{Type};
                            if ($geometry_types{$type}) {
                                $name = "ogrGeometry" if $name eq '';
                                $type = "geometry";
                                if (not exists $self->{config}{GeometryColumn}) {
                                    $ret->{GeometryColumn} //= $name; # pick the first
                                } elsif ($field->{Name} eq $self->{config}{GeometryColumn}) {
                                    $ret->{GeometryColumn} = $name;
                                }
                            }
                            $ret->{Schema}{$name}{in_type} = $type;
                        }
                    }
                }
                else {
                    # open policy: announce all in a data source except those denied
                    for my $t ($self->feature_types_in_data_source($type)) {
                        if ($t->{Name} eq $type_name) {
                            $ret = $t;
                        }
                    }
                }
            }
        };
        if ($@) {
            say STDERR $@;
            next;
        }
        if ($ret) {
            for my $in_name (keys %{$ret->{Schema}}) {
                my $in_type = $ret->{Schema}{$in_name}{in_type};
                my $out_type = $type_map{$in_type} // 'xs:string';
                my $out_name = $in_name;

                # field name adjustments as GDAL does them
                $out_name =~ s/ /_/g; 
                
                # extra name adjustments, needed by QGIS
                $out_name =~ s/[åö]/o/g;
                $out_name =~ s/ä/a/g;
                $out_name =~ s/[ÅÖ]/O/g;
                $out_name =~ s/Ä/A/g;

                # GDAL will use geometryProperty for geometry elements when producing GML:
                $out_name = 'geometryProperty' if $out_type eq 'gml:GeometryPropertyType';

                $ret->{Schema}{$in_name}{out_name} = $out_name;
                $ret->{Schema}{$in_name}{out_type} = $out_type;
                
            }
        }
        return $ret if $ret;
    }
}

sub feature_types_in_data_source {
    my ($self, $type) = @_;
    return unless $type->{DataSource} =~ /^Pg:/;

    my $dbi = DataSource2dbi($type->{DataSource});
    my ($connect, $user, $pass) = split / /, $dbi;
    my $dbh = DBI->connect($connect, $user, $pass, { PrintError => 0, RaiseError => 0 }) 
        or croak("Can't connect to database '$connect': ".$DBI::errstr);
    $dbh->{pg_enable_utf8} = 1;
    my $sth = $dbh->table_info( '', 'public', undef, "'TABLE','VIEW'" );
    my @tables;
    while (my $data = $sth->fetchrow_hashref) {
        my $n = $data->{TABLE_NAME};
        # in open policy, can restrict to specific table types (TABLE_TYPE is a Perl DBI attribute)
        next if $type->{TABLE_TYPE} && !$type->{TABLE_TYPE}{$data->{TABLE_TYPE}};
        # in open policy, can restrict to tables with a specific prefix
        next if $type->{TABLE_PREFIX} && !(index($n, $type->{TABLE_PREFIX}) != -1);
        $n =~ s/"//g;
        push @tables, $n;
    }
    my @feature_types;
    for my $table (@tables) {
        my $sth = $dbh->column_info( '', 'public', $table, '' );
        my %schema;
        my @geometry_columns;
        while (my $data = $sth->fetchrow_hashref) {
            my $n = $data->{COLUMN_NAME};
            $n =~ s/^"//;
            $n =~ s/"$//;
            $schema{$n}{in_type} = $data->{TYPE_NAME};
            push @geometry_columns, $n if $data->{TYPE_NAME} eq 'geometry';            
        }
        for my $geom (@geometry_columns) {
            my $sql = "select auth_name,auth_srid ".
                "from \"$table\" ".
                "join spatial_ref_sys on spatial_ref_sys.srid=st_srid(\"$geom\") ".
                "limit 1";
            my $sth = $dbh->prepare($sql) or croak($dbh->errstr);
            my $rv = $sth->execute;
            # the execute may fail because of no permission, in that case we skip but log an error
            unless ($rv) {
                print STDERR "No permission to serve table '$table'.\n";
                next;
            }
            my ($auth_name, $auth_srid)  = $sth->fetchrow_array;
            $auth_name //= 'unknown';
            $auth_srid //= -1;

            my $allow_empty_layers = 1;
            unless ($allow_empty_layers) {
                # check that the table contains at least one spatial feature
                $sql = "select \"$geom\" from \"$table\" where not \"$geom\" isnull limit 1";
                $sth = $dbh->prepare($sql) or croak($dbh->errstr);
                $rv = $sth->execute or croak($dbh->errstr); # error here is a configuration error
                my($g)  = $sth->fetchrow_array;
                next unless $g;
            }

            my $prefix = $type->{prefix};
            my $shortname = $table.'.'.$geom;
            # wash the name
            $shortname =~ s/ /_/g;
            $shortname =~ s/[åö]/o/g;
            $shortname =~ s/ä/a/g;
            $shortname =~ s/[ÅÖ]/O/g;
            $shortname =~ s/Ä/A/g;
            my $name = $prefix.'.'.$shortname;
            next if $type->{allow} and (!$type->{allow}{$shortname} or !$type->{allow}{$name});
            next if $type->{deny} and ($type->{deny}{$shortname} or $type->{deny}{$name});

            my $feature_type = {
                Title => "$table($geom)",
                Name => $name,
                Abstract => "Layer from $table in $prefix using column $geom",
                DefaultSRS => "$auth_name:$auth_srid",
                Table => $table,
                GeometryColumn => $geom,
                Schema => \%schema
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
            push @feature_types, $feature_type;
        }
    }
    return @feature_types;
}

sub get_layer {
    my ($self, $type) = @_;
    #my @n = $self->{DataSource}->GetLayerNames;
    return $self->{DataSource}->GetLayer($type->{Layer});
}

# return WFS request in a hash
# this function is written according to WFS 1.1.0 / 2.0.0
sub ogc_request {
    my($node) = @_;
    my $ns = '';
    my $name = $node->nodeName;
    ($ns, $name) = split /:/, $name if $name =~ /:/;
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
        if (defined $query->{srsName}) {
            ($query->{EPSG}) = $query->{srsName} =~ /EPSG:(\d+)/;
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
        $name =~ s/^\w+://; # remove possible namespace
        my $type = $self->feature_type($name);
        unless ($type) {
            say STDERR "No such feature type: '$name'";
            return;
        }
        unless ($type->{Table}) {
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
            if ($name eq 'geometryProperty' or $name eq 'null') {
                $col = $type->{GeometryColumn};
                $val = node2sql($field->firstChild);
            }  else {
                for my $c (keys %{$type->{Schema}}) {
                    $col = $c if $name eq $type->{Schema}{$c}{out_name};
                }
                next unless $col;
                $val = $field->firstChild->data;
                $val = "'".$val."'";
            }
            unless ($col) {
                say STDERR "Can't find column name for property '$name'.";
                next;
            }
            push @cols, '"'.$col.'"';
            push @vals, $val;
        }
        push @{$dbisql{$dbi}{Insert}{SQL}}, "INSERT INTO $type->{Table} (".join(',',@cols).") VALUES (".join(',',@vals).")";
        push @{$dbisql{$dbi}{Insert}{Table}}, $type->{Table};
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
            if ($name eq 'geometryProperty' or $name eq 'null') {
                $col = $type->{GeometryColumn};
                $val = @val ? node2sql($val[0]->firstChild) : 'NULL';
            } else {
                for my $c (keys %{$type->{Schema}}) {
                    $col = $c if $name eq $type->{Schema}{$c}{out_name};
                }                
                $val = @val ? "'".$val[0]->textContent()."'" : 'NULL';
            }
            unless ($col) {
                say STDERR "Can't find column name for property '$name'.";
                next;
            }
            $set .= "\"$col\" = $val, ";
        }
        $set =~ s/, $//;
        my $where = $get_filter->($node, $type);
        push @{$dbisql{$dbi}{Update}{SQL}}, "UPDATE $type->{Table} SET $set WHERE $where";
        push @{$dbisql{$dbi}{Update}{Table}}, $type->{Table};
    }
    for my $node (@{$self->{request}{Delete}}) {
        my ($type_name, $type, $dbi) = $get_type->($node->getAttribute('typeName'));
        next unless $type_name;
        my $where = $get_filter->($node, $type);
        push @{$dbisql{$dbi}{Delete}{SQL}}, "DELETE FROM $type->{Table} WHERE $where";
        push @{$dbisql{$dbi}{Delete}{Table}}, $type->{Table};
    }
    return \%dbisql;
}

# convert OGC Filter XML to SQL
sub filter2sql {
    my ($node, $type) = @_;
    if (!$node) {
        return "";
    }
    my ($ns, $name) = parse_tag($node);

    if ($name eq 'Literal') {
        return "'".$node->firstChild->data."'";

    } elsif ($name eq 'PropertyName') {
        my $ref = strip($node->textContent);
        $ref = $type->{GeometryColumn} if $ref eq 'geometryProperty';
        return '"'.$ref.'"';

    } elsif ($name eq 'ValueReference') {
        my $ref = strip($node->textContent);
        $ref =~ s/^\w+://;
        $ref = $type->{GeometryColumn} if $ref eq 'geometryProperty';
        return '"'.$ref.'"';

    } elsif ($name eq 'ResourceId') {
        my $id = $node->getAttribute('rid');
        return $type->{"gml:id"}." = '$id'"; # $type->{"gml:id"} may not be set!

    } elsif ($name eq 'FeatureId') {
        my $id = $node->getAttribute('fid');
        return $type->{"gml:id"}." = '$id'"; # $type->{"gml:id"} may not be set!

    } elsif ($name eq 'GmlObjectId') {
        my $id = $node->getAttribute('gml:id');
        return $type->{"gml:id"}." = '$id'"; # $type->{"gml:id"} may not be set!

    } elsif ($name eq 'PropertyIsEqualTo') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' = '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsNotEqualTo') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' != '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsLessThan') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' < '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsGreaterThan') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' > '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsLessThanOrEqualTo') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' <= '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsGreaterThanOrEqualTo') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' >= '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsBetween') {
        $node = $node->firstChild;
        my $property = filter2sql($node->firstChild, $type);
        $node = $node->nextSibling;
        return '('.$property.' >= '.filter2sql($node, $type).' AND '.$property.'<='.filter2sql($node->nextSibling).')';

    } elsif ($name eq 'PropertyIsLike') {
        $node = $node->firstChild;
        return '('.filter2sql($node, $type).' ~ '.filter2sql($node->nextSibling, $type).')';

    } elsif ($name eq 'PropertyIsNull') {
        return '('.filter2sql($node->firstChild, $type).' ISNULL)';

    } elsif ($name eq 'And') {
        $node = $node->firstChild;
        my $p = '('.filter2sql($node, $type);
        while ($node = $node->nextSibling) {
            $p .= ' AND '.filter2sql($node, $type);
        }
        return $p.')';

    } elsif ($name eq 'Or') {
        $node = $node->firstChild;
        my $p = '('.filter2sql($node, $type);
        while ($node = $node->nextSibling) {
            $p .= ' OR '.filter2sql($node, $type);
        }
        return $p.')';
    } elsif ($name eq 'Not') {
        return '(NOT '.filter2sql($node->firstChild, $type).')';

    } elsif ($ns eq 'gml' and $gml_geometry_type{$name}) {
        return node2sql($node);

    } elsif ($name eq 'BBOX') {
        $node = $node->firstChild;
        my $property = filter2sql($node, $type);
        $node = $node->nextSibling;
        my $envelope = filter2sql($node, $type);
        return "($property && $envelope)";

    } elsif ($spatial2op{$name}) {
        $node = $node->firstChild;
        my $geom1 = filter2sql($node, $type);
        $node = $node->nextSibling;
        my $geom2 = filter2sql($node, $type);
        return "ST_$name($geom1, $geom2)";

    } elsif ($name eq 'DWithin') {
        $node = $node->firstChild;
        my $geom1 = filter2sql($node, $type);
        $node = $node->nextSibling;
        my $geom2 = filter2sql($node, $type);
        $node = $node->nextSibling;
        my $dist = filter2sql($node, $type);
        return "ST_$name($geom1, $geom2, $dist)";

    } elsif ($name eq 'Filter') {
        my %id = ( ResourceId => 1,
                   FeatureId => 1,
                   GmlObjectId => 1 );
        $node = $node->firstChild;
        my ($ns2, $name2) = parse_tag($node);
        if ($id{$name2}) {
            my $where = '';
            for (my $id = $node; $id; $id = $id->nextSibling) {
                $where .= filter2sql($id, $type).' OR ';
            }
            $where =~ s/ OR $//;
            return $where;
        } else {
            return filter2sql($node, $type);
        }
    }
}

sub node2sql {
    my ($node) = @_;
    my $srs = $node->getAttribute('srsName');
    ($srs) = $srs =~ /EPSG:(\d+)/ if $srs;
    $srs = ",$srs" if $srs;
    $srs //= '';
    my ($ns, $name) = parse_tag($node);
    my $wkt;

    # should use Geo::OGC::Geometry

    if ($name eq 'Box') {
        my $env = strip($node->firstChild->textContent);
        $env =~ s/ /,/;
        return "ST_MakeEnvelope($env$srs)";
    } elsif ($name eq 'Envelope') {
        $node = $node->firstChild;
        my $lc = strip($node->textContent); # gml:lowerCorner
        $lc =~ s/ /,/;
        $node = $node->nextSibling;
        my $uc = strip($node->textContent); # gml:upperCorner
        $uc =~ s/ /,/;
        return "ST_MakeEnvelope($lc,$uc$srs)";
    } elsif ($name eq 'Point') {
        my $pos = $node->firstChild->firstChild->data;
        $pos =~ s/,/ /;
        $wkt = "POINT ($pos)";
    } elsif ($name eq 'MultiPoint') { # not ok
        my $pos = $node->firstChild->firstChild->data;
        $pos =~ s/,/ /;
        $wkt = "MULTIPOINT ($pos)";
    } elsif ($name eq 'LineString') {
        my @tmp = split /\s+/, $node->firstChild->firstChild->data;
        my @pos;
        for (my $i = 0; $i < @tmp; $i+=2) {
            push @pos, $tmp[$i].' '.$tmp[$i+1];
        }
        $wkt = "LINESTRING (".join(', ',@pos).")";
    } elsif ($name eq 'MultiLineString') { # not ok
        my @tmp = split / /, $node->firstChild->firstChild->data;
        my @pos;
        for (my $i = 0; $i < @tmp; $i+=2) {
            push @pos, $tmp[$i].' '.$tmp[$i+1];
        }
        $wkt = "MULTILINESTRING (".join(', ',@pos).")";
    } elsif ($name eq 'Polygon') {
        # Polygon.exterior.LinearRing.posList
        my @tmp = split /\s+/, $node->firstChild->firstChild->firstChild->textContent;
        my @pos;
        for (my $i = 0; $i < @tmp; $i+=2) {
            push @pos, strip($tmp[$i]).' '.strip($tmp[$i+1]);
        }
        $wkt = "POLYGON ((".join(', ',@pos)."))";
    } elsif ($name eq 'MultiPolygon') { # not ok
        my @tmp = split / /, $node->firstChild->firstChild->firstChild->data;
        my @pos;
        for (my $i = 0; $i < @tmp; $i+=2) {
            push @pos, $tmp[$i].' '.$tmp[$i+1];
        }
        $wkt = "MULTIPOLYGON ((".join(', ',@pos)."))";
    } elsif ($name eq 'MultiGeometry') { # not ok
        my @tmp = split / /, $node->firstChild->firstChild->firstChild->data;
        my @pos;
        for (my $i = 0; $i < @tmp; $i+=2) {
            push @pos, $tmp[$i].' '.$tmp[$i+1];
        }
        $wkt = "MULTIGEOMETRY ((".join(', ',@pos)."))";
    }
    
    return "ST_GeometryFromText('$wkt'$srs)";
}

sub error {
    my ($self, $msg) = @_;
    if (!$msg->{debug}) {
        Geo::OGC::Service::error($self->{responder}, $msg);
    } else {
        my $json = JSON->new;
        $json->allow_blessed([1]);
        my $writer = $self->{responder}->([200, [ 'Content-Type' => 'application/json',
                                                  'Content-Encoding' => 'UTF-8' ]]);
        $writer->write($json->encode($msg->{debug}));
        $writer->close;
    }
}

sub parse_tag {
    my $node = shift;
    my $ns = '';
    my $name = $node->nodeName;
    ($ns, $name) = split /:/, $name if $name =~ /:/;
    return ($ns, $name);
}

sub log {
    my ($self, $msg) = @_;
    say STDERR Dumper($msg);
}

sub DataSource2dbi {
    my $DataSource = shift;
    my $dbi = 'dbi:'.$DataSource;
    $dbi =~ s/ host/;host/;
    $dbi =~ s/user=//;
    $dbi =~ s/password=//;
    return $dbi;
}

sub latest_version {
    my $versions = shift;
    return undef unless defined $versions;
    my @versions = split /\s*,\s*/, $versions;
    for (@versions) {
        my ($a,$b,$c) = split /\./;
        $_ = $a*10000+$b*100+$c;
    }
    @versions = sort {$b <=> $a} @versions;
    my ($a,$b,$c) = $versions[0] =~ /^(\d+)(\d\d)(\d\d)$/;
    for ($a,$b,$c) {
        $_ = int($_);
    }
    return "$a.$b.$c";
}

sub strip {
    my $s = shift;
    $s =~ s/^\s+//;
    $s =~ s/\s+$//;
    return $s;
}
    
sub fallback {
    for my $arg (@_) {
        return $arg if defined $arg;
    }
}

sub get_integer {
    my $s = shift;
    if (not defined $s) {
        if (@_) {
            return get_integer(shift);
        } else {
            return undef;
        }
    }
    if ($s =~ /(\d+)/) {
        return $1;
    }
    return undef;
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

Copyright (C) 2015 by Ari Jolma

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.22.0 or,
at your option, any later version of Perl 5 you may have available.

=cut
