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

our $VERSION = '0.01';

=pod

=head3 process_request

The entry method into this service. Fails unless the request is well known.

=cut

sub process_request {
    my ($self, $responder) = @_;
    $self->{debug} //= 0;
    $self->{responder} = $responder;
    if ($self->{parameters}{debug}) {
        $self->error({ debug => { config => $self->{config}, parameters => $self->{parameters}, env => $self->{env} } });
        return;
    }
    for ($self->{parameters}{request} // '') {
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
reports them, and the keys of the deny and allow are table names.
TABLE_PREFIX is a string that sets a requirement for the table names
for to be listed.

=cut

sub GetCapabilities {
    my ($self) = @_;
    # defaults
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
    my $writer = Geo::OGC::Service::XMLWriter::Caching->new('text/xml');
    $writer->open_element(
        'wfs:WFS_Capabilities', 
        { version => $self->{config}{version},
          'xmlns:gml' => "http://www.opengis.net/gml",
          'xmlns:wfs' => "http://www.opengis.net/wfs",
          'xmlns:ows' => "http://www.opengis.net/ows",
          'xmlns:xlink' => "http://www.w3.org/1999/xlink",
          'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance",
          'xmlns:ogc' => "http://www.opengis.net/ogc",
          'xmlns' => "http://www.opengis.net/wfs",
          'xsi:schemaLocation' => "http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd" });
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
    my @operations = (['wfs:Operation', 'Query']);
    push @operations, list2element('wfs:Operation', $self->{config}{Transaction}) if $self->{config}{Transaction};
    $writer->element('wfs:Operations', \@operations);

    my $list_layer = sub {
        my ($type) = @_;
        $self->get_layer($type); # test .. could be just name
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
                            [list2element('wfs:Operation', $type->{Transaction})]] if exists $type->{Transaction};
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
                    my @feature_types = $self->feature_types_in_data_source($type);
                    for my $t (@feature_types) {
                        $list_layer->(cascading_clone($t));
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
    $writer->open_element('ogc:Filter_Capabilities');
    my @operands = ();
    for my $o (qw/Point LineString Polygon Envelope/) {
        push @operands, ['ogc:GeometryOperand', 'gml:'.$o];
    }
    my @operators = ();
    for my $o (qw/Equals Disjoint Touches Within Overlaps Crosses Intersects Contains DWithin Beyond BBOX/) {
        push @operators, ['ogc:SpatialOperator', { name => $o }];
    }
    $writer->element('ogc:Spatial_Capabilities', 
                [['ogc:GeometryOperands', \@operands],
                 ['ogc:SpatialOperators', \@operators]]);
    @operators = ();
    for my $o (qw/LessThan GreaterThan LessThanEqualTo GreaterThanEqualTo EqualTo NotEqualTo Like Between/) {
        push @operators, ['ogc:ComparisonOperator', $o];
    }
    $writer->element('ogc:Scalar_Capabilities', 
                [['ogc:LogicalOperators'],
                 ['ogc:ComparisonOperators', \@operators]]);
    $writer->element('ogc:Id_Capabilities', ['ogc:FID']);
    $writer->close_element;
}

=pod

=head3 DescribeFeatureType

Service the DescribeFeatureType request.

=cut

sub DescribeFeatureType {
    my ($self) = @_;

    unless ($self->{parameters}{typename}) {
        $self->error({ exceptionCode => 'MissingParameterValue',
                       locator => 'typeName' });
        return;
    }

    my @typenames = split(/\s*,\s*/, $self->{parameters}{typename});
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

        my ($pseudo_credentials) = pseudo_credentials($type);
        my @elements;
        for my $column_name (keys %{$type->{Schema}}) {
            next if $pseudo_credentials->{$column_name};
            my $column_type = $type->{Schema}{$column_name};

            #next if $column_type eq 'geometry' and not($self->{parameters}{typename} =~ /$column_name$/);

            $column_type = "gml:GeometryPropertyType" if $column_type eq 'geometry';
            $column_name =~ s/ /_/g; # field name adjustments as GDAL does them
            $column_name =~ s/ä/a/g; # extra name adjustments, needed by QGIS
            $column_name =~ s/ö/o/g;
            # GDAL will use geometryProperty for geometry elements when producing GML:
            $column_name = 'geometryProperty' if $column_type eq 'gml:GeometryPropertyType';
            my $minOccurs = 0;
            push @elements, ['element', 
                             { name => $column_name,
                               type => $column_type,
                               minOccurs => "$minOccurs",
                               maxOccurs => "1" } ];
        }
        $writer->element(
            'complexType', {name => $self->{parameters}{typename}.'Type'},
            ['complexContent', 
             ['extension', { base => 'gml:AbstractFeatureType' }, 
              ['sequence', \@elements
              ]]]);
        $writer->element(
            'element', { name => $type->{Name}, 
                         type => 'ogr:'.$self->{parameters}{typename}.'Type',
                         substitutionGroup => 'gml:_Feature' } );
    }
    
    $writer->close_element();
    $writer->stream($self->{responder});
}

sub GetPropertyValue {
}

=pod

=head3 GetFeature

Service the GetFeature request. The response is generated with the GML
driver of GDAL using options TARGET_NAMESPACE and PREFIX. They are
from the configuration (FeatureType falling back to root) falling back
to default ones "http://www.opengis.net/wfs" and "wfs". The content
type of the reponse is from configuration (root key 'Content-Type')
falling back to default "text/xml".

=cut

sub GetFeature {
    my ($self) = @_;

    my ($query) = $self->get_queries; # actually we should loop through all queries

    my $name = $query->{typename};
    $name =~ s/\w+://; # remove namespace

    my $type = $self->feature_type($name);
    my $layer;

    if ($type && $type->{Layer}) {
        $layer = $self->get_layer($type);
        if ($query->{BBox}) {
            my @bbox = split /,/, $query->{BBox};
            $layer->SetSpatialFilterRect(@bbox);
        }
    }

    elsif ($type && $type->{Table}) {

        # pseudo_credentials: these fields are required to be in the filter and they are not included as attributes
        my ($pseudo_credentials, @pseudo_credentials) = pseudo_credentials($type);
        if (@pseudo_credentials) {
            # test for pseudo credentials in filter
            my $pat1 = "\\(\\(\"$pseudo_credentials[0]\" = '.+?'\\) AND \\(\"$pseudo_credentials[1]\" = '.+?'\\)\\)";
            my $pat2 = "\\(\\(\"$pseudo_credentials[1]\" = '.+?'\\) AND \\(\"$pseudo_credentials[0]\" = '.+?'\\)\\)";
            my $filter = $query->{filter} // '';
            my $n = join(' and ', @pseudo_credentials);
            $self->error({ exceptionCode => 'InvalidParameterValue',
                           locator => 'filter',
                           ExceptionText => "Not authorized. Please provide '$n' in filter." })
                unless $filter and ($filter =~ /$pat1/ or $filter =~ /$pat2/);
            return;
        }
        
        my @cols;
        my $filter = $query->{filter}; # reverse the field names
        for my $field_name (keys %{$type->{Schema}}) {
            next if $field_name eq 'ID';
            next if $pseudo_credentials->{$field_name};
            my $n = $field_name;
            $n =~ s/ /_/g;
            $n =~ s/ä/a/g;
            $n =~ s/ö/o/g;

            # need to use the specified GeometryColumn and only it
            next if $type->{Schema}{$field_name} eq 'geometry' and not ($field_name eq $type->{GeometryColumn});

            if ($query->{EPSG} and $field_name eq $type->{GeometryColumn}) {
                push @cols, "st_transform(\"$field_name\",$query->{EPSG}) as \"$n\"";
            } else {
                $filter =~ s/$n/$field_name/g if $filter;
                push @cols, "\"$field_name\" as \"$n\"";
            }
        }

        my $sql = "select ".join(',',@cols)." from \"$type->{Table}\" where ST_IsValid($type->{GeometryColumn})";

        # test for $type->{$type->{Title}}{require_filter} vs $query->{filter}
        my $geom = $type->{GeometryColumn};
        $geom = "st_transform(\"$geom\",$query->{EPSG})" if defined $query->{EPSG};
        $filter =~ s/GeometryColumn/$geom/g if $filter;
        $sql .= " and $filter" if $filter;

        print STDERR "$sql\n" if $self->{debug} > 2;
        $layer = $self->{DataSource}->ExecuteSQL($sql);
    }

    unless ($layer) {
        $self->error({ exceptionCode => 'InvalidParameterValue',
                       locator => 'typeName',
                       ExceptionText => "Type '$name' is not available (or there was an internal or configuration error)" });
        return;
    }    

    # note that OpenLayers does not seem to like the default target namespace, at least with outputFormat: "GML2"
    # use "TARGET_NAMESPACE": "http://ogr.maptools.org/", "PREFIX": "ogr", in config or type section
    my $ns = $self->{config}{TARGET_NAMESPACE} // $type->{TARGET_NAMESPACE} // 'http://www.opengis.net/wfs';
    my $prefix = $self->{config}{PREFIX} // $type->{PREFIX} // 'wfs';

    my $vsi = '/vsistdout/';
    my $gml;
    my $l2;

    my $stdout = capture_stdout {
        $gml = Geo::OGR::Driver('GML')->Create($vsi, { TARGET_NAMESPACE => $ns, PREFIX => $prefix });
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

    my $i = 1;
    $layer->ResetReading;
    while (1) {
        my $f;
        my $stdout = capture_stdout {
            $f = $layer->GetNextFeature;
            if ($f) {
                $l2->CreateFeature($f);
                $i++;
            } else {
                undef $l2;
                undef $gml;
            }
        };
        $writer->write($stdout);
        last unless $f;
        last if defined $query->{count} and $i >= $query->{count};
    }
    print STDERR "$i features served, max is ",$query->{maxfeatures}||'not set',"\n" if $self->{debug};
}

sub GetFeatureWithLock {
}

sub LockFeature {
}

sub Transaction {
    my ($self) = @_;
    $self->error({ exceptionCode => 'ResourceNotFound',
                   locator => 'request',
                   ExceptionText => 'Not implemented yet' });
}

sub CreateStoredQuery {
}

sub DropStoredQuery {
}

sub ListStoredQueries {
}

sub DescribeStoredQueries {
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
                            $ret->{Schema}{$name} = $type;
                        }
                    }
                }
                else {
                    # open policy: announce all in a data source except those denied
                    for my $t ($self->feature_types_in_data_source($type)) {
                        if ($t->{Name} eq $type_name) {
                            $ret = cascading_clone($t, $type);
                        }
                    }
                }
            }
        };
        if ($@) {
            say STDERR $@;
            next;
        }
        return $ret if $ret;
    }
}

sub feature_types_in_data_source {
    my ($self, $type) = @_;
    return unless $type->{DataSource} =~ /^Pg:/;

    my $dbi = 'dbi:'.$type->{DataSource};
    $dbi =~ s/ host/;host/;
    $dbi =~ s/user=//;
    $dbi =~ s/password=//;
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
            $n =~ s/"//g;
            $schema{$n} = $data->{TYPE_NAME};
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
            my $name = "$prefix.$table.$geom";
            next if $type->{allow} and !$type->{allow}{$name};
            next if $type->{deny} and $type->{deny}{$name};

            my $feature_type = {
                Title => "$table($geom)",
                Name => $name,
                Abstract => "Layer from $table in $prefix using column $geom",
                DefaultSRS => "$auth_name:$auth_srid",
                Table => $table,
                GeometryColumn => $geom,
                Schema => \%schema
            };
            push @feature_types, $feature_type;
        }
    }
    return @feature_types;
}

sub get_queries {
    my $self = shift;
    my @queries;
    if ($self->{parameters}{typename}) {
        my $query = {
            typename => $self->{parameters}{typename},
            filter => $self->{filter},
            EPSG => get_integer($self->{parameters}{srsname}),
            count => get_integer($self->{parameters}{count}),
        };
        $query->{count} //= get_integer($self->{parameters}{maxfeatures});
        push @queries, $query;
    }
    return @queries;
}

sub get_layer {
    my ($self, $type) = @_;
    #my @n = $self->{DataSource}->GetLayerNames;
    return $self->{DataSource}->GetLayer($type->{Layer});
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

sub get_integer {
    my $s = shift;
    return undef unless defined $s;
    if ($s =~ /(\d+)/) {
        return $1;
    }
    return undef;
}

sub list2element {
    my($tag, $list) = @_;
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
    $c = $type->{$type->{Name}}{pseudo_credentials} if !$c && $type->{Name};
    return ({}) unless $c;
    my($c1,$c2) = $c =~ /(\w+),(\w+)/;
    return ({$c1 => 1,$c2 => 1},$c1,$c2);
}

sub cascading_clone {
    my $root = shift;
    my $clone = clone $root;
    for my $parent (@_) {
        for my $key (keys %$parent) {
            $clone->{$key} //= $parent->{$key};
        }
    }
    return $clone;
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
