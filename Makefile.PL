use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'AnyEvent::Curl::Multi',
    AUTHOR              => q{Michael S. Fischer <michael+cpan@dynamine.net>},
    VERSION_FROM        => 'lib/AnyEvent/Curl/Multi.pm',
    ABSTRACT_FROM       => 'lib/AnyEvent/Curl/Multi.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
	'AnyEvent'       => 5,
	'Object::Event'  => 1,
	'WWW::Curl'      => 4.10,
        'Test::More'     => 0,
	'HTTP::Response' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'AnyEvent-Curl-Multi-*' },
);
