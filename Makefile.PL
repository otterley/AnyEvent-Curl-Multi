use strict;
use warnings;
use ExtUtils::MakeMaker;

eval "use WWW::Curl 4.14;";
unless ($@) {
    no strict 'subs';
    my $version = WWW::Curl::Easy::version();
    my $delay = 10;
    unless ($version =~ /c-ares/) {
        print STDERR <<"EOF";
WARNING: Your libcurl doesn't appear to be built with c-ares support.  This
could significantly compromise the performance of programs that use
AnyEvent::Curl::Multi.   You will either need to contact your OS vendor to
obtain an update of libcurl with c-ares support, or compile a local version of
libcurl with c-ares support (e.g. in /usr/local or /opt/local) and ensure
WWW::Curl is linked against it.

(Continuing in $delay seconds.)
EOF
        sleep $delay;
    }
} 

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
        'WWW::Curl'      => 4.14,
        'Test::More'     => 0,
        'HTTP::Response' => 0,
        'HTTP::Request'  => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'AnyEvent-Curl-Multi-*' },
);

# vim:syn=perl:ts=4:sw=4:et:ai
