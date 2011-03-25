#!perl -T

use Test::More tests => 5;

BEGIN { 
    use_ok( 'AnyEvent' );
    use_ok( 'AnyEvent::Curl::Multi' );
    use_ok( 'HTTP::Request');
}

my $GOOD_TEST_URL = 'http://www.perl.org';
my $BAD_TEST_URL = 'http://bogus.url.beeblebr0x./';


subtest good => sub {
    plan tests => 7;

    my $client = new_ok('AnyEvent::Curl::Multi' => [timeout => 30]);
    my $request = new_ok('HTTP::Request' => [GET => $GOOD_TEST_URL]);
    my $handle = $client->request($request);
    isa_ok($handle, 'AnyEvent::Curl::Multi::Handle');
    eval {
        my ($response, $stats) = $handle->cv->recv;
        isa_ok($response, 'HTTP::Response');
        isa_ok($stats, 'HASH');
        ok($response->is_success, "HTTP response from $GOOD_TEST_URL was successful");
    };
    is ($@, '', "callback didn't die");
};

subtest bad => sub { 
    plan tests => 5;

    my $client = new_ok('AnyEvent::Curl::Multi' => [timeout => 30]);
    my $request = new_ok('HTTP::Request' => [GET => $BAD_TEST_URL]);

    my $handle = $client->request($request);
    isa_ok($handle, 'AnyEvent::Curl::Multi::Handle');

    eval {
        my ($response, $stats) = $handle->cv->recv;
    };
    ok (defined $@, 'callback died');
    ok ($@ ne '', 'error diagnostic message');
};

__END__

# vim:syn=perl:ts=4:sw=4:et:ai
