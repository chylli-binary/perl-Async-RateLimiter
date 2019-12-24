package Async::RateLimiter;

use strict;
use warnings;

our $VERSION = '0.001';

=head1 NAME

Async::RateLimiter - Module abstract

=head1 SYNOPSIS

    use Async::RateLimiter;
    my $instance = Async::RateLimiter->new;

=head1 DESCRIPTION

=cut

=head1 METHODS

=cut

use Carp qw();
use Future;
use mro;

use parent qw(IO::Async::Notifier);

=head1 NAME

WebService::Async::CustomerIO::RateLimitter - This class provide possibility to limit amount
of request in time interval

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

sub _init {
    my ($self, $args) = @_;
    for my $k (qw(limit interval)) {
        die "Missing required argument: $k" unless exists $args->{$k};
        die "Invalid value for $k: $args->{$k}" unless int($args->{$k}) > 0;
        $self->{$k} = delete $args->{$k} if exists $args->{$k};
    }

    $self->{queue}   = [];
    $self->{counter} = 0;

    return $self->next::method($args);
}

=head2 interval

=cut

sub interval { return shift->{interval} }

=head2 limit

=cut

sub limit { return shift->{limit} }

=head2 acquire

Method checks availability for free slot.
It returns future, when slot will be available, then future will be resolved.

=cut

sub acquire {
    my ($self) = @_;

    $self->_start_timer;
    return Future->done if ++$self->{counter} <= $self->limit;

    my $current = $self->_current_queue;
    $current->{counter}++;
    return $current->{future};
}

sub is_rate_limited {
    my ($self) = @_;
    return $self->{counter} >= $self->limit;
}
sub _current_queue {
    my ($self) = @_;

    # +1 for getting correct position for edge cases like: limit 2, counter 4, should be 0
    my $pos = int(($self->{counter} - ($self->limit + 1)) / $self->limit);

    $self->{queue}[$pos] //= {
        future  => $self->loop->new_future,
        counter => 0
    };

    return $self->{queue}[$pos];
}

sub _start_timer {
    my ($self) = @_;

    $self->{timer} //= $self->loop->delay_future(
        after => $self->interval,
        )->on_ready(
        sub {
            $self->{counter} = 0;
            delete $self->{timer};

            return unless @{$self->{queue}};

            $self->_start_timer;

            my $current = shift @{$self->{queue}};
            $self->{counter} = $current->{counter};
            $current->{future}->done;
        });

    return $self->{timer};
}

1;

=head1 AUTHOR

Binary.com

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2019 by Binary.com.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=head1 SEE ALSO

=over 4

=item *

=back

