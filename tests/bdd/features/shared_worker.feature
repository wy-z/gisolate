Feature: Shared Worker
  As a developer running several client processes
  I want them to share one isolated worker
  So that the isolated library is resident once, not once per client

  Scenario: An attached proxy runs its calls in the host process
    Given a host process serving an Adder
    And a proxy attached to the host
    When I call add with 2 and 3
    Then the result should be 5
    And the call should have run in the host process

  Scenario: One client leaving keeps the host serving the others
    Given a host process serving an Adder
    And a proxy attached to the host
    And a second proxy attached to the host
    When I shutdown the first proxy
    Then the second proxy should still reach the host
    And the host socket should still exist

  Scenario: A call that expired before the host existed never runs
    Given an address no host has bound yet
    And a proxy attached to that address
    When the call times out and a host starts afterwards
    Then the call should never have run in the host
