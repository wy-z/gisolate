Feature: Process Proxy
  As a developer using gevent
  I want to run objects in isolated subprocesses
  So that incompatible libraries don't interfere with monkey-patching

  Scenario: Execute a method on a remote object
    Given a ProcessProxy wrapping an Adder
    When I call add with 3 and 7
    Then the result should be 10

  Scenario: Remote exceptions propagate to the caller
    Given a ProcessProxy wrapping an Adder
    When I call a method that raises ValueError
    Then a ValueError should be raised with message "intentional error"

  Scenario: Child process runs in a separate PID
    Given a ProcessProxy wrapping an Adder
    When I request the child PID
    Then it should differ from the current PID

  Scenario: Proxy restart gives a new child process
    Given a ProcessProxy wrapping an Adder
    When I request the child PID
    And I restart the proxy
    And I request the child PID again
    Then the two PIDs should differ

  Scenario: Shutdown prevents further calls
    Given a ProcessProxy wrapping an Adder
    When I shutdown the proxy
    Then calling a method should raise RuntimeError
