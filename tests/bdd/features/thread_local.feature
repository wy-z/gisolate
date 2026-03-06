Feature: Thread-Local Proxy
  As a developer using gevent with real threads
  I want per-thread object instances
  So that state is isolated between threads

  Scenario: Each thread gets its own instance
    Given a ThreadLocalProxy wrapping a Counter
    When I increment from the main thread
    And I read the value from another thread
    Then the main thread value should be 1
    And the other thread value should be 0

  Scenario: Lazy instance creation
    Given a ThreadLocalProxy wrapping a Counter
    Then the factory should not have been called yet
    When I access an attribute on the proxy
    Then the factory should have been called once
