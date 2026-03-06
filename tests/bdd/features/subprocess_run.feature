Feature: Run in Subprocess
  As a developer
  I want to run functions in isolated subprocesses
  So that I can execute code without affecting the main process

  Scenario: Run a simple function
    When I run a function that adds 5 and 8 in a subprocess
    Then the subprocess result should be 13

  Scenario: Subprocess propagates exceptions
    When I run a function that raises ValueError in a subprocess
    Then a ValueError should be raised from the subprocess

  Scenario: Subprocess times out
    When I run a slow function with a short timeout
    Then a TimeoutError should be raised from the subprocess

  Scenario: Subprocess runs in a different process
    When I run a function that returns its PID in a subprocess
    Then the returned PID should differ from the current PID
