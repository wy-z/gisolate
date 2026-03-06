Feature: Smart Serialization
  As a developer
  I want transparent serialization that handles both simple and complex objects
  So that I don't have to worry about pickle vs dill

  Scenario Outline: Round-trip simple Python objects
    When I serialize and deserialize <description>
    Then the value should survive the round-trip

    Examples:
      | description      |
      | an integer       |
      | a string         |
      | a list           |
      | a dictionary     |
      | None             |

  Scenario: Lambda functions use dill
    When I serialize a lambda function
    Then the serialized data should use the dill prefix
    And the deserialized lambda should work correctly

  Scenario: Unpicklable exceptions become RemoteError
    When I wrap an unpicklable exception
    Then it should become a RemoteError
    And the RemoteError should contain the original type name
