"""BDD steps for serialization.feature."""

import pytest
from pytest_bdd import parsers, scenario, then, when

from gisolate._internal import RemoteError, SmartPickle, wrap_exception


@pytest.fixture
def ser_ctx():
    return {"data": None, "value": None, "original": None, "wrapped": None}


@scenario("serialization.feature", "Round-trip simple Python objects")
def test_roundtrip():
    pass


@scenario("serialization.feature", "Lambda functions use dill")
def test_lambda_dill():
    pass


@scenario("serialization.feature", "Unpicklable exceptions become RemoteError")
def test_unpicklable():
    pass


_OBJECTS = {
    "an integer": 42,
    "a string": "hello",
    "a list": [1, 2, 3],
    "a dictionary": {"a": 1},
    "None": None,
}


@when(parsers.parse("I serialize and deserialize {description}"))
def serialize_roundtrip(ser_ctx, description):
    obj = _OBJECTS[description]
    data = SmartPickle.dumps(obj)
    ser_ctx["value"] = SmartPickle.loads(data)
    ser_ctx["original"] = obj


@then("the value should survive the round-trip")
def check_roundtrip(ser_ctx):
    assert ser_ctx["value"] == ser_ctx["original"]


@when("I serialize a lambda function")
def serialize_lambda(ser_ctx):
    fn = lambda x: x + 1  # noqa: E731
    ser_ctx["data"] = SmartPickle.dumps(fn)


@then("the serialized data should use the dill prefix")
def check_dill_prefix(ser_ctx):
    assert ser_ctx["data"][:1] == b"D"


@then("the deserialized lambda should work correctly")
def check_lambda_works(ser_ctx):
    fn = SmartPickle.loads(ser_ctx["data"])
    assert fn(10) == 11


@when("I wrap an unpicklable exception")
def wrap_unpicklable(ser_ctx):
    class WeirdError(Exception):
        def __reduce__(self):
            raise TypeError("can't pickle")

    ser_ctx["wrapped"] = wrap_exception(WeirdError("oops"))


@then("it should become a RemoteError")
def check_is_remote_error(ser_ctx):
    assert isinstance(ser_ctx["wrapped"], RemoteError)


@then("the RemoteError should contain the original type name")
def check_remote_error_type(ser_ctx):
    assert ser_ctx["wrapped"].exc_type == "WeirdError"
