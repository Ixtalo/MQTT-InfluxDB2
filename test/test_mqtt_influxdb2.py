#!pytest
# -*- coding: utf-8 -*-
"""Unit tests."""

# pylint: disable=missing-function-docstring, line-too-long, invalid-name

from types import NoneType
import pytest
from influxdb_client import Point
from mqtt_influxdb2 import MyMqtt, evaluate_python_string_as_iterable, \
    identify_type_in_string, \
    decode_as_unicode


def test_evaluate_python_string_as_iterable():
    assert evaluate_python_string_as_iterable("[]") == []
    assert evaluate_python_string_as_iterable("[1]") == [1]
    assert evaluate_python_string_as_iterable("[1,2]") == [1, 2]
    assert evaluate_python_string_as_iterable("()") == ()
    assert evaluate_python_string_as_iterable("(1,)") == (1,)
    assert evaluate_python_string_as_iterable("(1,2)") == (1, 2)

    with pytest.raises(ValueError):
        evaluate_python_string_as_iterable(None)
    with pytest.raises(ValueError):
        evaluate_python_string_as_iterable(12)
    with pytest.raises(ValueError):
        evaluate_python_string_as_iterable("")
    with pytest.raises(ValueError):
        evaluate_python_string_as_iterable("         ")
    with pytest.raises(ValueError):
        evaluate_python_string_as_iterable("foobar")
    with pytest.raises(AssertionError):
        evaluate_python_string_as_iterable("123")
    with pytest.raises(AssertionError):
        evaluate_python_string_as_iterable("(1)")


def test_identify_type():
    assert identify_type_in_string("") is str
    assert identify_type_in_string("  ") is str
    assert identify_type_in_string("23") is int
    assert identify_type_in_string("23.4") is float
    assert identify_type_in_string("True") is bool
    assert identify_type_in_string("False") is bool
    assert identify_type_in_string("None") is NoneType

    with pytest.raises(ValueError):
        identify_type_in_string(None)
    with pytest.raises(ValueError):
        identify_type_in_string(True)
    with pytest.raises(ValueError):
        identify_type_in_string(12)
    with pytest.raises(ValueError):
        identify_type_in_string([])
    with pytest.raises(ValueError):
        identify_type_in_string([1, 2])
    with pytest.raises(ValueError):
        identify_type_in_string(())
    with pytest.raises(ValueError):
        identify_type_in_string((1, 2))


def test_decode_as_unicode():
    assert decode_as_unicode(b"") == ""
    assert decode_as_unicode(b"a") == "a"
    assert decode_as_unicode(b"12") == "12"
    assert decode_as_unicode(b"\xc3\xa4") == "ä"
    assert decode_as_unicode(b"\xc3\x9f") == "ß"

    with pytest.raises(AssertionError):
        decode_as_unicode("")
    with pytest.raises(AssertionError):
        decode_as_unicode("a")
    with pytest.raises(AssertionError):
        decode_as_unicode("12")
    with pytest.raises(AssertionError):
        decode_as_unicode("ä")


def test_mymqtt_create_data_point(monkeypatch):

    def mock_time(self, time):
        self._time = 1234
        return self

    monkeypatch.setattr(Point, "time", mock_time)

    actual = MyMqtt.create_data_point("foo", "bar", "1")
    expected = Point("foo").tag("topic", "bar").field("payload_float", value=float("1")).time(1234)
    assert actual.to_line_protocol() == expected.to_line_protocol()

    actual = MyMqtt.create_data_point("foo", "bar", "huhu")
    expected = Point("foo").tag("topic", "bar").field("payload_str", value="huhu").time(1234)
    assert actual.to_line_protocol() == expected.to_line_protocol()

    actual = MyMqtt.create_data_point("foo", "bar", '{"a": 123}')
    expected = Point("foo").tag("topic", "bar").field("payload_json", value='{"a": 123}').time(1234)
    assert actual.to_line_protocol() == expected.to_line_protocol()


def test_mymqtt_check_matches_pattern():
    assert MyMqtt.check_matches_pattern("#", "tele/gosundsp1_1/MARGINS")
    assert MyMqtt.check_matches_pattern("tele/#", "tele/gosundsp1_1/MARGINS")
    assert MyMqtt.check_matches_pattern("tele/+/MARGINS", "tele/gosundsp1_1/MARGINS")
    assert MyMqtt.check_matches_pattern("tele/gosundsp1_1/MARGINS", "tele/gosundsp1_1/MARGINS")
    assert not MyMqtt.check_matches_pattern("foo", "tele/gosundsp1_1/MARGINS")
    assert not MyMqtt.check_matches_pattern("foo/#", "tele/gosundsp1_1/MARGINS")
    assert not MyMqtt.check_matches_pattern("#/foo/#", "tele/gosundsp1_1/MARGINS")
