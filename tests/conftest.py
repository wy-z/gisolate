"""Shared fixtures for gisolate tests."""

import gevent.monkey

gevent.monkey.patch_all()

import multiprocessing  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture
def spawn_ctx():
    return multiprocessing.get_context("spawn")
