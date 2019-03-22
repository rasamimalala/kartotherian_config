import yaml
from invoke import Context
import pytest
import responses

from .tasks import generate_tiles

@pytest.fixture
def mocked_tilerator():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.POST, 'http://tilerator/add', status=201, json={})
        yield rsps


def test_generate_tiles_planet(mocked_tilerator):
    config = yaml.safe_load(open('invoke.yaml'))
    config['tiles']['planet'] = True
    generate_tiles(Context(config=config))

    assert len(mocked_tilerator.calls) == 3

    assert 'storageId=basemap' in mocked_tilerator.calls[0].request.url
    assert 'storageId=basemap' in mocked_tilerator.calls[1].request.url
    assert 'storageId=poi' in  mocked_tilerator.calls[2].request.url


def test_generate_tiles_single_tip(mocked_tilerator):
    config = yaml.safe_load(open('invoke.yaml'))
    config['tiles'].update({'x': 66, 'y':43, 'z': 7})
    generate_tiles(Context(config=config))

    assert len(mocked_tilerator.calls) == 2

    assert 'storageId=basemap' in mocked_tilerator.calls[0].request.url
    assert 'x=66&y=43' in mocked_tilerator.calls[0].request.url

    assert 'storageId=poi' in mocked_tilerator.calls[1].request.url
    assert 'x=66&y=43' in mocked_tilerator.calls[1].request.url


def test_generate_tiles_multiple_bases(mocked_tilerator):
    config = yaml.safe_load(open('invoke.yaml'))
    config['tiles'].update({'bases': '5/15/10,5/16/10,5/15/11,5/16/11'})
    generate_tiles(Context(config=config))

    # 2 jobs per base tile (basemap + poi) : 8 calls to tilerator
    assert len(mocked_tilerator.calls) == 8
    urls = [c.request.url for c in mocked_tilerator.calls]

    assert sum(1 for u in urls if 'storageId=basemap' in u) == 4
    assert sum(1 for u in urls if 'storageId=poi' in u) == 4
    assert sum(1 for u in urls if 'zoom=5' in u) == 8
    assert sum(1 for u in urls if 'x=15&y=10' in u) == 2
