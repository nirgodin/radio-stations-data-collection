import asyncio
from itertools import chain

from data_collectors import ShazamTopTracksCollector, ShazamTopTracksDatabaseInserter
from data_collectors.components import ComponentFactory
from data_collectors.logic.collectors.shazam.shazam_artists_collector import ShazamArtistsCollector


class ShazamTopTracksManager:
    def __init__(self,
                 shazam_top_tracks_collector: ShazamTopTracksCollector,
                 shazam_artists_collector: ShazamArtistsCollector,
                 shazam_tracks_inserter: ShazamTopTracksDatabaseInserter):
        self._top_tracks_collector = shazam_top_tracks_collector
        self._artists_collector = shazam_artists_collector
        self._tracks_inserter = shazam_tracks_inserter

    async def run(self):
        top_tracks = await self._top_tracks_collector.collect()
        flattened_tracks = list(chain.from_iterable(top_tracks.values()))
        artists = await self._artists_collector.collect(flattened_tracks)


if __name__ == '__main__':
    component_factory = ComponentFactory()
    manager = ShazamTopTracksManager(
        shazam_top_tracks_collector=component_factory.collectors.shazam.get_top_tracks_collector(),
        shazam_artists_collector=component_factory.collectors.shazam.get_artists_collector(),
        shazam_tracks_inserter=component_factory.inserters.shazam.get_top_tracks_inserter()
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(manager.run())
