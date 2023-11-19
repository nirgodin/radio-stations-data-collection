from data_collectors.components.collectors.billboard_collectors_component_factory import \
    BillboardCollectorsComponentFactory
from data_collectors.components.collectors.shazam_collectors_component_factory import ShazamCollectorsComponentFactory


class CollectorsComponentFactory:
    def __init__(self,
                 billboard: BillboardCollectorsComponentFactory = BillboardCollectorsComponentFactory(),
                 shazam: ShazamCollectorsComponentFactory = ShazamCollectorsComponentFactory()):
        self.billboard = billboard
        self.shazam = shazam
