from data_collectors.components.collectors.billboard_collectors_component_factory import \
    BillboardCollectorsComponentFactory


class CollectorsComponentFactory:
    def __init__(self, billboard: BillboardCollectorsComponentFactory = BillboardCollectorsComponentFactory()):
        self.billboard = billboard
