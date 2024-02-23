import logging

# future: remove the comments below when stubs for the library below are available
import geojson  # type: ignore
from overpy import Element, Overpass  # type: ignore
from pydeck import Layer, Deck, ViewState  # type: ignore


def _get_ids_layers(element: Element) -> dict[str, int | None]:
    # future: report bug (false positive) to JetBrains developers
    # noinspection PyUnresolvedReferences
    element_id = element.id
    element_layer = element.tags.get("layer", None)
    if element_layer is None:
        return {
            "element_id": element_id,
            "layer": None,
        }
    # future: report bug (false positive) to JetBrains developers
    # noinspection PyTypeChecker
    return {
        "element_id": element_id,
        "layer": int(element_layer),
    }


class OsmDownloader:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self) -> None:
        self._download()

    def _download(self) -> None:
        api = Overpass()

        self.logger.debug(f"Short query started!")
        result = api.query(
            """
            [out:json];
            
            (
                area["railway=station"];
                area["railway"="halt"];
                area["railway"="yard"];
                area["railway"="service_station"];
                area["railway"="junction"];
                area["railway"="crossover"];
                area["railway"="spur_junction"];
                area["railway"="site"];
            );
            (._;>;);
            out;
            """
        )
        self.logger.debug(f"Short query finished!")

        areas = [_get_ids_layers(way) for way in result.ways]

        query = """
        [out:json];
        
        
        area["ISO3166-1"="HU"]
            -> .country;
        
        node["railway"="milestone"](area.country);
        out;
        
        """
        for area in areas:
            if area["layer"]:
                query += f"""
                
                way({area["element_id"]}) -> .operatingSite;
                nw["layer"="{area["layer"]}"](area.operatingSite);
                (._;>;);
                out;
                """
            else:
                query += f"""

                way({area["element_id"]}) -> .operatingSite;
                (
                    nw[!"layer"](area.operatingSite);
                    nw["layer"="0"](area.operatingSite);
                );
                (._;>;);
                out;
                """

        self.logger.debug(f"Long query started!")
        result = api.query(query)
        self.logger.debug(f"Long query finished!")

        features = []
        for i, node in enumerate(result.nodes):
            # Create a Point geometry with the node's latitude and longitude
            point = geojson.Point((float(node.lon), float(node.lat)))
            # Create a Feature with the geometry and the node's tags as properties
            feature = geojson.Feature(
                geometry=point,
                properties=node.tags,
            )
            # Append the feature to the list
            features.append(feature)
        for i, way in enumerate(result.ways):
            # Create a LineString geometry with the latitudes and longitudes of the way's nodes
            line = geojson.LineString(
                [(float(node.lon), float(node.lat)) for node in way.nodes]
            )
            # Create a Feature with the geometry and the way's tags as properties
            feature = geojson.Feature(
                geometry=line,
                properties=way.tags,
            )
            # Append the feature to the list
            features.append(feature)
        feature_collection = geojson.FeatureCollection(features)

        geojson_layer = Layer(
            "GeoJsonLayer",
            data=feature_collection,
            pickable=True,
            line_width_min_pixels=4,
            get_line_color=[255, 255, 255],
            get_fill_color=[255, 255, 255],
        )
        view_state = ViewState(
            latitude=47.4979,
            longitude=19.0402,
            zoom=6,
        )
        deck = Deck(
            layers=[geojson_layer],
            initial_view_state=view_state,
        )

        deck.to_html("map_pydeck.html")

        pass
