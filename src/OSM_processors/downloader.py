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
            
            area["ISO3166-1"="HU"]
              -> .country;
            
            
            (
                area["operator"="MÁV"]["railway"="station"](area.country);
                area["operator"="GYSEV"]["railway"="station"](area.country);
                
                area["operator"="MÁV"]["railway"="halt"](area.country);
                area["operator"="GYSEV"]["railway"="halt"](area.country);
                
                area["operator"="MÁV"]["railway"="yard"](area.country);
                area["operator"="GYSEV"]["railway"="yard"](area.country);
                
                area["operator"="MÁV"]["railway"="service_station"](area.country);
                area["operator"="GYSEV"]["railway"="service_station"](area.country);
                
                area["operator"="MÁV"]["railway"="junction"](area.country);
                area["operator"="GYSEV"]["railway"="junction"](area.country);
                
                area["operator"="MÁV"]["railway"="crossover"](area.country);
                area["operator"="GYSEV"]["railway"="crossover"](area.country);
                
                area["operator"="MÁV"]["railway"="spur_junction"](area.country);
                area["operator"="GYSEV"]["railway"="spur_junction"](area.country);
                
                area["operator"="MÁV"]["railway"="site"](area.country);
                area["operator"="GYSEV"]["railway"="site"](area.country);
            );
            (._;>;);
            out;
            """
        )
        self.logger.debug(f"Short query finished!")

        operating_site_areas = [_get_ids_layers(operating_site) for operating_site in result.ways]

        query = """
        [out:json];
        
        area["ISO3166-1"="HU"]
            -> .country;
        
        (
            way["operator"="MÁV"]["railway"="rail"](area.country);
            way["operator"="GYSEV"]["railway"="rail"](area.country);
        );
        (._;>;);
        out;
        
        """
        for operating_site_area in operating_site_areas:
            if operating_site_area["layer"]:
                query += f"""
                way({operating_site_area["element_id"]}) -> .operatingSite;
                (
                    way["railway"="rail"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    way["disused:railway"="rail"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    way["abandoned:railway"="rail"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    
                    node["railway"="switch"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    node["disused:railway"="switch"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    node["abandoned:railway"="switch"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                );
                (._;>;);
                out;
                """
            else:
                query += f"""
                way({operating_site_area["element_id"]}) -> .operatingSite;
                (
                    way["railway"="rail"][!"layer"](area.operatingSite);
                    way["disused:railway"="rail"][!"layer"](area.operatingSite);
                    way["abandoned:railway"="rail"][!"layer"](area.operatingSite);
                    
                    way["railway"="rail"]["layer"="0"](area.operatingSite);
                    way["disused:railway"="rail"]["layer"="0"](area.operatingSite);
                    way["abandoned:railway"="rail"]["layer"="0"](area.operatingSite);
                    
                    
                    node["railway"="switch"][!"layer"](area.operatingSite);
                    node["disused:railway"="switch"][!"layer"](area.operatingSite);
                    node["abandoned:railway"="switch"][!"layer"](area.operatingSite);
                    
                    node["railway"="switch"]["layer"="0"](area.operatingSite);
                    node["disused:railway"="switch"]["layer"="0"](area.operatingSite);
                    node["abandoned:railway"="switch"]["layer"="0"](area.operatingSite);
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
            line_width_min_pixels=2,
            get_line_color=[255, 255, 255],
            get_fill_color=[255, 255, 255],
        )
        view_state = ViewState(
            latitude=47.180833,
            longitude=19.503056,
            zoom=7,
        )
        deck = Deck(
            layers=[geojson_layer],
            initial_view_state=view_state,
        )

        deck.to_html("map_pydeck.html")

        pass
