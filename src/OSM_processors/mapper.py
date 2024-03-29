import contextlib
from datetime import datetime

# future: remove the comments below when stubs for the library below are available
import geojson  # type: ignore
from overpy import Element, Overpass, Result  # type: ignore
from pydeck import Layer, Deck, ViewState  # type: ignore
from sqlalchemy.sql import text
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.SR import SR
from src.new_data_processors.common import DataProcessor


def extract_operating_site_polygons(
    operating_sites: Result,
) -> tuple[list[dict[str, int | None]], list[dict[str, int | None]]]:
    operating_site_areas = [
        get_ids_of_layers(operating_site) for operating_site in operating_sites.ways
    ]
    operating_site_relations = [
        get_ids_of_layers(operating_site)
        for operating_site in operating_sites.relations
    ]
    return operating_site_areas, operating_site_relations


def get_ids_of_layers(element: Element) -> dict[str, int | None]:
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


class Mapper(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        
        self.TODAY_SIMULATED = datetime(2024, 1, 18, 21, 59, 59)

        self.query_operating_sites: str = """
            [out:json];
            
            area["ISO3166-1"="HU"]
              -> .country;
            
            
            (
                area["operator"="MÁV"]["railway"="station"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="station"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="station"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="station"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="halt"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="halt"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="halt"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="halt"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="yard"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="yard"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="yard"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="yard"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="service_station"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="service_station"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="service_station"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="service_station"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="junction"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="junction"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="junction"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="junction"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="crossover"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="crossover"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="crossover"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="crossover"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="spur_junction"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="spur_junction"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="spur_junction"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="spur_junction"]["uic_ref"](area.country);
                
                area["operator"="MÁV"]["railway"="site"]["uic_ref"](area.country);
                area["operator"="GYSEV"]["railway"="site"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="MÁV"]["railway"="site"]["uic_ref"](area.country);
                relation["type"="multipolygon"]["operator"="GYSEV"]["railway"="site"]["uic_ref"](area.country);
            );
            (._;>;);
            out;
            """
        # future: replace query with uncommented lines below when https://github.com/drolbr/Overpass-API/issues/146 is closed
        # self.query_operating_sites: str = """
        # [out:json];
        #
        # area["ISO3166-1"="HU"]
        #     -> .country;
        #
        # (
        #     relation["route"="railway"]["ref"]["operator"~"(^MÁV(?=;))|((?<=;)MÁV(?=;))|((?<=;)MÁV$)"](area.country);
        #     relation["route"="railway"]["ref"]["operator"~"(^GYSEV(?=;))|((?<=;)GYSEV(?=;))|((?<=;)GYSEV$)"](area.country);
        # );
        # >>;
        # out;
        #
        # """
        self.query_final: str = """
        [out:json];
        
        area["ISO3166-1"="HU"]
            -> .country;
        
        (
            relation["route"="railway"]["ref"]["operator"~"MÁV"](area.country);
            relation["route"="railway"]["ref"]["operator"~"GYSEV"](area.country);
        );
        >>;
        out;
        
        """

        self.osm_data: Result = NotImplemented
        self.sr_ways: list[int] = []

    def run(self) -> None:
        self.download_osm_data()
        self.process_srs()
        self.visualise_srs()

    @retry(
        retry=retry_if_exception_type(ConnectionResetError),
        wait=wait_exponential(
            multiplier=1,
            min=4,
            max=10,
        ),
        stop=stop_after_attempt(2),
    )
    def download_osm_data(self) -> None:
        api = Overpass()

        operating_sites = self.download_operating_sites(api)
        self.download_final(
            api=api,
            operating_sites=operating_sites,
        )

    def download_operating_sites(self, api: Overpass) -> Result:
        self.logger.debug(f"Short query started...")
        result = api.query(self.query_operating_sites)
        self.logger.debug(f"...finished!")
        return result

    def download_final(self, api: Overpass, operating_sites: Result) -> None:
        operating_site_areas, operating_site_relations = (
            extract_operating_site_polygons(operating_sites)
        )
        
        for operating_site_area in operating_site_areas:
            self.query_final += f"""
            way({operating_site_area["element_id"]}) -> .operatingSite;
            """
            self.add_operating_site_elements(operating_site_area)
            
        for operating_site_relation in operating_site_relations:
            self.query_final += f"""
            relation({operating_site_relation["element_id"]});
            map_to_area -> .operatingSite;
            """
            self.add_operating_site_elements(operating_site_relation)
            
        self.logger.debug(f"Long query started...")
        self.osm_data = api.query(self.query_final)
        self.logger.debug(f"...finished!")
    
    def add_operating_site_elements(self, operating_site_area: dict[str, int | None]) -> None:
        if operating_site_area["layer"]:
            self.query_final += f"""
                (
                    way["railway"="rail"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    way["disused:railway"="rail"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                    way["abandoned:railway"="rail"]["layer"="{operating_site_area["layer"]}"](area.operatingSite);
                );
                """
        else:
            self.query_final += f"""
                (
                    way["railway"="rail"][!"layer"](area.operatingSite);
                    way["disused:railway"="rail"][!"layer"](area.operatingSite);
                    way["abandoned:railway"="rail"][!"layer"](area.operatingSite);
                    
                    way["railway"="rail"]["layer"="0"](area.operatingSite);
                    way["disused:railway"="rail"]["layer"="0"](area.operatingSite);
                    way["abandoned:railway"="rail"]["layer"="0"](area.operatingSite);
                );
                """
        self.query_final += f"""
            (._;>;);
            out;
            """
    
    def process_srs(self) -> None:
        with self.database.engine.begin() as connection:
            # TODO: replace query with uncommented lines below in production
            # query = """
            # select *
            # from speed_restrictions
            # where (time_to > now() or time_to is null) and time_from <= now();
            # """
            query = """
            select *
            from speed_restrictions
            """
            result = connection.execute(text(query))

        srs: list[SR] = []
        for row in result:
            srs.append(
                # future: report bug (false positive) to mypy developers
                SR(  # type: ignore
                    *row[1:],
                    sr_id=row[0],
                )
            )

        for sr in srs:
            if (
                sr.on_main_track
            ):  # future: remove this line when I have time to visualize SRs on more stations
                try:
                    for relation in self.osm_data.relations:
                        with contextlib.suppress(KeyError):
                            if (
                                relation.tags["route"] == "railway"
                                and relation.tags["ref"].upper() == sr.line.upper()
                            ):
                                for member in relation.members:
                                    self.sr_ways.append(member.ref)
                                break
                    else:
                        raise ValueError(f"Relation with `ref={sr.line}` not found!")
                except ValueError as exception:
                    self.logger.debug(exception)

    def visualise_srs(self) -> None:
        features = []
        for i, node in enumerate(self.osm_data.nodes):
            point = geojson.Point((float(node.lon), float(node.lat)))
            node.tags |= {"line_color": [0, 0, 0, 0]}
            feature = geojson.Feature(
                geometry=point,
                properties=node.tags,
            )
            features.append(feature)
        for i, way in enumerate(self.osm_data.ways):
            line = geojson.LineString(
                [(float(node.lon), float(node.lat)) for node in way.nodes]
            )
            if way.id in self.sr_ways:
                way.tags |= {"line_color": [255, 255, 255]}
                feature = geojson.Feature(
                    geometry=line,
                    properties=way.tags,
                )
            else:
                way.tags |= {"line_color": [255, 0, 0]}
                feature = geojson.Feature(
                    geometry=line,
                    properties=way.tags,
                )
            features.append(feature)
        feature_collection = geojson.FeatureCollection(features)

        geojson_layer = Layer(
            "GeoJsonLayer",
            data=feature_collection,
            pickable=True,
            line_width_min_pixels=2,
            get_line_color="line_color",
            get_fill_color=[0, 0, 0],
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

        self.logger.debug(f"Exporting map started...")
        deck.to_html(f"data/04_exported/map_pydeck_{self.TODAY}.html")
        self.logger.debug(f"...finished!")
