import contextlib
from datetime import datetime

# future: remove the comment below when stubs for the library below are available
import geojson  # type: ignore

# future: remove the comment below when stubs for the library below are available
from overpy import Element, Node, Overpass, Relation, Result, Way  # type: ignore

# future: remove the comment below when stubs for the library below are available
from pydeck import Layer, Deck, ViewState  # type: ignore

# future: remove the comment below when stubs for the library below are available
import shapely  # type: ignore

# future: remove the comment below when stubs for the library below are available
from shapely.ops import split  # type: ignore
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


def get_milestones(nodes: list[Node]) -> list[Node]:
    return [
        node
        for node in nodes
        if node.tags.get("railway", None) == "milestone"
        and node.tags.get("railway:position", None) is not None
    ]


def get_nearest_milestone(metre_post: int, milestones_current: list[Node]) -> Node:
    return min(
        milestones_current,
        key=lambda milestone: abs(
            float(milestone.tags["railway:position"]) * 1000 - metre_post
        ),
    )


def further_than_current_nearest_milestone(
    nearest_milestone_current: Node, nearest_milestones: list[Node], metre_post: int
):
    return (
        float(nearest_milestone_current.tags["railway:position"]) * 1000
        < float(nearest_milestones[-1].tags["railway:position"]) * 1000
        < metre_post
        or metre_post
        < float(nearest_milestones[-1].tags["railway:position"]) * 1000
        < float(nearest_milestone_current.tags["railway:position"]) * 1000
    )


def get_ways_of_milestones(
    nearest_milestones: list[Node], ways: list[Way]
) -> tuple[Way, Way]:
    way_of_lower_milestone: Way | None = None
    way_of_greater_milestone: Way | None = None

    for way in ways:
        for node in way.nodes:
            if node == nearest_milestones[0]:
                way_of_lower_milestone = way
            elif node == nearest_milestones[-1]:
                way_of_greater_milestone = way

            if way_of_lower_milestone and way_of_greater_milestone:
                return way_of_greater_milestone, way_of_lower_milestone

    raise ValueError("Ways of milestones not found!")


def get_distance_percentage_between_milestones(
    nearest_milestones: list[Node], sr: SR
) -> float:
    nearest_milestones_locations = [
        int(float(milestone.tags["railway:position"]) * 1000)
        for milestone in nearest_milestones
    ]
    distance_between_nearest_milestones = abs(
        nearest_milestones_locations[0] - nearest_milestones_locations[-1]
    )
    distance_between_sr_and_lower_milestone = abs(
        nearest_milestones_locations[0] - sr.metre_post_from
    )
    distance_percentage_between_milestones = (
        distance_between_sr_and_lower_milestone / distance_between_nearest_milestones
    )
    return distance_percentage_between_milestones


class Mapper(DataProcessor):
    def __init__(self, show_lines_with_no_data: bool) -> None:
        super().__init__()

        self.TODAY_SIMULATED = datetime(2024, 1, 18, 21, 59, 59)

        self.show_lines_with_no_data = show_lines_with_no_data
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
        self.srs: list[SR] = []
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

    def add_operating_site_elements(
        self, operating_site_area: dict[str, int | None]
    ) -> None:
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
        self.get_all_srs_from_database()

        if self.show_lines_with_no_data:
            self.get_id_of_sr_main_track_ways()

    def get_all_srs_from_database(self) -> None:
        with self.database.engine.begin() as connection:
            # TODO: replace query with uncommented lines below in production
            # query = """
            # select *
            # from speed_restrictions
            # where
            #     time_from <= now() and (now() < time_to or time_to is null);
            # """
            query = """
            select *
            from speed_restrictions
            where
                line = 146 and
                on_main_track = 1 and
                time_from <= :now and (:now < time_to or time_to is null);
            """
            result = connection.execute(
                text(query),
                {"now": self.TODAY_SIMULATED},
            )

        for row in result:
            self.srs.append(
                # future: report bug (false positive) to mypy developers
                SR(  # type: ignore
                    *row[1:],
                    sr_id=row[0],
                )
            )

    def get_id_of_sr_main_track_ways(self) -> None:
        for sr in self.srs:
            if sr.on_main_track:
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
        for node in self.osm_data.nodes:
            point = geojson.Point((float(node.lon), float(node.lat)))
            node.tags |= {"line_color": [0, 0, 0, 0]}
            feature = geojson.Feature(
                geometry=point,
                properties=node.tags,
            )
            features.append(feature)

        if self.show_lines_with_no_data:
            for way in self.osm_data.ways:
                way = geojson.LineString(
                    [(float(node.lon), float(node.lat)) for node in way.nodes]
                )
                if way.id in self.sr_ways:
                    way.tags |= {"line_color": [255, 255, 255]}
                    feature = geojson.Feature(
                        geometry=way,
                        properties=way.tags,
                    )
                else:
                    way.tags |= {"line_color": [255, 0, 0]}
                    feature = geojson.Feature(
                        geometry=way,
                        properties=way.tags,
                    )
                features.append(feature)
        else:
            for sr in self.srs:
                relation = self.get_corresponding_relation(sr)
                way_ids = [way.ref for way in relation.members]
                ways = [way for way in self.osm_data.ways if way.id in way_ids]
                nodes = [node for way in ways for node in way.nodes]
                milestones = get_milestones(nodes)
                milestones.sort(
                    key=lambda milestone: float(milestone.tags["railway:position"])
                )

                for i in range(2):
                    nearest_milestones: list[Node] = []
                    milestones_current = milestones.copy()
                    while len(nearest_milestones) < 2:
                        nearest_milestone_current = get_nearest_milestone(
                            metre_post=(
                                sr.metre_post_from if i == 0 else sr.metre_post_to
                            ),
                            milestones_current=milestones_current,
                        )
                        if nearest_milestones and not (
                            further_than_current_nearest_milestone(
                                nearest_milestone_current=nearest_milestone_current,
                                nearest_milestones=nearest_milestones,
                                metre_post=(
                                    sr.metre_post_from if i == 0 else sr.metre_post_to
                                ),
                            )
                        ):
                            if float(
                                nearest_milestone_current.tags["railway:position"]
                            ) != float(
                                nearest_milestones[-1].tags["railway:position"]
                            ):  # TODO: remove this line when track side selection is implemented
                                nearest_milestones.append(nearest_milestone_current)
                        else:
                            nearest_milestones.append(nearest_milestone_current)
                        milestones_current.remove(nearest_milestone_current)
                    nearest_milestones.sort(
                        key=lambda milestone: float(milestone.tags["railway:position"])
                    )

                    distance_percentage_between_milestones = (
                        get_distance_percentage_between_milestones(
                            nearest_milestones, sr
                        )
                    )
                    way_of_greater_milestone, way_of_lower_milestone = (
                        get_ways_of_milestones(nearest_milestones, ways)
                    )

                    neighbouring_ways_of_lower_milestone: tuple[
                        list[Way], list[Way]
                    ] = ([], [])
                    ways_between_milestones: list[Way] = NotImplemented
                    for way in ways:
                        if way is way_of_greater_milestone:
                            if way_of_lower_milestone.nodes[0] in way.nodes:
                                ways_between_milestones = (
                                    neighbouring_ways_of_lower_milestone[0]
                                )
                            elif way_of_lower_milestone.nodes[-1] in way.nodes:
                                ways_between_milestones = (
                                    neighbouring_ways_of_lower_milestone[1]
                                )

                            break
                        else:
                            if way_of_lower_milestone.nodes[0] in way.nodes:
                                neighbouring_ways_of_lower_milestone[0].append(way)
                            elif way_of_lower_milestone.nodes[-1] in way.nodes:
                                neighbouring_ways_of_lower_milestone[1].append(way)
                    ways_between_milestones.append(way_of_greater_milestone)

                    coordinates: list[tuple[float, float]] = []
                    for way in ways_between_milestones:
                        way_coordinates = [
                            (float(node.lon), float(node.lat)) for node in way.nodes
                        ]
                        coordinates.extend(way_coordinates)
                    merged_ways_between_milestones = shapely.LineString(coordinates)

                    split_lines_at_lower_milestone = split(
                        geom=merged_ways_between_milestones,
                        splitter=shapely.Point(
                            (
                                float(nearest_milestones[0].lon),
                                float(nearest_milestones[0].lat),
                            )
                        ),
                    )
                    split_lines_at_greater_milestone = split(
                        geom=merged_ways_between_milestones,
                        splitter=shapely.Point(
                            (
                                float(nearest_milestones[-1].lon),
                                float(nearest_milestones[-1].lat),
                            )
                        ),
                    )
                    line_between_milestones = (
                        split_lines_at_lower_milestone.intersection(
                            split_lines_at_greater_milestone
                        )
                    )

                    coordinate_of_metre_post = line_between_milestones.interpolate(
                        distance=distance_percentage_between_milestones,
                        normalized=True,
                    )

                    if i == 0:
                        sr.metre_post_from_coordinates = coordinate_of_metre_post
                    else:
                        sr.metre_post_to_coordinates = coordinate_of_metre_post
                pass

        feature_collection = geojson.FeatureCollection(features)
        self.export_map(feature_collection)

    def export_map(self, feature_collection):
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

    def get_corresponding_relation(self, sr: SR) -> Relation:
        relation = [
            relation
            for relation in self.osm_data.relations
            if relation.tags["ref"].upper() == sr.line.upper()
        ][0]
        return relation
