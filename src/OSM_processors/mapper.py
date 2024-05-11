import contextlib
from datetime import datetime
import json
from typing import Any, Final

# future: remove the comment below when stubs for the library below are available
import geojson  # type: ignore

# future: remove the comment below when stubs for the library below are available
from overpy import Area, Element, Node, Overpass, Relation, Result, Way  # type: ignore

# future: remove the comment below when stubs for the library below are available
from pydeck import Deck, Layer, ViewState  # type: ignore
import requests
from requests import HTTPError

# future: remove the comment below when stubs for the library below are available
import shapely  # type: ignore
from shapely import from_geojson, get_coordinates

# future: remove the comment below when stubs for the library below are available
from shapely.geometry import shape  # type: ignore

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
    areas: list[Area], multipolygons: list[Relation]
) -> tuple[list[dict[str, int | None]], list[dict[str, int | None]]]:
    operating_site_areas = [
        get_ids_of_layers(operating_site) for operating_site in areas
    ]
    operating_site_relations = [
        get_ids_of_layers(operating_site) for operating_site in multipolygons
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
    milestones = [
        node
        for node in nodes
        if node.tags.get("railway", None) == "milestone"
        and node.tags.get("railway:position", None) is not None
    ]
    milestones.sort(key=lambda milestone: float(milestone.tags["railway:position"]))
    return milestones


# noinspection PyShadowingNames
def get_nearest_milestone(exact_location: int, milestones: list[Node], sr: SR) -> Node:
    milestones.sort(
        key=lambda milestone: abs(get_milestone_location(milestone) - exact_location)
    )
    for i, milestone in enumerate(milestones):
        if sr.on_main_track:
            try:
                if milestone.tags["railway:track_side"] == sr.main_track_side:
                    return milestone
            except KeyError:
                continue
        else:
            return milestone
    raise ValueError("Nearest milestone not found!")


def get_milestone_location(milestone: Node) -> float:
    return float(milestone.tags["railway:position"]) * 1000


def further_than_current_nearest_milestone(
    nearest_milestone_current: Node, nearest_milestones: list[Node], metre_post: int
) -> bool:
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
                return way_of_lower_milestone, way_of_greater_milestone

    raise ValueError("Ways of milestones not found!")


def get_distance_percentage_between_milestones(
    nearest_milestones: list[Node], metre_post_boundary: int
) -> float:
    nearest_milestones_locations = [
        int(float(milestone.tags["railway:position"]) * 1000)
        for milestone in nearest_milestones
    ]
    distance_between_nearest_milestones = abs(
        nearest_milestones_locations[0] - nearest_milestones_locations[-1]
    )
    distance_between_sr_and_lower_milestone = abs(
        nearest_milestones_locations[0] - metre_post_boundary
    )
    distance_percentage_between_milestones = (
        distance_between_sr_and_lower_milestone / distance_between_nearest_milestones
    )
    return distance_percentage_between_milestones


def get_nearest_milestones(
    milestones: list[Node],
    nearest_milestones: list[Node],
    metre_post: int,
    sr: SR,
) -> None:
    while len(nearest_milestones) < 2:
        try:
            nearest_milestone_current = get_nearest_milestone(
                exact_location=metre_post,
                milestones=milestones,
                sr=sr,
            )
        except ValueError:
            raise
        if nearest_milestones and not (
            further_than_current_nearest_milestone(
                nearest_milestone_current=nearest_milestone_current,
                nearest_milestones=nearest_milestones,
                metre_post=metre_post,
            )
        ):
            nearest_milestones.append(nearest_milestone_current)
        milestones.remove(nearest_milestone_current)

    nearest_milestones.sort(
        key=lambda milestone: float(milestone.tags["railway:position"])
    )


def get_ways_between_milestones(
    way_of_greater_milestone: Way,
    way_of_lower_milestone: Way,
    ways_of_line: list[Way],
) -> list[Way]:
    neighbouring_ways_of_lower_milestone: tuple[list[Way], list[Way]] = (
        [],
        [],
    )
    ways_between_milestones: list[Way] = []
    for way in ways_of_line:
        if way is way_of_greater_milestone:
            if way_of_lower_milestone.nodes[0] in way.nodes:
                ways_between_milestones = neighbouring_ways_of_lower_milestone[0]
            elif way_of_lower_milestone.nodes[-1] in way.nodes:
                ways_between_milestones = neighbouring_ways_of_lower_milestone[1]

            break
        else:
            if way_of_lower_milestone.nodes[0] in way.nodes:
                neighbouring_ways_of_lower_milestone[0].append(way)
            elif way_of_lower_milestone.nodes[-1] in way.nodes:
                neighbouring_ways_of_lower_milestone[1].append(way)
    ways_between_milestones.append(way_of_greater_milestone)
    return ways_between_milestones


def merge_ways(ways_between_milestones: list[Way]) -> shapely.LineString:
    coordinates: list[tuple[float, float]] = []
    for way in ways_between_milestones:
        way_coordinates = [(float(node.lon), float(node.lat)) for node in way.nodes]
        coordinates.extend(way_coordinates)
    merged_ways_between_milestones = shapely.LineString(coordinates)
    return merged_ways_between_milestones


def split_lines(
    merged_ways_between_milestones: shapely.LineString,
    splitting_point: shapely.Point,
) -> shapely.MultiLineString:
    split_lines_at_lower_milestone = split(
        geom=merged_ways_between_milestones,
        splitter=splitting_point,
    )
    return split_lines_at_lower_milestone


class Mapper(DataProcessor):
    def __init__(self, show_lines_with_no_data: bool) -> None:
        super().__init__()

        self.TODAY_SIMULATED: Final = datetime(2024, 1, 18, 21, 59, 59)
        self.COLOR_TAG: Final = "line_color"
        self.QUERY_MAIN_PARAMETERS: Final = """
            [out:json];
            
            area["ISO3166-1"="HU"]
            // area["admin_level"="8"]["name"="Hegyeshalom"]
              -> .country;
              
        """
        self.OPERATORS: Final = ["MÁV", "GYSEV"]
        self.OPERATING_SITE_TAG_VALUES: Final = [
            "station",
            "halt",
            "yard",
            "service_station",
            "junction",
            "crossover",
            "spur_junction",
            "site",
        ]

        self._api: Final = Overpass()
        self._dowload_session: Final = requests.Session()

        self.show_lines_with_no_data = show_lines_with_no_data

        self.query_operating_site_elements = (
            self.QUERY_MAIN_PARAMETERS
            + """
            (
        """
        )
        for value in self.OPERATING_SITE_TAG_VALUES:
            for operator in self.OPERATORS:
                self.query_operating_site_elements += f"""
                    node["operator"="{operator}"]["railway"="{value}"]["name"](area.country);
                    area["operator"="{operator}"]["railway"="{value}"]["name"](area.country);
                    relation["type"="multipolygon"]["operator"="{operator}"]["railway"="{value}"]["name"](area.country);
                """
            self.query_operating_site_elements += "\n"
        self.query_operating_site_elements += """
            );
            out;
        """

        self.query_final: str = (
            self.QUERY_MAIN_PARAMETERS
            # future: replace lines below when https://github.com/drolbr/Overpass-API/issues/146 is closed
            #     relation["route"="railway"]["ref"]["operator"~"(^MÁV(?=;))|((?<=;)MÁV(?=;))|((?<=;)MÁV$)"](area.country);
            #     relation["route"="railway"]["ref"]["operator"~"(^GYSEV(?=;))|((?<=;)GYSEV(?=;))|((?<=;)GYSEV$)"](area.country);
            + """
            (
                relation["route"="railway"]["ref"]["operator"~"MÁV"](area.country);
                relation["route"="railway"]["ref"]["operator"~"GYSEV"](area.country);
            );
            >>;
            out;
            
        """
        )

        self.osm_data_raw: dict = NotImplemented
        self.osm_data: Result = NotImplemented
        self.srs: list[SR] = []
        self.sr_ways: list[int] = []

    def run(self) -> None:
        self.download_osm_data()
        self.process_srs()
        self.visualise_srs()

    @retry(
        retry=retry_if_exception_type(ConnectionResetError),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(2),
    )
    def download_osm_data(self) -> None:
        operating_site_elements = self.run_query(
            api=self._api,
            query_text=self.query_operating_site_elements,
        )

        node_polygons_query = self.QUERY_MAIN_PARAMETERS
        for node in operating_site_elements.nodes:
            node_polygons_query += f"""
                node({node.id}) -> .station;
                .station out;
                nwr(around.station:100)["landuse"="railway"];
                convert item ::geom=geom();
                out geom;
            """
        operating_site_node_polygons = geojson.loads(
            self.run_query_raw(
                api=self._api,
                query_text=node_polygons_query,
            )
        )

        self.download_final(
            node_polygons=operating_site_node_polygons,
            areas=operating_site_elements.areas,
            multipolygons=operating_site_elements.relations,
        )

    def run_query(self, api: Overpass, query_text: str) -> Result:
        self.logger.debug(f"Short query started...")
        result = api.query(query_text)
        self.logger.debug(f"...finished!")
        return result

    def run_query_raw(self, api: Overpass, query_text: str) -> bytes:
        url = api.url
        try:
            response = self._dowload_session.get(
                url=url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
                },
                data=query_text,
            )
            response.raise_for_status()
            self.logger.debug(f"File successfully downloaded from {url}!")
            return bytes(response.content)
        except HTTPError:
            self.logger.critical(f"Failed to download file from {url}!")
            raise

    def download_final(
        self,
        node_polygons: Any,
        areas: list[Area],
        multipolygons: list[Relation],
    ) -> None:
        self.add_node_poly_elements(node_polygons)

        operating_site_areas, operating_site_mp_relations = (
            extract_operating_site_polygons(
                areas=areas,
                multipolygons=multipolygons,
            )
        )

        for operating_site_area in operating_site_areas:
            self.query_final += f"""
            way({operating_site_area["element_id"]}) -> .operatingSite;
            """
            self.add_area_or_mp_relation_elements(operating_site_area)

        for operating_site_relation in operating_site_mp_relations:
            self.query_final += f"""
            relation({operating_site_relation["element_id"]});
            map_to_area -> .operatingSite;
            """
            self.add_area_or_mp_relation_elements(operating_site_relation)

        self.logger.debug(f"Long query started...")
        self.osm_data_raw = json.loads(
            self.run_query_raw(
                api=self._api,
                query_text=self.query_final,
            )
        )
        self.osm_data = Result.from_json(self.osm_data_raw)
        self.logger.debug(f"...finished!")
        pass

    def add_node_poly_elements(self, node_polygons: Any) -> None:
        for i, element in enumerate(node_polygons["elements"]):
            try:
                assert element["geometry"]
                operating_site_polygon_array = get_coordinates(
                    from_geojson(str(element["geometry"]))
                )
                operating_site_polygon = ""
                for coordinate in operating_site_polygon_array:
                    operating_site_polygon += f"{coordinate[1]} {coordinate[0]} "
                operating_site_polygon = operating_site_polygon.strip()
                try:
                    layer = node_polygons["elements"][i - 1]["tags"]["layer"]
                    self.query_final += f"""
                        (
                            way["railway"="rail"]["layer"="{layer}"](poly:"{operating_site_polygon}");
                            way["disused:railway"="rail"]["layer"="{layer}"](poly:"{operating_site_polygon}");
                            way["abandoned:railway"="rail"]["layer"="{layer}"](poly:"{operating_site_polygon}");
                        );"""
                except KeyError:
                    self.query_final += f"""
                        (
                            way["railway"="rail"][!"layer"](poly:"{operating_site_polygon}");
                            way["disused:railway"="rail"][!"layer"](poly:"{operating_site_polygon}");
                            way["abandoned:railway"="rail"][!"layer"](poly:"{operating_site_polygon}");
                            
                            way["railway"="rail"]["layer"="0"](poly:"{operating_site_polygon}");
                            way["disused:railway"="rail"]["layer"="0"](poly:"{operating_site_polygon}");
                            way["abandoned:railway"="rail"]["layer"="0"](poly:"{operating_site_polygon}");
                        );"""
                self.query_final += f"""
                    (._;>;);
                    out;
                    node(1);
                    out ids;
                """
            except KeyError:
                pass

    def add_area_or_mp_relation_elements(
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
            node(1);
            out ids;
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
        features_to_visualise: list[geojson.Feature] = []

        self.add_all_nodes(features_to_visualise)
        self.add_na_lines(features_to_visualise)

        for sr in self.srs:
            try:
                ways_of_line = self.get_ways_of_corresponding_line(sr)
                nodes_of_line = [node for way in ways_of_line for node in way.nodes]
                milestones_of_line = get_milestones(nodes=nodes_of_line)

                for i, sr_metre_post_boundary in enumerate(
                    (sr.metre_post_from, sr.metre_post_to)
                ):
                    nearest_milestones: list[Node] = []
                    milestones_of_line_copy = milestones_of_line.copy()
                    get_nearest_milestones(
                        milestones=milestones_of_line_copy,
                        nearest_milestones=nearest_milestones,
                        metre_post=sr_metre_post_boundary,
                        sr=sr,
                    )
                    metre_post_at_percentage_between_milestones = (
                        get_distance_percentage_between_milestones(
                            nearest_milestones, sr_metre_post_boundary
                        )
                    )
                    way_of_lower_milestone, way_of_greater_milestone = (
                        get_ways_of_milestones(nearest_milestones, ways_of_line)
                    )
                    ways_between_milestones = get_ways_between_milestones(
                        way_of_greater_milestone,
                        way_of_lower_milestone,
                        ways_of_line,
                    )
                    merged_ways_between_milestones = merge_ways(ways_between_milestones)

                    split_lines_at_lower_milestone = split_lines(
                        merged_ways_between_milestones,
                        shapely.Point(
                            (
                                float(nearest_milestones[0].lon),
                                float(nearest_milestones[0].lat),
                            )
                        ),
                    )
                    split_lines_at_greater_milestone = split_lines(
                        merged_ways_between_milestones,
                        shapely.Point(
                            (
                                float(nearest_milestones[-1].lon),
                                float(nearest_milestones[-1].lat),
                            )
                        ),
                    )
                    line_between_milestones = shapely.intersection(
                        split_lines_at_lower_milestone.geoms[1],
                        split_lines_at_greater_milestone.geoms[0],
                    )

                    coordinate_of_metre_post = line_between_milestones.interpolate(
                        distance=metre_post_at_percentage_between_milestones,
                        normalized=True,
                    )

                    # future: init `metre_post_from_coordinates` and `metre_post_to_coordinates` in the constructor
                    if sr_metre_post_boundary == sr.metre_post_from:
                        sr.metre_post_from_coordinates = coordinate_of_metre_post  # type: ignore
                    else:
                        sr.metre_post_to_coordinates = coordinate_of_metre_post  # type: ignore

                    pass
            except (IndexError, ValueError) as exception:
                prepared_lines = [
                    "1",
                    "1d",
                    "146",
                    "113 (1)",
                    "113 (2)",
                    "30",
                    "8",
                    "18",
                    "17",
                    "9",
                ]
                if sr.line not in prepared_lines:
                    pass
                else:
                    self.logger.debug(exception)
                    raise
            pass

        feature_collection_to_visualise = geojson.FeatureCollection(
            features_to_visualise
        )
        self.export_map(feature_collection_to_visualise)

    def get_ways_of_corresponding_line(self, sr: SR) -> list[Way]:
        relation = self.get_corresponding_relation(sr)
        way_ids = [way.ref for way in relation.members]
        ways = [way for way in self.osm_data.ways if way.id in way_ids]
        return ways

    def add_na_lines(self, features_to_visualise: list[geojson.Feature]) -> None:
        for way in self.osm_data.ways:
            way_line = geojson.LineString(
                [(float(node.lon), float(node.lat)) for node in way.nodes]
            )
            way.tags |= {
                self.COLOR_TAG: (
                    [255, 255, 255] if way.id in self.sr_ways else [65, 65, 65]
                )
            }

            feature = geojson.Feature(
                geometry=way_line,
                properties=way.tags,
            )
            features_to_visualise.append(feature)

    def add_all_nodes(self, features_to_visualise: list[geojson.Feature]) -> None:
        for node in self.osm_data.nodes:
            if node.id != 1:
                point = geojson.Point((float(node.lon), float(node.lat)))
                node.tags |= {self.COLOR_TAG: [0, 0, 0, 0]}

                feature = geojson.Feature(
                    geometry=point,
                    properties=node.tags,
                )
                features_to_visualise.append(feature)

    def export_map(self, feature_collection: geojson.FeatureCollection) -> None:
        geojson_layer = Layer(
            "GeoJsonLayer",
            data=feature_collection,
            pickable=True,
            line_width_min_pixels=2,
            get_line_color=self.COLOR_TAG,
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
