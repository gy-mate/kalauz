import contextlib
from datetime import datetime
import json
from typing import Any, Final, List

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
from shapely.ops import split, substring  # type: ignore
from sqlalchemy.sql import text
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.OSM_data_processors.Overpass_queries import *
from src.OSM_data_processors.map_data_helpers import *
from src.OSM_data_processors.map_data_helpers import (
    convert_node_to_point,
    line_between_points,
    milestones_are_in_reverse_order,
)
from src.SR import SR
from src.new_data_processors.common import DataProcessor


def get_nearest_milestones(
    milestones: list[Node],
    metre_post: int,
    sr: SR,
    on_ways: list[Way],
) -> list[Node]:
    if sr.line == "146":
        milestones = remove_irrelevant_duplicate_milestones(milestones, sr)

    nearest_milestones: list[Node] = []
    while len(nearest_milestones) < 2:
        seemingly_nearest_milestone = get_nearest_milestone(
            exact_location=metre_post,
            milestones=milestones,
            sr=sr,
            on_ways=on_ways,
        )
        if get_milestone_location(seemingly_nearest_milestone) == metre_post:
            return [seemingly_nearest_milestone]
        if not nearest_milestones or not (
            further_in_same_direction(
                milestone=seemingly_nearest_milestone,
                current_nearest_milestones=nearest_milestones,
                metre_post=metre_post,
            )
        ):
            nearest_milestones.append(seemingly_nearest_milestone)
        milestones.remove(seemingly_nearest_milestone)
    return nearest_milestones


class Mapper(DataProcessor):
    def __init__(self, show_lines_with_no_data: bool) -> None:
        super().__init__()

        self.TODAY_SIMULATED: Final = datetime(2024, 1, 18, 21, 59, 59)
        self.COLOR_TAG: Final = "line_color"
        self.QUERY_MAIN_PARAMETERS: Final = get_area_boundary()
        self.OPERATORS: Final = ["MÁV", "GYSEV"]
        self.OPERATING_SITE_TAG_VALUES: Final = [  # type: ignore
            # TODO: uncomment lines below when implementing stations
            # "station",
            # "halt",
            # "yard",
            # "service_station",
            # "junction",
            # "crossover",
            # "spur_junction",
            # "site",
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

        self.query_final: str = self.QUERY_MAIN_PARAMETERS + get_route_relations()

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
            self.query_final += get_ground_floor_tracks()
        self.query_final += get_operating_site_separator()

    def process_srs(self) -> None:
        self.get_all_srs_from_database()

        if self.show_lines_with_no_data:
            self.get_id_of_sr_main_track_ways()

    def get_all_srs_from_database(self) -> None:
        with self.database.engine.begin() as connection:
            # TODO: remove on_main_track filter when beginning station implementation
            # TODO: replace time filter with the line below in production
            #     time_from <= now() and (now() < time_to or time_to is null);
            query = """
            select *
            from speed_restrictions
            where
                on_main_track = 1 and
                time_from <= :now and (:now < time_to or time_to is null)
            order by line, metre_post_from, metre_post_to;
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
                    self.logger.warn(exception)

    def visualise_srs(self) -> None:
        features_to_visualise: list[geojson.Feature] = []

        self.add_all_ways(features_to_visualise)
        # self.add_all_nodes(features_to_visualise)

        self.get_sr_geometries()
        for sr in self.srs:
            sr.time_from = sr.time_from.strftime("%Y-%m-%d %H:%M:%S")  # type: ignore
            infos = sr.__dict__
            infos = {
                key: value
                for key, value in infos.items()
                if value is not None
                and key != "metre_post_from_coordinates"
                and key != "metre_post_to_coordinates"
                and key != "geometry"
            }

            with contextlib.suppress(AttributeError):
                feature = geojson.Feature(
                    geometry=convert_to_geojson(sr.geometry),  # type: ignore
                    properties=infos,
                )
                features_to_visualise.append(feature)

        feature_collection_to_visualise = geojson.FeatureCollection(
            features_to_visualise
        )
        self.export_map(feature_collection_to_visualise)

    def get_sr_geometries(self) -> None:
        self.logger.info(f"Visualising {len(self.srs)} speed restrictions started...")
        notification_percentage_interval = 2
        notify_at_indexes: list[int] = []
        for i in range(1, int(100 / notification_percentage_interval) - 1):
            notify_at_index = int(
                len(self.srs) * (notification_percentage_interval / 100) * i
            )
            notify_at_indexes.append(notify_at_index + 1)
        # future: implement multithreading for the loop below
        for sr_index, sr in enumerate(self.srs):
            try:
                ways_of_line = self.get_ways_of_corresponding_line(sr)
                nodes_of_line = [node for way in ways_of_line for node in way.nodes]
                milestones_of_line = get_milestones(nodes_of_line)

                self.get_coordinates_of_sr(milestones_of_line, sr, ways_of_line)

                # future: init `geometry` in the constructor
                sr.geometry = self.get_linestring_of_sr(sr, ways_of_line)  # type: ignore
                match int((1 - (sr.reduced_speed / sr.operating_speed)) * 100):
                    case reduced_by if reduced_by in range(0, 20):
                        setattr(sr, self.COLOR_TAG, [249, 255, 0])
                    case reduced_by if reduced_by in range(20, 30):
                        setattr(sr, self.COLOR_TAG, [255, 205, 0])
                    case reduced_by if reduced_by in range(30, 40):
                        setattr(sr, self.COLOR_TAG, [255, 150, 0])
                    case reduced_by if reduced_by in range(40, 99):
                        setattr(sr, self.COLOR_TAG, [255, 90, 0])
                    case _:
                        setattr(sr, self.COLOR_TAG, [255, 0, 0])
            except (IndexError, ValueError, ZeroDivisionError) as exception:
                prepared_lines = [
                    "1",
                    "1d",
                    "8",
                    "9",
                    "17 (1)",
                    "17 (2)",
                    "18",
                    "30",
                    "113 (1)",
                    "113 (2)",
                    "146",
                ]
                if sr.line not in prepared_lines:
                    pass
                else:
                    assert sr.id
                    self.logger.critical(
                        f"Fatal error with SR #{sr.id[-8:]}: {exception}"
                    )
                    raise
            if sr_index in notify_at_indexes:
                percentage = int(sr_index / len(self.srs) * 100)
                self.logger.info(
                    f"{"⏳" if percentage < 50 else "⌛️"} {percentage}% done..."
                )
        self.logger.info(f"✅ 100% done! Finished visualising speed restrictions.")

    def get_coordinates_of_sr(
        self, milestones_of_line: list[Node], sr: SR, ways_of_line: list[Way]
    ) -> None:
        for j, sr_metre_post_boundary in enumerate(
            (sr.metre_post_from, sr.metre_post_to)
        ):
            milestones_of_line_copy = milestones_of_line.copy()
            nearest_milestones = get_nearest_milestones(
                milestones=milestones_of_line_copy,
                metre_post=sr_metre_post_boundary,
                sr=sr,
                on_ways=ways_of_line,
            )
            if len(nearest_milestones) >= 2:
                at_percentage_between_milestones = (
                    get_distance_percentage_between_milestones(
                        nearest_milestones=nearest_milestones,
                        metre_post_boundary=sr_metre_post_boundary,
                    )
                )
                way_of_lower_milestone, way_of_greater_milestone = (
                    # future: use kwargs when https://github.com/beartype/plum/issues/40 is fixed
                    self.get_ways_at_locations(nearest_milestones, ways_of_line)
                )
                ways_between_milestones = self.get_ways_between_milestones(
                    way_of_greater_milestone=way_of_greater_milestone,
                    way_of_lower_milestone=way_of_lower_milestone,
                    ways_of_line=ways_of_line,
                )
                merged_ways_between_milestones = merge_ways_into_linestring(
                    ways_between_milestones
                )
                line_between_milestones = line_between_points(
                    full_line=merged_ways_between_milestones,
                    points=(
                        convert_node_to_point(nearest_milestones[0]),
                        convert_node_to_point(nearest_milestones[-1]),
                    ),
                )
                coordinate_of_metre_post = line_between_milestones.interpolate(
                    distance=at_percentage_between_milestones,
                    normalized=True,
                )
                if milestones_are_in_reverse_order(
                    coordinate_of_metre_post, nearest_milestones
                ):
                    coordinate_of_metre_post = line_between_milestones.interpolate(
                        distance=1 - at_percentage_between_milestones,
                        normalized=True,
                    )
            else:
                coordinate_of_metre_post = convert_node_to_point(nearest_milestones[0])

            # future: init `metre_post_from_coordinates` and `metre_post_to_coordinates` in the constructor
            if sr_metre_post_boundary == sr.metre_post_from:
                sr.metre_post_from_coordinates = coordinate_of_metre_post  # type: ignore
            else:
                sr.metre_post_to_coordinates = coordinate_of_metre_post  # type: ignore

    def get_linestring_of_sr(
        self, sr: SR, ways_of_line: list[Way]
    ) -> shapely.LineString:
        way_of_metre_post_from, way_of_metre_post_to = self.get_ways_at_locations(
            # future: use kwargs when https://github.com/beartype/plum/issues/40 is fixed
            [
                # future: init `metre_post_from_coordinates` and `metre_post_to_coordinates` in the constructor
                sr.metre_post_from_coordinates,  # type: ignore
                sr.metre_post_to_coordinates,  # type: ignore
            ],
            ways_of_line,
        )
        ways_between_metre_posts = self.get_ways_between_milestones(
            way_of_greater_milestone=way_of_metre_post_to,
            way_of_lower_milestone=way_of_metre_post_from,
            ways_of_line=ways_of_line,
        )
        merged_ways_between_metre_posts = merge_ways_into_linestring(
            ways_between_metre_posts
        )
        return line_between_points(
            full_line=merged_ways_between_metre_posts,
            points=(sr.metre_post_from_coordinates, sr.metre_post_to_coordinates),  # type: ignore
        )

    def get_ways_of_corresponding_line(self, sr: SR) -> list[Way]:
        relation = self.get_corresponding_relation(sr)
        way_ids = [way.ref for way in relation.members]
        ways = [way for way in self.osm_data.ways if way.id in way_ids]
        return ways

    @dispatch
    # future: make `nearest_milestones` a two-element tuple?
    def get_ways_at_locations(
        self, locations: List[Node], ways_to_search_in: List[Way]
    ) -> tuple[Way, Way]:
        way_of_lower_milestone: Way | None = None
        way_of_greater_milestone: Way | None = None

        for way in ways_to_search_in:
            for node in way.nodes:
                if node == locations[0]:
                    way_of_lower_milestone = way
                elif node == locations[-1]:
                    way_of_greater_milestone = way

                if way_of_lower_milestone and way_of_greater_milestone:
                    return way_of_lower_milestone, way_of_greater_milestone
        if not way_of_lower_milestone:
            self.logger.critical(
                f"Way of https://www.openstreetmap.org/node/{locations[0].id} "
                f"at {locations[0].lon}, {locations[0].lat} not found!"
            )
        if not way_of_greater_milestone:
            self.logger.critical(
                f"Way of https://www.openstreetmap.org/node/{locations[-1].id} "
                f"at {locations[-1].lon}, {locations[-1].lat} not found!"
            )
        raise ValueError

    # future: request mypy support from plum developers
    @dispatch  # type: ignore
    def get_ways_at_locations(
        self, locations: List[shapely.Point], ways_to_search_in: List[Way]
    ) -> tuple[Way, Way]:
        way_of_lower_metre_post: Way | None = None
        way_of_greater_metre_post: Way | None = None

        for way in ways_to_search_in:
            way_line = convert_to_linestring(way)
            if point_on_line_if_you_squint(point=locations[0], line=way_line):
                way_of_lower_metre_post = way
            if point_on_line_if_you_squint(point=locations[-1], line=way_line):
                way_of_greater_metre_post = way

            if way_of_lower_metre_post and way_of_greater_metre_post:
                return way_of_lower_metre_post, way_of_greater_metre_post
        if not way_of_lower_metre_post:
            self.logger.critical(f"Way of point at {locations[0].wkt} not found!")
        if not way_of_greater_metre_post:
            self.logger.critical(f"Way of point at {locations[-1].wkt} not found!")
        raise ValueError

    def get_ways_between_milestones(
        self,
        way_of_greater_milestone: Way,
        way_of_lower_milestone: Way,
        ways_of_line: list[Way],
    ) -> list[Way]:
        if way_of_lower_milestone is way_of_greater_milestone:
            return [way_of_lower_milestone]

        ways_of_line_copy = ways_of_line.copy()
        ways_of_line_copy.remove(way_of_lower_milestone)
        neighbouring_ways_of_lower_milestone: tuple[list[Way], list[Way]] = (
            [way_of_lower_milestone],
            [way_of_lower_milestone],
        )

        toggle = False
        return self.add_neighboring_ways(
            collection=neighbouring_ways_of_lower_milestone,
            ways_to_search_in=ways_of_line_copy,
            destination_way=way_of_greater_milestone,
            toggle=toggle,
        )

    def add_neighboring_ways(
        self,
        collection: tuple[list[Way], list[Way]],
        ways_to_search_in: list[Way],
        destination_way: Way,
        toggle: bool,
        one_side_is_dead_end: bool = False,
    ) -> list[Way]:
        for way in ways_to_search_in:
            found_neighbor_way = (collection[toggle][-1].nodes[0] in way.nodes) or (
                collection[toggle][-1].nodes[-1] in way.nodes
            )
            if found_neighbor_way:
                collection[toggle].append(way)
                if way is destination_way:
                    return collection[toggle]
                ways_to_search_in.remove(way)
                if not one_side_is_dead_end:
                    ways_to_search_in.reverse()
                    toggle = not toggle
                return self.add_neighboring_ways(
                    collection,
                    ways_to_search_in,
                    destination_way,
                    toggle,
                )
        if one_side_is_dead_end:
            raise ValueError(
                f"Couldn't reach destination_way (https://openstreetmap.org/way/{destination_way.id}) "
                f"from way_of_lower_milestone (https://openstreetmap.org/way/{collection[0][0].id})!"
            )
        else:
            one_side_is_dead_end = True
            ways_to_search_in.reverse()
            toggle = not toggle
            return self.add_neighboring_ways(
                collection,
                ways_to_search_in,
                destination_way,
                toggle,
                one_side_is_dead_end,
            )

    def add_all_ways(self, features_to_visualise: list[geojson.Feature]) -> None:
        self.logger.info(f"Adding all ways started...")
        for way in self.osm_data.ways:
            way_line = convert_to_geojson(way)
            way.tags |= {
                self.COLOR_TAG: (
                    [75, 75, 75] if way.id in self.sr_ways else [25, 25, 25]
                )
            }

            feature = geojson.Feature(
                geometry=way_line,
                properties=way.tags,
            )
            features_to_visualise.append(feature)
        self.logger.info(f"...finished!")

    def add_all_nodes(self, features_to_visualise: list[geojson.Feature]) -> None:
        self.logger.info(f"Adding all nodes started...")
        for node in self.osm_data.nodes:
            if node.id != 1:
                point = geojson.Point((float(node.lon), float(node.lat)))
                node.tags |= {self.COLOR_TAG: [0, 0, 0, 0]}

                feature = geojson.Feature(
                    geometry=point,
                    properties=node.tags,
                )
                features_to_visualise.append(feature)
        self.logger.info(f"...finished!")

    def export_map(self, feature_collection: geojson.FeatureCollection) -> None:
        geojson_layer = Layer(
            "GeoJsonLayer",
            data=feature_collection,
            pickable=True,
            line_width_min_pixels=3,
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
        try:
            relation = [
                relation
                for relation in self.osm_data.relations
                if relation.tags["ref"].upper() == sr.line.upper()
            ][0]
            return relation
        except IndexError:
            self.logger.warn(f"Relation with `ref={sr.line}` not found!")
            raise
