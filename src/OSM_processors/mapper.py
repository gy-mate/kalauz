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
    milestones.sort(key=lambda milestone: get_milestone_location(milestone))
    return milestones


def get_milestone_location(milestone: Node) -> float:
    try:
        return float(milestone.tags["railway:position"]) * 1000
    except ValueError:
        raise ValueError(
            f"Milestone position ('{milestone.tags["railway:position"]}') couldn't be converted to float! "
            f"Edit it here: https://www.openstreetmap.org/edit?node={milestone.id}"
        )


def further_in_same_direction(
    milestone: Node, current_nearest_milestones: list[Node], metre_post: int
) -> bool:
    return (
        float(milestone.tags["railway:position"]) * 1000
        < float(current_nearest_milestones[-1].tags["railway:position"]) * 1000
        < metre_post
        or metre_post
        < float(current_nearest_milestones[-1].tags["railway:position"]) * 1000
        < float(milestone.tags["railway:position"]) * 1000
    )


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


def merge_ways(ways_between_milestones: list[Way]) -> shapely.LineString:
    coordinates: list[tuple[float, float]] = []
    for way in ways_between_milestones:
        way_coordinates = [(float(node.lon), float(node.lat)) for node in way.nodes]
        if coordinates:
            fix_misaligned_list_orders(coordinates, way_coordinates)
        coordinates.extend(way_coordinates)
    merged_ways_between_milestones = shapely.LineString(coordinates)
    return merged_ways_between_milestones


def fix_misaligned_list_orders(
    coordinates: list[tuple[float, float]], way_coordinates: list[tuple[float, float]]
) -> None:
    for existing_coordinate_id in 0, -1:
        for way_coordinate_id in 0, -1:
            found_matching_coordinates = (
                coordinates[existing_coordinate_id]
                == way_coordinates[way_coordinate_id]
            )
            if found_matching_coordinates:
                if existing_coordinate_id == 0:
                    coordinates.reverse()
                if way_coordinate_id == -1:
                    way_coordinates.reverse()
                return


def split_lines(
    line: shapely.LineString,
    splitting_point: shapely.Point,
) -> shapely.MultiLineString:
    # future: debug why it gives "RuntimeWarning: invalid value encountered in line_locate_point
    #   return lib.line_locate_point(line, other)" at ./venv/lib/python3.12/site-packages/shapely/linear.py:88 when
    #   line.wkt is LINESTRING (19.1090192 47.4882755, 19.1092076 47.4882598, 19.1096989 47.488219, 19.1100772 47.4882149, 19.1104284 47.4882206, 19.1108077 47.4882435, 19.1112823 47.4882943, 19.1130505 47.4885548, 19.1136396 47.4885698, 19.1142464 47.4885517, 19.1148753 47.4884826, 19.1156548 47.4883176, 19.1161264 47.4881959, 19.1167118 47.4880177, 19.1171248 47.4878589, 19.1174105 47.4877445, 19.1175931 47.487661, 19.117763 47.4875706, 19.1178895 47.4874906, 19.1180338 47.4873916, 19.1180521 47.4873774, 19.1181963 47.4872654, 19.118378 47.4871053, 19.1185497 47.4869549, 19.1186807 47.4868195, 19.118776 47.4867018, 19.1188674 47.4865653, 19.1189273 47.4864621, 19.1190071 47.4863118, 19.1190717 47.4861743, 19.1191184 47.4860293, 19.1191634 47.4858775, 19.1191815 47.4857592, 19.1191993 47.4854503, 19.1191915 47.4851474, 19.1189492 47.4836573, 19.1189492 47.4836573, 19.118906 47.4833618, 19.118906 47.4833618, 19.1185928 47.4815841, 19.1184233 47.4809331, 19.118266 47.480416, 19.1178267 47.4795529, 19.1178267 47.4795529, 19.117767 47.4794579, 19.1177241 47.4793908, 19.1176786 47.4793254, 19.117484 47.4790628, 19.1174267 47.4789866, 19.1173673 47.4789118, 19.1172806 47.478805, 19.1171216 47.4786218, 19.1170269 47.4785158, 19.1169512 47.4784288, 19.1168806 47.478349, 19.116788 47.478246, 19.1167084 47.4781616, 19.1166264 47.4780777, 19.116551 47.4779976, 19.1164002 47.4778344, 19.1164002 47.4778344, 19.1163115 47.4777367, 19.1163115 47.4777367, 19.1159898 47.4773938, 19.1159099 47.4773088, 19.115792 47.4771889, 19.1156595 47.4770514, 19.1156269 47.4770205, 19.1152604 47.4766626, 19.1151741 47.476578, 19.1150855 47.4764934, 19.1149967 47.4764095, 19.1149044 47.4763264, 19.1146304 47.476095, 19.1146304 47.476095, 19.1144545 47.475952, 19.1143893 47.4758978, 19.1143008 47.4758301, 19.1142193 47.4757687, 19.1141395 47.4757097, 19.114063 47.4756577, 19.113604 47.47537, 19.1134839 47.4752959, 19.11336 47.4752207, 19.113235 47.4751466, 19.1131084 47.4750743, 19.112914 47.4749619, 19.1127883 47.4748892, 19.1126575 47.4748164, 19.1125435 47.474755, 19.1124268 47.474695, 19.1123053 47.474633, 19.1121836 47.4745731, 19.1114544 47.4742379, 19.1105156 47.4738509, 19.1095889 47.473512, 19.1069261 47.4726811, 19.1069261 47.4726811, 19.1064554 47.4725344, 19.1064554 47.4725344, 19.1062799 47.4724791, 19.106091 47.4724221, 19.1058994 47.4723659, 19.105706 47.4723112, 19.1055111 47.4722584, 19.1053648 47.4722184, 19.1052122 47.4721805, 19.1049803 47.472126, 19.1047393 47.4720709, 19.104497 47.4720199, 19.1042904 47.4719777, 19.104059 47.4719291, 19.1038323 47.4718797, 19.103596 47.4718255, 19.1033932 47.4717758, 19.1031971 47.4717261, 19.1028049 47.4716232, 19.1013903 47.4712538, 19.1013903 47.4712538, 19.1003118 47.4709719, 19.1003118 47.4709719, 19.0994393 47.4707451, 19.0980992 47.4703949, 19.0942493 47.4693886, 19.0940057 47.4693284, 19.0937514 47.4692701, 19.0934971 47.4692157, 19.0932394 47.4691652, 19.0929873 47.4691185, 19.0927312 47.4690752, 19.0924436 47.469028, 19.0922123 47.4689925, 19.0903963 47.4687161, 19.0902082 47.4686927, 19.0900073 47.4686674, 19.0898052 47.4686444, 19.0895985 47.4686215, 19.0894278 47.4686055, 19.089353 47.4685985, 19.0891311 47.4685803, 19.0889092 47.4685649, 19.088692 47.4685522, 19.088266 47.4685298, 19.0879508 47.4685128, 19.0871293 47.4684745)
    #   and splitting_point.wkt is POINT (19.1180521 47.4873774)

    # future: remove supression below when the issue above is fixed

    # future: report bug (false positive) to JetBrains developers
    # noinspection PyUnboundLocalVariable
    with contextlib.suppress(RuntimeWarning):
        return split(
            geom=line,
            splitter=splitting_point,
        )


# noinspection PyShadowingNames
def get_nearest_milestone(
    exact_location: int, milestones: list[Node], sr: SR, on_ways: list[Way]
) -> Node:
    milestones.sort(
        key=lambda milestone: abs(get_milestone_location(milestone) - exact_location)
    )
    for milestone in milestones:
        for way in on_ways:
            if milestone in way.nodes:
                if sr.main_track_side:
                    try:
                        if way.tags["railway:track_side"] == sr.main_track_side:
                            return milestone
                    except KeyError:
                        continue
                else:
                    return milestone
    assert sr.id
    raise ValueError(
        f"Nearest milestone not found for SR #{sr.id[-8:]} on line {sr.line}!"
    )


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
                        self.logger.warn(f"Relation with `ref={sr.line}` not found!")
                        raise ValueError
                except ValueError as exception:
                    self.logger.debug(exception)

    def visualise_srs(self) -> None:
        features_to_visualise: list[geojson.Feature] = []

        self.add_all_nodes(features_to_visualise)
        self.add_na_lines(features_to_visualise)

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
                milestones_of_line = get_milestones(nodes=nodes_of_line)

                for j, sr_metre_post_boundary in enumerate(
                    (sr.metre_post_from, sr.metre_post_to)
                ):
                    nearest_milestones: list[Node] = []
                    milestones_of_line_copy = milestones_of_line.copy()
                    self.get_nearest_milestones(
                        milestones=milestones_of_line_copy,
                        nearest_milestones=nearest_milestones,
                        metre_post=sr_metre_post_boundary,
                        sr=sr,
                        on_ways=ways_of_line,
                    )
                    at_percentage_between_milestones = (
                        get_distance_percentage_between_milestones(
                            nearest_milestones, sr_metre_post_boundary
                        )
                    )
                    way_of_lower_milestone, way_of_greater_milestone = (
                        self.get_ways_of_milestones(nearest_milestones, ways_of_line)
                    )
                    ways_between_milestones = self.get_ways_between_milestones(
                        way_of_greater_milestone,
                        way_of_lower_milestone,
                        ways_of_line,
                    )
                    merged_ways_between_milestones = merge_ways(ways_between_milestones)

                    split_lines_at_lower_milestone = split_lines(
                        line=merged_ways_between_milestones,
                        splitting_point=shapely.Point(
                            (
                                float(nearest_milestones[0].lon),
                                float(nearest_milestones[0].lat),
                            )
                        ),
                    )
                    split_lines_at_greater_milestone = split_lines(
                        line=merged_ways_between_milestones,
                        splitting_point=shapely.Point(
                            (
                                float(nearest_milestones[-1].lon),
                                float(nearest_milestones[-1].lat),
                            )
                        ),
                    )
                    line_between_milestones = shapely.intersection(
                        split_lines_at_lower_milestone.geoms[-1],
                        split_lines_at_greater_milestone.geoms[0],
                    )

                    coordinate_of_metre_post = line_between_milestones.interpolate(
                        distance=at_percentage_between_milestones,
                        normalized=True,
                    )

                    # future: init `metre_post_from_coordinates` and `metre_post_to_coordinates` in the constructor
                    if sr_metre_post_boundary == sr.metre_post_from:
                        sr.metre_post_from_coordinates = coordinate_of_metre_post  # type: ignore
                    else:
                        sr.metre_post_to_coordinates = coordinate_of_metre_post  # type: ignore

                    pass
            except (IndexError, ValueError, ZeroDivisionError) as exception:
                prepared_lines = [
                    "1",
                    "1d",
                    "146",
                    "113 (1)",
                    "113 (2)",
                    "30",
                    "8",
                    "18",
                    "17 (1)",
                    "17 (2)",
                    "9",
                ]
                if sr.line not in prepared_lines:
                    pass
                else:
                    self.logger.critical(exception)
                    raise
            if sr_index in notify_at_indexes:
                self.logger.info(f"⏳ {int(sr_index / len(self.srs) * 100)}% done...")
            pass
        self.logger.info(f"...finished visualising speed restrictions!")

        feature_collection_to_visualise = geojson.FeatureCollection(
            features_to_visualise
        )
        self.export_map(feature_collection_to_visualise)

    def get_ways_of_corresponding_line(self, sr: SR) -> list[Way]:
        relation = self.get_corresponding_relation(sr)
        way_ids = [way.ref for way in relation.members]
        ways = [way for way in self.osm_data.ways if way.id in way_ids]
        return ways

    def get_ways_of_milestones(
        self, nearest_milestones: list[Node], ways: list[Way]
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
        self.logger.critical("Ways of milestones not found!")
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

    def get_nearest_milestones(
        self,
        milestones: list[Node],
        nearest_milestones: list[Node],
        metre_post: int,
        sr: SR,
        on_ways: list[Way],
    ) -> None:
        while len(nearest_milestones) < 2:
            seemingly_nearest_milestone = get_nearest_milestone(
                exact_location=metre_post,
                milestones=milestones,
                sr=sr,
                on_ways=on_ways,
            )
            if not nearest_milestones or not (
                further_in_same_direction(
                    milestone=seemingly_nearest_milestone,
                    current_nearest_milestones=nearest_milestones,
                    metre_post=metre_post,
                )
            ):
                nearest_milestones.append(seemingly_nearest_milestone)
            milestones.remove(seemingly_nearest_milestone)

        nearest_milestones.sort(
            key=lambda milestone: float(milestone.tags["railway:position"])
        )

    def add_na_lines(self, features_to_visualise: list[geojson.Feature]) -> None:
        self.logger.debug(f"Adding N/A lines started...")
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
        self.logger.debug(f"...finished!")

    def add_all_nodes(self, features_to_visualise: list[geojson.Feature]) -> None:
        self.logger.debug(f"Adding all nodes started...")
        for node in self.osm_data.nodes:
            if node.id != 1:
                point = geojson.Point((float(node.lon), float(node.lat)))
                node.tags |= {self.COLOR_TAG: [0, 0, 0, 0]}

                feature = geojson.Feature(
                    geometry=point,
                    properties=node.tags,
                )
                features_to_visualise.append(feature)
        self.logger.debug(f"...finished!")

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
        try:
            relation = [
                relation
                for relation in self.osm_data.relations
                if relation.tags["ref"].upper() == sr.line.upper()
            ][0]
            return relation
        except IndexError:
            self.logger.critical(f"Relation with `ref={sr.line}` not found!")
            raise
