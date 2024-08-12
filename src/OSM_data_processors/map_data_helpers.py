# future: remove the comment below when stubs for the library below are available
import geojson  # type: ignore

# future: remove the comment below when stubs for the library below are available
from overpy import Area, Element, Node, Relation, Way  # type: ignore

# future: report bug (false positive) to JetBrains developers
# noinspection PyPackageRequirements
from plum import dispatch
from pyproj import Geod

import shapely
from shapely import distance

from shapely.ops import split, substring

from src.SR import SR


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
        return int(float(milestone.tags["railway:position"]) * 1000)
    except ValueError:
        raise ValueError(
            f"Milestone position ('{milestone.tags["railway:position"]}') couldn't be converted to float! "
            f"Edit it here: https://www.openstreetmap.org/edit?node={milestone.id}"
        )


def merge_ways_into_linestring(
    ways_between_milestones: list[Way],
) -> shapely.LineString:
    coordinates: list[tuple[float, float]] = []
    for way in ways_between_milestones:
        way_coordinates = [(float(node.lon), float(node.lat)) for node in way.nodes]
        if coordinates:
            fix_misaligned_list_orders(coordinates, way_coordinates)
        coordinates.extend(way_coordinates)
    merged_ways_between_milestones = shapely.LineString(coordinates)
    return merged_ways_between_milestones


def split_lines(
    line: shapely.LineString,
    splitting_point: shapely.Point,
) -> shapely.GeometryCollection:
    # future: debug why it gives "RuntimeWarning: invalid value encountered in line_locate_point
    #   return lib.line_locate_point(line, other)" at ./venv/lib/python3.12/site-packages/shapely/linear.py:88 when
    #   line.wkt is LINESTRING (19.1090192 47.4882755, 19.1092076 47.4882598, 19.1096989 47.488219, 19.1100772 47.4882149, 19.1104284 47.4882206, 19.1108077 47.4882435, 19.1112823 47.4882943, 19.1130505 47.4885548, 19.1136396 47.4885698, 19.1142464 47.4885517, 19.1148753 47.4884826, 19.1156548 47.4883176, 19.1161264 47.4881959, 19.1167118 47.4880177, 19.1171248 47.4878589, 19.1174105 47.4877445, 19.1175931 47.487661, 19.117763 47.4875706, 19.1178895 47.4874906, 19.1180338 47.4873916, 19.1180521 47.4873774, 19.1181963 47.4872654, 19.118378 47.4871053, 19.1185497 47.4869549, 19.1186807 47.4868195, 19.118776 47.4867018, 19.1188674 47.4865653, 19.1189273 47.4864621, 19.1190071 47.4863118, 19.1190717 47.4861743, 19.1191184 47.4860293, 19.1191634 47.4858775, 19.1191815 47.4857592, 19.1191993 47.4854503, 19.1191915 47.4851474, 19.1189492 47.4836573, 19.1189492 47.4836573, 19.118906 47.4833618, 19.118906 47.4833618, 19.1185928 47.4815841, 19.1184233 47.4809331, 19.118266 47.480416, 19.1178267 47.4795529, 19.1178267 47.4795529, 19.117767 47.4794579, 19.1177241 47.4793908, 19.1176786 47.4793254, 19.117484 47.4790628, 19.1174267 47.4789866, 19.1173673 47.4789118, 19.1172806 47.478805, 19.1171216 47.4786218, 19.1170269 47.4785158, 19.1169512 47.4784288, 19.1168806 47.478349, 19.116788 47.478246, 19.1167084 47.4781616, 19.1166264 47.4780777, 19.116551 47.4779976, 19.1164002 47.4778344, 19.1164002 47.4778344, 19.1163115 47.4777367, 19.1163115 47.4777367, 19.1159898 47.4773938, 19.1159099 47.4773088, 19.115792 47.4771889, 19.1156595 47.4770514, 19.1156269 47.4770205, 19.1152604 47.4766626, 19.1151741 47.476578, 19.1150855 47.4764934, 19.1149967 47.4764095, 19.1149044 47.4763264, 19.1146304 47.476095, 19.1146304 47.476095, 19.1144545 47.475952, 19.1143893 47.4758978, 19.1143008 47.4758301, 19.1142193 47.4757687, 19.1141395 47.4757097, 19.114063 47.4756577, 19.113604 47.47537, 19.1134839 47.4752959, 19.11336 47.4752207, 19.113235 47.4751466, 19.1131084 47.4750743, 19.112914 47.4749619, 19.1127883 47.4748892, 19.1126575 47.4748164, 19.1125435 47.474755, 19.1124268 47.474695, 19.1123053 47.474633, 19.1121836 47.4745731, 19.1114544 47.4742379, 19.1105156 47.4738509, 19.1095889 47.473512, 19.1069261 47.4726811, 19.1069261 47.4726811, 19.1064554 47.4725344, 19.1064554 47.4725344, 19.1062799 47.4724791, 19.106091 47.4724221, 19.1058994 47.4723659, 19.105706 47.4723112, 19.1055111 47.4722584, 19.1053648 47.4722184, 19.1052122 47.4721805, 19.1049803 47.472126, 19.1047393 47.4720709, 19.104497 47.4720199, 19.1042904 47.4719777, 19.104059 47.4719291, 19.1038323 47.4718797, 19.103596 47.4718255, 19.1033932 47.4717758, 19.1031971 47.4717261, 19.1028049 47.4716232, 19.1013903 47.4712538, 19.1013903 47.4712538, 19.1003118 47.4709719, 19.1003118 47.4709719, 19.0994393 47.4707451, 19.0980992 47.4703949, 19.0942493 47.4693886, 19.0940057 47.4693284, 19.0937514 47.4692701, 19.0934971 47.4692157, 19.0932394 47.4691652, 19.0929873 47.4691185, 19.0927312 47.4690752, 19.0924436 47.469028, 19.0922123 47.4689925, 19.0903963 47.4687161, 19.0902082 47.4686927, 19.0900073 47.4686674, 19.0898052 47.4686444, 19.0895985 47.4686215, 19.0894278 47.4686055, 19.089353 47.4685985, 19.0891311 47.4685803, 19.0889092 47.4685649, 19.088692 47.4685522, 19.088266 47.4685298, 19.0879508 47.4685128, 19.0871293 47.4684745)
    #   and splitting_point.wkt is POINT (19.1180521 47.4873774)
    return split(
        geom=line,
        splitter=splitting_point,
    )


def get_nearest_milestone(
    exact_location: int, milestones: list[Node], sr: SR, on_ways: list[Way]
) -> Node:
    milestones.sort(key=lambda x: abs(get_milestone_location(x) - exact_location))
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
        f"Nearest milestone not found for metre post {exact_location} of SR #{sr.id[-8:]} on line {sr.line}!"
    )


@dispatch
def convert_to_geojson(feature: Way) -> geojson.LineString:
    return geojson.LineString(
        [(float(node.lon), float(node.lat)) for node in feature.nodes]
    )


# future: request mypy support from plum developers
@dispatch  # type: ignore
def convert_to_geojson(feature: shapely.LineString) -> geojson.LineString:
    return geojson.LineString(
        [(float(point[0]), float(point[1])) for point in feature.coords]
    )


def convert_to_linestring(way: Way) -> shapely.LineString:
    return shapely.LineString(
        [(float(node.lon), float(node.lat)) for node in way.nodes]
    )


def point_on_line_if_you_squint(point: shapely.Point, line: shapely.LineString) -> bool:
    return line.distance(point) < 1e-14


def get_percentage(number_one: int, number_two: int) -> int:
    return int(number_one / number_two * 100)


def get_length(geod: Geod, linestring: shapely.LineString) -> float:
    return geod.geometry_length(linestring)


def further_in_same_direction(
    milestone: Node, current_nearest_milestones: list[Node], metre_post: int
) -> bool:
    return get_milestone_location(milestone) < get_milestone_location(
        current_nearest_milestones[-1]
    ) < metre_post or metre_post < get_milestone_location(
        current_nearest_milestones[-1]
    ) < get_milestone_location(
        milestone
    )


def get_distance_percentage_between_milestones(
    nearest_milestones: list[Node], metre_post_boundary: int
) -> float:
    try:
        nearest_milestones_locations = [
            get_milestone_location(milestone) for milestone in nearest_milestones
        ]
        distance_between_nearest_milestones = abs(
            nearest_milestones_locations[0] - nearest_milestones_locations[-1]
        )
        distance_between_sr_and_lower_milestone = abs(
            nearest_milestones_locations[0] - metre_post_boundary
        )
        distance_percentage_between_milestones = (
            distance_between_sr_and_lower_milestone
            / distance_between_nearest_milestones
        )
        return distance_percentage_between_milestones
    except ZeroDivisionError:
        raise ZeroDivisionError(
            f"Distance between closest milestones found near metre post {metre_post_boundary} is zero!"
        )


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


def remove_irrelevant_duplicate_milestones(
    milestones: list[Node], sr: SR
) -> list[Node]:
    milestones_near_kiskunfelegyhaza = [
        11909093553,
        11900570241,
        11900570240,
        11900570239,
        10177734431,
        11900570238,
        11900570237,
        10223953785,
        11900570236,
        11900570235,
    ]
    if sr.station_from in ["Kiskunfélegyháza", "Tiszaalpár"]:
        return [
            milestone
            for milestone in milestones
            if milestone.id in milestones_near_kiskunfelegyhaza
        ]
    else:
        return [
            milestone
            for milestone in milestones
            if milestone.id not in milestones_near_kiskunfelegyhaza
        ]


def get_tolerance_for_linestring_length(expected_length: int, sr: SR) -> float:
    if sr.line == "1":
        if (
            min(sr.metre_post_from, sr.metre_post_to)
            < 97700
            < max(sr.metre_post_from, sr.metre_post_to)
        ):  # inaccurate metre posts
            return 0.4

    if expected_length > 500:
        return 0.2  # 0.15 was too low
    elif 500 >= expected_length > 100:
        return 0.4  # 0.3 was too low
    elif 100 >= expected_length:
        return 1
    else:
        raise NotImplementedError


def length_of_found_linestring_is_reasonable(
    difference_from_expected_length: int, expected_length: int, sr: SR
) -> bool:
    return (
        difference_from_expected_length
        <= expected_length * get_tolerance_for_linestring_length(expected_length, sr)
    )


def found_linestring_accepted_as_point(expected_length: int) -> bool:
    return expected_length < 100  # 50 was too low


def line_between_points(
    full_line: shapely.LineString, points: tuple[shapely.Point, shapely.Point]
) -> shapely.LineString:
    return substring(
        geom=full_line,
        start_dist=full_line.project(
            other=points[0],  # type: ignore
            normalized=True,
        ),
        end_dist=full_line.project(
            other=points[1],  # type: ignore
            normalized=True,
        ),
        normalized=True,
    )


def convert_node_to_point(nearest_milestones: Node) -> shapely.Point:
    return shapely.Point(
        (
            float(nearest_milestones.lon),
            float(nearest_milestones.lat),
        )
    )


def milestones_are_in_reverse_order(
    coordinate_of_metre_post: shapely.Point, nearest_milestones: list[Node]
) -> bool:
    return distance(
        coordinate_of_metre_post,
        shapely.Point(nearest_milestones[0].lon, nearest_milestones[0].lat),
    ) > distance(
        coordinate_of_metre_post,
        shapely.Point(nearest_milestones[-1].lon, nearest_milestones[-1].lat),
    )


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
