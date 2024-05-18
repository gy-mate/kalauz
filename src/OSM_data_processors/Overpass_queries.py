def get_area_boundary() -> str:
    return """
        [out:json];
        
        area["ISO3166-1"="HU"]
        // area["admin_level"="8"]["name"="Hegyeshalom"]
          -> .country;
          
    """


def get_route_relations() -> str:
    # future: replace lines below when https://github.com/drolbr/Overpass-API/issues/146 is closed
    #     relation["route"="railway"]["ref"]["operator"~"(^MÁV(?=;))|((?<=;)MÁV(?=;))|((?<=;)MÁV$)"](area.country);
    #     relation["route"="railway"]["ref"]["operator"~"(^GYSEV(?=;))|((?<=;)GYSEV(?=;))|((?<=;)GYSEV$)"](area.country);
    return """
        (
            relation["route"="railway"]["ref"]["operator"~"MÁV"](area.country);
            relation["route"="railway"]["ref"]["operator"~"GYSEV"](area.country);
        );
        >>;
        out;
        
    """


def get_ground_floor_tracks() -> str:
    return f"""
        (
            way["railway"="rail"][!"layer"](area.operatingSite);
            way["disused:railway"="rail"][!"layer"](area.operatingSite);
            way["abandoned:railway"="rail"][!"layer"](area.operatingSite);
            
            way["railway"="rail"]["layer"="0"](area.operatingSite);
            way["disused:railway"="rail"]["layer"="0"](area.operatingSite);
            way["abandoned:railway"="rail"]["layer"="0"](area.operatingSite);
        );
    """


def get_operating_site_separator() -> str:
    return f"""
        (._;>;);
        out;
        node(1);
        out ids;
    """
