def get_area_boundary() -> str:
    return """
        [out:json][timeout:500];
        
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
