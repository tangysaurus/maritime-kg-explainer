from schema import *
from KnowledgeGraph import KnowledgeGraph
from fuzzywuzzy import fuzz

# Object construction
def construct_vessel(vessel_id: str, events: list, kg: KnowledgeGraph) -> Vessel:
    """Constructs a vessel object given a vessel id and related events"""
    info = kg.vessel_info(vessel_id)
    name = info["name"].astype(str).iloc[0]
    type = info["type"].astype(str).iloc[0]
    flag = info["flag"].astype(str).iloc[0]

    gap_events, port_events, fishing_events, weather_events = [], [], [], []

    for event in events[0]:
        gap_info = kg.event_info(event)
        location = gap_info["location"].astype(str).iloc[0]
        start_time = gap_info["start"].astype(str).iloc[0]
        end_time = gap_info["end"].astype(str).iloc[0]
        distance_km = gap_info["gap_distance"].astype(float).iloc[0]
        duration_hours = gap_info["gap_duration"].astype(float).iloc[0]
        speed_knots = gap_info["gap_speed"].astype(float).iloc[0]
        intentional_disabling = gap_info["gap_intentional"].astype(bool).iloc[0]

        gap_events.append(
            GapEvent(
                id = event, 
                location = location, 
                start_time = start_time, 
                end_time = end_time, 
                distance_km = distance_km, 
                duration_hours = duration_hours, 
                speed_knots = speed_knots, 
                intentional_disabling = intentional_disabling
            )
        )

    for event in events[1]:
        port_info = kg.event_info(event)
        port = port_info["port"].astype(str).iloc[0]
        location = port_info["location"].astype(str).iloc[0]
        start_time = port_info["start"].astype(str).iloc[0]
        end_time = port_info["end"].astype(str).iloc[0]
        start_dist_from_port_km = port_info["port_dist"].astype(float).iloc[0]
        start_dist_from_shore_km = port_info["shore_dist"].astype(float).iloc[0]

        port_events.append(
            PortEvent(
                id = event, 
                port = port, 
                location = location, 
                start_time = start_time, 
                end_time = end_time, 
                start_dist_from_port_km = start_dist_from_port_km, 
                start_dist_from_shore_km = start_dist_from_shore_km
            )
        )

    for event in events[2]:
        fishing_info = kg.event_info(event)
        location = fishing_info["location"].astype(str).iloc[0]
        start_time = fishing_info["start"].astype(str).iloc[0]
        end_time = fishing_info["end"].astype(str).iloc[0]
        fishing_effort_score = fishing_info["score"].astype(float).iloc[0]
        gear_type = fishing_info["gear_type"].astype(str).iloc[0]

        fishing_events.append(
            FishingEvent(
                id = event, 
                location = location, 
                start_time = start_time, 
                end_time = end_time, 
                fishing_effort_score = fishing_effort_score, 
                gear_type = gear_type
            )
        )

    for event in events[3]:
        weather_info = kg.event_info(event)
        location = weather_info["location"].astype(str).iloc[0]
        start_time = weather_info["start"].astype(str).iloc[0]
        end_time = weather_info["end"].astype(str).iloc[0]
        weather_type = weather_info["weather"].astype(str).iloc[0]
        severity = weather_info["severity"].astype(str).iloc[0]

        weather_events.append(
            WeatherEvent(
                id = event, 
                location = location, 
                start_time = start_time, 
                end_time = end_time, 
                weather_type = weather_type, 
                severity = severity
            )
        )

    observed_points, predicted_points = [], []
    trajectory_sequences = kg.extract_trajectory_sequences(vessel_id)
    points = []

    for seq in trajectory_sequences:
        points.extend(kg.extract_observations(seq))

    for point in points:
        point_info = kg.observation_info(point)

        if point[:3] == "ais":
            timestamp = point_info["time"].astype(str).iloc[0]
            lat = point_info["lat"].astype(float).iloc[0]
            lon = point_info["lon"].astype(float).iloc[0]
            speed_knots = point_info["speed"].astype(float).iloc[0]
            course_degrees = point_info["course"].astype(float).iloc[0]
            dist_from_port_km = point_info["port_dist"].astype(float).iloc[0]
            dist_from_shore_km = point_info["shore_dist"].astype(float).iloc[0]

            observed_points.append(
                Observation(
                    timestamp = timestamp,
                    lat = lat,
                    lon = lon,
                    speed_knots = speed_knots,
                    course_degrees = course_degrees,
                    dist_from_port_km = dist_from_port_km,
                    dist_from_shore_km = dist_from_shore_km
                )
            )
        else:
            timestamp = point_info["time"].astype(str).iloc[0]
            lat = point_info["lat"].astype(float).iloc[0]
            lon = point_info["lon"].astype(float).iloc[0]
            speed_knots = point_info["speed"].astype(float).iloc[0]
            course_degrees = point_info["course"].astype(float).iloc[0]

            predicted_points.append(
                Prediction(
                    timestamp = timestamp,
                    lat = lat,
                    lon = lon,
                    speed_knots = speed_knots,
                    course_degrees = course_degrees
                )
            )
    
    return Vessel(
        id = vessel_id,
        name = name,
        type = type,
        flag = flag,
        observed_points = observed_points,
        predicted_points = predicted_points,
        gap_events = gap_events,
        port_events = port_events,
        fishing_events = fishing_events,
        weather_events = weather_events
    )

def construct_cluster(vessel_id: str, neighbors: str, events: list, kg: KnowledgeGraph) -> Cluster:
    """Constructs a vessel object given a vessel id, nearby vessels, and related events"""

    vessel = construct_vessel(vessel_id, events, kg)

    nearby_vessels = []

    for neighbor in neighbors:
        nearby_vessels.append(
            construct_vessel(neighbor, kg.find_related_events(neighbor), kg)
        )

    return Cluster(
        vessel=vessel, 
        nearby_vessels = nearby_vessels
    )

# Benchmarking
def extract_facts(text: str, client) -> list[str]:
    """Extracts list of atomic facts from piece of text"""
    SYSTEM_PROMPT = "Extract a list of atomic facts from the given text or JSON."

    res = client.beta.chat.completions.parse(
        model = "gpt-5",
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Text: {text}"}
        ],
        response_format = AtomicFacts
    )

    return res.choices[0].message.parsed.facts

def fact_check(text1: str, text2: str, client) -> tuple[list[str], list[str]]:
    """
    Returns a dictionary where each key is an atomic fact from text1 whose 
    value is equal to list of referenced atomic fracts from text2
    """
    text1_facts = extract_facts(text1)
    text2_facts = extract_facts(text2)

    SYSTEM_PROMPT = """
        Given 2 lists of atomic facts, each corresponding to a unique piece of text, return 2 lists.
        The first list contains atomic facts from the first piece of text (text1).
        The second list contains lists of atomic facts from the second piece of text (text2) that are referenced by the corresponding key in the first list.
        To be more specific, the first element of the second list should be a list of atomic facts from text2 referenced by the first key in the first list.
        If no atomic facts are referenced, the value should be an empty list.
    """

    res = client.beta.chat.completions.parse(
        model = "gpt-5",
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"text1 facts: {text1_facts} \n\n text2 facts: {text2_facts}"}
        ],
        response_format = FactCheck
    )

    return res.choices[0].message.parsed.keys, res.choices[0].message.parsed.references

def precision(keys: list[str], references: list[list[str]]) -> float:
    """Returns precision (# relevant facts / total facts) of explanation"""
    num_relevant = 0

    for i in range(len(keys)):
        if references[i]:
            num_relevant += 1
            
    return num_relevant / len(keys)