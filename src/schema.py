from pydantic import BaseModel, Field
from KnowledgeGraph import KnowledgeGraph

# inputs
class Cluster(BaseModel):
    vessel: "Vessel"
    nearby_vessels: list["Vessel"]

class Vessel(BaseModel):
    id: str
    name: str
    type: str
    flag: str
    observed_points: list["Observation"] = Field(..., description = "List of observed points associated with vessel. Empty list if none.")
    predicted_points: list["Prediction"] = Field(..., description = "List of predicted points associated with vessel. Empty list if none.")
    gap_events: list["GapEvent"] = Field(..., description = "List of AIS gap events associated with vessel. Empty list if none.")
    port_events: list["PortEvent"] = Field(..., description = "List of port visit events associated with vesse. Empty list if none.")
    fishing_events: list["FishingEvent"] = Field(..., description = "List of fishing events associated with vessel. Empty list if none.")
    weather_events: list["WeatherEvent"] = Field(..., description = "List of weather events associated with vessel. Empty list if none")

class Observation(BaseModel):
    timestamp: str
    lat: float
    lon: float
    speed_knots: float
    course_degrees: float
    dist_from_port_km: float
    dist_from_shore_km: float

class Prediction(BaseModel):
    timestamp: str
    lat: float
    lon: float
    speed_knots: float
    course_degrees: float

class GapEvent(BaseModel):
    id: str
    location: str
    start_time: str
    end_time: str
    distance_km: float
    duration_hours: float
    speed_knots: float
    intentional_disabling: bool

class PortEvent(BaseModel):
    id: str
    port: str
    location: str
    start_time: str
    end_time: str
    start_dist_from_port_km: float
    start_dist_from_shore_km: float

class FishingEvent(BaseModel):
    id: str
    location: str
    start_time: str
    end_time: str
    fishing_effort_score: float
    gear_type: str

class WeatherEvent(BaseModel):
    id: str
    location: str
    start_time: str
    end_time: str
    weather_type: str
    severity: str

# outputs
class Response(BaseModel):
    encounter_events: list["EncounterEvent"] = Field(..., description = "List of encounter events associated with vessel. Empty list if there are none.")
    loitering_events: list["LoiteringEvent"] = Field(..., description = "List of loitering events associated with vessel. Empty list if there are none.")
    course_deviation_events: list["CourseDeviationEvent"] = Field(..., description = "List of encounter events associated with vessel. Empty list if there are none.")

class EncounterEvent(BaseModel):
    id: str
    type: str
    start_time: str 
    end_time: str
    location: str = Field(..., description = "Brief description of approximate location")
    vessel_A: "Vessel"
    vessel_B: "Vessel"
    cross_flag: bool
    cross_type: bool
    min_separation: float = Field(..., description = "Minimum separation between vessels in km")
    explanation: str = Field(..., description = "Explanation of why this event is believed to have occurred")

class LoiteringEvent(BaseModel):
    id: str
    type: str
    start_time: str
    end_time: str
    location: str = Field(..., description = "Brief description of approximate location")
    vessel: "Vessel"
    min_speed: float = Field(..., description = "Minimum speed of vessel during event")
    missing_ais: bool = Field(..., description = "Whether involved vessel had an AIS gap event around same time")
    explanation: str = Field(..., description = "Explanation of why this event is believed to have occurred")

class CourseDeviationEvent(BaseModel):
    id: str
    type: str
    start_time: str
    end_time: str
    expected_course: str = Field(..., description = "Brief description of expected course")
    actual_course: str = Field(..., description = "Brief description of actual course")
    vessel: "Vessel"
    explanation: str = Field(..., description = "Explanation of why this event is believed to have occurred")

# Benchmarking
class AtomicFacts(BaseModel):
    facts: list[str] = Field(..., description = "List of atomic facts in a piece of text")

class FactCheck(BaseModel):
    keys: list[str] = Field(..., description = "List of atomic facts provided from text1")
    references: list[list[str]] = Field(
        ..., 
        description = """
            List containing lists of referenced atomic facts from text2. 
            The first element of this list should be a list of atomic facts from text2 referenced by the first key.
        """
    )