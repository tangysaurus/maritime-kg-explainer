from franz.openrdf.connect import ag_connect
from pandas import DataFrame
from math import radians, sin, cos, asin, sqrt

class KnowledgeGraph:
    # Regex pattern for extracting ids
    PATTERN = r".*#(\w+)>"

    def __init__(self, repo_name):
        self.connection = ag_connect(repo_name)
    
    def extract_vessels(self) -> list[str]:
        """Extract all vessel ids"""
        query = f"""
            SELECT ?vessel
            WHERE {{
                ?vessel a :VesselIdentity 
            }}
        """
        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()

        if df.empty:
            vessels = []
        else:
            vessels = list(df["vessel"].str.extract(self.PATTERN, expand = False))
        return vessels

    def vessel_info(self, vessel_id: str) -> DataFrame:
        """Extract vessel data"""
        query = f"""
            SELECT ?name ?flag ?type
            WHERE {{
                :{vessel_id} 
                    :vesselName ?name ;
                    :flag ?flag ;
                    :vesselType ?type
            }}
        """
        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()
        
        if not df.empty:
            df["type"] = df["type"].str.extract(self.PATTERN, expand = False)
        return df

    def extract_trajectory_sequences(self, vessel_id: str) -> list[str]:
        """Extract trajectory sequences ids for each vessel"""
        query = f"""
            SELECT ?trajectory
            WHERE {{
                ?trajectory a :TrajectorySequence ;
                    :forVessel :{vessel_id}
            }}
        """
        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()

        if df.empty:
            trajectory_sequences = []
        else:
            trajectory_sequences = list(df["trajectory"].str.extract(self.PATTERN, expand = False))
        return trajectory_sequences

    def extract_observations(self, traj_seq_id: str) -> list[str]:
        """Extract observations from trajectory sequence"""
        query = f"""
            SELECT ?observations
            WHERE {{
                :{traj_seq_id} 
                    :usesObservation ?observations 
            }}
            ORDER BY ?observations
        """
        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()

        if df.empty:
            observations = []
        else:
            observations = list(df["observations"].str.extract(self.PATTERN, expand = False))
        return observations

    def observation_info(self, observation_id: str) -> DataFrame:
        """Extract observation data"""
        if observation_id[:3] == "ais":
            query = f"""
                SELECT ?lat ?lon ?speed ?course ?port_dist ?shore_dist ?time
                WHERE {{
                    :{observation_id} 
                        :lat ?lat ;
                        :lon ?lon ;
                        :speed ?speed ;
                        :course ?course ;
                        :distanceFromPort_km ?port_dist ;
                        :distanceFromShore_km ?shore_dist ;
                        :timestamp ?time
                }}
            """
        else:
            query = f"""
                SELECT ?lat ?lon ?speed ?course ?time
                WHERE {{
                    :{observation_id} 
                        :lat ?lat ;
                        :lon ?lon ;
                        :sog ?speed ;
                        :cog ?course ;
                        :timestamp ?time
                }}
            """

        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()
        return df

    def related_gap_events(self, vessel_id: str) -> list[str]:
        """Return list of AIS gap events related to a vessel"""
        query = f"""
            SELECT ?event
            WHERE {{
                ?event a :Event ;
                    :eventType "AISGapEvent" ;
                    :participantMembership ?membership .
                ?membership
                    :memberVessel :{vessel_id}
            }}
        """

        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()

        if df.empty:
            gap_events = []
        else:
            gap_events = list(df["event"].str.extract(self.PATTERN, expand = False))
        return gap_events

    def related_port_events(self, vessel_id: str) -> list[str]:
        """Returns a list of port visit events related to a vessel"""
        trajectory_sequences = self.extract_trajectory_sequences(vessel_id)
        port_visit_events = set()

        for seq in trajectory_sequences:
            query = f"""
                SELECT ?event ?obs_last_lat ?obs_last_lon ?port_lat ?port_lon
                WHERE {{
                    :{seq}
                        :hasFirstObservation ?obs_first ;
                        :hasLastObservation ?obs_last .

                    ?obs_first
                        :timestamp ?traj_start .
                    ?obs_last
                        :timestamp ?traj_end ;
                        :lat ?obs_last_lat ;
                        :lon ?obs_last_lon .
                    
                    ?event a :Event ;
                        :eventType "PortVisitEvent" ;
                        :startTime ?event_start ;
                        :endTime   ?event_end ;
                        :berthGeometry ?geom .

                    ?geom :asWKT ?wkt .

                    # Parse WKT "POINT(lon lat)"
                    BIND(REPLACE(?wkt, "POINT\\\\(|\\\\)", "") AS ?coords)
                    BIND(STRBEFORE(?coords, " ") AS ?lon_str)
                    BIND(STRAFTER(?coords, " ") AS ?lat_str)
                    BIND(xsd:decimal(?lon_str) AS ?port_lon)
                    BIND(xsd:decimal(?lat_str) AS ?port_lat)

                    BIND(STRBEFORE(STR(?traj_start), "T") AS ?traj_start_date)
                    BIND(STRBEFORE(STR(?traj_end), "T") AS ?traj_end_date)
                    BIND(STRBEFORE(STR(?event_start), "T") AS ?event_start_date)
                    BIND(STRBEFORE(STR(?event_end), "T") AS ?event_end_date)

                    FILTER(?traj_start_date <= ?event_end_date && ?event_start_date <= ?traj_end_date)

                }}
            """
            with self.connection.executeTupleQuery(query) as result:
                df = result.toPandas()

            if not df.empty:
                df["event"] = df["event"].str.extract(self.PATTERN, expand = False)

                dist_mask = df.apply(
                    lambda r: self.is_nearby(r["obs_last_lat"], r["obs_last_lon"], r["port_lat"], r["port_lon"], 30.0),
                    axis=1
                )

                port_visit_events.update(df[dist_mask]["event"].tolist())
        
        return list(port_visit_events)

    def related_fishing_events(self, vessel_id: str) -> list[str]:
        trajectory_sequences = self.extract_trajectory_sequences(vessel_id)
        fishing_events = set()

        for seq in trajectory_sequences:
            query = f"""
                PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

                SELECT ?event 
                WHERE {{
                    :{seq}
                        :hasFirstObservation ?obs_first ;
                        :hasLastObservation ?obs_last .

                    ?obs_first
                        :timestamp ?traj_start .
                    ?obs_last
                        :timestamp ?traj_end ;
                        :lat ?obs_last_lat ;
                        :lon ?obs_last_lon .
                    
                    ?event a :Event ;
                        :eventType "FishingEvent" ;
                        :startTime ?event_start ;
                        :endTime   ?event_end ;
                        :location ?location .
                    
                    ?location
                        :zoneGeometry ?zoneGeom .

                    ?zoneGeom geo:asWKT ?polyWKT .
                    BIND(
                        STRDT(
                            CONCAT("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(",
                                    STR(?obs_last_lon), " ", STR(?obs_last_lat), ")"),
                            geo:wktLiteral
                        ) AS ?ptWKT
                    )

                    FILTER( geof:sfContains(?polyWKT, ?ptWKT) )
                    FILTER(?traj_start <= ?event_end && ?event_start <= ?traj_end)

                }}
            """
            with self.connection.executeTupleQuery(query) as result:
                df = result.toPandas()
            if not df.empty:
                df["event"] = df["event"].str.extract(self.PATTERN, expand = False)

            fishing_events.update(list(df["event"]))
        return list(fishing_events)

    def related_weather_events(self, vessel_id: str) -> list[str]:
        """Returns a list of weather events related to a vessel"""
        trajectory_sequences = self.extract_trajectory_sequences(vessel_id)
        weather_events = set()

        for seq in trajectory_sequences:
            query = f"""
                PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

                SELECT ?event 
                WHERE {{
                    :{seq}
                        :hasFirstObservation ?obs_first ;
                        :hasLastObservation ?obs_last .

                    ?obs_first
                        :timestamp ?traj_start .
                    ?obs_last
                        :timestamp ?traj_end ;
                        :lat ?obs_last_lat ;
                        :lon ?obs_last_lon .
                    
                    ?event a :Event ;
                        :eventType "WeatherEvent" ;
                        :startTime ?event_start ;
                        :endTime   ?event_end ;
                        :location ?location .
                    
                    ?location
                        :zoneGeometry ?zoneGeom .

                    ?zoneGeom geo:asWKT ?polyWKT .
                    BIND(
                        STRDT(
                            CONCAT("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(",
                                    STR(?obs_last_lon), " ", STR(?obs_last_lat), ")"),
                            geo:wktLiteral
                        ) AS ?ptWKT
                    )

                    FILTER( geof:sfContains(?polyWKT, ?ptWKT) )
                    FILTER(?traj_start <= ?event_end && ?event_start <= ?traj_end)

                }}
            """
            with self.connection.executeTupleQuery(query) as result:
                df = result.toPandas()
            if not df.empty:
                df["event"] = df["event"].str.extract(self.PATTERN, expand = False)

            weather_events.update(list(df["event"]))
        return list(weather_events)

    def find_related_events(self, vessel_id: str) -> tuple:
        """Returns a list of events (1 list for each event type) related to a vessel"""
        gap_events = self.related_gap_events(vessel_id)
        port_events = self.related_port_events(vessel_id)
        fishing_events = self.related_fishing_events(vessel_id)
        weather_events = self.related_weather_events(vessel_id)

        return gap_events, port_events, fishing_events, weather_events

    def find_nearby_vessels(self, vessel_id: str, time_thresh: int = 600, dist_thresh: int = 30) -> list[str]:
        """
        Returns a list of nearby vessels
        2 vessels are nearby if they're in the same location at some point in time
        """
        nearby_vessels = set()
        query = f"""
            PREFIX geof:<http://www.opengis.net/def/function/geosparql/>
            PREFIX geo: <http://www.opengis.net/ont/geosparql#>

            SELECT ?vessel ?t1 ?t2 ?lat1 ?lon1 ?lat2 ?lon2
            WHERE {{
                :{vessel_id}
                    :mmsi ?v1 .

                ?o1 a :AISObservation ; :mmsi ?v1 ; :lat ?lat1 ; :lon ?lon1 ; :timestamp ?t1 .
                ?o2 a :AISObservation ; :mmsi ?v2 ; :lat ?lat2 ; :lon ?lon2 ; :timestamp ?t2 .

                ?vessel a :VesselIdentity ;
                    :mmsi ?v2 .

                FILTER(?v1 != ?v2)
            }}
        """
        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()
        if not df.empty:
            df["vessel"] = df["vessel"].str.extract(self.PATTERN, expand = False)
            same_time = (df["t1"]- df["t2"]).abs().dt.total_seconds() <= time_thresh

        dist_mask = df.apply(
            lambda r: self.is_nearby(r["lat1"], r["lon1"], r["lat2"], r["lon2"], dist_thresh),
            axis=1
        )

        hits = df[same_time & dist_mask]
        # ?v2 is the MMSI literal already
        nearby_vessels.update(hits["vessel"].astype(str).tolist())
        return list(nearby_vessels)

    def event_info(self, event_id: str) -> DataFrame:
        """Extract information about an event"""
        # if gap event
        if event_id[:3] == "gap":
            query = f"""
                SELECT ?type ?location ?start ?end ?gap_distance ?gap_duration ?gap_speed ?gap_intentional ?participant
                WHERE {{
                    :{event_id}
                        :eventType ?type ;
                        :location ?location ;
                        :startTime ?start ;
                        :endTime ?end ;
                        :gapDistance_km ?gap_distance ;
                        :gapDuration_hours ?gap_duration ;
                        :gapImpliedSpeed_knots ?gap_speed ;
                        :gapIntentionalDisabling ?gap_intentional ;
                        :participantMembership ?participant
                }}
            """
        # if port event
        elif event_id[:4] == "port":
            query = f"""
                SELECT ?type ?port ?location ?start ?end ?port_dist ?shore_dist
                WHERE {{
                    :{event_id}
                        :eventType ?type ;
                        :portName ?port ;
                        :location ?location ;
                        :startTime ?start ;
                        :endTime ?end ;
                        :startDistanceFromPort_km ?port_dist ;
                        :startDistanceFromShore_km ?shore_dist
                }}
            """
        # if fishing event
        elif event_id[:4] == "fish":
            query = f"""
                SELECT ?type ?location ?start ?end ?score ?gear_type
                WHERE {{
                    :{event_id}
                        :eventType ?type ;
                        :location ?location ;
                        :startTime ?start ;
                        :endTime ?end ;
                        :fishingEffortScore ?score ;
                        :gearType ?gear_type
                }}
            """
        # if weather event
        else:
            query = f"""
                SELECT ?type ?location ?start ?end ?weather ?severity
                WHERE {{
                    :{event_id}
                        :eventType ?type ;
                        :location ?location ;
                        :startTime ?start ;
                        :endTime ?end ;
                        :weatherType ?weather ;
                        :severity ?severity
                }}
            """

        with self.connection.executeTupleQuery(query) as result:
            df = result.toPandas()

        if "location" in df.columns:
            df["location"] = df["location"].str.extract(self.PATTERN, expand = False)
        
        if "participant" in df.columns:
            df["participant"] = df["participant"].str.extract(self.PATTERN, expand = False)
        
        return df

    def is_nearby(self, lat1: float, lon1: float, lat2: float, lon2: float, threshold: float = 10.0) -> bool:
        """
        Return True if (lat1, lon1) and (lat2, lon2) are within 10 km.
        Uses the haversine formula with inputs in decimal degrees.
        """
        R = 6371.0088  # Earth's radius in kilometers

        # Convert to radians
        lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(radians, (lat1, lon1, lat2, lon2))

        # Differences
        delta_lat = lat2_rad - lat1_rad
        delta_lon = lon2_rad - lon1_rad

        # Haversine formula
        a = sin(delta_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2) ** 2
        c = 2 * asin(sqrt(a))
        distance_km = R * c

        return distance_km <= threshold