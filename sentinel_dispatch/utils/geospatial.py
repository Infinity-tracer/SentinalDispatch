"""
Geospatial utility functions for distance and bearing calculations.
"""

import math


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points using Haversine formula.
    
    The Haversine formula calculates the great-circle distance between two points
    on a sphere given their longitudes and latitudes.
    
    Args:
        lat1: Latitude of first point in degrees
        lon1: Longitude of first point in degrees
        lat2: Latitude of second point in degrees
        lon2: Longitude of second point in degrees
        
    Returns:
        Distance in kilometers
        
    Example:
        >>> # Distance between Lahaina and Kaanapali (approximately 5.5 km)
        >>> distance = haversine_distance(20.8783, -156.6825, 20.9289, -156.6944)
        >>> assert 5.0 < distance < 6.0
    """
    # Earth radius in km
    R = 6371.0
    
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate bearing (initial compass direction) from point 1 to point 2.
    
    Args:
        lat1: Latitude of starting point in degrees
        lon1: Longitude of starting point in degrees
        lat2: Latitude of destination point in degrees
        lon2: Longitude of destination point in degrees
        
    Returns:
        Bearing in degrees (0=North, 90=East, 180=South, 270=West)
        Normalized to 0-360 range
        
    Example:
        >>> # Bearing from Lahaina to Kaanapali (roughly north)
        >>> bearing_deg = bearing(20.8783, -156.6825, 20.9289, -156.6944)
        >>> assert 0.0 <= bearing_deg <= 360.0
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)
    
    y = math.sin(dlon_rad) * math.cos(lat2_rad)
    x = (
        math.cos(lat1_rad) * math.sin(lat2_rad)
        - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad)
    )
    
    bearing_rad = math.atan2(y, x)
    bearing_deg = math.degrees(bearing_rad)
    
    # Normalize to 0-360
    return (bearing_deg + 360) % 360

