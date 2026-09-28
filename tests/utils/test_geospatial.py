"""
Tests for geospatial utility functions.
"""


from sentinel_dispatch.utils.geospatial import haversine_distance, bearing


class TestGeospatialUtils:
    """Test cases for geospatial utility functions."""
    
    def test_haversine_distance_lahaina_to_kaanapali(self):
        """Test Haversine distance between Lahaina and Kaanapali."""
        # Distance between Lahaina and Kaanapali (approximately 5.5 km)
        distance = haversine_distance(
            20.8783, -156.6825,  # Lahaina
            20.9289, -156.6944   # Kaanapali
        )
        
        assert 5.0 < distance < 6.0  # Should be approximately 5.5 km
    
    def test_haversine_distance_same_point(self):
        """Test Haversine distance for same point."""
        distance = haversine_distance(
            20.8783, -156.6825,
            20.8783, -156.6825
        )
        assert abs(distance) < 0.01  # Should be near zero
    
    def test_haversine_distance_known_values(self):
        """Test Haversine distance with known values."""
        # Distance between New York and Los Angeles (approximately 3944 km)
        distance = haversine_distance(
            40.7128, -74.0060,  # New York
            34.0522, -118.2437  # Los Angeles
        )
        
        # Allow some margin for calculation differences
        assert 3900 < distance < 4000
    
    def test_bearing_lahaina_to_kaanapali(self):
        """Test bearing from Lahaina to Kaanapali."""
        # Kaanapali is roughly north of Lahaina
        bearing_deg = bearing(
            20.8783, -156.6825,  # Lahaina
            20.9289, -156.6944   # Kaanapali (north)
        )
        
        assert 0.0 <= bearing_deg <= 360.0
        # Should be roughly north (0-45 degrees or 315-360 degrees)
        assert bearing_deg < 45 or bearing_deg > 315
    
    def test_bearing_same_point(self):
        """Test bearing for same point."""
        bearing_deg = bearing(
            20.8783, -156.6825,
            20.8783, -156.6825
        )
        assert 0.0 <= bearing_deg <= 360.0
    
    def test_bearing_cardinal_directions(self):
        """Test bearing for cardinal directions."""
        # North
        bearing_north = bearing(0.0, 0.0, 1.0, 0.0)
        assert 0.0 <= bearing_north < 45 or bearing_north > 315
        
        # East
        bearing_east = bearing(0.0, 0.0, 0.0, 1.0)
        assert 45 < bearing_east < 135
        
        # South
        bearing_south = bearing(0.0, 0.0, -1.0, 0.0)
        assert 135 < bearing_south < 225
        
        # West
        bearing_west = bearing(0.0, 0.0, 0.0, -1.0)
        assert 225 < bearing_west < 315

