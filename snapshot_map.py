class SnapshotMap:
    def __init__(self) -> None:
        # Main dictionary for current states
        self.data = {}
        # Snapshot counter
        self.snapshot_counter = 0
        # Track current snapshot
        self.current_snapshot = 0
        # Version history for each key
        # Format: {key: [(snapshot_id, value), ...]}
        self.version_history = {}
    
    def put(self, key, value):
        """Add or update a key-value pair in O(1) time"""
        # Get current value
        current_value = None
        
        # Check if key exists in version history and has entries
        if key in self.version_history and self.version_history[key]:
            # Get the most recent version from the last tuple
            _, current_value = self.version_history[key][-1]
        # Initialize version history for new keys
        elif key not in self.version_history:
            self.version_history[key] = []
        
        # Only add if the value is different from the current one
        if current_value != value:
            self.version_history[key].append((self.current_snapshot, value))
            self.data[key] = value
    
    def get(self, key, snapshot_id=None):
        """
        Get the value for a key, optionally at a specific snapshot.
        
        Args:
            key: The key to look up
            snapshot_id: Optional snapshot ID to get the value at that point in time
                         If None, returns the current value
                         
        Returns:
            The value at the specified snapshot, or None if the key didn't exist
        """
        if snapshot_id is None:
            # Return the current value
            return self.data.get(key)

        # Validate snapshot ID
        if snapshot_id < 0 or snapshot_id > self.snapshot_counter:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
        
        # If key has no version history, it never existed
        if key not in self.version_history or not self.version_history[key]:
            return None
        
        # Find the version at or before the requested snapshot
        _, value = self._binary_search_version(self.version_history[key], snapshot_id)
        return value
    
    def take_snapshot(self):
        """Take a snapshot in O(1) time"""
        self.snapshot_counter += 1
        self.current_snapshot = self.snapshot_counter
        return self.current_snapshot

    def restore_snapshot(self, snapshot_id):
        """Restore to a snapshot with sublinear complexity O(k log v) where k is number of keys and v is versions per key"""
        # Validate snapshot ID
        if snapshot_id < 0 or snapshot_id > self.snapshot_counter:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
        
        # Set current snapshot ID
        self.current_snapshot = snapshot_id
        
        # Step 1: Track all keys ever in version history
        all_keys = set(self.version_history.keys())
        
        # Step 2: Process all keys and update/remove as appropriate (with binary search)
        for key in all_keys:
            versions = self.version_history.get(key, [])
            
            # Use the binary search helper to find the correct version
            _, value = self._binary_search_version(versions, snapshot_id)
            
            # Update or remove the key based on result
            if value is not None:
                self.data[key] = value
            elif key in self.data:
                del self.data[key]
        
        # Step 3: Remove any keys in data that aren't in version_history
        # (might have been added after taking the latest snapshot)
        keys_to_remove = [k for k in self.data if k not in all_keys]
        for key in keys_to_remove:
            del self.data[key]

    def _binary_search_version(self, versions, snapshot_id):
        """
        Binary search to find the most recent version at or before the target snapshot.
        Returns (index, value) if found, or (-1, None) if no valid version exists.
        Time complexity: O(log v) where v is the number of versions.
        """
        if not versions or versions[0][0] > snapshot_id:
            return -1, None
            
        left, right = 0, len(versions) - 1
        found_idx = -1
        
        while left <= right:
            mid = (left + right) // 2
            mid_snapshot, _ = versions[mid]
            
            if mid_snapshot < snapshot_id:
                # Found a valid version, but continue looking for later ones
                found_idx = mid
                left = mid + 1
            else:
                # This version is too new
                right = mid - 1
        
        if found_idx != -1:
            _, value = versions[found_idx]
            return found_idx, value
        
        return -1, None

    def compact_history(self):
        """Optional: Remove redundant versions to save space"""
        for key, versions in self.version_history.items():
            if len(versions) <= 1:
                continue
            
            # Keep only versions that matter (different values)
            compressed = [versions[0]]
            for i in range(1, len(versions)):
                if versions[i][1] != compressed[-1][1]:
                    compressed.append(versions[i])
            
            self.version_history[key] = compressed

# Example usage:
if __name__ == "__main__":
    snap_map = SnapshotMap()

    # Add some initial data
    snap_map.put("name", "Alice")
    snap_map.put("age", 30)

    # Take a snapshot
    snapshot1 = snap_map.take_snapshot()
    print(f"Snapshot {snapshot1} taken: {snap_map.data}")

    # Modify data
    snap_map.put("name", "Bob")
    snap_map.put("location", "New York")

    # Take another snapshot
    snapshot2 = snap_map.take_snapshot()
    print(f"Snapshot {snapshot2} taken: {snap_map.data}")

    # Make more changes
    snap_map.put("age", 35)

    # Restore to first snapshot
    snap_map.restore_snapshot(snapshot1)
    print(f"Restored to snapshot {snapshot1}: {snap_map.data}")
    print(f"Version history: {snap_map.version_history}")
    
    # Add new data after restoring
    snap_map.put("hobby", "Reading")
    
    # Take a new snapshot
    snapshot3 = snap_map.take_snapshot()
    print(f"Snapshot {snapshot3} taken: {snap_map.data}")
