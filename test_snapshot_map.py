import time
import random
from snapshot_map import SnapshotMap  # Assumes the SnapshotMap class is in a file called snapshot_map.py

def measure_time(func, *args, **kwargs):
    """Measure execution time of a function."""
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    return end - start, result

def test_put_performance(sizes):
    """Test put() performance across different data sizes."""
    results = []
    
    for size in sizes:
        # Create new map for each test
        snap_map = SnapshotMap()
        
        # Measure time to insert size elements
        total_time = 0
        for i in range(size):
            key = f"key_{i}"
            value = f"value_{i}"
            time_taken, _ = measure_time(snap_map.put, key, value)
            total_time += time_taken
        
        # Calculate average time per operation
        avg_time = total_time / size
        results.append(avg_time)
        print(f"Put {size} items: {avg_time:.8f} seconds per operation")
    
    return results

def test_get_performance(sizes):
    """Test get() performance across different data sizes."""
    results_current = []
    results_historical = []
    
    for size in sizes:
        # Create and populate the map
        snap_map = SnapshotMap()
        for i in range(size):
            snap_map.put(f"key_{i}", f"value_{i}")
        
        # Take a snapshot after half the data
        mid_point = size // 2
        snap_map.take_snapshot()
        
        # Update the second half of data
        for i in range(mid_point, size):
            snap_map.put(f"key_{i}", f"new_value_{i}")
        
        # Take another snapshot
        snapshot_id = snap_map.take_snapshot()
        
        # Test current state get() performance
        total_time_current = 0
        for i in range(min(1000, size)):  # Sample up to 1000 random keys
            key = f"key_{random.randint(0, size-1)}"
            time_taken, _ = measure_time(snap_map.get, key)
            total_time_current += time_taken
        
        avg_time_current = total_time_current / min(1000, size)
        results_current.append(avg_time_current)
        
        # Test historical get() performance
        total_time_historical = 0
        for i in range(min(1000, size)):  # Sample up to 1000 random keys
            key = f"key_{random.randint(0, size-1)}"
            time_taken, _ = measure_time(snap_map.get, key, snapshot_id=snapshot_id-1)
            total_time_historical += time_taken
        
        avg_time_historical = total_time_historical / min(1000, size)
        results_historical.append(avg_time_historical)
        
        print(f"Get {size} items (current): {avg_time_current:.8f} seconds per operation")
        print(f"Get {size} items (historical): {avg_time_historical:.8f} seconds per operation")
    
    return results_current, results_historical

def test_snapshot_performance(sizes):
    """Test take_snapshot() performance across different data sizes."""
    results = []
    
    for size in sizes:
        # Create and populate the map
        snap_map = SnapshotMap()
        for i in range(size):
            snap_map.put(f"key_{i}", f"value_{i}")
        
        # Measure snapshot time
        time_taken, _ = measure_time(snap_map.take_snapshot)
        results.append(time_taken)
        
        print(f"Snapshot {size} items: {time_taken:.8f} seconds")
    
    return results

def test_restore_performance(sizes):
    """Test restore_snapshot() performance across different data sizes."""
    results = []
    
    for size in sizes:
        # Create and populate the map
        snap_map = SnapshotMap()
        for i in range(size):
            snap_map.put(f"key_{i}", f"value_{i}")
        
        # Take a snapshot
        snapshot_id = snap_map.take_snapshot()
        
        # Update some data
        update_count = min(size, 1000)  # Update up to 1000 items
        for i in range(update_count):
            key = f"key_{random.randint(0, size-1)}"
            snap_map.put(key, f"new_value_{i}")
        
        # Measure restore time
        time_taken, _ = measure_time(snap_map.restore_snapshot, snapshot_id)
        results.append(time_taken)
        
        print(f"Restore {size} items: {time_taken:.8f} seconds")
    
    return results

def run_full_performance_test():
    """Run complete performance tests with multiple data sizes up to > 1M."""
    # Test with increasing sizes
    sizes = [1000, 10000, 100000, 500000, 1000000, 2000000]
    
    print("Starting performance tests...")
    
    print("\nTesting put() performance...")
    put_times = test_put_performance(sizes)
    
    print("\nTesting get() performance...")
    get_times_current, get_times_historical = test_get_performance(sizes)
    
    print("\nTesting take_snapshot() performance...")
    snapshot_times = test_snapshot_performance(sizes)
    
    print("\nTesting restore_snapshot() performance...")
    restore_times = test_restore_performance(sizes)
    
    print("\nPerformance testing complete!")

def test_version_history_impact():
    """Test how the number of versions affects performance."""
    map_size = 100000
    version_counts = [1, 5, 10, 20, 50, 100]
    restore_times = []
    
    print("\nTesting impact of version history...")
    
    for version_count in version_counts:
        # Create and populate the map
        snap_map = SnapshotMap()
        
        # Create initial data
        for i in range(map_size):
            snap_map.put(f"key_{i}", f"value_0_{i}")
        
        snapshot_ids = []
        
        # Create multiple versions for each key
        for v in range(1, version_count+1):
            # Take a snapshot after each version
            snapshot_id = snap_map.take_snapshot()
            snapshot_ids.append(snapshot_id)
            
            # Update all keys with new version
            for i in range(map_size):
                snap_map.put(f"key_{i}", f"value_{v}_{i}")
        
        # Measure time to restore to first snapshot
        time_taken, _ = measure_time(snap_map.restore_snapshot, snapshot_ids[0])
        restore_times.append(time_taken)
        
        print(f"Restore with {version_count} versions per key: {time_taken:.8f} seconds")

if __name__ == "__main__":
    # Main performance tests across different sizes
    run_full_performance_test()
    
    # Test specifically how version count affects performance
    test_version_history_impact()