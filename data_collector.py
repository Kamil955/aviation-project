# data_collector.py

import time
import json
from opensky_api import OpenSkyApi

# --- Configuration ---
OUTPUT_FILE = 'sample_flights.json'
# A set to keep track of aircraft we've already saved in this session
# to avoid too many duplicates.
SEEN_AIRCRAFT = set()

print("Starting data collector...")
print(f"Data will be appended to '{OUTPUT_FILE}' every 10 seconds.")
print("Press Ctrl+C to stop.")

# --- Main Loop ---
try:
    while True:
        try:
            print("\nRequesting new data from OpenSky API...")
            api = OpenSkyApi()  # Anonymous access
            states = api.get_states()

            if states and states.states:
                new_flights_count = 0
                # Open the file in 'append' mode ('a')
                with open(OUTPUT_FILE, 'a') as f:
                    for s in states.states:
                        # Process only if the aircraft has not been seen before
                        if s.icao24 not in SEEN_AIRCRAFT:
                            flight_data = {
                                'icao24': s.icao24,
                                'callsign': s.callsign.strip() if s.callsign else None,
                                'origin_country': s.origin_country,
                                'longitude': s.longitude,
                                'latitude': s.latitude,
                                'baro_altitude': s.baro_altitude,
                                'on_ground': s.on_ground,
                                'velocity': s.velocity,
                                'true_track': s.true_track,
                                'vertical_rate': s.vertical_rate
                            }
                            # Write the new record as a new line in the file
                            f.write(json.dumps(flight_data) + '\n')
                            SEEN_AIRCRAFT.add(s.icao24)
                            new_flights_count += 1
                
                if new_flights_count > 0:
                    print(f"✅ Success! Appended {new_flights_count} new aircraft records.")
                else:
                    print("...No new aircraft found in this cycle.")

            else:
                print("❌ API did not return any flight data in this cycle.")

        except Exception as e:
            print(f"An error occurred during API call: {e}")
        
        # Wait for 10 seconds before the next cycle
        print("Waiting for 10 seconds...")
        time.sleep(10)

except KeyboardInterrupt:
    print("\nStopping data collector. Data saved in 'sample_flights.json'.")