# Farm pipeline
This project aims to establish a data-driven system for a modern agriculture company. The farm operates a vast plot of land, segmented into numerous scattered fields, and aims to optimize the cultivation of various crops through a robust, data-driven approach. The system will facilitate the automated ingestion and integration of telemetry data from diverse agricultural machinery to enhance operational efficiency and decision-making.

## Data warehouse marts

### Detection of New Vehicle Runs
Method that identifies when a new vehicle run begins based on the time elapsed between consecutive data samples: The standard data sampling interval is 10 seconds (there are other sampling periods but the most frequent one is 10). If the time difference between two consecutive samples for the same vehicle exceeds a predefined threshold of 1000 seconds, it is inferred that the vehicle has stopped and restarted, indicating the start of a new vehicle run. For each vehicle run metrics like fuel consumption and distance travelled were calculated.


### Vehicle runs coverage map
With the vehicle runs table established, for each tractor run (as defined in the aforementioned table), a coverage map is generated to represent the operation performed by the tractor on the field. The field is modeled as a geometric polygon, which corresponds to the area where the tractor operated.

The project leverages the following technologies:
dbt Cloud: Utilized as the backend for data warehouse development.
Google Cloud Platform (GCP): Employed for cloud storage and computing solutions.
Looker Studio: Used for data visualization.
Cloud Composer: A managed Apache Airflow service for orchestrating workflows.
