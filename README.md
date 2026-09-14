# Flight Delay Attribution Pipeline
An end-to-end data engineering project using Snowflake and SQL to cross-reference airline-reported flight delay causes against independently observed weather data.
Stack: Snowflake, SQL, Python, Meteostat API, BTS/DOT data
## The Problem
When a US flight is delayed 15+ minutes, the operating airline classifies the
cause into one of five categories: carrier fault, weather, air traffic control
(NAS), security, or late aircraft. This classification determines passenger
compensation eligibility and feeds the official on-time performance record.
Airlines have an incentive to attribute delays to weather (no compensation
owed) rather than causes within their control. No public dataset independently
verifies these attributions against observed weather. This pipeline does.
## Headline Finding
For January 2015 across five major US airports (ATL, DFW, JFK, LAX, ORD):
- 85.6% of flights delayed 15+ minutes during observed adverse weather at
  their origin airport were officially attributed to non-weather causes.
- 111 flights met the "delayed during adverse weather" criteria; 95 of them
  were blamed on carrier, late-aircraft, or air-traffic-control causes rather
  than weather.
By airport:
| Airport | Adverse-weather delays | Suspect attribution | Suspect % |
|---|---|---|---|
| JFK | 20 | 18 | 90.0% |
| DFW | 20 | 17 | 85.0% |
| ATL | 71 | 60 | 84.5% |
## Example
A flight departing Dallas-Fort Worth on January 22, 2015 was delayed
260 minutes (4h 20m). The airline attributed 87 minutes to carrier fault,
154 minutes to late aircraft, and 19 minutes to air traffic control --
zero minutes to weather -- despite measurable precipitation at DFW that
hour during a regional winter-weather event.
## Architecture
BTS Flight Data (Kaggle, CSV)
-> Snowflake RAW: FLIGHTS_RAW_V2, AIRLINES_RAW_V2, AIRPORTS_RAW
-> Staging: STG_FLIGHTS (cleaned, typed, keyed)
-> Dimensions: DIM_CARRIER, DIM_AIRPORT
-> Facts: FCT_FLIGHT
-> MART_FLIGHT_WEATHER (flights joined to weather at scheduled hour)
Meteostat Hourly Weather (5 stations, CSV.gz)
-> Snowflake RAW: WEATHER_ATL/DFW/LAX/ORD/JFK
-> FCT_WEATHER_OBSERVATION (airport-hour grain, adverse-weather flag)
-> MART_FLIGHT_WEATHER

<img width="2600" height="1800" alt="flight_delay_erd" src="https://github.com/user-attachments/assets/10e2f88b-aa8a-4205-b635-83675559b775" />


## Tech Stack
- Snowflake -- cloud data warehouse (raw, staging, marts schemas)
- SQL -- transformations, star schema, analytical marts
- Meteostat -- independent hourly weather observations
- BTS/DOT flight data -- historical on-time performance (via Kaggle)
- Python -- source data filtering (pandas)
## Dataset
- BTS Reporting Carrier On-Time Performance -- January 2015
- Meteostat hourly weather -- January 2015, five major US airport stations
- Airline and airport reference tables
Volume:
- 469,968 flight records
- 3,720 airport-hour weather observations (5 airports x 744 hours)
- 14 carriers, 322 airports in reference data
## Data Model
Kimball-style star schema:
- Dimensions: DIM_CARRIER, DIM_AIRPORT
- Facts: FCT_FLIGHT (one row per flight), FCT_WEATHER_OBSERVATION
  (one row per airport-hour)
- Mart: MART_FLIGHT_WEATHER -- each flight joined to origin-airport weather
  at its scheduled departure hour, with a derived
  IS_WEATHER_ATTRIBUTION_SUSPECT flag
## Pipeline Steps
1. Filter raw BTS annual file to January 2015 (Python/pandas)
2. Load flights, airlines, airports into Snowflake RAW schema
3. Load five airport weather files into RAW schema
4. Build dimensions and cleaned staging layer (SQL)
5. Build FCT_FLIGHT joined to dimensions
6. Union five weather tables into FCT_WEATHER_OBSERVATION with adverse-weather flag
7. Join flights to weather at scheduled hour -> MART_FLIGHT_WEATHER
8. Run attribution analysis to produce the headline finding
## Data Quality
- Adverse weather defined by Meteostat condition codes (rain/snow/storm),
  wind gusts >= 60 km/h, or precipitation >= 5mm
- Delay attribution uses BTS's own reported cause-minute breakdown
- Flight-to-weather join matched on airport + date + scheduled departure hour
- Row counts validated at each pipeline stage against source totals
## Carrier Reliability (January 2015)
| Carrier | Delay rate (15+ min) |
|---|---|
| American Eagle | 31.8% |
| Frontier | 31.5% |
| Spirit | 26.8% |
| SkyWest | 23.6% |
| JetBlue | 22.9% |
| Southwest | 18.9% |
| US Airways | 18.0% |
## Business Use Cases
- Passenger-compensation firms adjudicating delay claims
- Travel insurers pricing trip-delay policies
- Airports evaluating weather-resilience infrastructure investment
- Corporate travel managers setting carrier/route policy
- Regulators and journalists investigating airline classification patterns
## Limitations and Scope
- Proof of concept: one month, five airports. Chosen to prove the approach
  end-to-end rather than backfill years. Production version would extend to
  multiple years and all 380+ US airports.
- Sample sizes for the attribution finding are small (111 adverse-weather
  delays); the pattern is clear but requires full-scale validation.
- The suspect flag is a signal for investigation, not a definitive verdict --
  adverse origin weather is necessary but not sufficient to prove weather was
  the true cause.
## Future Enhancements
- Scale to full-year, all-airport coverage
- Add live aircraft tracking (OpenSky) for delay-propagation analysis
- Orchestrate with Airflow; encode transformations as dbt models with tests
- Build a claim-adjudication API on top of MART_FLIGHT_WEATHER
- ## Orchestration
## Orchestration
An Airflow DAG (`dags/flight_pipeline_dag.py`) is included showing the
intended production orchestration. It was not deployed in this proof of
concept: the shared Snowflake environment enforces MFA and network-policy
restrictions that block the programmatic authentication Airflow requires.
Transformations were run manually as SQL (`scripts/pipeline.sql`) to keep
the analytical timeline on track. Deploying the DAG against a properly
provisioned service account is the first item in Future Enhancements.
