# Import the dependencies.
import os
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine) 

# Save references to each table
measurement = Base.classes.measurement 
station = Base.classes.station 


# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
# Welcome page listing all available api routes

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/temp<br/>"
        f"/api/v1.0/temp/<start><br/>"
        f"/api/v1.0/temp/<start>/<end>"
    )

# Route to the precipitation page listing precipitation data by day/date for the last year

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Last year precipitation by date"""
    # Find the most recent date in the data set.
    most_recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()[0]
    # Set beginning and end dates for the last year
    recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')
    # Calculate the date one year from the last date in data set.
    one_year_ago = recent_date - dt.timedelta(days=365)
    # Turn date back into string
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
    
    # Query for date and precipitation filter to last year
    precipitation_data = session.query(
                measurement.date,
                measurement.prcp.label('precipitation')
    ).filter(
                measurement.date >= one_year_ago_str, 
                measurement.date <= most_recent_date
    ).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    measurement_df = pd.DataFrame(precipitation_data, columns=['date', 'precipitation'])

    # Sort the dataframe by date
    measurement_sort = measurement_df.sort_values(by='date', ascending=True)

    # Create a dictionary from the row data and append to a list of dates
    last_year_precipitation = []
    for date, precipitation in precipitation_data:
        precipitation_dict = {}
        precipitation_dict["date"] = date
        precipitation_dict["precipitation"] = precipitation
        last_year_precipitation.append(precipitation_dict)

    return jsonify(last_year_precipitation)

    session.close()
# Set route to display all stations
@app.route("/api/v1.0/stations")
def names():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query to find all of the stations
    results = session.query(station.name).all()

    session.close()

    # Convert list of tuples into normal list
    station_names = list(np.ravel(results))

    return jsonify(station_names)   

# Set route to display temperature for the last year at the most active station
# Note most active station was previously found in the 
@app.route("/api/v1.0/temp")
def temperature():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Most active station temperature data from the last year"""
    # Find the most recent date in the data set.
    most_recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()[0]
    # Set beginning and end dates for the last year
    recent_date = dt.datetime.strptime(most_recent_date, '%Y-%m-%d')
    # Calculate the date one year from the last date in data set.
    one_year_ago = recent_date - dt.timedelta(days=365)
    # Turn date back into string
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    # Run query for most active station, note first at end to return one result
    most_active_station = session.query(
                    station.station,  # Select the station identifier
                    station.name, # also include station name in the query to display in the results
                    func.count(measurement.id).label('count')  # Count of entries for each station
    ).join(
                    measurement,  # Join with the measurement table
                    measurement.station == station.station  # Join on station
    ).group_by(
                    station.station,  # Group by station to count rows per station
                    station.name #Inclue station name in the group by as well
    ).order_by(
                    func.count(measurement.id).desc()  # Order by count in descending order
    ).first()  

    #Assign results into a variable
    most_active_station_id = most_active_station[0]    
    
    # Query for date and temperature filter to last year and most active station
    temperature_data = session.query(
                measurement.date,
                measurement.tobs.label('temperature')
    ).filter(
                measurement.date >= one_year_ago_str, 
                measurement.date <= most_recent_date,
                station.station == most_active_station_id
    ).all()

    # Create a dictionary from the row data and append to a list of dates
    active_station_temperature = []
    for date, temperature in temperature_data:
        temperature_dict = {}
        temperature_dict["date"] = date
        temperature_dict["temperature"] = temperature
        active_station_temperature.append(temperature_dict)

    return jsonify(active_station_temperature)

    session.close()

    
# Set route to display TMIN, TAVG, TMAX for a specified start date or start-end range
@app.route("/api/v1.0/temp/<start>")
def temperature_range_start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Ensure the dates are in the correct format (DD-MM-YYYY)
    try:
        start_date = dt.datetime.strptime(start, '%m%d%Y')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use MMDDYYYY."}), 400

    # Query to calculate TMIN, TAVG, TMAX
    temperature_stats = session.query(
        func.min(measurement.tobs).label('TMIN'),
        func.avg(measurement.tobs).label('TAVG'),
        func.max(measurement.tobs).label('TMAX')
    ).filter(
        measurement.date >= start_date,
    ).all()

    # Close the session
    session.close()

    # Check if any results were found
    if temperature_stats:
        return jsonify({
            "start_date": start,
            "TMIN": temperature_stats[0][0],
            "TAVG": temperature_stats[0][1],
            "TMAX": temperature_stats[0][2]
        })
    else:
        return jsonify({"error": "No temperature data found for the specified date range."}), 404

@app.route("/api/v1.0/temp/<start>/<end>")
def temperature_range(start, end=None):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Ensure the dates are in the correct format (DD-MM-YYYY)
    try:
        start_date = dt.datetime.strptime(start, '%m%d%Y')
        end_date = dt.datetime.strptime(end, '%m%d%Y')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use MMDDYYYY."}), 400

    # Query to calculate TMIN, TAVG, TMAX
    temperature_stats = session.query(
        func.min(measurement.tobs).label('TMIN'),
        func.avg(measurement.tobs).label('TAVG'),
        func.max(measurement.tobs).label('TMAX')
    ).filter(
        measurement.date >= start_date,
        measurement.date <= end_date
    ).all()

    # Close the session
    session.close()

    # Check if any results were found
    if temperature_stats:
        return jsonify({
            "start_date": start,
            "end_date": end,
            "TMIN": temperature_stats[0][0],
            "TAVG": temperature_stats[0][1],
            "TMAX": temperature_stats[0][2]
        })
    else:
        return jsonify({"error": "No temperature data found for the specified date range."}), 404



if __name__ == "__main__":
    app.run(debug=True)


