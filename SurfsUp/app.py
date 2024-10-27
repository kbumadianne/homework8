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
#engine = create_engine("sqlite:///Resources/hawaii.sqlite")

database_path = os.path.join(os.path.dirname(__file__), 'Resources', 'hawaii.sqlite')
engine = create_engine(f"sqlite:///{database_path}")

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

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<end>"
    )

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

@app.route("/api/v1.0/stations")
def names():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query to find all of the stations
    results = session.query(station.station).all()

    session.close()

    # Convert list of tuples into normal list
    station_names = list(np.ravel(results))

    return jsonify(station_names)   

@app.route("/api/v1.0/tobs")
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

    # Assign most active station to station id
    most_active_station = 'USC00519281'    
    
    # Query for date and temperature filter to last year and most active station
    temperature_data = session.query(
                measurement.date,
                measurement.tobs.label('temperature')
    ).filter(
                measurement.date >= one_year_ago_str, 
                measurement.date <= most_recent_date,
                station.station == most_active_station
    ).all()

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    # measurement_df = pd.DataFrame(precipitation_data, columns=['date', 'precipitation'])

    # # Sort the dataframe by date
    # measurement_sort = measurement_df.sort_values(by='date', ascending=True)

    # Create a dictionary from the row data and append to a list of dates
    active_station_temperature = []
    for date, temperature in temperature_data:
        temperature_dict = {}
        temperature_dict["date"] = date
        temperature_dict["precipitation"] = temperature
        active_station_temperature.append(temperature_dict)

    return jsonify(active_station_temperature)

    session.close()


if __name__ == "__main__":
    app.run(debug=True)


