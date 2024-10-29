# Import the dependencies.
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
                measurement.date,  # select date from the measurement table
                measurement.prcp.label('precipitation')  # select precipitation from the measurement table and label 
    ).filter(
                measurement.date >= one_year_ago_str,  #filter to include all dates greater than or equal to a year ago
    ).all()  #display all results

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    measurement_df = pd.DataFrame(precipitation_data, columns=['date', 'precipitation'])

    # Sort the dataframe by date
    measurement_sort = measurement_df.sort_values(by='date', ascending=True)

    # Create a dictionary from the row data and append to a list of dates
    last_year_precipitation = []  #create an empty list
    for date, precipitation in precipitation_data: #begin for loop set date and precipitation values to store data into
        precipitation_dict = {} #create a dictionary for each iteration of the loop
        precipitation_dict["date"] = date  #store dates in the date variable
        precipitation_dict["precipitation"] = precipitation  #store precipitation data in the precipitation variable
        last_year_precipitation.append(precipitation_dict) #append the data stored in dictionary to the list

    #return the dictionary in a json format
    return jsonify(last_year_precipitation)
    
    #close the session    
    session.close()

# Set route to display all stations
@app.route("/api/v1.0/stations")
def names():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query to find all of the station names and return all
    results = session.query(station.name).all()

    #close the session
    session.close()

    # Convert list of tuples into normal list
    station_names = list(np.ravel(results))

    #return in a json format
    return jsonify(station_names)   

# Set route to display temperature for the last year at the most active station
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
                measurement.date,  #select date from the measurement table
                measurement.tobs.label('temperature')  #select tobs from the measurement table and label as temperature
    ).filter(
                measurement.date >= one_year_ago_str, #filter to any date greater than or equal to a year ago
                station.station == most_active_station_id #filter to the most active station id
    ).all()  #display all results in the query

    # Create a dictionary from the row data and append to a list of dates
    active_station_temperature = [] #create a list
    for date, temperature in temperature_data:  #run a for loop, set date and temperature as values to store data in the list
        temperature_dict = {} #create a dictionary to store data from the loop
        temperature_dict["date"] = date # store date data in the date variable
        temperature_dict["temperature"] = temperature #store temperature data in the temperature variable
        active_station_temperature.append(temperature_dict) #append the dict data into the list

    #display the data in a json format
    return jsonify(active_station_temperature)

    #close session
    session.close()

    
# Set route to display TMIN, TAVG, TMAX for a specified start date or start-end range
@app.route("/api/v1.0/temp/<start>")
def temperature_range_start(start):  #include start within the def to represent as a client request input
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Ensure the dates are in the correct format (DD-MM-YYYY), if not return error message
    try:
        start_date = dt.datetime.strptime(start, '%m%d%Y')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use MMDDYYYY."}), 400

    # Query to calculate TMIN, TAVG, TMAX
    temperature_stats = session.query(
        func.min(measurement.tobs).label('TMIN'),  #use min func to find the min temperature, label as TMIN
        func.avg(measurement.tobs).label('TAVG'), #use the avg func to find the avg temp, label as TAVG
        func.max(measurement.tobs).label('TMAX') #use the max fuc to find the max temp, label as TMAX
    ).filter(
        measurement.date >= start_date,  #filter to dates greater than or equal to the start date
    ).all() #return all values

    # Close the session
    session.close()

    # Check if any results were found
    if temperature_stats:  #check to see if values are stores in the temp status query above
        return jsonify({  #if there is data then return in a json format
            "start_date": start, #label and display start date
            "TMIN": temperature_stats[0][0],  #label and display TMIN as the first element of first row
            "TAVG": temperature_stats[0][1], #label and display TAVG as the second element first row
            "TMAX": temperature_stats[0][2]  #lable and display TMAX as the third element first row
        })
    else:  #otherwise return an error that no temp data was found with that start date
        return jsonify({"error": "No temperature data found for the specified date range."}), 404

@app.route("/api/v1.0/temp/<start>/<end>")
def temperature_range(start, end): #include start and end within the def to represent both start and end date as a client request input
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Ensure the dates are in the correct format (DD-MM-YYYY), if not return an error message
    try:
        start_date = dt.datetime.strptime(start, '%m%d%Y')
        end_date = dt.datetime.strptime(end, '%m%d%Y')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use MMDDYYYY."}), 400

    # Query to calculate TMIN, TAVG, TMAX
    temperature_stats = session.query(
        func.min(measurement.tobs).label('TMIN'),  #use min func to find the min temperature, label as TMIN
        func.avg(measurement.tobs).label('TAVG'), #use the avg func to find the avg temp, label as TAVG
        func.max(measurement.tobs).label('TMAX') #use the max fuc to find the max temp, label as TMAX
    ).filter(
        measurement.date >= start_date,  #filter to start on the start date
        measurement.date <= end_date    #filter to end on the end date
    ).all()  #return all results

    # Close the session
    session.close()

    # Check if any results were found
    if temperature_stats: #check to see if values are stores in the temp status query above
        return jsonify({   #if there is data then return in a json format
            "start_date": start,  #label and display start date
            "end_date": end,  #label and display end date
            "TMIN": temperature_stats[0][0],  #label and display TMIN as the first element of first row
            "TAVG": temperature_stats[0][1], #label and display TAVG as the second element first row
            "TMAX": temperature_stats[0][2]  #lable and display TMAX as the third element first row
        })
    else:  #otherwise return an error that no temp data was found with that date range
        return jsonify({"error": "No temperature data found for the specified date range."}), 404


#set debugging to false 
if __name__ == "__main__":
    app.run(debug=True)


