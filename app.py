import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
from datetime import datetime
import datetime as dt
import pandas as pd

# CONECTARSE A LA BASE DE DATOS SQLITE
engine = create_engine("sqlite:///Resources/hawaii.sqlite?check_same_thread=False")
Base = automap_base()
Base.prepare(engine, reflect=True)

# DEFINIR TABLAS A VARIABLES
measurment = Base.classes.measurement
station = Base.classes.station

# INICIAR SESION
session = Session(engine)

# INICIALIZAR FLASK 
app = Flask(__name__)

# RUTA DE INICIO
@app.route("/")
def INICIO():
    return ("RUTAS DISPONIBLES: </br>"+"/api/v1.0/precipitation <br>"+"/api/v1.0/stations <br>"+"/api/v1.0/tobs <br>"+"/api/v1.0/diaInicial <br>"+"/api/v1.0/diaInicial/diaFinal")


@app.route("/api/v1.0/precipitation")
def preciptiarion():
    maxDate =  session.query(func.max(measurment.date)).scalar()
    maxDate = datetime.strptime(maxDate, '%Y-%m-%d')
    last12months = dt.date(maxDate.year, maxDate.month, maxDate.day)-dt.timedelta(days=365)
    query = session.query(measurment.date,measurment.prcp).filter(measurment.date>=last12months).all()
    dataframe=pd.DataFrame(query,columns=['date', 'prcp'])
    dataframe=dataframe.sort_values(by=['date','prcp'])
    dataframe.set_index(dataframe['date'], inplace=True)
    precip={}
    for date,prcp in query:
        precip[date]=prcp
        print(date)
    return(precip)

@app.route("/api/v1.0/stations")
def stations():
    estaciones = session.query(station.station).all()
    print(estaciones)
    lista={}
    num=0
    for i in estaciones:
        lista[num]=i[0]
        num=num+1
    return (jsonify(lista))

@app.route("/api/v1.0/tobs")
def tobs():
    maxDate2 =  session.query(func.max(measurment.date)).scalar()
    maxDate2 = datetime.strptime(maxDate2, '%Y-%m-%d')
    last12months2 = dt.date(maxDate2.year, maxDate2.month, maxDate2.day)-dt.timedelta(days=365)
    tobs = session.query(measurment.date, measurment.tobs).filter(measurment.station == 'USC00519281').filter(measurment.date>=last12months2).all()
    temperatures={}
    for date,tobs in tobs:
        temperatures[date]=tobs
        print(date)
    return(temperatures)

@app.route("/api/v1.0/<start>")
def inicial(start=None):
    minimo = session.query(func.min(measurment.tobs)).filter(measurment.date >= start).all()
    prom= session.query(func.avg(measurment.tobs)).filter(measurment.date >= start).all()
    maximo = session.query(func.max(measurment.tobs)).filter(measurment.date >= start).all()
    temperaturas={"minimo":minimo[0][0], "maximo":maximo[0][0],"promedio":prom[0][0]}
    return(temperaturas)

@app.route("/api/v1.0/<start>/<end>")
def inicialfinal(start=None, end=None):
    minimo = session.query(func.min(measurment.tobs)).filter(measurment.date>=start).filter(measurment.date <= end).all()
    prom= session.query(func.avg(measurment.tobs)).filter(measurment.date>=start).filter(measurment.date<=end).all()
    maximo = session.query(func.max(measurment.tobs)).filter(measurment.date>=start).filter(measurment.date<=end).all()
    temperaturas={"minimo":minimo[0][0], "maximo":maximo[0][0],"promedio":prom[0][0]}
    return(temperaturas)


if __name__ == '__main__':
    app.run(debug=True, port=4000)