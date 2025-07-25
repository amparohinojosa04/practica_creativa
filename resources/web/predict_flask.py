import sys, os, re, json, time, uuid
import iso8601, datetime
from flask import Flask, render_template, request
from bson import json_util
from kafka import KafkaProducer, KafkaConsumer
from flask_socketio import SocketIO, emit
from pymongo import MongoClient

# Configuration details
import config
#prediction APIs
import predict_utils

app = Flask(__name__)

#PARA WEBSOCKETS
socketio = SocketIO(app, cors_allowed_origins="*")

uri_mongo = "mongodb://mongodb:27017/"
client = MongoClient(uri_mongo)


consumer = KafkaConsumer(
    'flight-delay-ml-response',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    api_version=(0, 10)
)


from pyelasticsearch import ElasticSearch
elastic = ElasticSearch(config.ELASTIC_URL)

producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10))
PREDICTION_TOPIC = 'flight-delay-ml-request'


@socketio.on('kafka_request')
def handle_kafka_message(json_data):
    print("Mensaje recibido desde WebSocket:", json_data)

    #UUID único
    unique_id = str(uuid.uuid4())
    json_data["UUID"] = unique_id

    json_data['Distance'] = predict_utils.get_flight_distance(
        client, json_data['Origin'], json_data['Dest']
    )
    json_data.update(predict_utils.get_regression_date_args(json_data['FlightDate']))
    json_data['Timestamp'] = predict_utils.get_current_timestamp()

    #PARA ENVIAR Y ESPERAR RESPUES DE KAFKA
    message_bytes = json.dumps(json_data).encode()
    producer.send(PREDICTION_TOPIC, message_bytes)
    timeout_seconds = 10
    start_time = time.time()
    found_msg = None

    while time.time() - start_time < timeout_seconds:
        msg_pack = consumer.poll(timeout_ms=1000)
        for tp, messages in msg_pack.items():
            for message in messages:
                try:
                    decoded = json.loads(message.value.decode("utf-8"))
                    if decoded.get("UUID") == unique_id:
                        emit("kafka_response", decoded)
                        return
                except Exception:
                    continue

    emit("kafka_response", {"status": "WAIT"})


@app.route("/on_time_performance")
def on_time_performance():
  
  carrier = request.args.get('Carrier')
  flight_date = request.args.get('FlightDate')
  flight_num = request.args.get('FlightNum')
  
  flight = client.agile_data_science.on_time_performance.find_one({
    'Carrier': carrier,
    'FlightDate': flight_date,
    'FlightNum': flight_num
  })
  
  return render_template('flight.html', flight=flight)

@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):
  
  flights = client.agile_data_science.on_time_performance.find(
    {
      'Origin': origin,
      'Dest': dest,
      'FlightDate': flight_date
    },
    sort = [
      ('DepTime', 1),
      ('ArrTime', 1),
    ]
  )

  flight_count = flights.count()
  
  return render_template(
    'flights.html',
    flights=flights,
    flight_date=flight_date,
    flight_count=flight_count
  )

# Controller: Fetch a flight table
@app.route("/total_flights")
def total_flights():
  total_flights = client.agile_data_science.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights.html', total_flights=total_flights)

# Serve the chart's data via an asynchronous request (formerly known as 'AJAX')
@app.route("/total_flights.json")
def total_flights_json():
  total_flights = client.agile_data_science.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return json_util.dumps(total_flights, ensure_ascii=False)

# Controller: Fetch a flight chart
@app.route("/total_flights_chart")
def total_flights_chart():
  total_flights = client.agile_data_science.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights_chart.html', total_flights=total_flights)




@app.route("/airplanes")
@app.route("/airplanes/")
def search_airplanes():

  search_config = [
    {'field': 'TailNum', 'label': 'Tail Number'},
    {'field': 'Owner', 'sort_order': 0},
    {'field': 'OwnerState', 'label': 'Owner State'},
    {'field': 'Manufacturer', 'sort_order': 1},
    {'field': 'Model', 'sort_order': 2},
    {'field': 'ManufacturerYear', 'label': 'MFR Year'},
    {'field': 'SerialNumber', 'label': 'Serial Number'},
    {'field': 'EngineManufacturer', 'label': 'Engine MFR', 'sort_order': 3},
    {'field': 'EngineModel', 'label': 'Engine Model', 'sort_order': 4}
  ]

  # Pagination parameters
  start = request.args.get('start') or 0
  start = int(start)
  end = request.args.get('end') or config.AIRPLANE_RECORDS_PER_PAGE
  end = int(end)

  # Navigation path and offset setup
  nav_path = predict_utils.strip_place(request.url)
  nav_offsets = predict_utils.get_navigation_offsets(start, end, config.AIRPLANE_RECORDS_PER_PAGE)

  print("nav_path: [{}]".format(nav_path))
  print(json.dumps(nav_offsets))

  # Build the base of our elasticsearch query
  query = {
    'query': {
      'bool': {
        'must': []}
    },
    'sort': [
      {'Owner': {'order': 'asc'} },
      '_score'
    ],
    'from': start,
    'size': config.AIRPLANE_RECORDS_PER_PAGE
  }

  arg_dict = {}
  for item in search_config:
    field = item['field']
    value = request.args.get(field)
    print(field, value)
    arg_dict[field] = value
    if value:
      query['query']['bool']['must'].append({'match': {field: value}})

  # Query elasticsearch, process to get records and count
  results = elastic.search(query)
  airplanes, airplane_count = predict_utils.process_search(results)

  # Persist search parameters in the form template
  return render_template(
    'all_airplanes.html',
    search_config=search_config,
    args=arg_dict,
    airplanes=airplanes,
    airplane_count=airplane_count,
    nav_path=nav_path,
    nav_offsets=nav_offsets,
  )

@app.route("/airplanes/chart/manufacturers.json")
@app.route("/airplanes/chart/manufacturers.json")
def airplane_manufacturers_chart():
  mfr_chart = client.agile_data_science.airplane_manufacturer_totals.find_one()
  return json.dumps(mfr_chart)

# Controller: Fetch a flight and display it
@app.route("/airplane/<tail_number>")
@app.route("/airplane/flights/<tail_number>")
def flights_per_airplane(tail_number):
  flights = client.agile_data_science.flights_per_airplane.find_one(
    {'TailNum': tail_number}
  )
  return render_template(
    'flights_per_airplane.html',
    flights=flights,
    tail_number=tail_number
  )

# Controller: Fetch an airplane entity page
@app.route("/airline/<carrier_code>")
def airline(carrier_code):
  airline_summary = client.agile_data_science.airlines.find_one(
    {'CarrierCode': carrier_code}
  )
  airline_airplanes = client.agile_data_science.airplanes_per_carrier.find_one(
    {'Carrier': carrier_code}
  )
  return render_template(
    'airlines.html',
    airline_summary=airline_summary,
    airline_airplanes=airline_airplanes,
    carrier_code=carrier_code
  )

# Controller: Fetch an airplane entity page
@app.route("/")
@app.route("/airlines")
@app.route("/airlines/")
def airlines():
  airlines = client.agile_data_science.airplanes_per_carrier.find()
  return render_template('all_airlines.html', airlines=airlines)

@app.route("/flights/search")
@app.route("/flights/search/")
def search_flights():

  # Search parameters
  carrier = request.args.get('Carrier')
  flight_date = request.args.get('FlightDate')
  origin = request.args.get('Origin')
  dest = request.args.get('Dest')
  tail_number = request.args.get('TailNum')
  flight_number = request.args.get('FlightNum')

  # Pagination parameters
  start = request.args.get('start') or 0
  start = int(start)
  end = request.args.get('end') or config.RECORDS_PER_PAGE
  end = int(end)

  # Navigation path and offset setup
  nav_path = predict_utils.strip_place(request.url)
  nav_offsets = predict_utils.get_navigation_offsets(start, end, config.RECORDS_PER_PAGE)

  # Build the base of our elasticsearch query
  query = {
    'query': {
      'bool': {
        'must': []}
    },
    'sort': [
      {'FlightDate': {'order': 'asc', 'ignore_unmapped' : True} },
      {'DepTime': {'order': 'asc', 'ignore_unmapped' : True} },
      {'Carrier': {'order': 'asc', 'ignore_unmapped' : True} },
      {'FlightNum': {'order': 'asc', 'ignore_unmapped' : True} },
      '_score'
    ],
    'from': start,
    'size': config.RECORDS_PER_PAGE
  }

  # Add any search parameters present
  if carrier:
    query['query']['bool']['must'].append({'match': {'Carrier': carrier}})
  if flight_date:
    query['query']['bool']['must'].append({'match': {'FlightDate': flight_date}})
  if origin:
    query['query']['bool']['must'].append({'match': {'Origin': origin}})
  if dest:
    query['query']['bool']['must'].append({'match': {'Dest': dest}})
  if tail_number:
    query['query']['bool']['must'].append({'match': {'TailNum': tail_number}})
  if flight_number:
    query['query']['bool']['must'].append({'match': {'FlightNum': flight_number}})

  # Query elasticsearch, process to get records and count
  results = elastic.search(query)
  flights, flight_count = predict_utils.process_search(results)

  # Persist search parameters in the form template
  return render_template(
    'search.html',
    flights=flights,
    flight_date=flight_date,
    flight_count=flight_count,
    nav_path=nav_path,
    nav_offsets=nav_offsets,
    carrier=carrier,
    origin=origin,
    dest=dest,
    tail_number=tail_number,
    flight_number=flight_number
    )

@app.route("/delays")
def delays():
  return render_template('delays.html')

# Load our regression model
import joblib
from os import environ


project_home = os.environ["PROJECT_HOME"]
# vectorizer = joblib.load("{}/models/sklearn_vectorizer.pkl".format(project_home))
# regressor = joblib.load("{}/models/sklearn_regressor.pkl".format(project_home))

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/regress", methods=['POST'])
def regress_flight_delays():
  
  api_field_type_map = \
    {
      "DepDelay": int,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }
  
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values
  prediction_features = {}
  prediction_features['Origin'] = api_form_values['Origin']
  prediction_features['Dest'] = api_form_values['Dest']
  prediction_features['FlightNum'] = api_form_values['FlightNum']
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(client, api_form_values['Origin'], api_form_values['Dest'])
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(api_form_values['FlightDate'])
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Vectorize the features
  feature_vectors = vectorizer.transform([prediction_features])
  
  # Make the prediction!
  result = regressor.predict(feature_vectors)[0]
  
  # Return a JSON object
  result_obj = {"Delay": result}
  return json.dumps(result_obj)

@app.route("/flights/delays/predict")
def flight_delays_page():
  """Serves flight delay predictions"""
  
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'},
    {'field': 'FlightNum', 'label': 'Flight Number', 'value': 1519},
  ]
  
  return render_template('flight_delays_predict.html', form_config=form_config)

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify", methods=['POST'])
def classify_flight_delays():
  """POST API for classifying flight delays"""
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }
  
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    client, api_form_values['Origin'],
    api_form_values['Dest']
  )
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  client.agile_data_science.prediction_tasks.insert_one(
    prediction_features
  )
  return json_util.dumps(prediction_features)

@app.route("/flights/delays/predict_batch")
def flight_delays_batch_page():
  """Serves flight delay predictions"""
  
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'},
    {'field': 'FlightNum', 'label': 'Flight Number', 'value': 1519},
  ]
  
  return render_template("flight_delays_predict_batch.html", form_config=form_config)

@app.route("/flights/delays/predict_batch/results/<iso_date>")
def flight_delays_batch_results_page(iso_date):
  """Serves page for batch prediction results"""
  
  # Get today and tomorrow's dates as iso strings to scope query
  today_dt = iso8601.parse_date(iso_date)
  rounded_today = today_dt.date()
  iso_today = rounded_today.isoformat()
  rounded_tomorrow_dt = rounded_today + datetime.timedelta(days=1)
  iso_tomorrow = rounded_tomorrow_dt.isoformat()
  
  # Fetch today's prediction results from Mongo
  predictions = client.agile_data_science.prediction_results.find(
    {
      'Timestamp': {
        "$gte": iso_today,
        "$lte": iso_tomorrow,
      }
    }
  )
  
  return render_template(
    "flight_delays_predict_batch_results.html",
    predictions=predictions,
    iso_date=iso_date
  )

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  """POST API for classifying flight delays"""
  
  # Define the form fields to process
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }

  # Fetch the values for each field from the form object
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    client, api_form_values['Origin'],
    api_form_values['Dest']
  )
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  # Create a unique ID for this message
  unique_id = str(uuid.uuid4())
  prediction_features['UUID'] = unique_id
  
  message_bytes = json.dumps(prediction_features).encode()
  producer.send(PREDICTION_TOPIC, message_bytes)

  response = {"status": "OK", "id": unique_id}
  return json_util.dumps(response)

@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
  """Serves flight delay prediction page with polling form"""
  
  form_config = [
    {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
    {'field': 'Carrier', 'value': 'AA'},
    {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
    {'field': 'Origin', 'value': 'ATL'},
    {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'}
  ]
  
  return render_template('flight_delays_predict_kafka.html', form_config=form_config)

import time

@app.route("/flights/delays/predict/classify_realtime/response/<unique_id>")
def classify_flight_delays_realtime_response(unique_id):
  """Serves predictions to polling requestors"""
  consumer.subscribe(['flight-delay-ml-response'])
  timeout_seconds = 20  # Tiempo máximo de espera para encontrar el mensaje
  start_time = time.time()
  found_msg = None
  
  while time.time() - start_time < timeout_seconds:
    # Poll para recibir mensajes; ajusta el timeout_ms
    msg_pack = consumer.poll(timeout_ms=1000)
    for tp, messages in msg_pack.items():
      for message in messages:
        try:
          data = json.loads(message.value.decode('utf-8'))
        except Exception as e:
          # Si no se puede decodificar, ignora este mensaje
          continue
        # Comprueba si el UUID del mensaje coincide con el parámetro
        if data.get("UUID") == unique_id:
          found_msg = data
          break
      if found_msg:
        break
    if found_msg:
      break
      
  if found_msg:
    prediction_value = found_msg.get("prediction", None)
    response = {"status": "OK", "id": unique_id, "prediction": found_msg}
  else:
    response = {"status": "WAIT", "id": unique_id}

# Devuelve la respuesta en formato JSON
  return json.dumps(response)


def shutdown_server():
  func = request.environ.get('werkzeug.server.shutdown')
  if func is None:
    raise RuntimeError('Not running with the Werkzeug Server')
  func()

@app.route('/shutdown')
def shutdown():
  shutdown_server()
  return 'Server shutting down...'

 
#PARA WEBSOCKETS
if __name__ == "__main__": 
    socketio.run(app, debug=True, host="0.0.0.0", port=5001)

  
