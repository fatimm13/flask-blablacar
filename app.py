from datetime import date, datetime, timedelta
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1 import GeoPoint
import requests ##csv, operator, 
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify 
from flask_cors import CORS
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="geoapiExercises")

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

CORS(app)
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()


URL_PRECIO_GASOLINA = "https://geoportalgasolineras.es/resources/files/preciosEESS_es.xls"
preciosGasolina = None
ultActGas = None

URL_DATOS_COVID = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Datos_Capacidad_Asistencial_Historico_"
covid = None
ultActCov = None
ultConsCov = None

@app.route("/")
def home():
    downloadCSV(URL_DATOS_COVID)
    return "Hello Flask"

def downloadCSVComplete(url):
    '''
    Recibe una URL y descarga los datos como CSV.
    '''
    req = requests.get(url)
    url_content = req.content
    #Estas tres lineas guardan el documento en local para luego leerlo
    csv_file = open('downloaded.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    #Realizamos operaciones de parseado sobre la fecha para poder utilizarla para ordenar los resultados
    global covid 
    dateparse = lambda x: datetime.strptime(x, '%d/%m/%Y')
    covid = pd.read_csv('downloaded.csv',encoding='latin-1',sep=";",parse_dates=True,date_parser=dateparse)
    covid = covid[["Fecha","Provincia","INGRESOS_COVID19"]]
    covid.columns =[column.replace(" ", "_") for column in covid.columns]

def downloadCSV(csv_url):
    '''
    Recibe una URL y descarga los datos del útlimo día como CSV.
    '''
    global covid, ultActCov, ultConsCov
    ahora = datetime.now()

    #Actualiza los datos desde el servidor si estos llevan más de un día sin ser actualizados en el servidor.
    if ultConsCov==None or (ahora-ultConsCov).total_seconds() >= 86400:
        ultConsCov = ahora
        try:
            #Creamos una request al url con el que trabajamos y luego obtenemos sus datos
            url = csv_url+datetime.strftime(ahora, '%d%m%Y')+".csv"
            downloadCSVComplete(url)
            ultActCov = ahora
            db.collection('configuracion').document('fechaActualizacion').update({"fechaCovid":ahora})
        except:
            #Creamos una request al url con el que trabajamos y luego obtenemos sus datos
            ultActCov = db.collection('configuracion').document('fechaActualizacion').get().to_dict()["fechaCovid"]
            url = csv_url+datetime.strftime(ultActCov, '%d%m%Y')+".csv"
            downloadCSVComplete(url)

def downloadXLS(xls_url):
    '''
    Recibe una URL y descarga los datos como XLS.
    '''
    global preciosGasolina, ultActGas
    ahora = datetime.now()

    #Actualiza los datos desde el servidor si estos llevan más media hora sin ser actualizados en el servidor.
    if ultActGas==None or (ahora-ultActGas).total_seconds() >= 1800:
        req = requests.get(xls_url)
        url_content = req.content
        
        #Estas tres lineas guardan el documento en local, no son necesarias para el funcionamiento del sistema una vez montado como servidor.
        csv_file = open('downloaded.xls', 'wb')
        csv_file.write(url_content)
        csv_file.close()
        
        #Parseamos Latitud y Longitud a float para facilitar trabajar con ellos más adelante.
        preciosGasolina = pd.read_excel(url_content, skiprows=np.arange(3))
        preciosGasolina.columns =[column.replace(" ", "_") for column in preciosGasolina.columns]
        preciosGasolina = preciosGasolina[["Provincia","Municipio","Localidad","Código_postal","Dirección","Longitud","Latitud","Precio_gasolina_95_E5","Precio_gasolina_98_E5","Precio_gasóleo_A","Rótulo","Horario"]]
        preciosGasolina["Longitud"] = [ float(i.replace(',', '.')) for i in preciosGasolina["Longitud"]]
        preciosGasolina["Latitud"] = [ float(i.replace(',', '.')) for i in preciosGasolina["Latitud"]]
        ultActGas = ahora
    

def getCollection(tabla):
    '''
    Función que pide una colección de la BD y la devuelve como JSON.
    '''
    query_ref = db.collection(tabla)
    return fromCollectionToJson(query_ref)

def fromCollectionToJson(query_ref):
    '''
    Función que recibe una referencia de una colección y la devuelve como JSON.
    '''
    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()
        for key, value in resp.items(): resp.update({key : stringify(value)})
        d.update({cont : resp})
        cont = cont+1
    return jsonify(d)

def fromCollectionToJson(query_ref):
    '''
    Función que recibe una referencia de una colección y la devuelve como JSON.
    '''
    d = []
    for i in query_ref.stream():
        resp = i.to_dict()
        for key, value in resp.items(): resp.update({key : stringify(value)})
        resp.update({"id":i.id})
        d.append(resp)
       
    return jsonify(d)

def stringify(value):
    '''
    str() controlando tipos de la BD que no disponen del mismo.
    '''
    string = ""
    if(isinstance(value,GeoPoint)):
        string = "["+str(value.latitude)+","+str(value.longitude)+"]"
    else:
        string = str(value)
    return string

numericos = ["edad","plazas","libres"]
validAttributesUsuarios = ["nombre","ubicacion"]
validAttributesViajes = ["nombre","origen","destino","libres","precio"]

def makeSimpleQuery(tabla, parametro, valor):
    '''
    Función que realiza una petición sobre una colección con 
    un único atributo y valor y devuelve la solución como JSON.
    '''
    usuario_ref = db.collection(tabla)

    if(parametro in numericos):
        query_ref = usuario_ref.where(parametro, '==', int(valor))
    else:
        query_ref = usuario_ref.where(parametro, '==', valor)
    
    return fromCollectionToJson(query_ref)

def makeComplexQuery(tabla, parametros):
    '''
    Función que realiza una petición sobre una colección con varios atributos
    y un único valor para cada uno y devuelve la solución como JSON.
    '''
    query_ref = db.collection(tabla) 
    
    for i in parametros:
        if(i[0] in numericos):
            query_ref = query_ref.where(i[0], '==', int(i[1]))
        else:
            query_ref = query_ref.where(i[0], '==', i[1])

    return fromCollectionToJson(query_ref)

def makeViajesQuery(parametros):
    '''
    Función que realiza una petición sobre viajes con varios atributos ("nombre","origen","destino")
    y filtra por otros dos ("libres","precio"), devolvindo la solución como JSON.
    '''
    query_ref = db.collection("viajes")
    
    valor = None
    for i in parametros:
        if(i[0] == "libres"):
            query_ref = query_ref.where(i[0], '>=', int(i[1]))  
        elif(i[0] == "precio"):
            valor = float(i[1])
        else:
            query_ref = query_ref.where(i[0], '==', i[1])

    d = dict()
    cont = 0

    for i in query_ref.stream():
        resp = i.to_dict()
        if(valor==None or resp["precio"]<=valor):
            for key, value in resp.items(): resp.update({key : stringify(value)})

            d.update({cont : resp})
            cont = cont+1

    return jsonify(d)
    

@app.route("/usuarios", methods = ['GET', 'POST'])
def conseguir_subir_usuarios():
    """
    GET: Devuelve los usuarios cuyo atributo coincide con el primer argumento pasado (nombre o ubicación).
    POST: Recibe un JSON y crea un documento en la BD con dichos datos.
    """
    
    if request.method == 'GET':
        #Cogemos el primer par de items, si no hay asignamos None
        item = next(request.args.items(),None)

        #Comprobamos si es None, si no lo es miramos que esté en los atributos válidos
        #En caso de estarlo hacemos una request de ese item.
        if item == None:

            return getCollection("usuarios")

        elif (item[0] in validAttributesUsuarios):
            return makeSimpleQuery("usuarios",item[0],item[1])
        else:
            return "Atributo no válido"
        
    elif request.method == 'POST':

        aux = datetime.now()

        content = {
            'descripcion' : request.json['descripcion'],
            'edad' : request.json['edad'],
            'fecha' : aux,
            'nombre' : request.json['nombre'],
            'ubicacion' : request.json['ubicacion']
        }

        db.collection('usuarios').document().set(content)
        return jsonify(content)
    else:
        return("400: BAD REQUEST.")

    

@app.route("/usuarios/<id>", methods = ['GET','PUT','DELETE'])
def conseguir_actualizar_eliminar_usuarios(id):
    """
    GET: Devuelve un usuario a partir del id pasado.
    PUT: Actualiza el usuario del id pasado con los parametros del JSON recibido.
    DELETE: Borra un usuario a partir de su id.
    """

    if request.method == 'GET':

        dict = db.collection('usuarios').document(str(id)).get().to_dict()
        return jsonify(dict)

    elif request.method == 'PUT':
        usu = db.collection('usuarios').document(str(id))
        content = {
            'descripcion' : request.json['descripcion'],
            'edad' : request.json['edad'],
            'nombre' : request.json['nombre'],
            'ubicacion' : request.json['ubicacion']
        }
        
        viajes = db.collection("viajes")
        resul = viajes.where("idConductor","==",str(id))
        for i in resul.stream():
            viajes.document(str(i.id)).update({'nombreConductor' : request.json['nombre']})
        
        usu.update(content)
        return jsonify(usu.get().to_dict())
        
    elif request.method == 'DELETE':
        usu = db.collection('usuarios').document(str(id)).delete()
        return "200: Borrado exitoso."
    else:
        
        return "400: BAD REQUEST."


@app.route("/usuarios/<id>/viajesConductor", methods = ['GET'])
def conseguir_viajes_conductor(id):
    """
    GET: Devuelve los viajes del conductor ordenados por horaDeSalida.
    """

    if request.method == 'GET':
        viajes = db.collection('viajes').where('idConductor', '==', str(id)).order_by('horaDeSalida', direction=firestore.Query.ASCENDING)
        return fromCollectionToJson(viajes)
    else:
        return "400: BAD REQUEST."

@app.route("/viajes", methods = ['GET', 'POST'])
def conseguir_subir_viajes():
    """
    GET: Devuelve los viajes cuyos atributos coinciden con los atributos válidos pasados ['nombre','origen','destino','libres','precio'].
    POST: Recibe un JSON y crea un documento en la BD con dichos datos.
    """

    if request.method == 'GET':
        #Cogemos el primer par de items, si no hay asignamos None
        items = [i for i in request.args.items() if i[0] in validAttributesViajes ]
        
        if len(items)==len(request.args):
            return fromCollectionToJson(db.collection('viajes'))
            #return makeViajesQuery(items)
        else:
            return "Algún atributo no es válido"
            
    elif request.method == 'POST': 

        ### Crear viaje
        aux = datetime.fromisoformat(request.json['hora'])

        latOrig = request.json['latOrig']
        longOrig = request.json['longOrig']
        coordOrig = GeoPoint(latOrig,longOrig)
        latDest = request.json['latDest']
        longDest = request.json['longDest'] 
        coordDest = GeoPoint(latDest, longDest)
        
        location = geolocator.reverse(str(latOrig)+","+str(longOrig))
        address = location.raw['address']
        origen = address.get('city', '')

        location = geolocator.reverse(str(latDest)+","+str(longDest))
        address = location.raw['address']
        destino = address.get('city', '')
        content = {
            'coordOrigen' : coordOrig,
            'coordDestino' : coordDest,
            'horaDeSalida' : aux,
            'nombre' : request.json['nombre'],
            'destino' : destino,
            'origen' : origen,
            'nombreConductor' : request.json['nombreConductor'],
            'idConductor' : request.json['idConductor'],
            'plazas' : request.json['plazas'],
            'libres' : request.json['libres'],
            'precio' : request.json['precio']
        }

        v = db.collection('viajes').document()
        v.set(content)
        c = db.collection('usuarios').document(str(request.json['idConductor']))
        c.collection("viajes").document(v.id).set({'nombre' : request.json['nombre'], 'esConductor' : True})
        
        content["coordOrigen"] = stringify(coordOrig)
        content["coordDestino"] = stringify(coordDest)
        return jsonify(content)
    else:
        return("400: BAD REQUEST.")


@app.route("/viajes/<id>", methods = ['GET','PUT','DELETE'])
def conseguir_actualizar_eliminar_viajes(id):
    """
    GET: Obtiene el documento de un viaje por ID.
    PUT: Actualiza el documento de un viaje por ID, recibe los datos por JSON. 
    DELETE: Borra el documento de un viaje por ID.
    """
    if request.method == 'GET':

        dict = db.collection('viajes').document(str(id)).get().to_dict()
        dict["coordOrigen"] = stringify(dict["coordOrigen"])
        dict["coordDestino"] = stringify(dict["coordDestino"])
        return jsonify(dict)

    elif request.method == 'PUT':
        viaje = db.collection('viajes').document(str(id))

        aux = datetime.fromisoformat(request.json['hora'])

        content = {
            'horaDeSalida' : aux,
            'libres' : request.json['libres']
        }
        viaje.update(content)
        
        dict = viaje.get().to_dict()
        dict["coordOrigen"] = stringify(dict["coordOrigen"])
        dict["coordDestino"] = stringify(dict["coordDestino"])
        return jsonify(dict)
        
    elif request.method == 'DELETE':
        dict = db.collection('viajes').document(str(id)).get().to_dict()
        idCond = dict['idConductor']
        db.collection('usuarios').document(str(idCond)).collection('viajes').document(str(id)).delete()
        #TODO Borrar donde es pasajero (Se hará cuando tengamos forma de registrar pasajeros)

        viaje = db.collection('viajes').document(str(id)).delete()
        return "200: Borrado exitoso."
    else:
        
        return "400: BAD REQUEST."


@app.route("/viajes/<id>/conductor", methods = ['GET'])
def conseguir_conductor_viaje(id):
    """
    GET: Obtiene el documento del conductor de un viaje por el ID del viaje.
    """
    if request.method == 'GET':
        viaje = db.collection('viajes').document(str(id)).get().to_dict()
        idConductor = viaje['idConductor']
        conductor = db.collection('usuarios').document(idConductor).get().to_dict()
        return jsonify(conductor)
    else:
        return "400: BAD REQUEST."

@app.route("/gasolinera", methods = ['GET'])
def conseguir_gasolinera():
    """
    GET: Obtiene las gasolineras dentro de un radio con respecto a unas cordenadas o las busca por provincia (Solo busca por una de las 2).
    """
    if request.method == 'GET':
        #Actualizamos los datos si es necesario(En el interior de la función se controla cuando actualizarlos).
        downloadXLS(URL_PRECIO_GASOLINA)
        keys = [i[0] for i in request.args.items()]
        global preciosGasolina
        if ("latitud" in keys and "longitud" in keys):
            
            data = preciosGasolina.copy()
            lat = float(request.args["latitud"])
            long = float(request.args["longitud"])
            radio = 0.1
            q = 'Latitud >= '+str(lat-radio)+' and '+'Latitud <= '+str(lat+radio)+' and ' + 'Longitud >= '+str(long-radio)+' and '+'Longitud <= '+str(long+radio)

            data.query(q, inplace = True)
            return jsonify(data.to_dict())
            
        elif("provincia" in keys):
            data = preciosGasolina.copy()
            q = 'Provincia == "'+request.args["provincia"]+'"'
            data.query(q, inplace = True)
            return jsonify(data.to_dict())
            
        else:
            return "Algún atributo no es válido"
    else:
        return("400: BAD REQUEST.")
     
@app.route("/covid", methods =['GET'])
def conseguir_datos_covid():
    """
    GET: Obtiene la cantidad de ingresos por covid en las últimas 15 entradas en una provincia por su nombre.
    """
    if request.method == 'GET':
        #Actualizamos los datos si es necesario(En el interior de la función se controla cuando actualizarlos).
        downloadCSV(URL_DATOS_COVID)
        keys = [i[0] for i in request.args.items()]
        global covid
        data = covid.copy()
        if("provincia" in keys):

            q ='Provincia == "' + request.args["provincia"]+'"'

            data.query(q, inplace=True)
            data["Fecha"] = [datetime.strptime(i, '%d/%m/%Y') for i in data["Fecha"]]
            data.sort_values(by = 'Fecha', ascending = False, inplace = True)
            data = data.head(15)
            data = data.reset_index(drop=True)
            return jsonify(data.to_dict())
        else:
            return "Algún atributo no es válido"
    else:
        return "400: BAD REQUEST."

        
