from datetime import date, datetime, timedelta
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import auth
from google.cloud.firestore_v1 import GeoPoint
import requests ##csv, operator, 
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify 
from flask_cors import CORS
from geopy.geocoders import Nominatim
import cloudinary
import cloudinary.uploader
import cloudinary.api
import tweepy 
import json

t_consumer_key = "fn1YuMm8f8ikI3DxaCfQg5ukS"
t_consumer_secret = "7W3tk5ujoKenkCNhf0ki8n2juRENXMVwuZyYYikdxNlDX1yqcd"

t_key = "1482356704281075717-CluK1cpO6vrvl6bJXLxfXt4oaGhGF8"
t_secret = "jl1rwotz9ZS0rvvQI7AuHiuEHMy9k5HzFlTeQpaC1gmzA"

t_auth = tweepy.OAuthHandler(t_consumer_key,t_consumer_secret)
t_auth.set_access_token(t_key,t_secret)

t_api = tweepy.API(t_auth, wait_on_rate_limit=True)

cloudinary.config( 
  cloud_name = "dugtth6er", 
  api_key = "917624729957762", 
  api_secret = "tLjX2_c7EoHRi-BxePmjtry6kjY",
  secure = True
)

tokens = dict()


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

"""
def validar(token,uid):
    token = token[7:]
    t = tokens.get(token, None)
    if(t is None):
        res = False
    else:
        aux = datetime.now().timestamp()
        if(aux >= t["exp"]):
            res = False
            tokens.pop(token,None)
        else:
            res = uid == t["uid"] 
    
    return res
"""

def verificarUsuario(token, uid):
    """
    Valida el token y comprueba que el usuario sea el correcto
    """
    token = token[7:]

    decoded_token = auth.verify_id_token(token)
    id = decoded_token['uid']
    return uid == id


@app.route("/")
def home():
    
    return "Hello Flask"

@app.route("/loginUsuario", methods = ['POST'])
def login():
    if(request.method=="POST"):
        
        token =  request.headers["Authorization"]
        token = token[7:]

        decoded_token = auth.verify_id_token(token)
        uid = decoded_token['uid']
        #exp = decoded_token['exp']

        #tokens[token] = {"uid":uid,"exp":exp}

        doc = db.collection('usuarios').document(uid).get()

        if(doc.exists):
            print("Existo")
            d = doc.to_dict()
            d.update({"id":uid})
            res = jsonify(d)
        else:
            aux = datetime.now()
            print("No existo")

            content = {
                'descripcion' : "",
                'edad' : 18,
                'fecha' : aux,
                'nombre' : request.json['nombre'],
                'ubicacion' : "Desconocida",
                'imagen' : "https://res.cloudinary.com/dugtth6er/image/upload/v1639832477/perfil_hont25.png"
            }
            print(content)

            db.collection('usuarios').document(uid).set(content)
            content.update({"id":uid})

            res = jsonify(content)

        return res

    else:
        return "No", 400


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
    Recibe una URL y descarga los datos del ??tlimo d??a como CSV.
    '''
    global covid, ultActCov, ultConsCov
    ahora = datetime.now()

    #Actualiza los datos desde el servidor si estos llevan m??s de un d??a sin ser actualizados en el servidor.
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

    #Actualiza los datos desde el servidor si estos llevan m??s media hora sin ser actualizados en el servidor.
    if ultActGas==None or (ahora-ultActGas).total_seconds() >= 1800:
        req = requests.get(xls_url)
        url_content = req.content
        
        #Estas tres lineas guardan el documento en local, no son necesarias para el funcionamiento del sistema una vez montado como servidor.
        csv_file = open('downloaded.xls', 'wb')
        csv_file.write(url_content)
        csv_file.close()
        
        #Parseamos Latitud y Longitud a float para facilitar trabajar con ellos m??s adelante.
        preciosGasolina = pd.read_excel(url_content, skiprows=np.arange(3))
        preciosGasolina.columns =[column.replace(" ", "_") for column in preciosGasolina.columns]
        preciosGasolina = preciosGasolina[["Provincia","Municipio","Localidad","C??digo_postal","Direcci??n","Longitud","Latitud","Precio_gasolina_95_E5","Precio_gasolina_98_E5","Precio_gas??leo_A","R??tulo","Horario"]]
        preciosGasolina["Longitud"] = [ float(i.replace(',', '.')) for i in preciosGasolina["Longitud"]]
        preciosGasolina["Latitud"] = [ float(i.replace(',', '.')) for i in preciosGasolina["Latitud"]]
        ultActGas = ahora
    

def getCollection(tabla):
    '''
    Funci??n que pide una colecci??n de la BD y la devuelve como JSON.
    '''
    query_ref = db.collection(tabla)
    return fromCollectionToJson(query_ref)


def fromCollectionToJson(query_ref):
    '''
    Funci??n que recibe una referencia de una colecci??n y la devuelve como JSON.
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
        string = [value.latitude,value.longitude]
    else:
        string = str(value)
    return string


numericos = ["edad","plazas","libres"]
validAttributesUsuarios = ["nombre","ubicacion"]
validAttributesViajes = ["nombre","origen","destino","libres","precio"]


def makeSimpleQuery(tabla, parametro, valor):
    '''
    Funci??n que realiza una petici??n sobre una colecci??n con 
    un ??nico atributo y valor y devuelve la soluci??n como JSON.
    '''
    usuario_ref = db.collection(tabla)

    if(parametro in numericos):
        query_ref = usuario_ref.where(parametro, '==', int(valor))
    else:
        query_ref = usuario_ref.where(parametro, '==', valor)
    
    return fromCollectionToJson(query_ref)

def makeComplexQuery(tabla, parametros):
    '''
    Funci??n que realiza una petici??n sobre una colecci??n con varios atributos
    y un ??nico valor para cada uno y devuelve la soluci??n como JSON.
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
    Funci??n que realiza una petici??n sobre viajes con varios atributos ("nombre","origen","destino")
    y filtra por otros dos ("libres","precio"), devolvindo la soluci??n como JSON.
    '''
    query_ref = db.collection("viajes")
    nombre = ""
    valor = None
    for i in parametros:
        if (i[0] == "nombre"):
            nombre = i[1]
        elif(i[0] == "libres"):
            query_ref = query_ref.where(i[0], '>=', int(i[1]))  
        elif(i[0] == "precio"):
            valor = float(i[1])
        else:
            query_ref = query_ref.where(i[0], '==', i[1])

    d = []

    for i in query_ref.stream():
        resp = i.to_dict()
        if( (nombre.lower() in resp["nombre"].lower()) and (valor==None or resp["precio"]<=valor)):
            for key, value in resp.items(): resp.update({key : stringify(value)})
            resp.update({"id":i.id})
            d.append(resp)

    return jsonify(d)
    

@app.route("/usuarios", methods = ['GET', 'POST'])
def conseguir_subir_usuarios():
    """
    GET: Devuelve los usuarios cuyo atributo coincide con el primer argumento pasado (nombre o ubicaci??n).
    POST: Recibe un JSON y crea un documento en la BD con dichos datos.
    """
    
    if request.method == 'GET':
        #Cogemos el primer par de items, si no hay asignamos None
        item = next(request.args.items(),None)

        #Comprobamos si es None, si no lo es miramos que est?? en los atributos v??lidos
        #En caso de estarlo hacemos una request de ese item.
        if item == None:

            return getCollection("usuarios")

        elif (item[0] in validAttributesUsuarios):
            return makeSimpleQuery("usuarios",item[0],item[1])
        else:
            return "Atributo no v??lido"
        
    elif request.method == 'POST':

        aux = datetime.now()

        content = {
            'descripcion' : request.json['descripcion'],
            'edad' : request.json['edad'],
            'fecha' : aux,
            'nombre' : request.json['nombre'],
            'ubicacion' : request.json['ubicacion'],
            'imagen' : "https://res.cloudinary.com/dugtth6er/image/upload/v1639832477/perfil_hont25.png"
        }

        db.collection('usuarios').document().set(content)
        return jsonify(content)
    else:
        return("400: BAD REQUEST.")


@app.route("/usuarios/<id>/foto", methods = ['PUT','DELETE'])
def actualizar_borrar_imagen(id):
    """
    PUT: Actualiza la foto de perfil de un usuario.
    DELETE: Elimina la foto de perfil de un usuario y vuelve a establecer la de por defecto.
    """
    if request.method == 'PUT':

        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],id)):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")

        res = cloudinary.uploader.upload(request.files["file"])
        #Obtener otros datos del for
        id = request.form["id"]
        db.collection('usuarios').document(id).update({"imagen":res["url"]})
        return jsonify(res["url"])
    elif request.method == 'DELETE':

        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],id)):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")
        
        url = "https://res.cloudinary.com/dugtth6er/image/upload/v1639832477/perfil_hont25.png"
        db.collection('usuarios').document(id).update({"imagen":url})
        return jsonify(url)


@app.route("/usuarios/<id>", methods = ['GET','PUT','DELETE'])
def conseguir_actualizar_eliminar_usuarios(id):
    """
    GET: Devuelve un usuario a partir del id pasado.
    PUT: Actualiza el usuario del id pasado con los parametros del JSON recibido.
    DELETE: Borra un usuario a partir de su id.
    """
    if request.method == 'GET':
        doc = db.collection('usuarios').document(str(id)).get()
        if(doc.exists):
            res = jsonify(doc.to_dict())
        else:
            res =  "", 204

        return res

    elif request.method == 'PUT':

        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],id)):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")

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

        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],id)):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")

        usu = db.collection('usuarios').document(str(id))
        
        for i in usu.collection('viajes').stream():
            id = i.id
            ref = db.collection('viajes').document(id)
            viaje = ref.get().to_dict()
            l = viaje["libres"] + i.to_dict().get("reservadas",0)
            ref.update({'libres': l})
            usu.collection('viajes').document(id).delete()

        usu.delete()

        auth.delete_user(id)
        
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


@app.route("/usuarios/<id>/reservados", methods = ['GET'])
def conseguir_viajes_reservados_de_usuario(id):
    """
    GET: Devuelve los viajes del usuario(No Conductor) que ha reservado.
    """
    if request.method == 'GET':
        viajes = db.collection('usuarios').document(str(id)).collection('viajes').where('reservadas', '>', 0)
        lista = [] 
        for i in viajes.stream():
            dic = db.collection('viajes').document(str(i.id)).get().to_dict()
            dic["coordOrigen"] = stringify(dic["coordOrigen"])
            dic["coordDestino"] = stringify(dic["coordDestino"])
            res =  i.to_dict().get("reservadas",0)
            dic.update({'id': i.id, 'reservadas': res})
            lista.append(dic)
        return jsonify(lista)

    else:
        return "400: BAD REQUEST."


@app.route("/usuarios/<idUsuario>/reservas/<idViaje>", methods = ['PUT','DELETE'])
def crear_reserva_de_usuario(idUsuario, idViaje):
    """
    PUT: Actualiza la cantidad de plazas reservadas por un usuario (si no existia reserva, la crea)
    DELETE: Borra las reservas que habia realizado un usuario en un viaje
    """
    print("Prueba de verificaci??n:")
    try:

        if(not verificarUsuario(request.headers["Authorization"],idUsuario)):
            print("Usuario incorrecto")

            return "Usuario incorrecto", 401

    except Exception as e:
        print(e) 
        return str(e), 401

    print("Verificado")

    if request.method == 'PUT':
        
        viaje_ref = db.collection('viajes').document(str(idViaje))
        viaje = viaje_ref.get().to_dict()
        reservadas = request.json['reservadas']
        libres = viaje["libres"]
        if libres >= reservadas :
            viajeUsuario_ref = db.collection('usuarios').document(str(idUsuario)).collection('viajes').document(str(idViaje))
            viajeUsuario = viajeUsuario_ref.get()

            contentViaje = {
                'libres' : libres - reservadas
            }
            
            viaje_ref.update(contentViaje)

            if viajeUsuario.exists:
                reservadas += viajeUsuario.to_dict().get("reservadas",0)

            content = {
                'esConductor' : idUsuario == viaje["idConductor"],
                'nombre' : viaje["nombre"],
                'reservadas' : reservadas
            }

            viajeUsuario_ref.set(content)

            return jsonify(content)

        else:
            return "412: : PRECONDITION FAILED"

    elif request.method == 'DELETE':
        
        viaje_ref = db.collection('viajes').document(str(idViaje))
        viaje = viaje_ref.get().to_dict()
        libres = viaje["libres"]
        
        viajeUsuario_ref = db.collection('usuarios').document(str(idUsuario)).collection('viajes').document(str(idViaje))
        viajeUsuario = viajeUsuario_ref.get()

        if viajeUsuario.exists:

            aux = viajeUsuario.to_dict()

            if not(aux["esConductor"]):
                viajeUsuario_ref.delete()
            else:
                viajeUsuario_ref.update({"reservadas":0})

            li = libres + aux.get("reservadas",0)

            contentViaje = {
                'libres' : li
            }
            viaje_ref.update(contentViaje)
            return "200. Borrado exitoso"

        else:
            return "404: DATA NOT FOUND."
            
    else:
        return "400: BAD REQUEST."


@app.route("/viajes", methods = ['GET', 'POST'])
def conseguir_subir_viajes():
    """
    GET: Devuelve los viajes cuyos atributos coinciden con los atributos v??lidos pasados ['nombre','origen','destino','libres','precio'].
    POST: Recibe un JSON y crea un documento en la BD con dichos datos.
    """

    if request.method == 'GET':
        #Cogemos el primer par de items, si no hay asignamos None
        items = [i for i in request.args.items() if i[0] in validAttributesViajes ]
        
        if len(items)==len(request.args):
            #return fromCollectionToJson(db.collection('viajes'))
            return makeViajesQuery(items)
        else:
            return "Alg??n atributo no es v??lido"
            
    elif request.method == 'POST': 

        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],request.json['idConductor'])):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")

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
        origen = address.get('city', address.get('state',  address.get('country', '')))

        
        location = geolocator.reverse(str(latDest)+","+str(longDest))
        address = location.raw['address']
        destino = address.get('city', address.get('state', address.get('country', '')))

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
        
        mensaje = "Uno de nuestros usuarios os trae la oportunidad de embarcaros en '"+request.json['nombre']+"'.\r"
        mensaje = mensaje + "Este viaje sale de "+origen+" direcci??n "+destino+" a las "+request.json['hora']+".\r"
        mensaje = mensaje + "Cada plaza vale "+str(request.json['precio'])+" euros y quedan "+str(request.json['libres'])+" plazas libres.\r??No te lo pierdas!"

        t_api.update_status(mensaje)


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

        dict = viaje.get().to_dict()

        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],dict["idConductor"])):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")
        
        viaje.update(content)
        
        dict = viaje.get().to_dict()
        dict["coordOrigen"] = stringify(dict["coordOrigen"])
        dict["coordDestino"] = stringify(dict["coordDestino"])
        
        return jsonify(dict)
        
    elif request.method == 'DELETE':
        dict = db.collection('viajes').document(str(id)).get().to_dict()
        idCond = dict['idConductor']
        
        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],idCond)):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")

        for i in db.collection("usuarios").stream():
            db.collection('usuarios').document(i.id).collection('viajes').document(str(id)).delete()
        
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
        #Actualizamos los datos si es necesario(En el interior de la funci??n se controla cuando actualizarlos).
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
            return data.to_json(orient='records')
            
        elif("provincia" in keys):
            data = preciosGasolina.copy()
            q = 'Provincia == "'+request.args["provincia"]+'"'
            data.query(q, inplace = True)
            return data.to_json(orient='records')
            
        else:
            return "Alg??n atributo no es v??lido"
    else:
        return("400: BAD REQUEST.")
     
@app.route("/covid", methods =['GET'])
def conseguir_datos_covid():
    """
    GET: Obtiene la cantidad de ingresos por covid en las ??ltimas 15 entradas en una provincia por su nombre.
    """
    if request.method == 'GET':
        #Actualizamos los datos si es necesario(En el interior de la funci??n se controla cuando actualizarlos).
        downloadCSV(URL_DATOS_COVID)
        keys = [i[0] for i in request.args.items()]
        global covid
        data = covid.copy()
        if("provincia" in keys):

            q ='Provincia == "' + request.args["provincia"]+'"'

            data.query(q, inplace=True)
            data["Fecha"] = [datetime.strptime(i, '%d/%m/%Y') for i in data["Fecha"]]
            data.sort_values(by = 'Fecha', ascending = False, inplace = True)
            data = data.groupby(["Fecha","Provincia"]).sum()
            data = data.head(1)
            data = data.reset_index(drop=False)
            data["Fecha"] = [datetime.strftime(i,'%d/%m/%Y') for i in data["Fecha"]]
            return data.to_json(orient='records')
        else:
            return "Alg??n atributo no es v??lido"
    else:
        return "400: BAD REQUEST."

@app.route("/mensajes", methods = ["GET","POST"])
def conseguir_subir_mensajes():
    """
    GET: Devuelve los mensajes entre un usuario creador y un usuario destino.
    POST: Recibe un JSON y crea un mensaje en la BD con dichos datos.
    """

    

    if request.method == 'GET':

        items = [i for i in request.args.items() if i[0] in ["destino","creador"] ]
       
        if len(items) == 2:

            creador =  request.args["creador"]
            destino =  request.args["destino"]
            
            print("Prueba de verificaci??n:")
            try:

                if(not verificarUsuario(request.headers["Authorization"],creador)):
                    print("Usuario incorrecto")

                    return "Usuario incorrecto", 401

            except Exception as e:
                print(e) 
                return str(e), 401

            print("Verificado")

            ref1 = db.collection("mensajes").where("creador","==",creador)
            ref1 = ref1.where("destino","==",destino)
            ref1 = ref1.order_by('fecha', direction=firestore.Query.ASCENDING)

            ref2 = db.collection("mensajes").where("creador","==",destino)
            ref2 = ref2.where("destino","==",creador)
            ref2 = ref2.order_by('fecha', direction=firestore.Query.ASCENDING)

            d = []

            for i in ref1.stream():
                resp = i.to_dict()
                
                for key, value in resp.items(): resp.update({key : stringify(value)})
                resp.update({"id":i.id})
                d.append(resp)

            for i in ref2.stream():
                resp = i.to_dict()
                
                for key, value in resp.items(): resp.update({key : stringify(value)})
                resp.update({"id":i.id})
                d.append(resp)
            
            d.sort(key=getDate)
            
            return jsonify(d)
        else:
            return "Alg??n atributo no es v??lido"
        
    elif request.method == 'POST':
                
        print("Prueba de verificaci??n:")
        try:

            if(not verificarUsuario(request.headers["Authorization"],request.json['creador'])):
                print("Usuario incorrecto")

                return "Usuario incorrecto", 401

        except Exception as e:
            print(e) 
            return str(e), 401

        print("Verificado")

        aux = datetime.now()
        
        content = {
            'creador' : request.json['creador'],
            'destino' : request.json['destino'],
            'fecha' : aux,
            'contenido' : request.json['contenido']
        }

        db.collection('mensajes').document().set(content)
        return jsonify(content)

    else:
        return "400: BAD REQUEST."

def getDate(e):
  return e['fecha']
