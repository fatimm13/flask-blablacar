from datetime import date, datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1 import GeoPoint
import requests ##csv, operator, 
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify 
app = Flask(__name__)
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

preciosGasolina = None
covid = None
URL_PRECIO_GASOLINA = "https://geoportalgasolineras.es/resources/files/preciosEESS_es.xls"
URL_DATOS_COVID = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/Datos_Capacidad_Asistencial_Historico_19112021.csv"

@app.route("/")
def home():
    downloadXLS(URL_PRECIO_GASOLINA)
    return "Hello Flask"

def downloadCSV(csv_url):
    '''
        Recibe una URL y descarga los datos como CSV
    '''

    req = requests.get(csv_url)
    url_content = req.content
    csv_file = open('downloaded.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    
    global covid 
    covid = pd.read_csv(url_content)

    covid.columns =[column.replace(" ", "_") for column in covid.columns]


def downloadXLS(xls_url):
    '''
        Recibe una URL y descarga los datos como XLS
    '''

    req = requests.get(xls_url)
    url_content = req.content
    
    csv_file = open('downloaded.xls', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    global preciosGasolina 
    preciosGasolina = pd.read_excel(url_content, skiprows=np.arange(3))
    preciosGasolina.columns =[column.replace(" ", "_") for column in preciosGasolina.columns]
    preciosGasolina = preciosGasolina[["Provincia","Municipio","Localidad","Código_postal","Dirección","Longitud","Latitud","Precio_gasolina_95_E5","Precio_gasolina_98_E5","Precio_gasóleo_A","Rótulo","Horario"]]
    preciosGasolina["Longitud"] = [ float(i.replace(',', '.')) for i in preciosGasolina["Longitud"]]
    preciosGasolina["Latitud"] = [ float(i.replace(',', '.')) for i in preciosGasolina["Latitud"]]
    
##TODO el resultado de pd.readexcel tiene un query   

def getCollection(tabla):
    '''Función que pide una colección de la BD
    y la devuelve como JSON
    '''
    query_ref = db.collection(tabla)
    return fromCollectionToJson(query_ref)

def fromCollectionToJson(query_ref):
    '''Función que recibe una referencia de una colección
    y la devuelve como JSON
    '''
    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()
        for key, value in resp.items(): resp.update({key : stringify(value)})
        d.update({cont : resp})
        cont = cont+1
    return jsonify(d)

def stringify(value):
    '''
    str controlando tipos de la BD que no disponen del mismo
    '''

    string = ""
    if(isinstance(value,GeoPoint)):
        string = "["+str(value.latitude)+","+str(value.longitude)+"]"
    else:
        string = str(value)
    return string

numericos = ["edad","precio","plazas","libres"]
validAttributesUsuarios = ["nombre","ubicacion"]
validAttributesViajes = ["nombre","origen","destino","libres"]

def makeSimpleQuery(tabla, parametro, valor):
    '''Función que realiza una petición sobre una colección con 
    un único atributo y valor y devuelve la solución como JSON
    '''
    usuario_ref = db.collection(tabla)

    if(parametro in numericos):
        query_ref = usuario_ref.where(parametro, '==', int(valor))
    else:
        query_ref = usuario_ref.where(parametro, '==', valor)
    
    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()

        for key, value in resp.items(): resp.update({key : stringify(value)})

        d.update({cont : resp})
        cont = cont+1
    return jsonify(d)

###Función que realiza una petición sobre una tabla, una columna y un valor y devuelve la solución como JSON
def makeComplexQuery(tabla, parametros):
    '''Función que realiza una petición sobre una colección con 
    varios atributos y un único valor para cada uno y devuelve la solución como JSON
    '''
    query_ref = db.collection(tabla) 
    
    for i in parametros:
        if(i[0] in numericos):
            query_ref = query_ref.where(i[0], '==', int(i[1]))
        else:
            query_ref = query_ref.where(i[0], '==', i[1])

    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()
        
        for key, value in resp.items(): resp.update({key : stringify(value)})

        d.update({cont : resp})
        cont = cont+1

    return jsonify(d)

def makeViajesQuery(parametros):
    query_ref = db.collection("viajes") 
    for i in parametros:
        if(i[0] == "libres"):
            query_ref = query_ref.where(i[0], '>=', int(i[1]))
        else:
            query_ref = query_ref.where(i[0], '==', i[1])

    d = dict()
    cont = 0

    for i in query_ref.stream():
        resp = i.to_dict()
        
        for key, value in resp.items(): resp.update({key : stringify(value)})

        d.update({cont : resp})
        cont = cont+1

    return jsonify(d)
    

@app.route("/usuarios", methods = ['GET', 'POST'])
def conseguir_subir_usuarios():
    # Siguiendo el ejemplo de la página 44,
    # se debería hacer GET "/usuarios" para pillar todos los usuarios
    # y POST "/usuarios" para crear un usuario. Los datos para el post los
    # sacariamos de un form. He pasado un tutorial por el grupo de Discord.

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
        print("")

        aux = datetime.now()

        print(aux)
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

    #En vez de retornar un content tambien se puede devolver 
    # un html usando los metodos adecuados. 
    # Tutoriales guay en Discord de Flask con "Tech With Tim"
    #content = "<h1> Ejemplo </h1>"
    

@app.route("/usuarios/<id>", methods = ['GET','PUT','DELETE'])
def conseguir_actualizar_eliminar_usuarios(id):
    # Siguiendo el ejemplo de la página 44,
    # se debería hacer GET "/usuario/3" para pillar un usuario concreto
    # y PUT "/usuario/3" para actualizar un usuario, lo mismo para 
    # DELETE "/usuario/3" el cual borra un usuario. 

    #El profe hace todo esto en teoria para evitar el uso de 
    # verbos en las rutas, lo cual es simplemente un estandar,
    # sin embargo, lo que siempre se suele hacer es tener un metodo
    # delete_usuario, otro update_usuario y asi tener todo más atómico
    # y sencillo, pero bueno en principio él parece tenerlo de la forma 
    # que he comentado más arriba. No sé si quiere que usemos un estándar 
    # o que usemos el suyo, tengo dudas por la última práctica.

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
        ### TODO
        # Buscar documentacion sobre las queries
        
        viajes = db.collection("viajes")
        resul = viajes.where("idConductor","==",str(id))
        for i in resul.stream():
            viajes.document(str(i.id)).update({"nombreConductor":request.json['nombre']})
        
        
        return jsonify(usu.get().to_dict())
        
    elif request.method == 'DELETE':
        usu = db.collection('usuarios').document(str(id)).delete()
        return "200: Borrado exitoso."
    else:
        
        return "400: BAD REQUEST."

    #En vez de retornar un content tambien se puede devolver 
    # un html usando los metodos adecuados. 
    # Tutoriales guay en Discord de Flask con "Tech With Tim"

@app.route("/usuarios/<id>/viajesConductor", methods = ['GET'])
def conseguir_viajes_conductor(id):
    """
    Devuelve los viajes del conductor ordenados por horaDeSalida
    """

    if request.method == 'GET':
        viajes = db.collection('viajes').where('idConductor', '==', str(id)).order_by('horaDeSalida', direction=firestore.Query.ASCENDING)
        return fromCollectionToJson(viajes)
    else:
        return "400: BAD REQUEST."

@app.route("/viajes", methods = ['GET', 'POST'])
def conseguir_subir_viajes():
    # Siguiendo el ejemplo de la página 44,
    # se debería hacer GET "/viajes" para pillar todos los viajes
    # y POST "/viajes" para crear un viaje. Los datos para el post los
    # sacariamos de un form. He pasado un tutorial por el grupo de Discord.

    if request.method == 'GET':
        #Cogemos el primer par de items, si no hay asignamos None
        items = [i for i in request.args.items() if i[0] in validAttributesViajes ]
        
        if len(items)==len(request.args):
            
            return makeViajesQuery(items)
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
        
        content = {
            'coordOrigen' : coordOrig,
            'coordDestino' : coordDest,
            'horaDeSalida' : aux,
            'nombre' : request.json['nombre'],
            'destino' : request.json['destino'],
            'origen' : request.json['origen'],
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
    # Siguiendo el ejemplo de la página 44,
    # se debería hacer GET "/usuario/3" para pillar un usuario concreto
    # y PUT "/usuario/3" para actualizar un usuario, lo mismo para 
    # DELETE "/usuario/3" el cual borra un usuario. 

    #El profe hace todo esto en teoria para evitar el uso de 
    # verbos en las rutas, lo cual es simplemente un estandar,
    # sin embargo, lo que siempre se suele hacer es tener un metodo
    # delete_usuario, otro update_usuario y asi tener todo más atómico
    # y sencillo, pero bueno en principio él parece tenerlo de la forma 
    # que he comentado más arriba. No sé si quiere que usemos un estándar 
    # o que usemos el suyo, tengo dudas por la última práctica.

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
        #TODO Borrar donde es pasajero ( Se hará cuando tengamos forma de registrar pasajeros)

        viaje = db.collection('viajes').document(str(id)).delete()
        return "200: Borrado exitoso."
    else:
        
        return "400: BAD REQUEST."

    #En vez de retornar un content tambien se puede devolver 
    # un html usando los metodos adecuados. 
    # Tutoriales guay en Discord de Flask con "Tech With Tim"

@app.route("/viajes/<id>/conductor", methods = ['GET'])
def conseguir_conductor_viaje(id):
    
    if request.method == 'GET':
        viaje = db.collection('viajes').document(str(id)).get().to_dict()
        idConductor = viaje['idConductor']
        conductor = db.collection('usuarios').document(idConductor).get().to_dict()
        return jsonify(conductor)
    else:
        return "400: BAD REQUEST."

validAttributesGasolinera = ["latitud","longitud","provincia"]
@app.route("/gasolinera", methods = ['GET'])
def conseguir_gasolinera():
    if request.method == 'GET':
        items = [i for i in request.args.items() if i[0] in validAttributesGasolinera ]
        keys = [i[0] for i in request.args.items()]
        global preciosGasolina
        if ("latitud" in keys and "longitud" in keys):
            
            data = preciosGasolina.copy()
            lat = float(request.args["latitud"])
            long = float(request.args["longitud"])
            radio = 0.1
            q = 'Latitud >= '+str(lat-radio)+' and '+'Latitud <= '+str(lat+radio)+' and ' + 'Longitud >= '+str(long-radio)+' and '+'Longitud <= '+str(long+radio)

            #data = data[data.Latitud >= (lat-radio) and data.Latitud <= (lat+radio) and data.Longitud >= (long-radio) and data.Longitud <= (long+radio) ]
            data.query(q, inplace = True)
            return data.to_json()
            
        elif("provincia" in keys):
            data = preciosGasolina.copy()
            q = 'Provincia == "'+request.args["provincia"]+'"'
            data.query(q, inplace = True)
            return jsonify(data.to_dict())
            
        else:
            return "Algún atributo no es válido"
    else:
        return("400: BAD REQUEST.")

validAttributesCovid = ["provincia"]        
@app.route("/covid", methods =['GET'])
def conseguir_datos_covid():
    if request.method == 'GET':
        items = [i for i in request.args.items() if i[0] in validAttributesCovid ]
        keys = [i[0] for i in request.args.items()]
        global covid
        data = covid.copy()
        if("provincia" in keys):
            q ='Provincia ==' + items["provincia"]
            data.query(q, inplace=True)
            return jsonify(data.to_dict())
        else:
            return "Algún atributo no es válido"
    else:
        return "400: BAD REQUEST."

        
        
"""
#Pruebas by Pablo

#EJEMPLO SENCILLO PARA AÑADIR DATOS A FIRESTORE

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

pers1 = {
    'nombre' : 'Manolo',
    'edad' : 21
}
pers2 = {
    'nombre' : 'Mariana',
    'edad' : 24
}
valoracion1 = {
    'texto' : 'conduce to bien',
    'puntuacion' : 4
}
valoracion2 = {
    'texto' : 'conduce to mal',
    'puntuacion' : 1
}
valoracion3 = {
    'texto' : 'conduce to regular',
    'puntuacion' : 2
}

db.collection('personas').document('new-persona-id').set(pers1)
db.collection('personas').document('new-persona-id').collection('valoraciones').document('val1').set(valoracion1)

"lo mejor es guardarse las colecciones para hacer los doc luego"
personas = db.collection('personas')
valManolo = db.collection('personas').document('new-persona-id').collection('valoraciones')

personas.document('mariana-id').set(pers2)
valManolo.document('val2').set(valoracion2)

valMariana = db.collection('personas').document('mariana-id').collection('valoraciones')
valMariana.document('val3').set(valoracion3)

valMas2 = db.collection_group('valoraciones').where('puntuacion', '>=', 2)
docs = valMas2.stream()
for doc in docs:
    print(f'{doc.id} => {doc.to_dict()}')

# Ejemplo de un servicio web para pillar datos de un usuario
# en base a su nombre, hacer una query a la BD, procesar la 
# info devuelta y mostrarla en el navegador :) 

@app.route("/usuario/<nombre>")
def hello_there(nombre):
    
    # Create a reference to the cities collection
    usuario_ref = db.collection('usuario')

    # Create a query against the collection
    query_ref = usuario_ref.where('nombre', '==', nombre)

    content = "<h1> Tengo "

    for i in query_ref.stream():
        content = content + f"{i.to_dict()['edad']}"
    
    content = content + " años </h1>"
 
    return content
"""





    



""" 
EJEMPLO PARA LEER DATOS DE UN USUARIO CONCRETO SABIENDO SU ID DEL DOCUMENTO EN EL QUE ESTÁ.
EL DOCUMENTO SERÍA COMO LA PÁGINA/FILA DE UNA TABLA DONDE ESTA GUARDADA LA INFO DE UN USUARIO
(se entiende mejor viendo Firestore)

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

doc_ref = db.collection(u'usuario').document(u'FLtfVhWk26Beqtjpq3Bu')

doc = doc_ref.get()

if doc.exists:
    print(f'Document data: {doc.to_dict()}')
else:
    print(u'No such document!')

"""