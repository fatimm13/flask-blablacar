from datetime import date, datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from flask import Flask, request, jsonify 
app = Flask(__name__)
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

@app.route("/")
def home():
    return "Hello, Flask!"

def getCollection(tabla):
    query_ref = db.collection(tabla)
    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()
        d.update({cont : resp})
        cont = cont+1
    return jsonify(d)

###Función que realiza una petición sobre una tabla, una columna y un valor y devuelve la solución como JSON
def makeSimpleQuery(tabla, parametro, valor):

    usuario_ref = db.collection(tabla)
    query_ref = usuario_ref.where(parametro, '==', valor)
    
    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()
        d.update({cont : resp})
        cont = cont+1
    return jsonify(d)

validAttributesUsuarios = ["nombre","ubicacion"]

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
        return("400: BAD RESQUEST.")

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
        usu.update(content)
        return jsonify(usu.get().to_dict())
        
    elif request.method == 'DELETE':
        usu = db.collection('usuarios').document(str(id)).delete()
        return "200: Borrado exitoso."
    else:
        
        return "400: BAD RESQUEST."

    #En vez de retornar un content tambien se puede devolver 
    # un html usando los metodos adecuados. 
    # Tutoriales guay en Discord de Flask con "Tech With Tim"

###Función que realiza una petición sobre una tabla, una columna y un valor y devuelve la solución como JSON
def makeComplexQuery(tabla, parametros):

    query_ref = db.collection(tabla) 
    
    for i in parametros:
        query_ref.where(i[0], '==', i[1])

    d = dict()
    cont = 0
    for i in query_ref.stream():
        resp = i.to_dict()

        aux = dict()
        for key, value in resp.items():
            string = ""

            if isinstance('GeoPoint', type(value)):

                string = "["+str(value.latitude)+","+str(value.longitude)+"]"

            else:
                string = str(value)
                
        aux = {key: string }

        d.update({cont : aux})
        cont = cont+1

    return jsonify(d)

validAttributesViajes = ["nombre","origen","destino","libres"]
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
            return makeComplexQuery("viajes",items)
        else:
            return "Algún atributo no es válido"
        #Comprobamos si es None, si no lo es miramos que esté en los atributos válidos
        #En caso de estarlo hacemos una request de ese item.
        
        
        
        
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
        return("400: BAD RESQUEST.")

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