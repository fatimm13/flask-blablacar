
"""
EJEMPLO SENCILLO PARA AÑADIR DATOS A FIRESTORE

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

db.collection('usuario').add({'nombre':'Pepe', 'edad': 30 })
"""


import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from flask import Flask
app = Flask(__name__)
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

@app.route("/")
def home():
    return "Hello, Flask!"

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