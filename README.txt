# Only if you have an older version:
rm -r venv/                 

#Create a new virtual enviroment:        
python -m venv venv/

#Select the new enviroment:
venv\Scripts\activate

#Install requirements:       
pip install -r requirements.txt

#Update requirements.txt:
pip freeze > requirements.txt

#When we’re done working on our project, we can exit the environment with:
deactivate