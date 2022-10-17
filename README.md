#coneccion a la base de datos.
Hay que instalar lo que se muestra a continuacion e importar para poder usar sqlalchemy que nos ayuda con la coneccion a la base de datos. 
pip install sqlalchemy
pip install PyMySQL

Dentro del archivo db_connection.py se creó una funcion llamada connect a la cual se le debe pasar el nombre de la base de datos y nos retornará una sesión(Para la coneccion a la DB usa las variables de entorno del archivo properties).

#Properties
Para usar un archivo .properties se intala la siguiente libreria
pip install jproperties

luego dentro de la carpeta util
se crea un archivo propertiesConfig. py el cual nos ayudara a leer las propiedades.
dentro de este archivo existe un funcion la cual recibe un nombre y te regresa una propiedad.

#Librerias 
pandas se utiliza para leer y escribir archivos en db con python
pip install pandas