
#Este algoritmo sirve para extraer las bases de datos de la plataforma Sisovid (Sistema de Información sobre Víctimas de Desaparición)
#La data está dentro de un script que puede visualizarse con al inspeccionar el código html de la misma
#El gobierno de Jalisco no la tiene disponible para consulta accesible
#Los productos finales que se extraen son las bases de datos que alimentan las gráficas y mapas del Sisovid
#Con base en ellas, cada persona con acceso a las mismas, puede generar gráficos, análisis y demás.

#importamos las librerías
import requests #para llamar a la página
import re #para usar expresiones regulares
import json #para manipular archivos json
import pandas as pd #para los data frames


#definimos la función para la extracción
def extract_json(pattern, html_content):
    json_match = pattern.search(html_content) #comparamos y buscamos el pattern con el html extraido
    if json_match:
        json_data = json_match.group(1)
        json_data = json_data.replace("'", "\"") # Eliminamos caracteres no deseados
        return json.loads(json_data) #cargamos la data
    return None

#definimos la función para la extracción de la data específica
def get_df(data_json, specific_data): #cada elemento tiene dos json, por lo tanto, con esta función extraemos una a la vez
    if specific_data in data_json: 
        new_json = data_json[specific_data]
        df = pd.DataFrame(new_json) #creamos el df
    return df

#creamos el df final y lo exportamos en csv
def final_df(data_json, specific_data1, specific_data2, tipe_on):
    df1 = get_df(data_json, specific_data1) #extraemos el json 1 y generamos el df
    df2 = get_df(data_json, specific_data2) #extraemos el json 2 y generamos el df
    final_data = pd.merge(df1, df2, on=tipe_on, how="outer", suffixes=('_denuncia', '_reporte')) #uno los dos en uno
    return final_data


# URL de la página web
url = 'https://sisovid.jalisco.gob.mx/'

# Realizamos la solicitud GET para obtener el contenido HTML de la página
response = requests.get(url)
html_content = response.text #nos desacarga el html del sitio


# Definimos los patrones para cada objeto JSON, usamos las expresiones regulares (r'frase previa al json = (.*?);'. Extraerá todo entre la frase previa y un ;)
json_patterns = {
    "total_localizados": re.compile(r'let gpersonasLocalizadas = (.*?);', re.DOTALL),
    "mujeres_localizadas": re.compile(r'let gMujeres = (.*?);', re.DOTALL),
    "hombres_localizados": re.compile(r'let gHombres = (.*?);', re.DOTALL),
    "anio_denuncia": re.compile(r'LineaTemporalDenuncia = (.*?);', re.DOTALL),
    "anio_avistamiento": re.compile(r'gLineaTemporalAnio = (.*?);', re.DOTALL),
    "edad_desaparecidos": re.compile(r'gLineaTemporalEdad = (.*?);', re.DOTALL),
    "mujeres_municipio": re.compile(r'dataMapaMujeres = (.*?);', re.DOTALL),
    "hombres_municipio": re.compile(r'dataMapaHombres = (.*?);', re.DOTALL),
    "total_municipio": re.compile(r'dataMapaPersonas = (.*?);', re.DOTALL)    
}

# Definimos un diccionario para almacenar los datos extraídos
data_dict = {}

# Extraemos y parseamos cada objeto JSON usando las funciones definidas
for key, pattern in json_patterns.items():
    data = extract_json(pattern, html_content) #aquí usamos una de las funciones que creamos al inicio
    if data:
        data_dict[key] = data

#carpeta de destino de los df creados
ruta = 'sisovid/jul/' #cuando sea agosto, creamos la carpeta en la raiz con el nombre 'ago' y cambiamos jul por ago
ruta_destino_bd_limpia = f'{ruta}bd_limpias/'
ruta_destino_bd_sisovid = f'{ruta}bd_sisovid/'
#importamos los municipios
municipios = pd.read_csv('sisovid\municipios_jalisco.csv')
municipios.rename(columns={'nombre': 'municipio'}, inplace=True) #cambiamos de nombre a la columna
#print(municipios.info()) #se puede eliminar, lo usé para saber qué tipo de dato era la columna 'cvemun_char'

#Envía la data a variables y enviamos a array
personas_localizadas = data_dict.get("total_localizados", {}) #la data la extraemos del diccionario. Cada elemento tiene dos json que dividiremos con las funciones creadas antes
mujeres_localizadas= data_dict.get("mujeres_localizadas",{}) #son dos json porque en uno están las denuncias y en otro los reportes, pero almacenados en el mismo elemento
hombres_localizados= data_dict.get("hombres_localizados", {})
anio_denuncia= data_dict.get("anio_denuncia", {})
anio_avistamiento= data_dict.get("anio_avistamiento", {})
edad_desaparecidos= data_dict.get("edad_desaparecidos", {})
mujeres_municipio= data_dict.get("mujeres_municipio", {})
hombres_municipio= data_dict.get("hombres_municipio", {})
total_municipio= data_dict.get("total_municipio", {})

#Hacemos una lista con cada elemento para poderlos iterar
data_json= [personas_localizadas,mujeres_localizadas,hombres_localizados,anio_denuncia,
            anio_avistamiento,edad_desaparecidos, mujeres_municipio, hombres_municipio, total_municipio] 
            
#obtenemos los nombres de los json número 1 de cada elemento. El nombre está en el script original
specific_data_1 = ["personasLocalizadas", "mujeres", "hombres", "denuncia", 
                  "ultimoAvistamiento", "pendientesEdad", "mujeres", "hombres", "personas"]

#obtenermos los nombres de los json número 2 de cada elemento. El nombre está en el script original
specific_data_2 = ["personasLocalizadas_c", "mujeres_c", "hombres_c", "denuncia_c",
                  "ultimoAvistamiento_c", "pendientesEdad_c", "mujeres_c", "hombres_c", ""]

#enlistamos los parámetros para la combinación de las bases de datos. Las que corresponden a municipios las dejamos vacías, pues no tienen datos de reportes, sólo de denuncias
type_on = ["anio", "edad", "edad","anio", "anio", "edad", "", "", ""]

#enlistamos los títulos con los que guardaremos los archivos csv.
titles = ["personas_localizadas", "mujeres_localizadas", "hombres localizados",
          "anio_denuncia", "anio_avistamiento", "edad_desaparecidos", "mujeres_municipio",
          "hombres_municipio", "total_municipio"]

#iteramos la lista con los elementos json
i = 0
for item in data_json:
   
    if type_on[i] != "": #para comenzar a filtrar, si el type_on está vacío debe realizar otro proceso
        df_item = final_df(item, specific_data_1[i], specific_data_2[i], type_on[i]) #combinamos los df de denuncias y reportes
        if specific_data_1[i] == "mujeres": #Si las bases están divididas en hombres y mujeres, enviamos el csv a una carpeta distinta de las bases de datos limpias
           df_item.to_csv(f'{ruta_destino_bd_sisovid}bd_{titles[i]}.csv', index=False)
        elif specific_data_1[i] == "hombres":
            df_item.to_csv(f'{ruta_destino_bd_sisovid}bd_{titles[i]}.csv', index=False)
        else: #Si no están divididas por sexo, se va directo el csv a la carpeta de bases de datos limpias
           df_item.to_csv(f'{ruta_destino_bd_limpia}bd_{titles[i]}.csv', index=False)
            
    else:
        df_item = get_df(item, specific_data_1[i]) #si el type_on está vacío, hará este proceso 
        df_item["cvemun_char"] = pd.to_numeric(df_item["cvemun_char"], errors="coerce") #convertimos la columna a int, pues la extrae como string y da problemas al unir con la de municipios para tener los nombres y no sólo claves
        df_item = pd.merge(municipios, df_item, on="cvemun_char", how="outer") #unimos para tener el nombre del municipio, además de la clave
        df_item.to_csv(f'{ruta_destino_bd_sisovid}bd_{titles[i]}.csv', index=False) #mandamos los csv a otra carpeta, pues aún no son las finales para los objetivos que quiero
    i = i+1

#df hombres y mujeres localizados por edad
localizadas = pd.read_csv(f'{ruta_destino_bd_sisovid}bd_mujeres_localizadas.csv') #obtenemos las bd creadas antes y enviadas a las carpetas
localizados = pd.read_csv(f'{ruta_destino_bd_sisovid}bd_hombres localizados.csv')
hombres_mujeres_localizados = localizadas.merge(localizados, on="edad", how="outer") #las unimos
hombres_mujeres_localizados.to_csv(f'{ruta_destino_bd_limpia}bd_hombres_mujeres_localizados.csv', index=False) #creamos un nuevo csv directo a la carpeta de bd limpias

#df total de hombres y mujeres desaparecidos por municipio
mujeres_mun = pd.read_csv(f'{ruta_destino_bd_sisovid}bd_mujeres_municipio.csv')
hombres_mun = pd.read_csv(f'{ruta_destino_bd_sisovid}bd_hombres_municipio.csv')
total_mun = pd.read_csv(f'{ruta_destino_bd_sisovid}bd_total_municipio.csv')
tiempo_denuncia = pd.read_csv(f'{ruta_destino_bd_limpia}bd_anio_denuncia.csv') #esta la usamos para obtener los 'sin dato'
desaparecidos_municipios_prev = mujeres_mun.merge(hombres_mun, on=["cvemun_char", "municipio"], how="outer").merge(total_mun, on=["cvemun_char", "municipio"], how="outer") #unimos los tres df
mujeres_sin_dato = tiempo_denuncia['mujeres_denuncia'].sum() - desaparecidos_municipios_prev["mujeres"].sum() #encontramos el total de mujeres que no están incluidas en municipios
hombres_sin_dato = tiempo_denuncia['hombres_denuncia'].sum() - desaparecidos_municipios_prev["hombres"].sum()
total_sin_dato = mujeres_sin_dato + hombres_sin_dato 
#cremos los datos que se incluirán al df de mun, para especificar que esos no están incluidos en ninguno
sin_dato = [{"cvemun_char": None, 
              "municipio":"Sin dato", 
              "mujeres": mujeres_sin_dato, 
              "hombres": hombres_sin_dato, 
              "total": total_sin_dato}]
#convertimos el json en df
sin_dato_df = pd.DataFrame(sin_dato)
#concatenamos el 'sin_dato_df' con 'desaparecidos_municipios_prev', la agrega al final
desaparecidos_municipios = pd.concat([desaparecidos_municipios_prev, sin_dato_df])
desaparecidos_municipios.to_csv(f'{ruta_destino_bd_limpia}bd_desaparecidos_municipios.csv', index=False) #exportamos

#NOTAS
#La base de datos para municipios sólo contempla las denuncias. No tiene datos a nivel municipal de los reportes de cédulas
#Aún así, la suma total de los casos por municipio no concuerda con el total de las denuncias. En este ejercicio las catalogué como 'Sin dato'
#Este código sólo genera seis bases de datos que considero prioritarias para mi labor. Pero, pueden generarse bd individuales por cada json contenido en le Sisovid, sólo hay que descomentar la línea de código correspondeinte
#Aunque ya es un avance, aún queda a deber la base de datos

#Este código es de uso libre y se puede mejorar (mucho). Fue hecho por un principiante en python.