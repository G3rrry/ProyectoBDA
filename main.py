import mysql.connector
import pymongo
from bson.decimal128 import Decimal128
import decimal
import neo4j

# Configuración para la conexión a MySQL
config_mysql = {
    'host': 'localhost',
    'user': 'archgerry',
    'password': 'ArchGerry1312',
    'database': 'world',
}

# Conexión a MongoDB
cliente_mongo = pymongo.MongoClient("mongodb://localhost:27017/")
db_mongo = cliente_mongo["world_mongo"]
country_collection = db_mongo['country']


def convertir_valor(valor):
    if isinstance(valor, decimal.Decimal):
        return Decimal128(valor)
    elif isinstance(valor, Decimal128):
        return float(valor.to_decimal())
    return valor


# Función para procesar la tabla country y transferir a MongoDB
def procesar_countries(cursor):
    cursor.execute("SELECT * FROM country")
    countries = cursor.fetchall()

    for country in countries:
        country_id = country.pop('Code')
        country['_id'] = country_id

        # Convertir los valores decimales a Decimal128
        for key, value in country.items():
            country[key] = convertir_valor(value)

        # Obtener lenguajes, clasificarlos y crear subdocumentos sin 'IsOfficial'
        cursor.execute("SELECT * FROM countrylanguage WHERE CountryCode = %s", (country_id,))
        languages = cursor.fetchall()
        official_languages = []
        non_official_languages = []
        for language in languages:
            lang_doc = {
                'Language': language['Language'],
                'Percentage': convertir_valor(language['Percentage'])
            }
            if language['IsOfficial'] == 'T':
                official_languages.append(lang_doc)
            else:
                non_official_languages.append(lang_doc)

        country['OfficialLanguages'] = official_languages
        country['NonOfficialLanguages'] = non_official_languages

        # Insertar o actualizar el documento en MongoDB
        db_mongo.country.replace_one({'_id': country['_id']}, country, upsert=True)

# Función para procesar la tabla city y transferir a MongoDB
def procesar_cities(cursor):
    cursor.execute("SELECT * FROM city")
    cities = cursor.fetchall()

    for city in cities:
        city_id = city.pop('ID')
        city['_id'] = city_id

        # Convertir los valores decimales a Decimal128 y limpiar los datos
        city_data = {k: convertir_valor(v) for k, v in city.items()}

        # Insertar o actualizar el documento de city en MongoDB
        db_mongo.city.replace_one({'_id': city['_id']}, city_data, upsert=True)

def create_nodes(tx, label, data):
    for item in data:
        properties = {}
        for key, value in item.items():
            if isinstance(value, decimal.Decimal):
                properties[key] = convertir_valor(value)
            else:
                properties[key] = value
        query = (
            f"CREATE (n:{label} $properties)"
            " RETURN n"
        )
        tx.run(query, properties=properties)

def extract_languages():
    # "Extrae todos los idiomas únicos de los documentos country en MongoDB
    unique_languages = country_collection.aggregate([
        {"$project": {"allLanguages": {"$setUnion": ["$OfficialLanguages", "$NonOfficialLanguages"]}}},
        {"$unwind": "$allLanguages"},
        {"$group": {"_id": None, "uniqueLanguages": {"$addToSet": "$allLanguages.Language"}}}
    ])
    if unique_languages.alive:
        return [lang for lang in list(unique_languages)[0]['uniqueLanguages'] if lang]
    return []


def create_nodes_and_relationships():
    languages = extract_languages()
    with driver.session() as session:
        for language in languages:
            session.run("MERGE (:Language {name: $language})", language=language)

    language_data = country_collection.aggregate([
        {
            "$project": {
                "co_code": "$_id",  # Usar _id como código del país
                "Name": 1,
                "languages": {
                    "$concatArrays": [
                        {
                            "$map": {
                                "input": "$OfficialLanguages",
                                "as": "lang",
                                "in": {
                                    "Language": "$$lang.Language",
                                    "Percentage": "$$lang.Percentage",
                                    "Official": True
                                }
                            }
                        },
                        {
                            "$map": {
                                "input": "$NonOfficialLanguages",
                                "as": "lang",
                                "in": {
                                    "Language": "$$lang.Language",
                                    "Percentage": "$$lang.Percentage",
                                    "Official": False
                                }
                            }
                        }
                    ]
                }
            }
        },
        {"$unwind": "$languages"},
        {"$group": {
            "_id": "$co_code",
            "countryName": {"$first": "$Name"},
            "languages": {"$addToSet": "$languages"}
        }}
    ])

    with driver.session() as session:
        for data in language_data:
            country_code = data['_id']
            country_name = data['countryName']
            session.run("MERGE (c:Country {code: $code, name: $name})", code=country_code, name=country_name)
            for lang in data['languages']:
                language = lang['Language']
                percentage = convertir_valor(lang['Percentage'])
                official_status = lang['Official']
                relation_type = "SPEAKS_OFFICIALLY" if official_status else "SPEAKS_NON_OFFICIALLY"
                session.run(f"""
                MATCH (c:Country {{code: $code}}), (l:Language {{name: $language}})
                MERGE (c)-[r:{relation_type} {{percentage: $percentage}}]->(l)
                ON CREATE SET r.percentage = $percentage
                ON MATCH SET r.percentage = $percentage
                """, code=country_code, language=language, percentage=percentage)


def city_nodes(cities):
    with driver.session() as session:
        session.run("MATCH (c:City) DETACH DELETE c")
        session.run("MATCH (d:District) DETACH DELETE d")
        for city in cities:
            session.run(
                "CREATE (c:City {name: $name, id: $id, population: $population}) "
                "MERGE (d:District {name: $district, country: $co_code})"
                "MERGE (c)-[:FROM_DISTRICT]->(d)",
                id=city['_id'],
                name=city['Name'],
                co_code=city['CountryCode'],
                population=city['Population'],
                district=city['District']
            )


def country_nodes(countries):
    with driver.session() as session:
        for country in countries:
            session.run(
                "CREATE (c:Country {name: $name, code: $co_code, population: $population, surface_area: $surface_area,"
                "local_name: $local_name, code2: $code2, head_state: $head_state, life_expectancy: $life_expectancy, "
                "gnp: $gnp, gnp_old: $gnp_old, independence_year: $indep_year}) "
                "MERGE (g:Government {name: $form})"
                "MERGE (c)-[:FORM_OF_GOVERNMENT]->(g)"
                "MERGE (r:Region {name: $region})"
                "MERGE (c)-[:FROM_REGION]->(r)"
                "MERGE (cont:Continent {name: $continent})"
                "MERGE (r)-[:FROM_CONTINENT]->(cont)",
                name=country["Name"],
                co_code=country["_id"],
                population=country["Population"],
                surface_area=convertir_valor(country["SurfaceArea"]),
                local_name=country["LocalName"],
                code2=country["Code2"],
                form=country["GovernmentForm"],
                head_state=country["HeadOfState"],
                life_expectancy=convertir_valor(country["LifeExpectancy"]),
                gnp=convertir_valor(country["GNP"]),
                gnp_old=convertir_valor(country["GNPOld"]),
                indep_year=country["IndepYear"],
                region=country["Region"],
                continent=country["Continent"]
            )

def country_to_district():
    with driver.session() as session:
        session.run(
            "MATCH (c:Country), (d:District) "
            "WHERE c.code = d.country "
            "MERGE (d)-[:BELONGS_TO]->(c)",
        )

print("ATENCION: neo4j tiene que estar abierto antes de correr este programa para exportar de manera exitosa")

try:
    # Conexión a MongoDB
    cliente_mongo = pymongo.MongoClient("mongodb://localhost:27017/")
    db_mongo = cliente_mongo["world_mongo"]
    country_collection = db_mongo['country']

    # neo4j
    driver = neo4j.GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", 'neo4j123!'))

    # Establecer la conexión a MySQL
    conexion_mysql = mysql.connector.connect(**config_mysql)
    cursor = conexion_mysql.cursor(dictionary=True)
    print("Conexion a SQL exitosa")

    # Procesar la tabla de countries y sus lenguajes
    procesar_countries(cursor)
    print("Countries en Mongo procesadas")

    # Procesar la tabla de cities y transferir a su propia colección en MongoDB
    procesar_cities(cursor)
    print("Cities en Mongo procesadas")
    print('BD de Mongo Creada')

    # Empezando procesos con neoj4
    print('Limpiando Neo4j...')
    driver.session().run("MATCH (n) DETACH DELETE n")
    print('Neo4j Vacio')

    cursor = db_mongo.country.find()
    list_countries = [doc for doc in cursor]
    country_nodes(list_countries)
    print("Nodos de Paises y Otros Creados")

    cursor = db_mongo.city.find()
    list_cities = [doc for doc in cursor]
    city_nodes(list_cities)
    print("Nodos de Ciudades y Otros Creados")

    create_nodes_and_relationships()
    print("Nodos de Lenguajes Creados y Conectados")

    country_to_district()
    print("Nodos de Distrito Conectados a Pais")

    print("Neo4j Terminado")

    cursor.close()
    conexion_mysql.close()
    print("Conexión a MySQL cerrada.")
    cliente_mongo.close()
    print("Conexión a Mongo cerrada.")
    driver.close()
    print("Driver de Neo4j cerrado.")

except Exception as error:
    print("Error: ", error)

finally:
    print("Proceso Terminado")