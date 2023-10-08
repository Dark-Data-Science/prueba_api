from fastapi import FastAPI,Query
import pandas as pd
from fastapi.responses import JSONResponse




app = FastAPI()

data_reviews = pd.read_parquet("funcion_countreviews.parquet")

# Definir la función countreviews
def countreviews(start_date, end_date, data_reviews):
    try:
        # Convertir las fechas en formato YYYY-MM-DD al tipo de dato de fecha
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Validar que la fecha inicial sea menor que la fecha final
        if start_date >= end_date:
            return "Error: La fecha inicial debe ser menor que la fecha final."
        
        # Filtrar el DataFrame por la condición de fechas y contar usuarios únicos
        filtered_reviews = data_reviews[
            (data_reviews['posted'] >= start_date) &
            (data_reviews['posted'] <= end_date)
        ]
        num_users = filtered_reviews['user_id'].nunique()
        
        porcentaje = (num_users / (data_reviews['user_id'].nunique())) * 100
        return f"En el rango de fechas {start_date} a {end_date}:\n{num_users} usuarios con diferentes posteos. Porcentaje de recomendación {porcentaje:.2f} %"

    except Exception as e:
        return f"Ocurrió un error: {e}"
    

# Crear una ruta con el método GET
@app.get("/countreviews/")
async def get_countreviews(start_date: str = Query(...), end_date: str = Query(...)):
    result = countreviews(start_date, end_date, data_reviews)  # Reemplaza data_reviews con tu DataFrame real
    return {"result": result}


data = pd.read_parquet("def_userdata.parquet")
data4 = pd.read_parquet("funcion_UserData2.parquet")

def cantidad_gastada(userid):
    df = data
    df2 = df[df["user_id"] == userid]
    cantidad = sum(df2["price"].astype(float))
    return float(cantidad)

def cantidad_items(userid):
    df = data
    df3 = df[df["user_id"] == userid]
    
    if df3.empty:
        # No hay elementos para el usuario dado
        return 0
    
    items = df3["items_count"].iloc[0]
    return int(items)


def porcentaje_recomendacion(userid):
    df4 = data4
    df4 = df4[df4["user_id"] == userid]
    falsos = (df4['recommend'] == False).sum()
    verdaderos = (df4['recommend'] == True).sum()
    totales = falsos + verdaderos

    if verdaderos > 0:
        recomendacion = (verdaderos / totales) * 100
    else:
        recomendacion = 0

    return round(recomendacion, 2)

def userdata(userid):
    userid = userid
    cantidad = round(cantidad_gastada(userid), 2)
    items = cantidad_items(userid)
    recomendacion = porcentaje_recomendacion(userid)
    
    return {"cantidad": cantidad, "items": items, "recomendacion": recomendacion}

@app.get("/userdata/{userid}")
async def get_user_data(userid: str):
    result = userdata(userid)
    return result


df_combined = pd.read_parquet("def_genre_Ranking.parquet")

@app.get("/genre/{genero}")
async def genre(genero: str):
    try:
        # Busca el ranking para el género de interés
        rank = df_combined[df_combined['genres'] == genero]['ranking'].iloc[0]
        rank = str(rank)
        return {'El genero se encuentra en el Ranking numero': rank}
    except IndexError:
        return {'message': f"El género '{genero}' no se encuentra en el DataFrame."}@app.get("/genre/{genero}")
    

df_forgenre = pd.read_parquet("def_genres_top_5.parquet")    

def userforgenre(genero: str):
    try:
        # Filtra el DataFrame para obtener solo las filas correspondientes al género dado
        genre_df = df_forgenre[df_forgenre['genres'].str.contains(genero, case=False, na=False)]

        if genre_df.empty:
            return {'message': f"No se encontraron registros para el género '{genero}'."}

        # Ordena el DataFrame por playtime_forever en orden descendente y toma los primeros 5 registros
        top_users = genre_df.sort_values(by='playtime_forever', ascending=False).head(5)

        # Selecciona las columnas relevantes y convierte el resultado en un diccionario
        result = top_users[['user_id', 'user_url', 'playtime_forever']].to_dict(orient='records')

        return result
    except Exception as e:
        return {'error': str(e)}
    

df_developer = pd.read_parquet("def_developer.parquet")

@app.get("/developer/{desarrollador}")
async def developer(desarrollador: str):
    # Filtrar el DataFrame por el desarrollador de interés
    developer_df = df_developer[df_developer['developer'] == desarrollador]

    if developer_df.empty:
        return {"mensaje": f"No se encontraron datos para el desarrollador {desarrollador}"}

    # Calcular la cantidad de items por año
    cantidad_por_año = developer_df.groupby('release_year')['item_name'].count()

    # Calcular la cantidad de elementos gratis por año
    elementos_gratis_por_año = developer_df[developer_df['es_gratuito'] == True].groupby('release_year')['item_name'].count()

    # Calcular el porcentaje de elementos gratis por año
    porcentaje_gratis_por_año = (elementos_gratis_por_año / cantidad_por_año * 100).fillna(0).astype(int)

    result = {
        'cantidad_por_año': cantidad_por_año.to_dict(),
        'porcentaje_gratis_por_año': porcentaje_gratis_por_año.to_dict()
    }
    
    return result


df_sentiment =  pd.read_parquet("def_Sentimiento.parquet")

@app.get("/sentiment_analysis/{empresa_desarrolladora}")
async def sentiment_analysis(empresa_desarrolladora: str):
    # Filtrar el DataFrame por la empresa desarrolladora
    data_por_desarrolladora = df_sentiment[df_sentiment['developer'].apply(lambda x: empresa_desarrolladora in x)]
    
    # Agrupar por año de lanzamiento y análisis de sentimiento, contar el número de registros
    analysis_counts = data_por_desarrolladora.groupby(['release_year', 'sentiment_analysis']).size().unstack(fill_value=0)
    
    # Convertir los resultados a un diccionario
    result_dict = {
        'Negative': analysis_counts[0.0].to_dict(),
        'Neutral': analysis_counts[1.0].to_dict(),
        'Positive': analysis_counts[2.0].to_dict()  # Asumiendo que '3.0' representa 'Positive'
    }
    
    return result_dict

modelo_recomendacion1 = pd.read_parquet("Lista_items_items_recomendacion_final.parquet")

@app.get("/recomendacion_juego/{id_producto}")
async def obtener_recomendaciones_dict_por_id(id_producto: float):
    recomendaciones = modelo_recomendacion1[modelo_recomendacion1['id'] == id_producto]['recomendaciones'].iloc[0]
    
    # Verificar si la lista de recomendaciones no está vacía
    if len(recomendaciones) > 0:
        recomendaciones_dict = {i + 1: juego for i, juego in enumerate(recomendaciones)}
        return recomendaciones_dict
    else:
        # Si la lista de recomendaciones está vacía, devolver un diccionario vacío
        return {}