import pandas as pd


def procesar_visitas(semana1_path, semana2_path):
    try:
        df_semana1 = pd.read_csv("C:\\Users\\hp\\PycharmProjects\\PythonProject1\\Dataset\\semana1.csv")
        df_semana2 = pd.read_csv("C:\\Users\\hp\\PycharmProjects\\PythonProject1\\Dataset\\semana2.csv")
        print("Archivos CSV cargados exitosamente.")
    except Exception as e:
        print(f"Error al cargar los archivos CSV: {e}")
        return None, None, None

    df_combinado = pd.concat([df_semana1, df_semana2], ignore_index=True)

    df_combinado['Popular'] = df_combinado['Visitas'] > 140

    try:
        df_combinado.to_csv('visitas_combinadas.csv', index=False)
        print("Datos combinados guardados en visitas_combinadas.csv")
    except Exception as e:
        print(f"Error al guardar el archivo combinado: {e}")

    total_dias = len(df_combinado)
    conteo_popular = df_combinado['Popular'].value_counts()

    porcentaje_popular = (conteo_popular.get(True, 0) / total_dias) * 100
    porcentaje_no_popular = (conteo_popular.get(False, 0) / total_dias) * 100

    return df_combinado, round(porcentaje_popular, 2), round(porcentaje_no_popular, 2)

if __name__ == "__main__":
    semana1 = "semana1.csv"
    semana2 = "semana2.csv"

    df_resultado, pop, no_pop = procesar_visitas(semana1, semana2)

    print("\nDataFrame combinado:")
    print(df_resultado)

    print("\nPorcentaje de días populares:", pop, "%")
    print("Porcentaje de días no populares:", no_pop, "%")